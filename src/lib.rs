//! A bounded, multi-producer multi-consumer (MPMC) queue.
//!
//! Uses a Vyukov-style ring buffer with per-slot atomic sequence numbers for
//! lock-free coordination between producers and consumers. Blocking operations
//! fall back to parking via `Mutex`+`Condvar` when the queue is full or empty.
//!
//! # Design
//!
//! The ring buffer has `capacity + 1` slots (the extra slot ensures the
//! sequence protocol works correctly even when `capacity == 1`). Each slot
//! contains:
//! - An atomic `sequence` number used for lock-free synchronization
//! - An `UnsafeCell<MaybeUninit<T>>` for the stored value
//!
//! Two atomic cursors — `enqueue_pos` and `dequeue_pos` — are used by producers
//! and consumers respectively. Each cursor is monotonically increasing; the
//! actual slot index is `pos % buf_len` where `buf_len = capacity + 1`.
//!
//! The sequence number protocol:
//! - Initially, `slot[i].sequence = i` (slot is empty and ready for slot `i`)
//! - A producer at position `pos` claims slot `pos % cap` when `seq == pos`,
//!   writes data, then sets `seq = pos + 1` (signaling "data is here")
//! - A consumer at position `pos` claims slot `pos % cap` when `seq == pos + 1`,
//!   reads data, then sets `seq = pos + cap` (signaling "slot is free for reuse")
//!
//! # Safety
//!
//! `unsafe` is used in three places, each justified:
//! 1. `UnsafeCell` access — safe because the sequence protocol guarantees
//!    exclusive access: only one producer writes (when seq == pos) and only
//!    one consumer reads (when seq == pos + 1). There is no concurrent access.
//! 2. `MaybeUninit::assume_init_read` — safe because we only call it after a
//!    producer has written a valid `T` into the slot.
//! 3. `Sync` impl — safe because all shared state uses atomics or is protected
//!    by the sequence protocol.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};

// ─────────────────────────────────────────────────────────────────────────────
// Trait
// ─────────────────────────────────────────────────────────────────────────────

pub trait BoundedQueue<T: Send>: Send + Sync {
    fn new(capacity: usize) -> Self
    where
        Self: Sized;

    /// Push an item into the queue. Blocks if the queue is full.
    fn push(&self, item: T);

    /// Pop an item from the queue. Blocks if the queue is empty.
    fn pop(&self) -> T;

    /// Try to push without blocking. Returns `Err(item)` if full.
    fn try_push(&self, item: T) -> Result<(), T>;

    /// Try to pop without blocking. Returns `None` if empty.
    fn try_pop(&self) -> Option<T>;
}

// ─────────────────────────────────────────────────────────────────────────────
// Slot
// ─────────────────────────────────────────────────────────────────────────────

/// A single slot in the ring buffer.
///
/// `sequence` follows the Vyukov protocol described above.
/// `value` is accessed only when the sequence number guarantees exclusivity.
struct Slot<T> {
    sequence: AtomicUsize,
    value: UnsafeCell<MaybeUninit<T>>,
}

// ─────────────────────────────────────────────────────────────────────────────
// MpmcQueue
// ─────────────────────────────────────────────────────────────────────────────

pub struct MpmcQueue<T> {
    /// Ring buffer of slots. Length == capacity + 1 to ensure the sequence
    /// protocol can distinguish "pushed" from "freed" states even at capacity 1.
    buffer: Box<[Slot<T>]>,
    /// User-requested capacity (max items the queue can hold).
    /// Note: buffer.len() == capacity + 1 for the sequence protocol.
    capacity: usize,
    /// Monotonically increasing position for the next enqueue.
    enqueue_pos: AtomicUsize,
    /// Monotonically increasing position for the next dequeue.
    dequeue_pos: AtomicUsize,

    // -- Parking primitives for blocking push/pop --
    /// Number of parked producers.
    parked_producers: AtomicUsize,
    /// Number of parked consumers.
    parked_consumers: AtomicUsize,

    /// Signaled when a slot becomes available (after a dequeue).
    not_full: Condvar,
    /// Signaled when data becomes available (after an enqueue).
    not_empty: Condvar,
    /// Dummy mutex required by Condvar.  We never store meaningful state in it;
    /// the real synchronization happens via the atomics.
    mu: Mutex<()>,
}

// SAFETY: All mutable shared state is behind atomics or UnsafeCell guarded by
// the sequence protocol which guarantees exclusive access per slot.
unsafe impl<T: Send> Sync for MpmcQueue<T> {}

impl<T> Drop for MpmcQueue<T> {
    fn drop(&mut self) {
        // Drop any items still in the queue.
        let head = *self.dequeue_pos.get_mut();
        let tail = *self.enqueue_pos.get_mut();
        for pos in head..tail {
            let idx = pos % self.buffer.len();
            // SAFETY: items between dequeue_pos and enqueue_pos have been
            // written by producers and not yet consumed.
            unsafe {
                self.buffer[idx].value.get_mut().assume_init_drop();
            }
        }
    }
}

const SPIN_LIMIT: usize = 1_000;

impl<T: Send> BoundedQueue<T> for MpmcQueue<T> {
    fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "capacity must be > 0");

        // Allocate capacity + 1 slots so that the sequence protocol works
        // correctly even when capacity == 1.  With exactly `capacity` slots,
        // `pos + 1` (push marker) would equal `pos + capacity` (free marker)
        // when capacity == 1, making them indistinguishable.
        let buf_len = capacity + 1;
        let buffer: Vec<Slot<T>> = (0..buf_len)
            .map(|i| Slot {
                sequence: AtomicUsize::new(i),
                value: UnsafeCell::new(MaybeUninit::uninit()),
            })
            .collect();

        Self {
            buffer: buffer.into_boxed_slice(),
            capacity,
            enqueue_pos: AtomicUsize::new(0),
            dequeue_pos: AtomicUsize::new(0),
            parked_producers: AtomicUsize::new(0),
            parked_consumers: AtomicUsize::new(0),
            not_full: Condvar::new(),
            not_empty: Condvar::new(),
            mu: Mutex::new(()),
        }
    }

    fn push(&self, mut item: T) {
        // Fast path: Try non-blocking push with minimal spinning
        // This keeps throughput high under moderate contention without parking.
        for _ in 0..SPIN_LIMIT {
            match self.try_push(item) {
                Ok(()) => return,
                Err(returned) => item = returned,
            }
            std::thread::yield_now();
        }

        self.parked_producers.fetch_add(1, Ordering::SeqCst);

        loop {
            // Re-check before taking the lock
            match self.try_push_inner(item) {
                Ok(notify) => {
                    self.parked_producers.fetch_sub(1, Ordering::SeqCst);
                    if notify {
                        let _guard = self.mu.lock().unwrap();
                        self.not_empty.notify_one();
                    }
                    return;
                }
                Err(returned) => item = returned,
            }

            // Slow path: wait for a slot to open up.
            let guard = self.mu.lock().unwrap();
            match self.try_push_inner(item) {
                Ok(notify) => {
                    self.parked_producers.fetch_sub(1, Ordering::SeqCst);
                    if notify {
                        self.not_empty.notify_one();
                    }
                    return;
                }
                Err(returned) => item = returned,
            }
            let guard = self.not_full.wait(guard).unwrap();
            drop(guard);
        }
    }

    fn pop(&self) -> T {
        // Fast path
        for _ in 0..SPIN_LIMIT {
            if let Some(item) = self.try_pop() {
                return item;
            }
            std::thread::yield_now();
        }

        self.parked_consumers.fetch_add(1, Ordering::SeqCst);

        loop {
            // Re-check before taking the lock
            match self.try_pop_inner() {
                Some((item, notify)) => {
                    self.parked_consumers.fetch_sub(1, Ordering::SeqCst);
                    if notify {
                        let _guard = self.mu.lock().unwrap();
                        self.not_full.notify_one();
                    }
                    return item;
                }
                None => {}
            }

            // Slow path: wait for data to arrive.
            let guard = self.mu.lock().unwrap();
            match self.try_pop_inner() {
                Some((item, notify)) => {
                    self.parked_consumers.fetch_sub(1, Ordering::SeqCst);
                    if notify {
                        self.not_full.notify_one();
                    }
                    return item;
                }
                None => {}
            }
            let guard = self.not_empty.wait(guard).unwrap();
            drop(guard);
        }
    }

    fn try_push(&self, item: T) -> Result<(), T> {
        match self.try_push_inner(item) {
            Ok(notify) => {
                if notify {
                    let _guard = self.mu.lock().unwrap();
                    self.not_empty.notify_one();
                }
                Ok(())
            }
            Err(item) => Err(item),
        }
    }

    fn try_pop(&self) -> Option<T> {
        match self.try_pop_inner() {
            Some((item, notify)) => {
                if notify {
                    let _guard = self.mu.lock().unwrap();
                    self.not_full.notify_one();
                }
                Some(item)
            }
            None => None,
        }
    }
}

impl<T: Send> MpmcQueue<T> {
    /// Core lock-free push. Returns `Ok(needs_notify)` on success.
    /// The caller is responsible for notifying `not_empty` if `needs_notify` is true.
    fn try_push_inner(&self, item: T) -> Result<bool, T> {
        let mut pos = self.enqueue_pos.load(Ordering::Relaxed);

        loop {
            // Explicit occupancy check: the internal buffer has capacity+1 slots,
            // but we only allow `capacity` items to be enqueued.
            let deq = self.dequeue_pos.load(Ordering::Relaxed);
            if pos.wrapping_sub(deq) >= self.capacity {
                return Err(item);
            }

            let buf_len = self.buffer.len();
            let idx = pos % buf_len;
            let slot = &self.buffer[idx];
            let seq = slot.sequence.load(Ordering::Acquire);

            let diff = seq.wrapping_sub(pos) as isize;

            if diff == 0 {
                // Slot is free and it's our turn.  Try to claim it.
                match self.enqueue_pos.compare_exchange_weak(
                    pos,
                    pos + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: We uniquely own this slot via the CAS on
                        // enqueue_pos + the sequence check.  No other producer
                        // can write here, and no consumer will read it until
                        // we advance the sequence.
                        unsafe {
                            (*slot.value.get()).write(item);
                        }
                        slot.sequence.store(pos + 1, Ordering::SeqCst);

                        let notify = self.parked_consumers.load(Ordering::SeqCst) > 0;
                        return Ok(notify);
                    }
                    Err(actual) => {
                        pos = actual;
                    }
                }
            } else if diff < 0 {
                // Queue is full — this slot hasn't been freed by a consumer yet.
                return Err(item);
            } else {
                // Another producer already claimed this position; reload.
                pos = self.enqueue_pos.load(Ordering::Relaxed);
            }
        }
    }

    /// Core lock-free pop. Returns `Some((item, needs_notify))` on success.
    /// The caller is responsible for notifying `not_full` if `needs_notify` is true.
    fn try_pop_inner(&self) -> Option<(T, bool)> {
        let mut pos = self.dequeue_pos.load(Ordering::Relaxed);

        loop {
            let buf_len = self.buffer.len();
            let idx = pos % buf_len;
            let slot = &self.buffer[idx];
            let seq = slot.sequence.load(Ordering::Acquire);

            let diff = seq.wrapping_sub(pos.wrapping_add(1)) as isize;

            if diff == 0 {
                // Data is available.  Try to claim it.
                match self.dequeue_pos.compare_exchange_weak(
                    pos,
                    pos + 1,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        // SAFETY: The producer wrote a valid T here and set
                        // seq = pos + 1.  We're the only consumer that won
                        // the CAS, so we have exclusive read access.
                        let item = unsafe { (*slot.value.get()).assume_init_read() };
                        slot.sequence
                            .store(pos.wrapping_add(buf_len), Ordering::SeqCst);

                        let notify = self.parked_producers.load(Ordering::SeqCst) > 0;
                        return Some((item, notify));
                    }
                    Err(actual) => {
                        pos = actual;
                    }
                }
            } else if diff < 0 {
                // Queue is empty.
                return None;
            } else {
                // Another consumer already claimed this position; reload.
                pos = self.dequeue_pos.load(Ordering::Relaxed);
            }
        }
    }
}

