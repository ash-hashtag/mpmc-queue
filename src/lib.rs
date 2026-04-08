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
        for _ in 0..1_000 {
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
        for _ in 0..1_000 {
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

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::sync::atomic::AtomicBool;
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    // ── Basic single-threaded tests ──────────────────────────────────────

    #[test]
    fn single_push_pop() {
        let q = MpmcQueue::new(4);
        q.push(42);
        assert_eq!(q.pop(), 42);
    }

    #[test]
    fn try_push_pop() {
        let q = MpmcQueue::new(2);
        assert!(q.try_push(1).is_ok());
        assert!(q.try_push(2).is_ok());
        // Queue is full
        assert_eq!(q.try_push(3), Err(3));

        assert_eq!(q.try_pop(), Some(1));
        assert_eq!(q.try_pop(), Some(2));
        // Queue is empty
        assert_eq!(q.try_pop(), None);
    }

    #[test]
    fn fifo_ordering() {
        let q = MpmcQueue::new(8);
        for i in 0..8 {
            q.push(i);
        }
        for i in 0..8 {
            assert_eq!(q.pop(), i);
        }
    }

    #[test]
    fn fill_and_drain_multiple_rounds() {
        let q = MpmcQueue::new(4);
        for round in 0..10 {
            for i in 0..4 {
                q.push(round * 4 + i);
            }
            for i in 0..4 {
                assert_eq!(q.pop(), round * 4 + i);
            }
        }
    }

    #[test]
    fn capacity_one() {
        let q = MpmcQueue::new(1);
        for i in 0..100 {
            q.push(i);
            assert_eq!(q.pop(), i);
        }
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn zero_capacity_panics() {
        let _q = MpmcQueue::<i32>::new(0);
    }

    // ── Blocking behavior tests ──────────────────────────────────────────

    #[test]
    fn push_blocks_when_full() {
        let q = Arc::new(MpmcQueue::new(2));
        q.push(1);
        q.push(2);

        let q2 = Arc::clone(&q);
        let pushed = Arc::new(AtomicBool::new(false));
        let pushed2 = Arc::clone(&pushed);

        let handle = thread::spawn(move || {
            q2.push(3); // Should block
            pushed2.store(true, Ordering::SeqCst);
        });

        // Give the producer time to block.
        thread::sleep(Duration::from_millis(50));
        assert!(!pushed.load(Ordering::SeqCst), "push should be blocking");

        // Free a slot.
        assert_eq!(q.pop(), 1);

        handle.join().unwrap();
        assert!(pushed.load(Ordering::SeqCst));
    }

    #[test]
    fn pop_blocks_when_empty() {
        let q = Arc::new(MpmcQueue::<i32>::new(4));

        let q2 = Arc::clone(&q);
        let popped = Arc::new(AtomicBool::new(false));
        let popped2 = Arc::clone(&popped);

        let handle = thread::spawn(move || {
            let val = q2.pop(); // Should block
            popped2.store(true, Ordering::SeqCst);
            val
        });

        thread::sleep(Duration::from_millis(50));
        assert!(!popped.load(Ordering::SeqCst), "pop should be blocking");

        q.push(99);
        let val = handle.join().unwrap();
        assert_eq!(val, 99);
    }

    // ── Concurrent correctness tests ─────────────────────────────────────

    /// Every value pushed by producers is popped exactly once by consumers.
    /// This is the core correctness test under contention.
    #[test]
    fn spmc_no_lost_or_duplicate_items() {
        const NUM_PRODUCERS: usize = 4;
        const NUM_CONSUMERS: usize = 4;
        const ITEMS_PER_PRODUCER: usize = 50_000;
        const TOTAL: usize = NUM_PRODUCERS * ITEMS_PER_PRODUCER;

        let q = Arc::new(MpmcQueue::new(256));
        let barrier = Arc::new(Barrier::new(NUM_PRODUCERS + NUM_CONSUMERS));

        // Producers: each pushes a unique range of values.
        let producers: Vec<_> = (0..NUM_PRODUCERS)
            .map(|p| {
                let q = Arc::clone(&q);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let start = p * ITEMS_PER_PRODUCER;
                    for i in start..start + ITEMS_PER_PRODUCER {
                        q.push(i);
                    }
                })
            })
            .collect();

        // Consumers: pop until they've collectively received TOTAL items.
        let received = Arc::new(Mutex::new(Vec::with_capacity(TOTAL)));
        let items_left = Arc::new(AtomicUsize::new(TOTAL));

        let consumers: Vec<_> = (0..NUM_CONSUMERS)
            .map(|_| {
                let q = Arc::clone(&q);
                let received = Arc::clone(&received);
                let items_left = Arc::clone(&items_left);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut local = Vec::new();
                    while items_left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 { Some(n - 1) } else { None }
                    }).is_ok() {
                        local.push(q.pop());
                    }
                    received.lock().unwrap().extend(local);
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }
        for c in consumers {
            c.join().unwrap();
        }

        let received = received.lock().unwrap();
        assert_eq!(received.len(), TOTAL, "wrong number of items received");

        let set: HashSet<_> = received.iter().copied().collect();
        assert_eq!(set.len(), TOTAL, "duplicate items detected");
        for i in 0..TOTAL {
            assert!(set.contains(&i), "missing item {i}");
        }
    }

    /// Stress test with many threads and small queue to maximize contention.
    #[test]
    fn high_contention_small_queue() {
        const THREADS: usize = 16;
        const ITEMS_PER_THREAD: usize = 10_000;
        const TOTAL: usize = THREADS * ITEMS_PER_THREAD;

        let q = Arc::new(MpmcQueue::new(4)); // tiny queue
        let barrier = Arc::new(Barrier::new(THREADS * 2));

        let producers: Vec<_> = (0..THREADS)
            .map(|t| {
                let q = Arc::clone(&q);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let start = t * ITEMS_PER_THREAD;
                    for i in start..start + ITEMS_PER_THREAD {
                        q.push(i as u64);
                    }
                })
            })
            .collect();

        let received = Arc::new(Mutex::new(Vec::with_capacity(TOTAL)));
        let items_left = Arc::new(AtomicUsize::new(TOTAL));

        let consumers: Vec<_> = (0..THREADS)
            .map(|_| {
                let q = Arc::clone(&q);
                let received = Arc::clone(&received);
                let items_left = Arc::clone(&items_left);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut local = Vec::new();
                    while items_left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 { Some(n - 1) } else { None }
                    }).is_ok() {
                        local.push(q.pop());
                    }
                    received.lock().unwrap().extend(local);
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }
        for c in consumers {
            c.join().unwrap();
        }

        let received = received.lock().unwrap();
        assert_eq!(received.len(), TOTAL);
        let set: HashSet<_> = received.iter().copied().collect();
        assert_eq!(set.len(), TOTAL, "duplicates or lost items");
    }

    /// Asymmetric: many producers, one consumer.
    #[test]
    fn many_producers_one_consumer() {
        const PRODUCERS: usize = 8;
        const PER_PRODUCER: usize = 25_000;
        const TOTAL: usize = PRODUCERS * PER_PRODUCER;

        let q = Arc::new(MpmcQueue::new(64));
        let barrier = Arc::new(Barrier::new(PRODUCERS + 1));

        let producers: Vec<_> = (0..PRODUCERS)
            .map(|p| {
                let q = Arc::clone(&q);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..PER_PRODUCER {
                        q.push(p * PER_PRODUCER + i);
                    }
                })
            })
            .collect();

        let q2 = Arc::clone(&q);
        let barrier2 = Arc::clone(&barrier);
        let consumer = thread::spawn(move || {
            barrier2.wait();
            let mut items = Vec::with_capacity(TOTAL);
            for _ in 0..TOTAL {
                items.push(q2.pop());
            }
            items
        });

        for p in producers {
            p.join().unwrap();
        }

        let items = consumer.join().unwrap();
        assert_eq!(items.len(), TOTAL);
        let set: HashSet<_> = items.into_iter().collect();
        assert_eq!(set.len(), TOTAL);
    }

    /// Asymmetric: one producer, many consumers.
    #[test]
    fn one_producer_many_consumers() {
        const CONSUMERS: usize = 8;
        const TOTAL: usize = 200_000;

        let q = Arc::new(MpmcQueue::new(64));
        let barrier = Arc::new(Barrier::new(CONSUMERS + 1));
        let items_left = Arc::new(AtomicUsize::new(TOTAL));
        let received = Arc::new(Mutex::new(Vec::with_capacity(TOTAL)));

        let consumers: Vec<_> = (0..CONSUMERS)
            .map(|_| {
                let q = Arc::clone(&q);
                let barrier = Arc::clone(&barrier);
                let items_left = Arc::clone(&items_left);
                let received = Arc::clone(&received);
                thread::spawn(move || {
                    barrier.wait();
                    let mut local = Vec::new();
                    while items_left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 { Some(n - 1) } else { None }
                    }).is_ok() {
                        local.push(q.pop());
                    }
                    received.lock().unwrap().extend(local);
                })
            })
            .collect();

        let q2 = Arc::clone(&q);
        let barrier2 = Arc::clone(&barrier);
        let producer = thread::spawn(move || {
            barrier2.wait();
            for i in 0..TOTAL {
                q2.push(i);
            }
        });

        producer.join().unwrap();
        for c in consumers {
            c.join().unwrap();
        }

        let received = received.lock().unwrap();
        assert_eq!(received.len(), TOTAL);
        let set: HashSet<_> = received.iter().copied().collect();
        assert_eq!(set.len(), TOTAL);
    }

    /// Test with a non-Copy, heap-allocated type to verify no double-frees
    /// or leaks.
    #[test]
    fn string_values_no_leaks() {
        let q = Arc::new(MpmcQueue::new(32));
        let barrier = Arc::new(Barrier::new(8));

        let producers: Vec<_> = (0..4)
            .map(|p| {
                let q = Arc::clone(&q);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..1000 {
                        q.push(format!("{p}-{i}"));
                    }
                })
            })
            .collect();

        let received = Arc::new(Mutex::new(Vec::new()));
        let items_left = Arc::new(AtomicUsize::new(4000));

        let consumers: Vec<_> = (0..4)
            .map(|_| {
                let q = Arc::clone(&q);
                let received = Arc::clone(&received);
                let items_left = Arc::clone(&items_left);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut local = Vec::new();
                    while items_left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 { Some(n - 1) } else { None }
                    }).is_ok() {
                        local.push(q.pop());
                    }
                    received.lock().unwrap().extend(local);
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }
        for c in consumers {
            c.join().unwrap();
        }

        let received = received.lock().unwrap();
        assert_eq!(received.len(), 4000);
        let set: HashSet<_> = received.iter().cloned().collect();
        assert_eq!(set.len(), 4000, "duplicate or lost strings");
    }

    /// Verify items remaining in the queue at drop time are properly dropped.
    #[test]
    fn drop_with_items_in_queue() {
        use std::sync::atomic::AtomicUsize;

        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug)]
        struct DropCounter;
        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, Ordering::SeqCst);
            }
        }

        DROP_COUNT.store(0, Ordering::SeqCst);

        {
            let q = MpmcQueue::new(8);
            q.push(DropCounter);
            q.push(DropCounter);
            q.push(DropCounter);
            // Pop one — its Drop runs immediately.
            let _ = q.pop();
            // Queue drops here with 2 items remaining.
        }

        assert_eq!(
            DROP_COUNT.load(Ordering::SeqCst),
            3,
            "all items must be dropped"
        );
    }

    /// Rapid alternation between full and empty states.
    #[test]
    fn rapid_full_empty_transitions() {
        const THREADS: usize = 4;
        const ITERS: usize = 50_000;

        let q = Arc::new(MpmcQueue::new(1)); // capacity 1 = max transitions
        let barrier = Arc::new(Barrier::new(THREADS * 2));

        let producers: Vec<_> = (0..THREADS)
            .map(|t| {
                let q = Arc::clone(&q);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..ITERS {
                        q.push(t * ITERS + i);
                    }
                })
            })
            .collect();

        let total = THREADS * ITERS;
        let items_left = Arc::new(AtomicUsize::new(total));
        let received = Arc::new(Mutex::new(Vec::with_capacity(total)));

        let consumers: Vec<_> = (0..THREADS)
            .map(|_| {
                let q = Arc::clone(&q);
                let items_left = Arc::clone(&items_left);
                let received = Arc::clone(&received);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut local = Vec::new();
                    while items_left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 { Some(n - 1) } else { None }
                    }).is_ok() {
                        local.push(q.pop());
                    }
                    received.lock().unwrap().extend(local);
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }
        for c in consumers {
            c.join().unwrap();
        }

        let received = received.lock().unwrap();
        assert_eq!(received.len(), total);
        let set: HashSet<_> = received.iter().copied().collect();
        assert_eq!(set.len(), total);
    }

    /// try_push / try_pop under contention — no item must be lost.
    #[test]
    fn try_operations_under_contention() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 20_000;
        const TOTAL: usize = THREADS * PER_THREAD;

        let q = Arc::new(MpmcQueue::new(64));
        let barrier = Arc::new(Barrier::new(THREADS * 2));

        let producers: Vec<_> = (0..THREADS)
            .map(|t| {
                let q = Arc::clone(&q);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    for i in 0..PER_THREAD {
                        let val = t * PER_THREAD + i;
                        // Spin on try_push.
                        loop {
                            match q.try_push(val) {
                                Ok(()) => break,
                                Err(_) => std::thread::yield_now(),
                            }
                        }
                    }
                })
            })
            .collect();

        let received = Arc::new(Mutex::new(Vec::with_capacity(TOTAL)));
        let items_left = Arc::new(AtomicUsize::new(TOTAL));

        let consumers: Vec<_> = (0..THREADS)
            .map(|_| {
                let q = Arc::clone(&q);
                let received = Arc::clone(&received);
                let items_left = Arc::clone(&items_left);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    barrier.wait();
                    let mut local = Vec::new();
                    while items_left.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 { Some(n - 1) } else { None }
                    }).is_ok() {
                        // Spin on try_pop.
                        loop {
                            match q.try_pop() {
                                Some(v) => {
                                    local.push(v);
                                    break;
                                }
                                None => std::thread::yield_now(),
                            }
                        }
                    }
                    received.lock().unwrap().extend(local);
                })
            })
            .collect();

        for p in producers {
            p.join().unwrap();
        }
        for c in consumers {
            c.join().unwrap();
        }

        let received = received.lock().unwrap();
        assert_eq!(received.len(), TOTAL);
        let set: HashSet<_> = received.iter().copied().collect();
        assert_eq!(set.len(), TOTAL);
    }
}
