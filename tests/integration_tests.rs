use mpmc_queue::{BoundedQueue, MpmcQueue};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier, Mutex};
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
                while items_left
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
                {
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
                while items_left
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
                {
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
                while items_left
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
                {
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
                while items_left
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
                {
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
                while items_left
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
                {
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
                while items_left
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                        if n > 0 {
                            Some(n - 1)
                        } else {
                            None
                        }
                    })
                    .is_ok()
                {
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
