//! Throughput benchmarks for the MPMC queue.
//!
//! Measures operations/sec across:
//! - Thread counts: 1, 2, 4, 8, 16 producer/consumer pairs
//! - Queue capacities: 64, 256, 1024
//! - Asymmetric workloads: many producers + few consumers, few producers + many consumers
//!
//! Run with: `cargo bench`

use mpmc_queue::{BoundedQueue, MpmcQueue};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

const WARMUP_ITERS: usize = 10_000;
const BENCH_DURATION: Duration = Duration::from_secs(1);

/// Measures symmetric throughput: N producers + N consumers.
fn bench_symmetric(num_threads: usize, capacity: usize) -> f64 {
    let q = Arc::new(MpmcQueue::<u64>::new(capacity));
    let total_ops = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let barrier = Arc::new(Barrier::new(num_threads * 2 + 1)); // producers + consumers + main

    // Warmup: push/pop in small batches to avoid blocking.
    {
        let wq = MpmcQueue::<u64>::new(capacity);
        for i in 0..WARMUP_ITERS {
            let _ = wq.try_push(i as u64);
            let _ = wq.try_pop();
        }
    }

    let producers: Vec<_> = (0..num_threads)
        .map(|_| {
            let q = Arc::clone(&q);
            let running = Arc::clone(&running);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut i = 0u64;
                while running.load(Ordering::Relaxed) {
                    q.push(i);
                    i += 1;
                }
            })
        })
        .collect();

    let consumers: Vec<_> = (0..num_threads)
        .map(|_| {
            let q = Arc::clone(&q);
            let total_ops = Arc::clone(&total_ops);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut count = 0usize;
                loop {
                    let val = q.pop();
                    if val == u64::MAX {
                        break;
                    }
                    count += 1;
                }
                total_ops.fetch_add(count, Ordering::Relaxed);
            })
        })
        .collect();

    // Start all threads
    barrier.wait();
    let start = Instant::now();

    // Let them run for the benchmark duration
    thread::sleep(BENCH_DURATION);
    running.store(false, Ordering::Relaxed);

    // Join producers first. They will eventually exit because consumers are still popping.
    for p in producers {
        p.join().unwrap();
    }

    // Now all producers have finished. Push sentinels to stop consumers.
    for _ in 0..num_threads {
        q.push(u64::MAX);
    }

    for c in consumers {
        c.join().unwrap();
    }

    let elapsed = start.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);
    ops as f64 / elapsed.as_secs_f64()
}

/// Measures asymmetric throughput: `n_producers` producers + `n_consumers` consumers.
fn bench_asymmetric(n_producers: usize, n_consumers: usize, capacity: usize) -> f64 {
    let q = Arc::new(MpmcQueue::<u64>::new(capacity));
    let total_ops = Arc::new(AtomicUsize::new(0));
    let running = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let barrier = Arc::new(Barrier::new(n_producers + n_consumers + 1));

    let producers: Vec<_> = (0..n_producers)
        .map(|_| {
            let q = Arc::clone(&q);
            let running = Arc::clone(&running);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut i = 0u64;
                while running.load(Ordering::Relaxed) {
                    q.push(i);
                    i += 1;
                }
            })
        })
        .collect();

    let consumers: Vec<_> = (0..n_consumers)
        .map(|_| {
            let q = Arc::clone(&q);
            let total_ops = Arc::clone(&total_ops);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                let mut count = 0usize;
                loop {
                    let val = q.pop();
                    if val == u64::MAX {
                        break;
                    }
                    count += 1;
                }
                total_ops.fetch_add(count, Ordering::Relaxed);
            })
        })
        .collect();

    barrier.wait();
    let start = Instant::now();

    thread::sleep(BENCH_DURATION);
    running.store(false, Ordering::Relaxed);

    for p in producers {
        p.join().unwrap();
    }

    for _ in 0..n_consumers {
        q.push(u64::MAX);
    }

    for c in consumers {
        c.join().unwrap();
    }

    let elapsed = start.elapsed();
    let ops = total_ops.load(Ordering::Relaxed);
    ops as f64 / elapsed.as_secs_f64()
}

fn format_throughput(ops_per_sec: f64) -> String {
    if ops_per_sec >= 1_000_000.0 {
        format!("{:.2}M ops/sec", ops_per_sec / 1_000_000.0)
    } else if ops_per_sec >= 1_000.0 {
        format!("{:.2}K ops/sec", ops_per_sec / 1_000.0)
    } else {
        format!("{:.2} ops/sec", ops_per_sec)
    }
}

fn main() {
    let thread_counts = [1, 2, 4, 8, 16];
    let capacities = [64, 256, 1024];

    println!("╔══════════════════════════════════════════════════════════════════╗");
    println!("║              MPMC Queue Throughput Benchmarks                   ║");
    println!("╠══════════════════════════════════════════════════════════════════╣");
    println!("║  Duration per test: {:?}                                    ║", BENCH_DURATION);
    println!("╚══════════════════════════════════════════════════════════════════╝");
    println!();

    // ── Symmetric benchmarks ─────────────────────────────────────────────
    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│  Symmetric: N producers + N consumers                           │");
    println!("├──────────┬──────────────┬──────────────┬──────────────┤");
    println!("│ Threads  │   Cap 64     │   Cap 256    │  Cap 1024    │");
    println!("├──────────┼──────────────┼──────────────┼──────────────┤");

    for &threads in &thread_counts {
        let mut results = Vec::new();
        for &cap in &capacities {
            let throughput = bench_symmetric(threads, cap);
            results.push(format_throughput(throughput));
        }
        println!(
            "│ {:>2}P/{:>2}C  │ {:>12} │ {:>12} │ {:>12} │",
            threads, threads, results[0], results[1], results[2]
        );
    }
    println!("└──────────┴──────────────┴──────────────┴──────────────┘");
    println!();

    // ── Asymmetric benchmarks ────────────────────────────────────────────
    let asymmetric_configs: Vec<(usize, usize, &str)> = vec![
        (8, 1, " 8P/ 1C"),
        (16, 1, "16P/ 1C"),
        (8, 2, " 8P/ 2C"),
        (1, 8, " 1P/ 8C"),
        (1, 16, " 1P/16C"),
        (2, 8, " 2P/ 8C"),
        (16, 4, "16P/ 4C"),
        (4, 16, " 4P/16C"),
    ];

    println!("┌──────────────────────────────────────────────────────────────────┐");
    println!("│  Asymmetric workloads (capacity = 256)                          │");
    println!("├──────────┬───────────────────────────────────────────────────────┤");
    println!("│  Config  │  Throughput                                          │");
    println!("├──────────┼───────────────────────────────────────────────────────┤");

    for (producers, consumers, label) in &asymmetric_configs {
        let throughput = bench_asymmetric(*producers, *consumers, 256);
        println!(
            "│ {} │  {:>52} │",
            label,
            format_throughput(throughput)
        );
    }
    println!("└──────────┴───────────────────────────────────────────────────────┘");
}
