# MPMC Queue

A high-performance, bounded, multi-producer multi-consumer (MPMC) queue for Rust.

This implementation uses a Vyukov-style ring buffer with per-slot atomic sequence numbers to achieve lock-free coordination between producers and consumers. Blocking operations gracefully fall back to thread parking using `std::sync::Condvar` when the queue is full or empty.

## Features

- **Bounded Capacity**: Fixed-size buffer to prevent unbounded memory growth.
- **Lock-Free Coordination**: Atomics are used for the primary data path, avoiding global locks.
- **Blocking & Non-Blocking API**:
  - `push` / `pop`: Blocking operations with efficient spinning before parking.
  - `try_push` / `try_pop`: Immediate non-blocking operations.
- **Comprehensive Testing**: Validated under high contention, including edge cases like capacity-1 queues.
- **Resource Cleanup**: Properly handles `Drop` for items remaining in the queue.

## Design

The queue uses a ring buffer of `capacity + 1` slots. The extra slot is a design choice that ensures the Vyukov sequence protocol can unambiguously distinguish between "pushed" (data available) and "freed" (slot ready for reuse) states even when the capacity is 1.

Each slot contains:
- An atomic `sequence` number for synchronization.
- An `UnsafeCell<MaybeUninit<T>>` for the stored value.

Two atomic cursors, `enqueue_pos` and `dequeue_pos`, manage the head and tail of the queue.

## Performance

The following benchmarks were conducted on an optimized build. Throughput is measured in operations per second (one push + one pop = one operation).

### Symmetric Workload
*N producers and N consumers competing for the queue.*

| Threads | Cap 64 | Cap 256 | Cap 1024 |
| :--- | :--- | :--- | :--- |
| **1P / 1C** | 12.36M | 12.38M | 12.73M |
| **2P / 2C** | 6.62M | 6.79M | 6.98M |
| **4P / 4C** | 4.69M | 4.38M | 4.80M |
| **8P / 8C** | 3.87M | 4.19M | 3.94M |
| **16P / 16C** | 3.96M | 4.34M | 3.82M |

### Asymmetric Workload
*Varying number of producers and consumers (Capacity = 256).*

| Config | Throughput |
| :--- | :--- |
| **8P / 1C** | 5.82M ops/s |
| **16P / 1C** | 4.95M ops/s |
| **8P / 2C** | 8.41M ops/s |
| **1P / 8C** | 7.20M ops/s |
| **1P / 16C** | 6.27M ops/s |
| **2P / 8C** | 18.70M ops/s |
| **16P / 4C** | 6.11M ops/s |
| **4P / 16C** | 5.69M ops/s |

## Usage

Add the library to your `Cargo.toml`. The `MpmcQueue` implements the `BoundedQueue` trait.

```rust
use mpmc_queue::{BoundedQueue, MpmcQueue};
use std::sync::Arc;
use std::thread;

fn main() {
    let queue = Arc::new(MpmcQueue::new(128));

    // Producer
    let q1 = Arc::clone(&queue);
    thread::spawn(move || {
        q1.push("Hello, World!");
    });

    // Consumer
    let q2 = Arc::clone(&queue);
    let msg = q2.pop();
    println!("{}", msg);
}
```

## Running Benchmarks

To run the throughput benchmarks yourself:

```bash
cargo bench --bench throughput
```
