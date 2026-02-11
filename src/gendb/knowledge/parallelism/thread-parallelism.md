# Thread Parallelism

## What It Is

Distributing query execution across multiple CPU cores using threads, typically with morsel-driven parallelism (processing fixed-size chunks) and lock-free data structures to avoid contention.

## When To Use

- Large scans (>100K rows) where single-threaded CPU is the bottleneck
- Hash joins and aggregations on multi-core machines
- Parallel sort, partition, and shuffle operations
- Independent subqueries that can run concurrently

## Key Implementation Ideas

- **Morsel-driven parallelism**: divide table into fixed-size chunks (~10K rows) and let threads pull morsels from a shared atomic counter (DuckDB/HyPer model)

- **Thread-local aggregation**: each thread maintains its own hash table, then merge all local results at the end to avoid locks during the hot loop

- **Work-stealing thread pool**: threads try their own task queue first, then steal from other threads' queues when idle to balance load dynamically

- **Partition-based parallel join**: hash-partition both inputs by join key so each partition pair can be joined independently without coordination

- **Lock-free data structures**: use atomics (fetch_add, CAS) instead of mutexes for shared counters and queues

- **Local-then-merge pattern**: accumulate results thread-locally first, then merge globally in a second phase to minimize synchronization

- **Morsel size tuning**: size morsels to fit in L2/L3 cache for optimal data locality

- **Adaptive scheduling**: dynamically adjust work distribution based on runtime partition sizes to avoid stragglers

## Performance Characteristics

- **Speedup**: near-linear scaling up to memory bandwidth limit (typically 4-8 cores)
- **Overhead**: thread creation ~10-50us; use thread pools to amortize
- **Contention**: lock-free aggregation avoids 10-100x slowdown from mutex contention

## Pitfalls

- **False sharing**: align thread-local data to 64-byte cache lines to prevent cross-core invalidation
- **Over-parallelization**: more threads than cores causes context-switching overhead; >8 threads rarely help due to DRAM bottleneck
- **Small data**: thread creation overhead dominates for <10K rows
