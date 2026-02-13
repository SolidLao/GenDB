# Thread Parallelism

## What It Is

Distributing query execution across multiple CPU cores using threads, typically with morsel-driven parallelism (processing fixed-size chunks) and lock-free data structures to avoid contention.

## Key Implementation Ideas

- **Morsel-driven parallelism**: divide table into fixed-size chunks (~10K rows) and let threads pull morsels from a shared atomic counter (DuckDB/HyPer model)

- **Thread-local aggregation**: each thread maintains its own hash table, then merge all local results at the end to avoid locks during the hot loop

- **Work-stealing thread pool**: threads try their own task queue first, then steal from other threads' queues when idle to balance load dynamically

- **Partition-based parallel join**: hash-partition both inputs by join key so each partition pair can be joined independently without coordination

- **Lock-free data structures**: use atomics (fetch_add, CAS) instead of mutexes for shared counters and queues

- **Local-then-merge pattern**: accumulate results thread-locally first, then merge globally in a second phase to minimize synchronization

- **Morsel size tuning**: size morsels to fit in L2/L3 cache for optimal data locality

- **Adaptive scheduling**: dynamically adjust work distribution based on runtime partition sizes to avoid stragglers
