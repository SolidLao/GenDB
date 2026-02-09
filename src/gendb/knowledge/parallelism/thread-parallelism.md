# Thread Parallelism

## What It Is
Distributing query execution across multiple CPU cores using threads, typically with morsel-driven parallelism (processing fixed-size chunks) and lock-free data structures to avoid contention.

## When To Use
- Large scans (>100K rows) where single-threaded CPU is the bottleneck
- Hash joins and aggregations on multi-core machines
- Parallel sort, partition, and shuffle operations
- Independent subqueries that can run concurrently

## Key Implementation Ideas

### Morsel-Driven Parallelism (DuckDB/HyPer model)
```cpp
// Process table in ~10K row chunks (morsels)
struct Morsel {
    size_t start_row;
    size_t count;
};

void parallel_scan(const Table& table, Pipeline& pipeline, size_t num_threads) {
    constexpr size_t MORSEL_SIZE = 10000;
    std::atomic<size_t> next_morsel{0};
    size_t total_morsels = (table.row_count + MORSEL_SIZE - 1) / MORSEL_SIZE;

    auto worker = [&]() {
        while (true) {
            size_t idx = next_morsel.fetch_add(1);
            if (idx >= total_morsels) break;

            Morsel m{idx * MORSEL_SIZE,
                     std::min(MORSEL_SIZE, table.row_count - idx * MORSEL_SIZE)};
            pipeline.process(table, m);
        }
    };

    std::vector<std::thread> threads;
    for (size_t i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker);
    }
    for (auto& t : threads) t.join();
}
```

### Lock-Free Aggregation with Thread-Local Buffers
```cpp
// Each thread maintains local hash table, merge at end
struct ThreadLocalAggState {
    absl::flat_hash_map<int64_t, int64_t> local_sums;
};

void parallel_aggregate(const int64_t* keys, const int64_t* values,
                        size_t count, size_t num_threads) {
    std::vector<ThreadLocalAggState> thread_states(num_threads);

    // Phase 1: Local aggregation (no locks)
    parallel_for(0, count, [&](size_t i, size_t thread_id) {
        thread_states[thread_id].local_sums[keys[i]] += values[i];
    });

    // Phase 2: Merge (single-threaded or parallel with locking)
    absl::flat_hash_map<int64_t, int64_t> global_result;
    for (auto& state : thread_states) {
        for (auto& [key, sum] : state.local_sums) {
            global_result[key] += sum;
        }
    }
}
```

### Work-Stealing Thread Pool
```cpp
// Simple work-stealing queue
class ThreadPool {
    std::vector<std::thread> workers;
    std::vector<std::deque<std::function<void()>>> task_queues;
    std::vector<std::mutex> queue_mutexes;

    void worker_loop(size_t worker_id) {
        while (running) {
            // Try local queue first
            if (auto task = pop_local(worker_id)) {
                (*task)();
            }
            // Steal from other queues
            else if (auto task = steal_from_others(worker_id)) {
                (*task)();
            } else {
                std::this_thread::yield();
            }
        }
    }
};
```

### Partition-Based Parallelism (for joins)
```cpp
// Partition input by hash(key) % num_partitions
void parallel_partition(const Table& table, size_t num_partitions) {
    std::vector<std::vector<Row>> partitions(num_partitions);
    std::vector<std::mutex> partition_locks(num_partitions);

    parallel_for_each_morsel(table, [&](const Morsel& m) {
        std::vector<std::vector<Row>> local_partitions(num_partitions);

        // Local partitioning (no locks)
        for (size_t i = m.start_row; i < m.start_row + m.count; ++i) {
            size_t partition = hash(table.key[i]) % num_partitions;
            local_partitions[partition].push_back(table.rows[i]);
        }

        // Merge into global partitions (with locks)
        for (size_t p = 0; p < num_partitions; ++p) {
            std::lock_guard lock(partition_locks[p]);
            partitions[p].insert(partitions[p].end(),
                                 local_partitions[p].begin(),
                                 local_partitions[p].end());
        }
    });
}
```

## Performance Characteristics
- **Speedup**: Near-linear scaling up to memory bandwidth limit (typically 4-8 cores)
- **Overhead**: Thread creation ~10-50us per thread (use thread pools)
- **Contention**: Lock-free aggregation avoids 10-100x slowdown from mutex contention
- **Cache**: Morsel size tuned to fit in L2/L3 cache (10K-100K rows)

## Real-World Examples
- **DuckDB**: Morsel-driven parallelism with adaptive work-stealing
- **PostgreSQL**: Parallel sequential scan, parallel hash join (PG11+)
- **ClickHouse**: Thread-per-core model, partition-wise parallelism
- **HyPer**: Pioneered morsel-driven execution

## Pitfalls
- **False Sharing**: Align thread-local data to cache lines (64 bytes): `alignas(64) ThreadState states[N]`
- **Over-Parallelization**: More threads than cores causes context-switching overhead
- **Small Data**: Thread creation overhead dominates for <10K rows
- **Unbalanced Work**: Static partitioning leads to stragglers; use work-stealing
- **Memory Bandwidth**: >8 threads rarely help due to DRAM bottleneck
- **Lock Contention**: Global locks in hot paths kill scalability; use lock-free or thread-local
