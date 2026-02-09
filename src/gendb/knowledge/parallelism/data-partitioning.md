# Data Partitioning

## What It Is
Splitting data into disjoint subsets based on a key (hash or range) to enable parallel processing without coordination. Each partition can be processed independently by different threads or cores.

## When To Use
- Hash joins: partition both inputs on join key, then local joins per partition
- GROUP BY aggregations: partition on group key, aggregate locally
- Parallel sort: partition into ranges, sort each partition independently
- Reducing lock contention: each partition has its own lock/data structure

## Key Implementation Ideas

### Hash Partitioning for Parallel Aggregation
```cpp
// Partition data by hash(key) % num_partitions
struct Partition {
    std::vector<int64_t> keys;
    std::vector<int64_t> values;
};

std::vector<Partition> hash_partition(const int64_t* keys, const int64_t* values,
                                       size_t count, size_t num_partitions) {
    std::vector<Partition> partitions(num_partitions);

    // Pre-allocate to avoid reallocation
    for (auto& p : partitions) {
        p.keys.reserve(count / num_partitions * 1.2); // +20% slack
        p.values.reserve(count / num_partitions * 1.2);
    }

    for (size_t i = 0; i < count; ++i) {
        size_t partition_id = hash_murmur64(keys[i]) % num_partitions;
        partitions[partition_id].keys.push_back(keys[i]);
        partitions[partition_id].values.push_back(values[i]);
    }

    return partitions;
}

// Process partitions in parallel (no coordination needed)
void parallel_aggregate(std::vector<Partition>& partitions) {
    std::vector<std::thread> threads;
    for (auto& partition : partitions) {
        threads.emplace_back([&partition]() {
            absl::flat_hash_map<int64_t, int64_t> local_agg;
            for (size_t i = 0; i < partition.keys.size(); ++i) {
                local_agg[partition.keys[i]] += partition.values[i];
            }
            // Store result in partition
        });
    }
    for (auto& t : threads) t.join();
}
```

### Radix Partitioning (Cache-Friendly Multi-Pass)
```cpp
// Use low N bits of hash for partitioning (cache-aligned)
void radix_partition(const int64_t* keys, size_t count,
                     int radix_bits = 8) { // 256 partitions
    size_t num_partitions = 1 << radix_bits;
    std::vector<size_t> partition_counts(num_partitions, 0);

    // Pass 1: Count partition sizes
    for (size_t i = 0; i < count; ++i) {
        size_t partition = keys[i] & ((1 << radix_bits) - 1);
        partition_counts[partition]++;
    }

    // Compute partition offsets (prefix sum)
    std::vector<size_t> partition_offsets(num_partitions);
    std::exclusive_scan(partition_counts.begin(), partition_counts.end(),
                        partition_offsets.begin(), 0);

    // Pass 2: Scatter data into partitions
    std::vector<int64_t> output(count);
    std::vector<size_t> write_positions = partition_offsets; // copy
    for (size_t i = 0; i < count; ++i) {
        size_t partition = keys[i] & ((1 << radix_bits) - 1);
        output[write_positions[partition]++] = keys[i];
    }
}
```

### Partition-Wise Join
```cpp
// Hash join with partitioned inputs
void partition_wise_hash_join(const Table& left, const Table& right,
                               size_t num_partitions) {
    // Partition both inputs on join key
    auto left_partitions = hash_partition(left, num_partitions);
    auto right_partitions = hash_partition(right, num_partitions);

    // Join each partition pair independently
    std::vector<std::thread> threads;
    std::vector<ResultSet> partition_results(num_partitions);

    for (size_t p = 0; p < num_partitions; ++p) {
        threads.emplace_back([&, p]() {
            // Build hash table from smaller partition
            absl::flat_hash_map<int64_t, std::vector<size_t>> hash_table;
            for (size_t i = 0; i < left_partitions[p].size(); ++i) {
                hash_table[left_partitions[p].keys[i]].push_back(i);
            }

            // Probe with larger partition
            for (size_t i = 0; i < right_partitions[p].size(); ++i) {
                auto it = hash_table.find(right_partitions[p].keys[i]);
                if (it != hash_table.end()) {
                    for (size_t left_idx : it->second) {
                        partition_results[p].add_match(left_idx, i);
                    }
                }
            }
        });
    }
    for (auto& t : threads) t.join();
}
```

### Range Partitioning for Parallel Sort
```cpp
// Partition into sorted ranges, then concatenate
void range_partition_sort(int64_t* data, size_t count, size_t num_partitions) {
    // Sample data to determine range boundaries
    std::vector<int64_t> samples;
    for (size_t i = 0; i < std::min(count, 1000UL); i += count / 1000) {
        samples.push_back(data[i]);
    }
    std::sort(samples.begin(), samples.end());

    std::vector<int64_t> boundaries;
    for (size_t i = 1; i < num_partitions; ++i) {
        boundaries.push_back(samples[i * samples.size() / num_partitions]);
    }

    // Partition and sort each partition
    auto partitions = partition_by_range(data, count, boundaries);
    parallel_for(partitions, [](auto& partition) {
        std::sort(partition.begin(), partition.end());
    });
}
```

## Performance Characteristics
- **Scalability**: Linear speedup with number of partitions (up to core count)
- **Cache**: Smaller partitions fit in L2/L3 cache (target 100KB-1MB per partition)
- **Overhead**: Partitioning pass adds ~5-10% overhead, pays off with >4 cores
- **Load Balance**: Hash partitioning can skew; radix partitioning more uniform

## Real-World Examples
- **DuckDB**: Radix partitioning for hash joins (8-bit radix, 256 partitions)
- **PostgreSQL**: Parallel hash join with hash-based partitioning
- **Spark/Flink**: Shuffle operations use hash partitioning across nodes
- **ClickHouse**: Partition tables by key for parallel query execution

## Pitfalls
- **Skewed Data**: Hash partitioning fails on skewed keys; use radix or hybrid approaches
- **Too Many Partitions**: >256 partitions thrash cache; sweet spot is 8-64 for joins
- **Memory Overhead**: Partitioning doubles memory usage temporarily
- **Small Data**: Partitioning overhead dominates for <100K rows
- **Cache-Unfriendly Access**: Random scattering during partitioning; radix partitioning with multiple passes improves locality
- **Partition Sizing**: Power-of-2 sizes enable fast modulo via bitwise AND
