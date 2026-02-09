# Hash Aggregation

## What It Is
Hash aggregation builds a hash table mapping group keys to running aggregate states, updating in-place as tuples stream through. Combines grouping and aggregation in a single pass, avoiding materialization and sort overhead.

## When To Use
- GROUP BY on unordered data
- Medium-to-high cardinality group keys (100-10M distinct groups)
- Aggregate functions with fixed-size state (SUM, COUNT, MIN, MAX, AVG)
- Memory sufficient to hold hash table (< 1-2 GB per thread)
- OLAP queries with selective filters reducing input size

## Key Implementation Ideas

### Pre-sized Open Addressing Hash Table
```cpp
// Pre-allocate based on cardinality estimate to avoid resizing
struct AggregateEntry {
    uint64_t hash;          // Cached hash value
    GroupKey key;           // Group-by columns
    AggregateState state;   // Running aggregate (e.g., sum, count)
    bool occupied;          // Slot occupancy flag
};

class HashAggregation {
    std::vector<AggregateEntry> table;
    size_t num_groups;

    void Initialize(size_t estimated_groups) {
        // Size to 75% load factor, power of 2 for fast modulo
        size_t table_size = NextPowerOfTwo(estimated_groups * 4 / 3);
        table.resize(table_size);
        num_groups = 0;
    }

    void ProcessTuple(const Tuple& tuple) {
        uint64_t hash = Hash(tuple.group_key);
        size_t slot = hash & (table.size() - 1);

        // Linear probing
        while (table[slot].occupied) {
            if (table[slot].hash == hash &&
                table[slot].key == tuple.group_key) {
                // Update existing group
                UpdateAggregate(table[slot].state, tuple);
                return;
            }
            slot = (slot + 1) & (table.size() - 1);
        }

        // New group
        table[slot].hash = hash;
        table[slot].key = tuple.group_key;
        table[slot].state = InitializeAggregate(tuple);
        table[slot].occupied = true;
        num_groups++;
    }
};
```

### Cache-Friendly Layout
```cpp
// Pack aggregate state for cache locality
struct CompactAggState {
    int64_t sum;      // 8 bytes
    int64_t count;    // 8 bytes
    int32_t min;      // 4 bytes
    int32_t max;      // 4 bytes
    // Total: 24 bytes, fits 2.6 entries per cache line
};

// Separate keys from values for better locality during lookup
std::vector<GroupKey> keys;
std::vector<AggState> states;
```

### Combined Hash and Aggregate
```cpp
// Single-pass: hash and aggregate simultaneously
void HashAggregate(const std::vector<Tuple>& input) {
    // Pre-size based on estimate
    size_t est_groups = EstimateDistinctCount(input);
    Initialize(est_groups);

    for (const auto& tuple : input) {
        // Apply WHERE filters inline
        if (!PassesFilter(tuple)) continue;

        // Hash group key
        uint64_t hash = Hash(ExtractGroupKey(tuple));
        size_t slot = FindOrInsert(hash);

        // Update aggregate in-place
        aggregate_table[slot].sum += tuple.value;
        aggregate_table[slot].count++;
    }
}
```

### Perfect Hashing for Low Cardinality
```cpp
// For small domains (e.g., date, enum), use direct indexing
template<typename T>
class PerfectHashAggregation {
    std::vector<AggState> direct_table;
    T min_key, max_key;

    void Initialize(T min, T max) {
        min_key = min;
        max_key = max;
        direct_table.resize(max - min + 1);
    }

    void ProcessTuple(T key, const Tuple& tuple) {
        size_t idx = key - min_key;  // Direct index, no hashing
        UpdateAggregate(direct_table[idx], tuple);
    }
};
```

### Adaptive Aggregation
```cpp
// Switch strategy based on observed cardinality
class AdaptiveAggregation {
    enum Strategy { PERFECT_HASH, HASH_TABLE, SORT_BASED };
    Strategy current_strategy;
    size_t groups_seen = 0;

    void ProcessBatch(const std::vector<Tuple>& batch) {
        groups_seen += CountNewGroups(batch);

        // Adapt strategy
        if (groups_seen < 1000) {
            current_strategy = PERFECT_HASH;
        } else if (groups_seen < 1000000) {
            current_strategy = HASH_TABLE;
        } else {
            // Too many groups, spill to sort-based
            current_strategy = SORT_BASED;
        }
    }
};
```

## Performance Characteristics
- Single-pass: O(n) time for n input tuples
- Memory: O(g) for g distinct groups, typically 50-100 bytes per group
- Cache efficiency: 80-90% hit rate with good hash function and load factor
- Throughput: 100-500M rows/sec/core for simple aggregates (SUM, COUNT)
- Hash table lookup: 10-20 CPU cycles per tuple (1-2 cache misses)
- Beats sort-based aggregation by 2-4x when hash table fits in L3 cache

## Real-World Examples
- **DuckDB**: Uses perfect hashing for low cardinality, open addressing for medium, spills to disk for high
- **ClickHouse**: Two-level aggregation with thread-local hash tables, SIMD-optimized hash functions
- **PostgreSQL**: Simple chaining hash table, switches to sort-based when memory limit exceeded
- **Velox**: Adaptive hash aggregation with runtime cardinality tracking, vectorized updates

## Pitfalls
- **Underestimating groups**: Hash table resizes mid-query, 2-3x slowdown from rehashing
- **Overestimating groups**: Wastes memory, reduces cache hit rate, prefer conservative estimates
- **Large group keys**: Strings or compound keys increase memory 5-10x, consider key encoding
- **High cardinality**: >10M groups exhaust memory, switch to external sort-based aggregation
- **No spilling**: OOM crashes instead of graceful degradation, implement disk spill strategy
- **Poor hash function**: Collisions degrade to O(n^2), use high-quality hash (XXHash, Murmur)
- **Ignoring SIMD**: Vectorize aggregate updates (e.g., 4-8 sums at once) for 2-3x speedup
