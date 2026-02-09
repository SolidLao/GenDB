# Hash Join Variants

## What It Is
Hash join builds a hash table on the smaller relation (build side) and probes it with tuples from the larger relation (probe side). Variants optimize for different data sizes, cardinalities, and memory constraints through partitioning, table design, and execution strategies.

## When To Use
- Unordered data with no useful indexes
- High-cardinality joins where nested loops are prohibitive
- Build side fits in L3 cache (simple hash join) or memory (partitioned variants)
- Equi-joins only (hash joins require equality predicates)
- Grace hash join when build side exceeds memory: partition both sides to disk, join partition pairs

## Key Implementation Ideas

### Build Side Selection
```cpp
// Choose smaller relation as build side based on cardinality estimates
size_t left_card = left_relation->GetCardinality();
size_t right_card = right_relation->GetCardinality();
auto build_side = (left_card < right_card) ? left_relation : right_relation;
auto probe_side = (left_card < right_card) ? right_relation : left_relation;
```

### Linear Probing Hash Table
```cpp
// Cache-friendly open addressing with linear probing
struct HashEntry {
    uint64_t hash;      // Store hash to avoid recomputation
    Tuple* tuple;       // Pointer to build tuple
    HashEntry* next;    // For chaining overflows
};

// Pre-size to ~75% load factor: size = build_cardinality / 0.75
size_t table_size = NextPowerOfTwo(build_card * 4 / 3);
std::vector<HashEntry> hash_table(table_size);

// Probe with linear probing
size_t slot = hash & (table_size - 1);
while (hash_table[slot].tuple != nullptr) {
    if (hash_table[slot].hash == hash &&
        KeyEquals(hash_table[slot].tuple, probe_tuple)) {
        EmitJoinResult(hash_table[slot].tuple, probe_tuple);
    }
    slot = (slot + 1) & (table_size - 1);  // Linear probe
}
```

### Partitioned Hash Join
```cpp
// Phase 1: Partition both relations (radix partitioning)
constexpr int NUM_PARTITIONS = 256;  // Fits L3 cache per partition
constexpr int RADIX_BITS = 8;

for (auto& tuple : build_relation) {
    int partition_id = Hash(tuple.join_key) >> (64 - RADIX_BITS);
    build_partitions[partition_id].push_back(tuple);
}

// Phase 2: Join each partition pair independently
for (int i = 0; i < NUM_PARTITIONS; i++) {
    // Build hash table for this partition (fits in cache)
    HashTable ht;
    for (auto& tuple : build_partitions[i]) {
        ht.Insert(tuple);
    }
    // Probe with corresponding partition
    for (auto& tuple : probe_partitions[i]) {
        ht.Probe(tuple, output);
    }
}
```

### Pre-filtering Probe Side
```cpp
// Apply filters before probing to reduce work
for (auto& probe_tuple : probe_relation) {
    if (!PassesLocalFilters(probe_tuple)) continue;  // Filter early

    size_t slot = Hash(probe_tuple.join_key) & mask;
    // ... probe logic
}
```

## Performance Characteristics
- Simple hash join: 2-3x faster than sort-merge when build side fits in L3 cache (1-8 MB)
- Partitioned hash join: Linear scaling to 100M+ rows, ~1.5x overhead from partitioning
- Linear probing: 10-20% faster than chaining for low collision rates (<20%)
- Cache misses dominate: Pre-sizing table reduces rehashing, partitioning improves locality
- Memory overhead: 1.3-2x build relation size (hash table + overflow chains)

## Real-World Examples
- **PostgreSQL**: Uses simple hash join with chaining, switches to batch/hash when memory exhausted
- **DuckDB**: Partitioned hash join with perfect hashing for low-cardinality keys, SIMD-optimized probing
- **ClickHouse**: Uses flat hash map (linear probing), pre-aggregates during build phase
- **Velox**: Adaptive radix partitioning, adjusts partition count based on data distribution

## Pitfalls
- **Wrong build side**: Building on large side wastes memory and causes cache thrashing
- **Poor hash function**: Collisions degrade to O(n) lookup, use MurmurHash3 or XXHash
- **Over-partitioning**: Too many partitions (>512) increases coordination overhead
- **Under-sizing table**: Triggers expensive rehashing mid-build, pre-size with cardinality estimates
- **No bloom filter**: For selective joins, bloom filter on build keys avoids 80-90% of probes
- **Ignoring null keys**: Hash nulls to separate bucket, nulls never match in SQL joins
