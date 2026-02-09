# Hash Indexes

## What It Is
Hash indexes provide O(1) average-case lookups by mapping keys to buckets using a hash function. Modern implementations use open addressing with optimized probe sequences and power-of-2 sizing for fast modulo operations.

## When To Use
- Point lookups (equality predicates: `WHERE key = value`)
- Hash joins (build hash table on smaller relation)
- GROUP BY aggregations (hash-based grouping)
- NOT suitable for range queries or prefix matches

## Key Implementation Ideas

### Robin Hood Hashing
```cpp
// Probe distance invariant: minimize variance in probe sequence lengths
struct HashEntry {
    uint64_t hash;
    uint32_t key;
    uint32_t value;
    uint8_t probe_distance;  // Distance from ideal position
};

void insert(HashEntry entry) {
    size_t pos = entry.hash & (capacity - 1);  // Power-of-2 mask
    while (true) {
        if (table[pos].empty()) {
            table[pos] = entry;
            return;
        }
        // Rob from the rich: swap if current entry traveled less
        if (table[pos].probe_distance < entry.probe_distance) {
            std::swap(table[pos], entry);
        }
        pos = (pos + 1) & (capacity - 1);
        entry.probe_distance++;
    }
}
```

### Swiss Table (Google Abseil)
```cpp
// Use SIMD to check 16 slots in parallel
struct ControlByte {
    int8_t h2;  // Top 7 bits of hash (negative = empty, -128 = deleted)
};

bool find(uint64_t hash, Key key) {
    size_t group = (hash >> 7) & mask;
    int8_t h2 = hash & 0x7F;

    __m128i target = _mm_set1_epi8(h2);
    __m128i ctrl = _mm_load_si128(&control[group]);
    __m128i match = _mm_cmpeq_epi8(ctrl, target);

    uint16_t bitmask = _mm_movemask_epi8(match);
    while (bitmask) {
        size_t offset = __builtin_ctz(bitmask);
        if (slots[group * 16 + offset].key == key) return true;
        bitmask &= bitmask - 1;  // Clear lowest set bit
    }
    return false;
}
```

### Cuckoo Hashing (for guaranteed O(1) lookup)
```cpp
// Two hash functions, two tables, relocate on collision
bool insert(Key key, Value value) {
    for (int attempts = 0; attempts < MAX_KICKS; attempts++) {
        size_t pos1 = hash1(key) % capacity;
        if (table1[pos1].empty()) { table1[pos1] = {key, value}; return true; }

        std::swap(key, table1[pos1].key);
        std::swap(value, table1[pos1].value);

        size_t pos2 = hash2(key) % capacity;
        if (table2[pos2].empty()) { table2[pos2] = {key, value}; return true; }

        std::swap(key, table2[pos2].key);
        std::swap(value, table2[pos2].value);
    }
    resize();  // Rebuild with larger capacity
}
```

### Power-of-2 Sizing
```cpp
// Replace expensive modulo with bitwise AND
size_t capacity = 1ULL << log2_capacity;  // Always power of 2
size_t index = hash & (capacity - 1);     // Fast modulo
```

## Performance Characteristics
- Expected speedup: 5-10x over B-tree for point lookups
- Cache behavior: 1-3 cache misses (probe sequence locality)
- Memory overhead: 20-50% empty space (load factor 0.5-0.8)
- Swiss Table: ~2x faster than std::unordered_map (SIMD + dense layout)
- Robin Hood: Better worst-case probe distance (max ~10 vs ~100)

## Real-World Examples
- **PostgreSQL**: Hash indexes rebuilt on crash (not WAL-logged until PG 10)
- **DuckDB**: Aggr Hash Table uses Robin Hood hashing with vectorized probing
- **ClickHouse**: Uses Google's Swiss Table (absl::flat_hash_map)
- **HyPer**: Morsel-driven hash joins with NUMA-aware partitioning

## Pitfalls
- Resizing is expensive: Pre-size if cardinality known (avoid 2^n threshold)
- Poor hash functions cause clustering (use MurmurHash, XXHash, or hardware CRC32)
- High load factors (>0.9) degrade performance exponentially
- Open addressing wastes memory on deletions (tombstones)
- Cuckoo hashing: Resize storms when load factor too high (>0.85)
- Cold cache: First probe misses L1/L2, subsequent probes may miss L3
