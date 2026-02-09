# Compact Hash Tables

## What It Is
Cache-friendly hash tables using open addressing (all data in a single array) instead of chaining (linked lists). Modern variants like Robin Hood hashing and Swiss Tables minimize memory overhead and maximize cache locality.

## When To Use
- Hash joins and aggregations (GROUP BY) in query execution
- Symbol tables for query compilation
- Deduplication and set operations (DISTINCT, UNION)
- Caching frequently accessed metadata or statistics

## Key Implementation Ideas

### Robin Hood Hashing (DuckDB approach)
```cpp
// Each entry tracks "distance from ideal position" (DIB)
template<typename K, typename V>
struct RobinHoodEntry {
    K key;
    V value;
    uint8_t dib; // distance from ideal bucket
    bool occupied;
};

template<typename K, typename V>
class RobinHoodHashMap {
    std::vector<RobinHoodEntry<K, V>> entries;
    size_t size_ = 0;

    void insert(K key, V value) {
        size_t idx = hash(key) & (entries.size() - 1); // power-of-2 size
        uint8_t dib = 0;

        while (true) {
            if (!entries[idx].occupied) {
                entries[idx] = {key, value, dib, true};
                size_++;
                return;
            }

            // Robin Hood: steal from rich (high DIB), give to poor (low DIB)
            if (dib > entries[idx].dib) {
                std::swap(key, entries[idx].key);
                std::swap(value, entries[idx].value);
                std::swap(dib, entries[idx].dib);
            }

            idx = (idx + 1) & (entries.size() - 1); // linear probing
            dib++;
        }
    }
};
```

### Swiss Tables (Abseil flat_hash_map)
```cpp
// Uses SIMD to check 16 control bytes at once
struct SwissTableGroup {
    alignas(16) int8_t ctrl[16]; // control bytes: empty=-128, deleted=-2, hash[0..127]
    // Followed by 16 key-value pairs
};

// Probe with SIMD
bool find_in_group(int8_t hash_prefix, const int8_t ctrl[16]) {
    __m128i target = _mm_set1_epi8(hash_prefix);
    __m128i ctrl_vec = _mm_load_si128((__m128i*)ctrl);
    __m128i cmp = _mm_cmpeq_epi8(target, ctrl_vec);
    int mask = _mm_movemask_epi8(cmp);
    return mask != 0; // Check if any byte matched
}
```

### Tombstone-Free Deletion (Backward Shift)
```cpp
// Delete without tombstones by shifting entries backward
void erase(K key) {
    size_t idx = find_index(key);
    if (idx == npos) return;

    // Shift subsequent entries backward to fill gap
    size_t next = (idx + 1) & (entries.size() - 1);
    while (entries[next].occupied && entries[next].dib > 0) {
        entries[idx] = entries[next];
        entries[idx].dib--;
        idx = next;
        next = (idx + 1) & (entries.size() - 1);
    }

    entries[idx].occupied = false;
    size_--;
}
```

### Power-of-2 Sizing and Fast Modulo
```cpp
// Always size table to power of 2 for fast modulo
size_t compute_capacity(size_t min_capacity) {
    size_t capacity = 16; // minimum
    while (capacity < min_capacity) capacity *= 2;
    return capacity;
}

// Fast modulo for power-of-2 sizes
size_t index(uint64_t hash, size_t capacity) {
    return hash & (capacity - 1); // instead of hash % capacity
}
```

### Prefetching for Hash Table Probes
```cpp
// Prefetch hash table bucket before accessing
void prefetch_lookup(K key) {
    size_t idx = hash(key) & (entries.size() - 1);
    __builtin_prefetch(&entries[idx], 0, 3); // read, high temporal locality
}

// Batch lookups with prefetching
void batch_lookup(const K* keys, V* results, size_t count) {
    constexpr size_t PREFETCH_DISTANCE = 8;

    for (size_t i = 0; i < count; ++i) {
        if (i + PREFETCH_DISTANCE < count) {
            prefetch_lookup(keys[i + PREFETCH_DISTANCE]);
        }
        results[i] = lookup(keys[i]);
    }
}
```

## Performance Characteristics
- **Cache Locality**: 2-3x faster than std::unordered_map due to contiguous storage
- **Memory Overhead**: ~20% overhead vs 100-200% for chaining (std::unordered_map)
- **Load Factor**: Optimal at 50-90% full; resize at 87.5% (7/8 full)
- **Lookup Time**: O(1) average, O(log n) worst-case for Robin Hood
- **SIMD Speedup**: Swiss Tables 10-20% faster on lookups due to parallel control byte checks

## Real-World Examples
- **Abseil**: flat_hash_map/flat_hash_set (Swiss Tables, used by Google)
- **DuckDB**: ART (Adaptive Radix Tree) for large tables, Robin Hood for aggregations
- **ClickHouse**: Custom hash tables with open addressing
- **Folly**: F14FastMap (Facebook's Swiss Tables variant)

## Pitfalls
- **Small Key/Value**: Overhead dominates for tiny types; use specialized compact tables
- **High Load Factor**: >90% causes long probe chains; keep at 75-87.5%
- **Poor Hash Function**: Clustering/collisions kill performance; use high-quality hash (XXH3, Murmur3)
- **Resizing Cost**: Rehashing all entries is expensive; pre-allocate if size known
- **Deletion Churn**: Tombstones (in naive open addressing) waste space; use backward shift deletion
- **Non-Power-of-2 Sizes**: Requires expensive modulo instead of bitwise AND
