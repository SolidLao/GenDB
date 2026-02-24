---
name: hash-tables
description: Hash table implementation patterns for database operations. Load when implementing hash joins, hash aggregation, or any custom hash map. Covers open-addressing, Robin Hood hashing, sizing rules, bounded probing, sentinel initialization, multi-value patterns.
user-invocable: false
---

# Skill: Hash Table Implementation Patterns

## When to Load
Queries needing hash joins or hash aggregation (most OLAP queries).

## When NOT to Use Hash Tables
For the full decision framework of when to use hash tables vs alternatives, see data-structures skill.
Hash tables are suboptimal for:
- Very small entry counts where linear scan fits in a cache line or two (overhead of hashing exceeds sequential scan)
- Existence-only checks where the equivalent hash table would exceed LLC (bloom filter — see data-structures skill: Bloom Filter)
- Range queries (sorted array + binary search)
- Dense integer keys with known range [0,N) where N×sizeof(V) fits comfortably in cache (direct flat array)

## Key Principles
- Open-addressing with Robin Hood hashing: cache-friendly, predictable performance
- Power-of-2 sizing: capacity = next_power_of_2(expected_entries * 2) for ≤50% load factor
- Bounded probing: for-loop with probe < capacity, NOT unbounded while loop
- Sentinel initialization: std::fill(keys, keys+CAP, EMPTY_KEY). NEVER memset for multi-byte sentinels.

## GenDB Hash Table Template
Two-array design for joins (separate keys[] and values[]):
```
struct HashTable {
    uint32_t* keys;     // EMPTY_KEY sentinel
    uint32_t* values;   // payload (row indices, aggregates)
    uint32_t capacity;  // power of 2
    uint32_t mask;      // capacity - 1
};
```

### Insert Pattern
```
uint32_t h = hash(key) & mask;
for (uint32_t p = 0; p < capacity; ++p) {
    if (keys[h] == EMPTY_KEY) { keys[h] = key; values[h] = val; break; }
    h = (h + 1) & mask;
}
```

### Probe Pattern
```
uint32_t h = hash(key) & mask;
for (uint32_t p = 0; p < capacity; ++p) {
    if (keys[h] == EMPTY_KEY) break;  // not found
    if (keys[h] == key) { /* found: values[h] */ break; }
    h = (h + 1) & mask;
}
```

## Multi-Value Hash Table (for 1:N joins)

### Chain-Based Pattern
keys[] stores first-occurrence index, chain[] links duplicates.
Good for variable-count duplicates, probe-heavy workloads.
```
// Build: keys[h] = key; if collision with same key, values[pos] = value, next[pos] = head[h], head[h] = pos
// Probe: h = hash(key) & mask; pos = head[h]; while (pos != EMPTY) { process(values[pos]); pos = next[pos]; }
```

### Offset-Based Pattern (Preferred)
offsets[] + positions[] — sorted positions per key for cache-friendly iteration.
Better cache locality for iteration-heavy patterns (star joins, aggregation with payload).
```
// Layout: [uint32_t num_entries][positions...][uint32_t num_buckets][offsets...]
// Probe: start = offsets[h]; end = offsets[h+1]; for (i = start; i < end; ++i) process(positions[i]);
```

## Sizing Rules
- Join build side: capacity = next_power_of_2(build_cardinality * 2)
- Aggregation: capacity = next_power_of_2(estimated_groups * 2)
- Thread-local: EACH thread sized for FULL group cardinality (any thread may see all groups)
- **CRITICAL:** Size based on FILTERED row count, not raw table size (see experience P23)
- Hash function: multiply-shift `(key * 0x9E3779B97F4A7C15ULL) >> shift` preferred over std::hash (which may be identity on some compilers, causing clustering with power-of-2 tables)

## Common Pitfalls
→ See experience skill: C9 (capacity overflow), C20 (memset sentinel), C24 (unbounded probing), P1 (std::unordered_map), P7 (std::map with pair keys)
