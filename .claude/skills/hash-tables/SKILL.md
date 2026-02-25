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

### LLC-Aware Capacity Tuning
When the hash table at standard 50% load slightly exceeds LLC (e.g., by <2×), consider whether reducing capacity to fit LLC improves overall performance:
- Increasing load factor from 50% to 60-75% increases average probe length (from ~1.5 to ~2-3 for Robin Hood) but makes every probe an LLC hit (~20ns) instead of LLC miss (~100ns)
- Net effect can be positive when the table is within ~2× of LLC size and a bloom pre-filter already eliminates most non-matching probes
- Trade-off: higher load degrades worst-case probe length. Robin Hood hashing bounds probe variance, making this safer than linear probing at higher load
- Decision rule: if `cap_50pct × slot_size > LLC` AND `cap_70pct × slot_size < LLC × 0.8`, the higher load factor is worth evaluating

## CAS-Based Concurrent Insert

When the build phase inserts unique keys (e.g., PK joins) and sequential insertion is a bottleneck, consider lock-free parallel insert using `compare_exchange_strong`:

Conditions favoring this approach:
- Keys are unique (PK column) — no duplicate-key conflicts between threads
- Build-side cardinality is large enough (>100K entries) that sequential insert is measurable
- Standard Robin Hood hashing is NOT compatible with concurrent insert (displacement chains create read-write conflicts). Use simple linear probing with CAS instead.

Pattern: each thread CAS-claims empty slots. If CAS fails (another thread claimed it), re-read and continue probing. SoA layout (separate keys[], values[] arrays) works well since only keys[] needs atomic operations during build.

Trade-off: CAS-based linear probing has slightly longer average probe chains than Robin Hood at the same load factor. But parallel build can be 5-10× faster than sequential Robin Hood for >1M entries, which typically outweighs the probe-length increase.

## SoA vs AoS Layout

**Struct-of-Arrays (SoA)**: separate arrays for keys[], values[], metadata[]. Advantages:
- During probe, only keys[] array is touched for non-matching slots → smaller cache footprint
- Enables selective atomic operations (atomic keys[] during build, atomic values[] during aggregation) without bloating non-atomic fields
- Better for combined join+aggregation HTs where build phase touches keys+metadata but probe phase touches keys+accumulators

**Array-of-Structs (AoS)**: single array of {key, value, metadata} slots. Advantages:
- On a match, key + all payload in same cache line → one access
- Simpler code, better for small slot sizes (≤16B)

Guideline: when the hash table serves multiple phases with different access patterns (e.g., build writes keys+metadata, probe reads keys+accumulates values), SoA tends to perform better because each phase only warms the arrays it needs.

## Software Prefetching for Hash Probes

When the hash table fits LLC but individual probes still incur L3 latency (~10-15ns), consider software prefetching to hide latency during sequential scans:

```
// During probe-side scan, prefetch hash table slot for a future row
if (r + PFDIST < r_end) {
    uint32_t ph = hash(probe_keys[r + PFDIST]) & mask;
    __builtin_prefetch(ht_keys + ph, 0, 1);    // read, L2 hint
    __builtin_prefetch(ht_values + ph, 1, 1);   // write, L2 hint (if accumulating)
}
```

Conditions favoring this:
- HT fits LLC but is too large for L2 — individual probes hit L3
- Probe-side scan is sequential (predictable access to probe columns, but random into HT)
- Prefetch distance (PFDIST) should be tuned: 16-64 rows typical, depends on L3 latency / per-row work

When NOT helpful: HT fits L2 (already fast), or HT exceeds LLC (prefetch into LLC still misses on eviction).

## Common Pitfalls
→ See experience skill: C9 (capacity overflow), C20 (memset sentinel), C24 (unbounded probing), P1 (std::unordered_map), P7 (std::map with pair keys), P34 (SoA + CAS concurrent HT)
