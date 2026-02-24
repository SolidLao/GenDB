---
name: data-structures
description: Data structure selection for database query operations. ALWAYS load for query planning, code generation, and optimization. Covers when to use hash tables vs bloom filters vs direct arrays vs sorted arrays, with size thresholds, memory estimates, and cost models. Essential for choosing the right structure for joins, aggregation, filtering, and existence checks.
user-invocable: false
---

# Skill: Data Structure Selection for Query Operations

## When to Load
ALWAYS for Query Planner, Code Generator, and Query Optimizer. This skill provides the foundational decision framework for choosing the right data structure for any query operation.

## Data Structure Selection Framework

The thresholds below are guidelines, not sharp cutoffs. The real decision depends on `entry_count × slot_size` relative to cache hierarchy (L1/L2/L3). Compute actual memory footprint and compare against hardware cache sizes.

| Scenario | Best Structure | Memory per Item | Lookup Cost | When to Use |
|----------|---------------|----------------|-------------|-------------|
| Dense integer keys, known range [0,N) where N×sizeof(V) fits in cache | Direct flat array | sizeof(V) | O(1), zero hash | Dimension lookups where key = row index |
| Equality lookup, small enough for cache-resident linear scan | Linear scan or sorted array | sizeof(K)+sizeof(V) | O(n) or O(log n) | Small dimension tables after filtering |
| Equality lookup, fits in LLC as hash table | Open-addressing hash table | ~2x (sizeof(K)+sizeof(V)) | O(1) amortized | Standard joins and aggregation |
| Existence check only, hash table would exceed LLC | Bloom filter | ~1.5 bytes/item | O(k) hash probes | Semi-join, anti-join, deduplication |
| Hash table exceeds LLC | Partitioned hash (LLC-sized partitions) | same as hash | O(1) + partition | Large fact tables, NUMA systems |
| Range queries (BETWEEN, <, >) | Sorted array + binary search | sizeof(K)+sizeof(V) | O(log n) | Range joins, ordered iteration |
| Pre-built and available as mmap file | Pre-built mmap index | 0 (runtime) | O(1) | ALWAYS preferred when available (P11) |

**Critical decision rule:** Calculate hash table memory = `next_power_of_2(N * 2) * slot_size`. If this exceeds the machine's LLC total, the structure will cause cache thrashing — consider partitioning into LLC-sized chunks, using bloom filter for existence checks, or pre-building the index during data ingestion.

## Bloom Filter

### When to Use
- Existence check only (no value retrieval needed), when equivalent hash table would exceed LLC
- Ideal for semi-join (EXISTS), anti-join (NOT EXISTS), and deduplication
- NOT suitable when you need the associated value (use hash table)
- For small build sides where a hash set fits comfortably in cache, a simple hash set may be simpler and equally fast

### Sizing Formula
`m_bits = ceil(-n * ln(p) / (ln(2))^2)` where n = entries, p = target FPR (typically 0.01)
- Optimal k hash functions: `k = (m/n) * ln(2)` (typically k=7 for 1% FPR)

### Memory Comparison
| Entries | Bloom (1% FPR) | Hash Table (8B slot) | Hash Table (16B slot) |
|---------|---------------|---------------------|----------------------|
| 1M | 1.2 MB | 16 MB | 32 MB |
| 10M | 12 MB | 160 MB | 320 MB |
| 100M | 120 MB | 1.6 GB | 3.2 GB |

### C++ Implementation Pattern
```
// Build (parallel-safe with atomic OR)
const size_t m_bits = (size_t)ceil(-n * log(0.01) / (log(2) * log(2)));
const size_t m_bytes = (m_bits + 7) / 8;
uint8_t* bloom = (uint8_t*)mmap(nullptr, m_bytes, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
memset(bloom, 0, m_bytes);

// Insert (thread-safe)
auto bloom_insert = [&](uint64_t key) {
    uint64_t h1 = key * 0x9E3779B97F4A7C15ULL;
    uint64_t h2 = key * 0x517CC1B727220A95ULL;
    for (int i = 0; i < 7; ++i) {
        uint64_t bit = (h1 + i * h2) % m_bits;
        __atomic_fetch_or(&bloom[bit / 8], (uint8_t)(1 << (bit % 8)), __ATOMIC_RELAXED);
    }
};

// Probe (read-only, no synchronization needed)
auto bloom_probe = [&](uint64_t key) -> bool {
    uint64_t h1 = key * 0x9E3779B97F4A7C15ULL;
    uint64_t h2 = key * 0x517CC1B727220A95ULL;
    for (int i = 0; i < 7; ++i) {
        uint64_t bit = (h1 + i * h2) % m_bits;
        if (!(bloom[bit / 8] & (1 << (bit % 8)))) return false;
    }
    return true;  // may be false positive
};
```

### Anti-Join Bloom Pattern
1. Build bloom on inner table keys
2. Scan outer table: bloom-negative rows → emit immediately (guaranteed non-match)
3. Bloom-positive candidates → collect into buffer
4. Build small exact hash set from matching inner subset (or full inner if small enough)
5. Probe candidates against exact hash → emit non-matches

### Semi-Join Bloom Pattern
1. Build bloom on inner table keys
2. Scan outer table: bloom-negative rows → skip immediately
3. Bloom-positive candidates → verify against exact hash set → emit matches

For hash table implementation details (Robin Hood, bounded probing, templates), see hash-tables skill.

## Composite Key Hashing

Pattern for multi-column keys (joins on (a,b,c), GROUP BY multiple columns):

### Key Struct Approach
```
struct CompositeKey {
    int32_t col1;
    int32_t col2;
    // Pack contiguously, align to 8 bytes
};
```

### Hash Function
```
// Knuth multiplicative constants for composite keys
uint64_t composite_hash(int32_t a, int32_t b) {
    return ((uint64_t)a * 0x9E3779B97F4A7C15ULL) ^ ((uint64_t)b * 0x517CC1B727220A95ULL);
}

uint64_t composite_hash3(int32_t a, int32_t b, int32_t c) {
    return ((uint64_t)a * 0x9E3779B97F4A7C15ULL) ^ ((uint64_t)b * 0x517CC1B727220A95ULL) ^ ((uint64_t)c * 0x6C62272E07BB0142ULL);
}
```

### Pack Optimization
When columns fit in 64 bits (e.g., two int32_t), pack into single uint64_t for single-instruction comparison:
```
uint64_t packed = ((uint64_t)(uint32_t)col1 << 32) | (uint32_t)col2;
```

## Memory Budget Estimation

How to estimate total memory for a query's data structures:
- **Hash table:** `next_power_of_2(N * 2) * slot_size`
- **Bloom filter:** `ceil(N * 12 / 8)` bytes at 1% FPR (~1.5 bytes/item)
- **Thread-local copies:** multiply by thread count (see parallelism skill for sizing rule)
- **Selection vectors:** `N * sizeof(uint32_t) * selectivity`
- **Rule:** total working set should fit in physical memory; hot working set should fit in aggregate L3

## Common Pitfalls
→ See experience skill: P19 (large hash table LLC thrashing), P21 (bloom filter for anti-join), P23 (hash table sized on filtered count)
→ See hash-tables skill for implementation details (Robin Hood, bounded probing)
