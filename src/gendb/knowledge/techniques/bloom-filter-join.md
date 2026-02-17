# Bloom Filter Acceleration for Hash Joins

## What It Is
A probabilistic filter applied before hash table probes to skip non-matching keys early, reducing cache misses and hash table lookups.

## When to Use
- Hash joins where the build side is significantly smaller than the probe side
- Many probe keys have no match in the build side (low join selectivity)
- The hash table is too large for L3 cache, causing expensive cache misses on probe

## When NOT to Use
- Most probe keys match (high join selectivity)
- Build and probe sides are similar in size
- Hash table fits in L1/L2 cache (bloom filter adds overhead without benefit)

## Key Implementation Ideas

### Bloom Filter Implementation
```cpp
class BloomFilter {
    static constexpr size_t SIZE = 1 << 17;  // 128KB — fits in L2 cache
    static constexpr size_t MASK = SIZE * 8 - 1;
    uint8_t bits[SIZE] = {};

public:
    void insert(uint64_t hash) {
        uint32_t h1 = hash & MASK;
        uint32_t h2 = (hash >> 17) & MASK;
        uint32_t h3 = (hash >> 34) & MASK;
        bits[h1 >> 3] |= (1 << (h1 & 7));
        bits[h2 >> 3] |= (1 << (h2 & 7));
        bits[h3 >> 3] |= (1 << (h3 & 7));
    }

    bool maybe_contains(uint64_t hash) const {
        uint32_t h1 = hash & MASK;
        uint32_t h2 = (hash >> 17) & MASK;
        uint32_t h3 = (hash >> 34) & MASK;
        return (bits[h1 >> 3] & (1 << (h1 & 7))) &&
               (bits[h2 >> 3] & (1 << (h2 & 7))) &&
               (bits[h3 >> 3] & (1 << (h3 & 7)));
    }
};
```

### Usage Pattern
```cpp
// Step 1: Build hash table AND bloom filter together
CompactHashTable<int32_t, RowData> ht(build_size * 2);
BloomFilter bf;

for (int64_t i = 0; i < build_rows; i++) {
    int32_t key = build_key_col[i];
    uint64_t h = hash_fn(key);
    ht.insert(key, data[i], h);
    bf.insert(h);
}

// Step 2: Probe — check bloom filter BEFORE hash table
for (int64_t i = 0; i < probe_rows; i++) {
    int32_t key = probe_key_col[i];
    uint64_t h = hash_fn(key);

    // Bloom filter check — skip 80-90% of non-matching keys
    if (!bf.maybe_contains(h)) continue;

    // Only probe hash table for potential matches
    auto it = ht.find(key, h);
    if (it) {
        emit(i, it);
    }
}
```

### Sizing the Bloom Filter
- Target false positive rate: 1-5%
- Rule of thumb: 10 bits per element → ~1% FP rate
- For 100K build keys: 128KB bloom filter (fits L2 cache)
- For 1M build keys: 1.25MB bloom filter (fits L3 cache)

## Performance Impact
- Without bloom: Every probe row → hash table lookup (cache miss if table > L3)
- With bloom: 80-90% of non-matching rows filtered by L2-resident bloom check
- Typical speedup: 2-5x on large hash joins with low selectivity
