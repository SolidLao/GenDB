# Bloom Filters

## What It Is
Bloom filters are probabilistic data structures that test set membership with no false negatives but controlled false positives. Hash-based bit arrays enable space-efficient filtering for join reduction and existence checks.

## When To Use
- Semi-join reduction (filter probe side before expensive hash join)
- Existence checks on large datasets (avoid disk I/O for non-existent keys)
- Distributed joins (broadcast small bloom filter, filter before network shuffle)
- Multi-column predicates (combine bloom filters with AND/OR)
- NOT for: Exact membership (use hash table), deletions (use counting bloom filter)

## Key Implementation Ideas

### Classic Bloom Filter
```cpp
class BloomFilter {
    std::vector<uint64_t> bits;  // Bit array
    size_t k;  // Number of hash functions

    BloomFilter(size_t n, double fpr) {
        // Optimal size: m = -n * ln(fpr) / (ln(2)^2)
        size_t m = std::ceil(-n * std::log(fpr) / (std::log(2) * std::log(2)));
        bits.resize((m + 63) / 64, 0);

        // Optimal k: k = (m / n) * ln(2)
        k = std::round((m / (double)n) * std::log(2));
    }

    void insert(uint64_t hash) {
        for (size_t i = 0; i < k; i++) {
            uint64_t h = hash_combine(hash, i);
            size_t bit = h % (bits.size() * 64);
            bits[bit / 64] |= (1ULL << (bit % 64));
        }
    }

    bool contains(uint64_t hash) const {
        for (size_t i = 0; i < k; i++) {
            uint64_t h = hash_combine(hash, i);
            size_t bit = h % (bits.size() * 64);
            if (!(bits[bit / 64] & (1ULL << (bit % 64))))
                return false;  // Definitely not in set
        }
        return true;  // Probably in set (may be false positive)
    }
};
```

### Blocked Bloom Filter (Cache-Optimized)
```cpp
// Each key accesses single cache line (64 bytes = 512 bits)
class BlockedBloomFilter {
    static constexpr size_t BLOCK_SIZE = 512;  // Bits per block
    std::vector<std::array<uint64_t, 8>> blocks;

    void insert(uint64_t hash) {
        size_t block_idx = hash % blocks.size();
        auto& block = blocks[block_idx];

        // k hash functions within single cache line
        for (size_t i = 0; i < k; i++) {
            uint32_t h = hash_combine(hash, i) & 0x1FF;  // 0-511
            block[h / 64] |= (1ULL << (h % 64));
        }
    }

    bool contains(uint64_t hash) const {
        size_t block_idx = hash % blocks.size();
        const auto& block = blocks[block_idx];

        for (size_t i = 0; i < k; i++) {
            uint32_t h = hash_combine(hash, i) & 0x1FF;
            if (!(block[h / 64] & (1ULL << (h % 64))))
                return false;
        }
        return true;
    }
};
// Benefit: 1 cache miss per lookup (vs k misses for classic)
```

### Counting Bloom Filter (Supports Deletions)
```cpp
class CountingBloomFilter {
    std::vector<uint8_t> counters;  // 4-bit counters (0-15)

    void insert(uint64_t hash) {
        for (size_t i = 0; i < k; i++) {
            size_t idx = hash_idx(hash, i);
            if (counters[idx] < 15) counters[idx]++;
        }
    }

    void remove(uint64_t hash) {
        for (size_t i = 0; i < k; i++) {
            size_t idx = hash_idx(hash, i);
            if (counters[idx] > 0) counters[idx]--;
        }
    }

    bool contains(uint64_t hash) const {
        for (size_t i = 0; i < k; i++) {
            if (counters[hash_idx(hash, i)] == 0) return false;
        }
        return true;
    }
};
// Memory: 4x larger than standard bloom filter
```

### Optimal Sizing
```cpp
// Bits per element for target false positive rate
double bits_per_element(double fpr) {
    return -std::log(fpr) / (std::log(2) * std::log(2));
}

// Examples:
// 1% FPR:   9.6 bits/element
// 0.1% FPR: 14.4 bits/element
// 0.01% FPR: 19.2 bits/element

// For 1M elements at 1% FPR: 1.2 MB (vs 32 MB for hash table)
```

### Vectorized Bloom Filter Probing
```cpp
// Check 4 keys in parallel (AVX2)
void probe_batch_simd(const uint64_t* hashes, bool* results, size_t n) {
    for (size_t i = 0; i < n; i += 4) {
        __m256i hash_vec = _mm256_loadu_si256((__m256i*)&hashes[i]);

        // Compute 4 block indices
        __m256i block_idx = _mm256_rem_epu64(hash_vec, _mm256_set1_epi64x(blocks.size()));

        // Gather 4 cache lines (AVX2 gather)
        __m256i block0 = _mm256_i64gather_epi64((const long long*)blocks.data(), block_idx, 8);

        // SIMD bit tests (simplified - actual impl more complex)
        // Set results[i:i+3] based on membership
    }
}
```

## Performance Characteristics
- Expected speedup: 5-100x on semi-joins (filter 90-99% of non-matching rows)
- Cache behavior: Blocked filters = 1 cache miss, classic = k misses (k=4-7)
- Memory overhead: 8-20 bits/element (vs 32-64 bytes for hash table)
- False positive rate: 1% typical (configurable: 0.01%-10%)
- Build time: O(n * k) hashes, ~5-10ns per insert (100M keys in 0.5-1s)

## Real-World Examples
- **PostgreSQL**: Bloom filter indexes for multi-column searches (pg_bloom extension)
- **DuckDB**: Runtime bloom filters for hash join optimization
- **Spark/Databricks**: Broadcast bloom filters for data skipping in joins
- **ClickHouse**: Bloom filters on skip indexes (tokenbf_v1, ngrambf_v1)
- **Parquet**: Page-level bloom filters (512-byte blocks, MURMUR3 hash)

## Pitfalls
- Over-sizing: Too many bits wastes memory and cache (diminishing returns <0.1% FPR)
- Under-sizing: High false positive rate negates filtering benefit
- Classic design: k cache misses kill performance (use blocked variant)
- Hash quality: Poor hash functions increase collision rate (use XXHash or MurmurHash)
- Multiple filters: AND combines well, OR accumulates false positives
- Counting filters: 4x memory overhead, saturation at count=15
- Dynamic sizing: Can't resize without rebuild (pre-allocate for worst case)
