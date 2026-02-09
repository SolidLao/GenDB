# Sorted Indexes

## What It Is
Sorted indexes maintain keys in sorted order, enabling efficient range scans and binary search lookups. B-trees dominate disk-based systems; in-memory databases often use cache-optimized variants or sorted arrays.

## When To Use
- Range queries (`WHERE key BETWEEN a AND b`)
- Ordered scans (ORDER BY, MIN/MAX aggregates)
- Prefix matches (string LIKE 'prefix%')
- When updates are infrequent (sorted arrays) or batched (LSM-trees)

## Key Implementation Ideas

### Binary Search on Sorted Arrays
```cpp
// Cache-friendly, branch-predictor-friendly
template<typename T>
size_t lower_bound(const T* arr, size_t n, T key) {
    size_t left = 0, right = n;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (arr[mid] < key) left = mid + 1;
        else right = mid;
    }
    return left;
}

// Prefetch next level to hide latency
size_t mid = left + (right - left) / 2;
__builtin_prefetch(&arr[mid + (right - mid) / 2]);
__builtin_prefetch(&arr[left + (mid - left) / 2]);
```

### SIMD Binary Search
```cpp
// Compare key against 4 pivots simultaneously (AVX2)
size_t simd_lower_bound(const int32_t* arr, size_t n, int32_t key) {
    size_t pos = 0, step = n / 4;
    __m256i search = _mm256_set1_epi32(key);

    while (step > 0) {
        __m256i data = _mm256_loadu_si256((__m256i*)&arr[pos]);
        __m256i cmp = _mm256_cmpgt_epi32(search, data);
        int mask = _mm256_movemask_ps(_mm256_castsi256_ps(cmp));
        pos += __builtin_popcount(mask) * step;
        step /= 4;
    }
    return pos;
}
```

### Cache-Oblivious B-Tree (van Emde Boas layout)
```cpp
// Recursive layout optimizes cache usage at all levels
struct VEBNode {
    static constexpr size_t FANOUT = 16;
    int32_t keys[FANOUT - 1];
    VEBNode* top_tree;      // Top sqrt(n) keys
    VEBNode* bottom_trees[FANOUT];  // sqrt(n) bottom trees
};

// Layout in memory: [top_tree][bottom_0]...[bottom_k]
// Ensures O(log_B N) cache misses for block size B
```

### Interpolation Search (for uniform distributions)
```cpp
// O(log log n) for uniform data, O(n) worst-case
size_t interpolation_search(const int32_t* arr, size_t n, int32_t key) {
    size_t left = 0, right = n - 1;
    while (left <= right && key >= arr[left] && key <= arr[right]) {
        // Estimate position based on value distribution
        size_t pos = left + ((double)(key - arr[left]) /
                             (arr[right] - arr[left]) * (right - left));
        if (arr[pos] == key) return pos;
        if (arr[pos] < key) left = pos + 1;
        else right = pos - 1;
    }
    return n;  // Not found
}
```

### B-Tree Variants
```cpp
// B+ Tree: All values in leaves, internal nodes are pure index
struct BPlusNode {
    bool is_leaf;
    uint16_t count;
    int32_t keys[FANOUT - 1];
    union {
        BPlusNode* children[FANOUT];  // Internal node
        int32_t values[FANOUT - 1];   // Leaf node
    };
    BPlusNode* next_leaf;  // Leaf linked list for scans
};

// ART (Adaptive Radix Tree): Variable fanout, path compression
// Nodes adapt: Node4, Node16, Node48, Node256
```

## Performance Characteristics
- Binary search: O(log n) comparisons, ~log2(n) cache misses
- SIMD binary search: ~2x speedup on sorted arrays (4-8 comparisons/cycle)
- Interpolation search: O(log log n) on uniform data (10x faster for millions of keys)
- B-tree: O(log_B n) I/Os, but high constant factor vs hash tables
- Cache-oblivious: 2-3x fewer cache misses than pointer-based B-trees
- Memory overhead: 10-30% for B-trees (internal nodes), 0% for sorted arrays

## Real-World Examples
- **PostgreSQL**: B-tree indexes (FANOUT=~300 for disk pages)
- **DuckDB**: ART index for in-memory lookups, sorted vectors for column data
- **ClickHouse**: MergeTree uses sorted primary key, sparse index every 8192 rows
- **HyPer**: SIMD-optimized binary search on sorted runs
- **LevelDB/RocksDB**: LSM-tree with sorted runs, bloom filters per level

## Pitfalls
- Sorted arrays: Inserts are O(n), use only for read-heavy or batch updates
- Binary search: Branch mispredictions hurt (use branchless or SIMD variants)
- Interpolation search: Degrades to O(n) on skewed data (use hybrid approach)
- Deep B-trees: Pointer chasing kills performance (keep in L3 cache or use hugepages)
- Over-indexing: Every index doubles write cost (choose selective indexes)
- String comparisons: Expensive, use prefix compression or integer encoding
