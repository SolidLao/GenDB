# Sorted Indexes

## What It Is
Sorted indexes maintain keys in sorted order, enabling efficient range scans and binary search lookups. B-trees dominate disk-based systems; in-memory databases often use cache-optimized variants or sorted arrays.

## When To Use
- Range queries (`WHERE key BETWEEN a AND b`)
- Ordered scans (ORDER BY, MIN/MAX aggregates)
- Prefix matches (string LIKE 'prefix%')
- When updates are infrequent (sorted arrays) or batched (LSM-trees)
- Multi-version concurrency control where key ordering enables efficient snapshots

## Key Implementation Ideas
- **Binary Search on Sorted Arrays**: Cache-friendly O(log n) lookup; prefetch both children of the next comparison to hide latency
- **SIMD Binary Search**: Compare key against 4-8 pivots simultaneously using AVX2, achieving ~2x speedup over scalar binary search
- **Interpolation Search**: Estimate position based on value distribution for O(log log n) on uniform data; degrades on skewed data
- **B+ Tree**: All values stored in leaves with a leaf linked list for sequential scans; internal nodes are pure routing index
- **Adaptive Radix Tree (ART)**: Variable fanout nodes (4/16/48/256) with path compression for space-efficient in-memory indexing
- **Cache-Oblivious B-Tree (van Emde Boas Layout)**: Recursive memory layout optimizes cache usage at all levels without knowing cache size
- **LSM-Tree**: Batch writes into sorted runs with background merge compaction; pairs well with bloom filters per level
- **Prefix Compression**: Reduce key storage by sharing common prefixes in adjacent entries, especially for string keys
- **Branchless Binary Search**: Eliminate branch mispredictions using conditional moves for more predictable performance

## Performance Characteristics
- Binary search: O(log n) comparisons, ~log2(n) cache misses
- B-tree: O(log_B n) I/Os; cache-oblivious layout gives 2-3x fewer cache misses than pointer-based B-trees
- Memory overhead: 10-30% for B-trees (internal nodes), 0% for sorted arrays

## Pitfalls
- Sorted arrays have O(n) inserts; use only for read-heavy or batch-update workloads
- Branch mispredictions hurt binary search; use branchless or SIMD variants on hot paths
- Over-indexing doubles write cost per index; choose selective indexes carefully
