# Sorted Indexes

## What It Is
Sorted indexes maintain keys in sorted order, enabling efficient range scans and binary search lookups. B-trees dominate disk-based systems; in-memory databases often use cache-optimized variants or sorted arrays.

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
