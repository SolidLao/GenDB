# Sorted Indexes & B-Trees

## What It Is
Sorted indexes maintain keys in sorted order, enabling efficient range scans, binary search lookups, and ordered iteration. B+ Trees are the most widely-used index structure in traditional databases and excel at range queries.

## B+ Tree

B+ Trees are the best index for **range predicates** (date ranges, value ranges, ordered scans):
- **Structure**: Internal nodes contain routing keys only; all data in leaf nodes; leaves linked for sequential scan
- **Fan-out**: Typically 100-1000 keys per node (sized to cache line or page), giving O(log_F N) height — N rows (from workload analysis) with fanout 256 = typically 3-4 levels
- **Range scans**: Find start key in O(log N), then follow leaf links — ideal for `WHERE date BETWEEN X AND Y`
- **Point lookups**: O(log N), comparable to hash indexes for practical sizes
- **Ordered iteration**: Natural — just scan leaf level left-to-right

### When to Use B+ Trees (vs other indexes)
| Use Case | Best Index | Why |
|----------|-----------|-----|
| Range predicate (`col BETWEEN X AND Y`) | **B+ Tree** | Jump directly to start, scan to end |
| Highly selective range (<10% rows) | **B+ Tree** | Precise — only touches qualifying rows |
| Moderate selectivity range (10-80%) | **Zone Map + B+ Tree** | Zone map for coarse block skip, B+ Tree for precise lookup within blocks |
| Low selectivity range (>80% rows) | **Zone Map** (or full scan) | B+ Tree overhead not worth it; most blocks qualify anyway |
| Point lookup (equality) | **Hash Index** | O(1) vs O(log N), but B+ Tree is acceptable |
| Join (FK lookup) | **Hash Index** (multi-value) | O(1) probe per key during join; B+ Tree works for sort-merge joins |
| Ordered output / merge join | **B+ Tree** or sorted data | Already in order — no sort needed |

### Building a B+ Tree on Binary Columnar Data

For GenDB's pre-built binary columns, B+ Tree construction is straightforward:

1. **If data is already sorted by the index column** (e.g., fact table sorted by date column): The column file IS the leaf level. Only build internal routing nodes.
2. **If data is not sorted**: Create a sorted index (key, position) array, then build internal nodes on top.

**Internal node format** (binary):
```
[level] [num_keys] [key_0, child_ptr_0, key_1, child_ptr_1, ..., child_ptr_N]
```
Where `child_ptr` is an offset into the leaf level or the next internal level.

**Leaf node format** (binary):
```
[num_entries] [key_0, position_0, key_1, position_1, ...] [next_leaf_offset]
```

**Node size**: Target L3 cache line (64 bytes) or page size (4KB). For `int32_t` keys with `uint32_t` positions: 4KB page = ~500 entries per leaf.

### B+ Tree vs Zone Map — Complementary, Not Competing

Zone maps and B+ Trees solve different granularity problems:
- **Zone map**: Block-level (100K rows per zone). Answers "does this block contain any matching rows?" Very cheap.
- **B+ Tree**: Row-level. Answers "exactly which rows match?" More precise but larger index.

**Best practice**: Use zone maps for coarse pruning (always cheap), add B+ Tree when selectivity warrants precise row-level access.

## Other Sorted Index Techniques

- **Binary Search on Sorted Arrays**: Cache-friendly O(log n) lookup; prefetch both children of the next comparison to hide latency
- **SIMD Binary Search**: Compare key against 4-8 pivots simultaneously using AVX2, achieving ~2x speedup over scalar binary search
- **Interpolation Search**: Estimate position based on value distribution for O(log log n) on uniform data; degrades on skewed data
- **Adaptive Radix Tree (ART)**: Variable fanout nodes (4/16/48/256) with path compression for space-efficient in-memory indexing
- **Cache-Oblivious B-Tree (van Emde Boas Layout)**: Recursive memory layout optimizes cache usage at all levels without knowing cache size
- **Branchless Binary Search**: Eliminate branch mispredictions using conditional moves for more predictable performance
- **Prefix Compression**: Reduce key storage by sharing common prefixes in adjacent entries, especially for string keys

## Construction Efficiency

- **mmap** for reading binary column files (zero-copy)
- **OpenMP** for parallel construction: partition keys, build subtrees independently, merge
- **If data already sorted**: Only build internal nodes — O(N/fanout) work, very fast
- **Compile with**: `-O3 -march=native -fopenmp`
