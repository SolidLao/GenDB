# Iteration 4: Replace hash structures in Q16 partsupp scan with flat arrays

## Quantitative Diagnosis

The `fused_scan_partsupp` region consumes 630ms — 33.8% of the 1863ms dispatcher_total. This is the single largest bottleneck. The function scans 8M partsupp rows for Q16, which groups by (brand, type, size) and counts distinct suppliers.

The current implementation uses three hash-based structures in the hot loop:
1. `unordered_set<int> excluded_types` — hash probe per row for type filtering
2. `unordered_set<int> target_sizes` — hash probe per row for size filtering
3. `unordered_map<Q16Key, unordered_set<int32_t>> q16_groups` — hash map lookup + hash set insert per qualifying row

With ~14% selectivity after part filters, ~1.1M rows qualify and trigger the expensive hash map + hash set path. The `unordered_set<int32_t>` per group uses node-based allocation, creating millions of small heap allocations with random access patterns. This far exceeds L3 cache (44MB) in effective working set due to pointer chasing.

## Why Category B (Algorithm Change)

The hash-based data structures are fundamentally mismatched to the hardware. No tuning of the hash function or bucket count will fix the cache-hostile random access pattern inherent in chained hash tables with node-allocated sets. The algorithm must change.

## What Changed

1. **Filter arrays**: Replaced `unordered_set` for excluded_types and target_sizes with boolean arrays indexed directly by dictionary code / size value — O(1) with no hashing.

2. **Part pre-classification**: Built a `part_group[partkey]` array (2M × 4B = 8MB, fits in L3) that maps each qualifying partkey to a sequential group_id. This moves all part-attribute checking out of the hot 8M-row loop.

3. **Pair collection + sort**: Instead of inserting into per-group hash sets, the hot loop collects (group_id, suppkey) pairs into a flat vector. After scanning, pairs are sorted and distinct suppkeys counted in a single linear pass. Sorting ~1.1M 8-byte pairs is cache-friendly and fast (~50ms).

The hot loop body is now: one array lookup (part_group[pk]), one branch, one bitset check (complaint), one vector push_back — all sequential/cache-friendly operations.

## Risks

- Memory for part_group array (8MB) and pairs vector (~9MB) is modest relative to L3 (44MB).
- Sort-based distinct count produces identical results to hash set approach.
- This is single-threaded (no parallelism change), avoiding the regression seen in iteration 3 which attempted OpenMP parallelization with per-group bitsets.
