# Iteration 3: Parallelize and optimize Q16 in fused_scan_partsupp

## Quantitative Diagnosis

The profile shows `fused_scan_partsupp` at 630ms — 34% of the 1863ms dispatcher total and the single largest bottleneck. This region contains Q16's scan of 8M partsupp rows. The scan was entirely single-threaded on a 64-core machine.

The data structures used were problematic:
- `unordered_set<int>` for target_sizes (8 elements) and excluded_types — hash probes 8M times each for small sets
- `unordered_map<Q16Key, unordered_set<int32_t>>` — nested hash containers with heavy heap allocation per insert. Each qualifying row triggers an `insert()` into an `unordered_set`, causing pointer chasing and cache misses.

## Why Category B+D

This is a combined parallelism (D) and algorithm (B) fix:
1. **D — No parallelism**: 8M rows processed by a single core while 63 cores sit idle. With 64-way parallelism, the scan portion drops from O(8M) to O(125K) per thread.
2. **B — Wrong data structures**: `unordered_set<int32_t>` for distinct supplier counting is cache-hostile (heap-allocated nodes with pointer chasing). Supplier keys are bounded [1, 100000], so a bitset (100K bits = 12.5KB) provides O(1) insert and merge via bitwise OR, with excellent cache behavior.

## Changes Made

1. **Pre-computed filter arrays**: Replaced `unordered_set<int>` for target_sizes with a bool array[51] and excluded_types with a vector<uint8_t> — both O(1) lookup.
2. **Pre-classified parts**: Built a `part_qualifies[2M]` array upfront so the hot loop only checks one byte per partkey instead of three hash lookups.
3. **OpenMP parallelization**: Split the 8M row scan across all threads with `schedule(static)`. Each thread maintains a local `unordered_map<uint32_t, vector<uint64_t>>` mapping composite keys to supplier bitsets.
4. **Bitset-based distinct counting**: Each group's supplier set is a 12.5KB bitset (1563 uint64_t words). Thread-local bitsets are merged via bitwise OR, then `__builtin_popcountll` counts distinct suppliers.
5. **Sequential merge**: Thread maps are merged sequentially (trivial cost — few thousand groups, each merge is 1563 OR operations).

## Expected Impact

The 630ms single-threaded scan should drop to ~15-30ms with 64-way parallelism and cheaper per-row operations. This represents a ~600ms savings on a 2037ms batch total — roughly 30% improvement.

## Risks

- Thread-local map allocation could cause NUMA effects, but with `schedule(static)` and small maps this should be minimal.
- The composite key encoding assumes brand/type/size values fit their allocated ranges — safe for TPC-H data.
