# TPC-H Retrospective — 2026-02-22T08-30-57

## Overall Results

| Query | Final Class | Iters | Start (ms) | Final (ms) | Speedup | Compile Failures |
|-------|-------------|-------|-----------|-----------|---------|-----------------|
| Q1    | SUCCESS     | 1     | 147.0     | 40.2      | 3.7×    | 0               |
| Q6    | SUCCESS     | 0     | 17.4      | 17.4      | 1.0×    | 0               |
| Q3    | SUCCESS     | 5     | 2012.2    | 79.0      | 25.5×   | 0               |
| Q9    | SUCCESS     | 4†    | 229.6     | 82.3      | 2.8×    | 2 (iter_2, iter_5) |
| Q18   | SUCCESS     | 3     | 562.2     | 132.0     | 4.3×    | 0               |

†Q9 had 6 iterations total; 2 compile-failed and were skipped by optimizer.

**Summary**: 5/5 queries ended in SUCCESS. No incorrect results. 2 compile failures occurred in Q9 during optimization but were recovered automatically.

---

## Per-Query Analysis

### Q1 — SUCCESS (3.7×)
- **iter_0 → iter_1**: 147ms → 40ms. data_loading dropped 98ms → 0.13ms (madvise/zone-map prefetch for hot run).
- main_scan stable at ~37ms across both iterations — fully parallelized already.
- **Final bottleneck**: main_scan (37ms, 93% of hot runtime). Zone-map filtering possible if lineitem has zone maps on l_shipdate.

### Q6 — SUCCESS (baseline, no optimizations)
- iter_0 only: 17ms total, main_scan=5ms. Already fast; optimizer correctly recognized no optimizations needed.
- No bottleneck — well-optimized initial generation.

### Q3 — SUCCESS (25.5×)
- **iter_0**: 2012ms. Dominant bottlenecks: main_scan=1769ms, aggregation_merge=411ms. Classic cold std::unordered_map merge + serial scan.
- **iter_1**: 256ms. aggregation_merge dropped to 0 (thread-local merge with atomics adopted — P15 pattern). main_scan=52ms (parallelism applied).
- **iter_2**: 127ms. data_loading 79ms → 1ms (OS page cache hot; madvise tuned). build_joins 70ms → 34ms.
- **iter_3**: 95ms. build_joins 34ms → 18ms (better hash table sizing or pre-built index use).
- **iter_4**: 79ms (best). dim_filter 12ms → 3ms, build_joins 18ms → 8ms.
- **iter_5**: 81ms (marginal regression, optimizer correctly rejected).
- **Root bottleneck eliminated**: aggregation_merge (411ms → 0ms) by switching from sequential std::unordered_map merge to thread-local aggregation.

### Q9 — SUCCESS (2.8×, 2 compile failures)
- **iter_0**: 230ms. data_loading=144ms (63%), dim_filter=29ms, main_scan=48ms.
- **iter_1**: 207ms. dim_filter dropped 29ms → 5ms (partkey bitset precomputed in parallel). build_order_year phase introduced (47ms — direct year-lookup array). data_loading=118ms still dominant.
- **iter_2**: COMPILE FAIL. Root cause: partial pre-built index refactoring — usage code referred to `PSSlot`, `ps_mask`, `ps_ht_size`, `ps_positions`, `ps_hash()` which were never declared. Meanwhile `PSLocalSlot` + `ps_hash()` were defined but the probe loop used mismatched names. `ps_local_ht` declared but unused.
- **iter_3**: 249ms (regression vs iter_1). build_green_ps=42ms added overhead without net gain. Optimizer correctly noted regression.
- **iter_4**: 82ms (best, 2.8× from iter_0). data_loading=0.23ms — all I/O pushed into background via madvise + deferred loads. dim_filter=15ms, main_scan=60ms.
- **iter_5**: COMPILE FAIL. Root cause: optimizer attempted pre-built mmap index (partsupp_keys_hash + orders_orderkey_hash) but: (a) mmap calls for `ps_index_raw`/`ord_index_raw` were never added to data_loading, (b) `PSSlot`/`OrdSlot` structs not defined, (c) used `ps_hash()` but only `cps_hash()` was defined. Classic partial-refactor disconnect.

### Q18 — SUCCESS (4.3×)
- **iter_0**: 562ms. build_joins=385ms (69%) dominant — runtime-built hash table over large lineitem/orders join.
- **iter_1**: 289ms. build_joins 385ms → 169ms. data_loading 110ms → 47ms.
- **iter_2**: 217ms. build_joins 169ms → 114ms. Continued hash table optimization.
- **iter_3**: 132ms (best). build_joins 114ms → 74ms. data_loading 47ms → 38ms.
- **Remaining bottleneck**: build_joins=74ms (56% of runtime). Pre-built hash index for lineitem or orders orderkey could eliminate this phase entirely (P11 pattern, 100-500ms impact stated).

---

## Key Patterns

### Compile Failure Pattern (Q9 iter_2 and iter_5)
Both failures were **partial index-refactoring disconnects**: the optimizer introduced new struct/variable/function names in declarations but left probe-loop usage sites referencing the old names (or vice versa). Specifically:
- Declared `PSLocalSlot` + `ps_hash()` but probe loop used `PSSlot` + `ps_mask` + `ps_ht_size` + `ps_positions`
- Declared `PSCompact` + `cps_hash()` but probe loop and index-parse section used `ps_hash()`, `PSSlot`, `OrdSlot`, `ps_index_raw`, `ord_index_raw` — none of which were declared

**Rule**: When adopting a pre-built hash index, all 5 elements must be updated atomically: struct definition, mmap call, hash function, mask/size variables, probe loop.

### Performance: aggregation_merge is a high-risk bottleneck
Q3 iter_0 had 411ms in aggregation_merge — this dwarfed all other phases. The fix (thread-local merge) reduced it to ~0ms. This pattern should be proactively detected in future generation.

### Performance: data_loading dominance signals zone-map/madvise opportunity
Both Q9 (143ms = 63%) and Q3 iter_0 (85ms) had data_loading as the #1 or #2 hot cost. Zone-map-guided prefetch or madvise tuning consistently reduces this by 50%+.
