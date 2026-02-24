# Retrospective Summary — SEC EDGAR Run 2026-02-23T23-33-40

## Overall Result: 5 SUCCESS, 1 SLOW, 0 FAILED (6 queries total)

All 6 queries passed validation every accepted iteration — zero correctness failures this run.

---

## Per-Query Classification

| Query | Classification | Initial ms | Final ms | Improvement | Iters | Dominant Bottleneck |
|-------|----------------|-----------|----------|-------------|-------|---------------------|
| Q1    | **SUCCESS**    | 146.1     | 33.8     | 77%         | 1     | —                   |
| Q2    | **SUCCESS**    | 176.2     | 101.9    | 42%         | 2     | main_scan (47ms)    |
| Q3    | **SUCCESS**    | 126.2     | 79.1     | 37%         | 1     | main_scan (52ms)    |
| Q4    | **SUCCESS**    | 258.1     | 120.9    | 53%         | 2     | main_scan (49ms), data_loading (44ms) |
| Q6    | **SLOW**       | 1046.1    | 822.4    | 21%         | 4     | data_loading (258ms, 32%) + agg_merge (261ms, 32%) + build_joins (121ms, 15%) |
| Q24   | **SUCCESS**    | 255.6     | 152.6    | 40%         | 1     | main_scan (62ms), agg_merge (40ms) |

---

## Q1 — SUCCESS (33.8ms)
Massive improvement iter_0→iter_1 (146ms→34ms). Final timing: data_loading=5.9ms, main_scan=8.2ms, agg_merge=0.7ms. All phases lean. Optimization converged in a single step.

## Q2 — SUCCESS (101.9ms)
3-table join with sort/topK. Improved in 2 accepted steps (iter_1→iter_2→iter_5). Final breakdown: main_scan=46.9ms, agg_merge=28.6ms, data_loading=15ms. Aggregation merge is still the second-largest cost but acceptable. build_joins=1.85ms confirms pre-built index usage (P11).

## Q3 — SUCCESS (79.1ms)
Single accepted iteration. main_scan=52.3ms dominates (66% of total). agg_merge=19.7ms. data_loading=0.6ms (near-zero). Very efficient I/O; scan bottleneck is expected for a full num-table pass.

## Q4 — SUCCESS (120.9ms; best at iter_3)
Largest relative improvement: 258ms→120ms (53%). data_loading dropped from 119ms to 44ms — zone-map prefetch optimization effective. main_scan=48.7ms, agg_merge=10ms. Final iter_5 (125ms) regressed slightly, confirming iter_3 as best.

## Q6 — SLOW (822ms)
Most complex query (3-table join: num×sub×pre; 34M scan rows; 300K output groups; two pre-built mmap indexes). Improved 21% (1046ms→822ms) but stalled at 822ms. Four accepted iterations; iter_5 regressed (843ms).

**Root cause — two equal bottlenecks each ~260ms:**
1. **data_loading (258ms, 32%)**: Loads num columns (39M rows), sub_adsh_index, AND pre_join_index (1.73M entries, multi-value payload). Sequential mmap of two large index files under hot-path loading dominates.
2. **aggregation_merge (261ms, 32%)**: Partitioned merge across 64 threads × 524288-slot maps. T×G = 64 × 300K ≈ 19M slot scans to redistribute+sum. Even with partitioned merge (P25) and correctly sized AGG_CAP (P23), 19M random-access slot scans cost ~260ms.
3. **build_joins (121ms, 15%)**: Two pre-built mmap indexes (zero runtime build cost per P11). Reported under build_joins but likely reflects madvise/fault time allocated to this phase.

**Improvement stalled because**: P23 + P25 (sized AGG_CAP to 524288, used partitioned merge) were already applied in iter_1's plan. Remaining cost is fundamental to the data volume: 34M row scan, 300K groups, 64-thread merge.

**Next-run levers**:
- Concurrent madvise on multiple index files in parallel (see P27 in proposals)
- Reduce column set loaded during data_loading (only load columns needed for filters/output)
- Two-level aggregation: reduce 300K groups early via coarser key, then refine — but complex to implement

## Q24 — SUCCESS (152.6ms)
40% improvement in single iteration (255ms→152ms). data_loading=42ms, main_scan=62ms, agg_merge=40ms. Balanced profile; no single dominant bottleneck.

---

## Cross-Query Patterns

1. **data_loading optimization was effective**: Q4 (119ms→44ms), Q24 (91ms→42ms), Q2 (61ms→15ms) — zone-map-guided column prefetch consistently cut data_loading 50-75%.
2. **aggregation_merge diverges by query complexity**: Q1 (0.7ms), Q2 (28ms), Q3 (20ms), Q4 (10ms) all fast. Q24 (40ms) borderline. Q6 (261ms) pathological due to 300K group count at 64-thread scale.
3. **All correctness rules active**: C29 (int64_t cents for num.value), C15/C30 (all GROUP BY keys), C31 (CSV double-quoting), C32 (index header scope), C33 (stable tiebreaker) — all referenced in Q6 iter_1 plan, confirming these rules are being enforced by code generation.
4. **No hash table sentinel or bounded-probe bugs**: Zero correctness failures from C20 or C24, indicating these rules are well-internalized.
