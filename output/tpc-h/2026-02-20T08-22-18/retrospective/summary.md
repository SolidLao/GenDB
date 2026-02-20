# TPC-H Retrospective — 2026-02-20T08-22-18

**Workload:** TPC-H · **Scale Factor:** 10 · **Model:** haiku · **Queries:** Q1, Q3, Q6, Q9, Q18

---

## Per-Query Classification

| Query | Status  | Initial ms | Best ms | Speedup | Best Iter | Oscillations |
|-------|---------|-----------|---------|---------|-----------|--------------|
| Q1    | SUCCESS | 216        | 87      | 2.5×    | iter_5    | Minor (iter_3: +5%, iter_4: +9%) |
| Q3    | SUCCESS | 328        | 91      | 3.6×    | iter_4    | **Severe** (iter_3: +66%, iter_5: +105%) |
| Q6    | SUCCESS | 134        | 21      | 6.3×    | iter_1    | None |
| Q9    | SUCCESS | 244        | 60      | 4.1×    | iter_5    | None (monotonic) |
| Q18   | SUCCESS | 182        | 72      | 2.5×    | iter_4    | Minor (iter_1: +10%, iter_2: +3%) |

All 5 queries produced **correct results** in every iteration. No FAILED queries.

---

## Detailed Findings

### Q1 — SUCCESS (87 ms)
- **Bottleneck:** `main_scan` plateaued at ~39 ms across iter_1–iter_4 before breaking to 32 ms at iter_5.
- **mmap_open cold-start:** iter_0 paid 114 ms for mmap (cold OS page cache); iter_1 dropped to 0.6 ms (warm). This inflated the iter_0 timing by ~117 ms and is not a real cost.
- **Oscillation:** iter_3 (96.6 ms) and iter_4 (99.7 ms) both regressed from the iter_2 best (91.6 ms) before recovering at iter_5. Regressions were <20%, so P12 threshold was not breached, but 2 wasted cycles occurred.
- **Phase timings (best — iter_5):** mmap_open 0.3 ms · zone_map 0.06 ms · main_scan 31.7 ms · merge 0.0 ms · output 0.2 ms · **total 87 ms**

### Q3 — SUCCESS (91 ms) ⚠️ Severe Oscillation
- **Bottleneck:** `build_joins` dominated and oscillated wildly: 136 → 77 → 39 → **119** → 35 → **110 ms**. This pattern indicates the optimizer alternated between two join-build strategies (e.g., different hash table sizes or join ordering), with one strategy 3× more expensive than the other.
- **P12 violated twice:**
  - iter_3: 178 ms vs. best 107 ms → **+66% regression** (exceeds 20% threshold)
  - iter_5: 186 ms vs. best 91 ms → **+105% regression** (exceeds 20% threshold)
- Both regression iterations were correctly marked `improved: false` in the history, but they were still emitted/compiled/run — one wasted compile-run cycle each.
- **Phase timings (best — iter_4):** dim_filter 10.1 ms · build_joins 34.8 ms · main_scan 19.5 ms · aggregation_merge 14.3 ms · sort_topk 2.7 ms · **total 91 ms**

### Q6 — SUCCESS (21 ms) ✅ Best Convergence
- **Fast convergence in 1 optimization step:** 6.3× speedup.
- **Root cause of iter_0 slowness:** `build_joins` consumed 82.7 ms despite Q6 being a pure filter-aggregate on LINEITEM (no joins). iter_0 apparently loaded or built an unnecessary join structure (likely a pre-built index not needed for this query). iter_1 eliminated it, reducing build_joins to 1.9 ms.
- **Phase timings (best — iter_1):** dim_filter 0.03 ms · build_joins 1.9 ms · main_scan 11.8 ms · output 0.1 ms · **total 21 ms**

### Q9 — SUCCESS (60 ms) ✅ Ideal Monotonic Improvement
- **Every iteration improved.** This is the only query with zero oscillation across 5 optimization steps.
- **Primary driver:** `build_joins` reduced from 96.5 ms → 62.9 → 52.7 → 60.3 → 46.9 → **24.4 ms** (4× reduction). `part_scan` also improved from 18.6 → 7.4 ms via zone-map or selective scan optimization.
- `main_scan` remained stable at ~26–27 ms throughout, indicating the LINEITEM scan is near the memory bandwidth floor.
- **Phase timings (best — iter_5):** dim_filter 0.6 ms · part_scan 7.4 ms · build_joins 24.4 ms · main_scan 26.2 ms · output 0.9 ms · **total 61 ms**

### Q18 — SUCCESS (72 ms)
- **Bottleneck:** `dim_filter` was dominant (>50% of total time) in iter_0–iter_2 at ~100 ms. The sub-query aggregation over LINEITEM to find qualifying orders was the slow path.
- **Oscillation (minor):** iter_1 (200 ms) and iter_2 (187 ms) both regressed from iter_0 (182 ms) before iter_3 broke through by reducing dim_filter from ~100 ms to 38.7 ms.
- **iter_4 best:** Despite dim_filter rising slightly to 48.9 ms, main_scan grew to 22.7 ms, but total was best at 72 ms — indicating a structural rebalancing that cut overall wall time.
- Compiler warnings from `date_utils.h` (sprintf) appeared in all iterations but did not affect correctness.
- **Phase timings (best — iter_4):** dim_filter 48.9 ms · main_scan 22.7 ms · build_joins 0.2 ms · sort_topk 0.1 ms · **total 73 ms**

---

## Cross-Query Patterns

### Pattern 1: P12 Oscillation — Q3 (severe), Q1 & Q18 (minor)
Three of five queries exhibited optimizer oscillation. Q3's two regressions each exceeded the 20% P12 threshold. Root cause in Q3: the optimizer alternated between two join-build strategies producing 3× different build_joins times. The P12 guard (revert to best if regression >20%) is in the experience base but was not enforced strongly enough.

### Pattern 2: Unnecessary Join Build for Single-Table Queries (Q6)
Q6 iter_0 spent 82.7 ms in `build_joins` despite requiring no joins. The code generator produced a join-scaffolding for a pure aggregation query. Eliminated in 1 step.

### Pattern 3: dim_filter Dominance in Sub-Query Aggregation (Q18)
When a query requires aggregating a large table as a filter step (e.g., `HAVING SUM(...) > N` to qualify orders), the dim_filter phase can consume >50% of total time. Q18 needed 3 sub-optimal iterations before the optimizer found an efficient approach.

### Pattern 4: mmap Cold-Start Inflation (Q1, Q9)
First iterations show inflated total times due to cold OS page cache (Q1 iter_0: +114 ms for mmap_open; Q9 iter_0 shows 244 ms total vs. 112 ms warm). The optimizer correctly identifies warm-run timing from iteration 1 onward.

### Pattern 5: Stable main_scan Floor
Across Q1, Q9, Q18, the `main_scan` phase stabilizes around a floor (Q1: ~32–40 ms, Q9: ~26 ms) consistent with sequential LINEITEM read bandwidth at SF10. Further speedups come from reducing join-build and filter costs, not scan costs.

---

## Summary Statistics
- **Queries total:** 5
- **SUCCESS:** 5
- **SLOW (correct but slow):** 0
- **FAILED (incorrect):** 0
- **P12 violations (>20% regression):** 2 (both in Q3)
- **Wasted compile-run cycles from oscillation:** ~4 (Q3×2, Q1×2, Q18×2)
- **Average speedup from initial to best:** 3.8×
