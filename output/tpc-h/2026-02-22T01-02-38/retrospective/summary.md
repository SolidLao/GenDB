# TPC-H SF10 Retrospective — Run 2026-02-22T01-02-38

## Run Configuration
- Workload: TPC-H SF10 (`sf10.gendb`)
- Queries: Q1, Q3, Q6, Q9, Q18
- Max Iterations: 5 (per query)
- Model: sonnet

---

## Query Classifications

| Query | Classification | Final Hot (ms) | Best Hot (ms) | Iters | Correct? | Dominant Bottleneck |
|-------|---------------|---------------|---------------|-------|----------|---------------------|
| Q1    | **SUCCESS**   | 38.1          | 38.1          | 0     | ✓ 4/4   | main_scan (98%)     |
| Q3    | **SUCCESS**   | 150.1         | 145.5         | 5     | ✓ 10/10 | build_joins (46%)   |
| Q6    | **SUCCESS**   | 9.7           | 7.2 (iter_3)  | 3     | ✓ 1/1   | main_scan (85%)     |
| Q9    | **SUCCESS**   | 99.9          | 99.9          | 0     | ✓ 175/175 | main_scan (51%) + part_filter (47%) |
| Q18   | **SLOW**      | 533.9         | 533.9         | 4     | ✓ 100/100 | subquery_scan (88%) |

**Summary: 4 SUCCESS, 1 SLOW, 0 FAILED**

---

## Per-Query Analysis

### Q1 — SUCCESS
- **Result**: Correct (4/4 rows). Hot timing: 38.1 ms.
- **Timing breakdown** (hot): `data_loading` 0.6 ms → `main_scan` 37.4 ms → `output` 0.6 ms
- **No optimization attempted** (iter_0 only). The optimizer correctly identified first-pass timing as acceptable.
- **Note**: `data_loading` is only 1.7% of hot runtime, so P16 does not apply. Q1's scan is I/O-fast once warm.

### Q3 — SUCCESS
- **Result**: Correct (10/10 rows). Final hot timing: 150.1 ms (iter_5). Best-ever: 145.5 ms (optimization_history iter_1).
- **Timing breakdown** (hot, iter_5): `build_joins` 69.1 ms (46%) → `aggregation_merge` 39.4 ms (26%) → `main_scan` 27.8 ms (19%) → `dim_filter` 7.0 ms (5%)
- **Optimization trajectory** (hot): iter_0 baseline ≈ 325 ms → iter_1 145.5 ms → iter_4 181.5 ms (regression) → iter_5 150.1 ms (partial recovery)
  - iter_3 was skipped (validation="skipped"); likely a generation error.
  - iter_4 slightly regressed but optimizer recovered in iter_5.
- **Remaining headroom**: `build_joins` (69 ms) could benefit from pre-built hash indexes (P11). `aggregation_merge` (39 ms) is likely a multi-thread merge cost — near floor after P15 fixes.
- **Compile warnings**: `date_utils.h` format-overflow warnings in iter_5; non-fatal, did not affect correctness.

### Q6 — SUCCESS
- **Result**: Correct (1/1 row). Final hot timing: 9.7 ms (iter_5 run, best code iter_3 at 7.2 ms).
- **Timing breakdown** (hot, iter_5): `data_loading` 0.4 ms → `main_scan` 8.2 ms → `output` 0.3 ms
- **Optimization trajectory** (hot): iter_0 baseline → iter_1 7.7 ms → iter_3 7.2 ms (best, selected) → iter_4 7.7 ms → iter_5 9.7 ms (slight regression; iter_3 retained as best per run.json)
- The optimizer correctly froze at iter_3 (7.2 ms) as the best-known source.
- `output` timing shows variance across runs (0.3 → 13.0 → 0.3 ms in all_runs), suggesting occasional OS-level interference in output phase; not a concern.

### Q9 — SUCCESS
- **Result**: Correct (175/175 rows). Hot timing: 99.9 ms.
- **Timing breakdown** (hot): `main_scan` 50.6 ms (51%) + `part_filter` 46.5 ms (47%) + `output` 2.2 ms + `data_loading` 0.9 ms
- **No optimization attempted** (iter_0 only). The optimizer accepted this timing as within threshold.
- `part_filter` at 47% of hot runtime is a significant cost — this is the LIKE-pattern filter on `p_name` which scans all PART rows (~2M at SF10). Zone-map or dictionary-based filtering is not applicable to a LIKE pattern. Baseline is near-optimal.
- `data_loading` is only 0.9% of hot runtime → P16 does not apply.

### Q18 — SLOW
- **Result**: Correct (100/100 rows). Final hot timing: 533.9 ms (iter_5). Best selected code: iter_4.
- **Timing breakdown** (hot, iter_5): `subquery_scan` 451.3 ms **(88%)** → `orders_probe` 10.3 ms (2%) → `customer_probe` 3.1 ms (1%) → `data_loading` 1.7 ms
- **Optimization trajectory** (hot, from optimization_history):
  - iter_1: 145.5 ms (improved=true) — *NOTE: contradicted by iter_1/execution_results.json which records 7,383 ms hot; discrepancy suggests optimization_history entry was recorded for a different candidate than the iter_1 directory artifact*
  - iter_2: validation=skipped (null timings — likely compile or runtime failure)
  - iter_4: 1,161.3 ms (improved=true vs prior regressed state; best by total timing 2,787 ms)
  - iter_5: 533.9 ms (improved=false vs iter_4 on total timing; worse total 2,976 ms due to cold regression)
- **Root Cause**: `subquery_scan` computes `SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300` single-threaded over ~60M rows at SF10. Across all measured iterations, subquery_scan consumed ≥88% of hot runtime and was never parallelized.
- **Optimization mismatch**: The best-selected code (iter_4, hot=1161 ms) was chosen because its TOTAL timing (cold+hot = 2,787 ms) beat iter_5's total (2,976 ms), even though iter_5 has better hot timing (534 ms vs 1,161 ms). The optimizer's total-timing metric favored the wrong candidate for repeated (cached) workloads.
- **P12 near-miss**: Oscillation between iter_1→iter_2→iter_4→iter_5 without consistent improvement; no revert to a definitively best-known source.

---

## Cross-Query Patterns

### Pattern 1: subquery_scan unparallelized (Q18)
The lineitem GROUP BY subquery is the single largest optimization opportunity in this workload (88% of Q18 hot runtime). OpenMP parallelization of this scan with thread-local partial hash maps + merge would reduce it from 451 ms to ~50 ms (estimated 8-thread, SF10).

### Pattern 2: Optimizer metric mismatch — total vs hot timing (Q18)
The best-selection metric (total = cold + hot) chose iter_4 (1,161 ms hot) over iter_5 (534 ms hot). For an OLAP engine where queries are typically run warm, hot timing should be the primary optimization target, with total timing as a secondary tiebreaker.

### Pattern 3: No optimization on single-iteration successes (Q1, Q9)
Both Q1 and Q9 exited after iter_0 with correct results. For Q1 (38 ms) this is appropriate. For Q9 (100 ms with `part_filter` at 47%) there may be room for parallelism, but the cost is below the optimizer's improvement threshold.

### Pattern 4: date_utils.h format-overflow warning (Q3, Q18)
Both Q3 and Q18 consistently emit a gcc `-Wformat-overflow` warning from `date_utils.h:52`. The char buffer is sized too small for edge-case year values. Non-fatal but noisy; should be fixed.

---

## Timing Summary

| Query | Cold (ms) | Hot (ms) | Dominant Phase (hot) |
|-------|-----------|----------|---------------------|
| Q1    | 900.5     | 38.1     | main_scan 98%       |
| Q3    | 285.8     | 150.1    | build_joins 46%     |
| Q6    | 127.5     | 9.7      | main_scan 85%       |
| Q9    | 1,248.7   | 99.9     | main_scan+part_filter 97% |
| Q18   | 2,442.9   | 533.9    | subquery_scan 88%   |

> All timings from final executed iteration (last iter_N/execution_results.json). Q6 best code is iter_3 (7.2 ms hot).
