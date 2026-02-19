# TPC-H Run Retrospective
**Run:** 2026-02-18T17-49-49
**Workload:** TPC-H SF10
**Queries:** Q1, Q3, Q6, Q9, Q18
**Results:** 2 SUCCESS · 1 SLOW · 0 FAILED (terminal) · 2 NOT EVALUATED

---

## Per-Query Classification

### Q1 — SUCCESS ✅
- **Final timing:** 108.22 ms (iter_10, best across 11 iterations)
- **Validation:** PASS throughout all 11 iterations (100% stability)
- **Timing breakdown (final):** dim_filter 0.54 ms · build_joins 0.06 ms · main_scan 40.19 ms · aggregation_merge 0.01 ms · output 0.89 ms
- **Dominant phase:** `main_scan` (40.19 ms / 37% of wall time). The ~70 ms overhead is startup/mmap cost.
- **Optimization trajectory:** Improved from 135.91 ms (iter_0) → 108.22 ms (iter_10). Zone-map usage was already in place from iter_0.
- **Assessment:** Well-optimized. No correctness issues encountered in any iteration.

---

### Q3 — SLOW ⚠️ (correct at exit, unstable during optimization)
- **Final timing:** 357.34 ms (iter_10, best accepted = 360.13 ms at iter_5)
- **Validation:** PASS in iters 0–5, 8, 10; **FAIL in iters 6, 7, 9** (3/11 = 27% failure rate)
- **Timing breakdown (final):** dim_filter 10.23 ms · build_joins 135.65 ms · main_scan 153.34 ms · aggregation_merge 42.87 ms · sort_topk 0.49 ms · output 0.13 ms
- **Root cause of failures (iters 6, 7, 9):** Custom lineitem zone-map skip using binary search on `l_shipdate` blocks assumed lineitem was sorted by `l_shipdate`. Lineitem is **not** sorted by `l_shipdate`; it is sorted by `l_orderkey`. The binary search yielded `li_start_row > 0` and skipped the first N rows of lineitem entirely, dropping all qualifying rows in those blocks. This produced a completely different top-10 result set (all wrong `l_orderkey`, `o_orderdate`, `revenue` values). The failure pattern was identical across iters 6, 7, 9, confirming the same root cause persisted across re-tries.
- **Dominant bottleneck:** `build_joins` (135.65 ms) + `main_scan` (153.34 ms) together account for 81% of time. The join builds a hash table over ~1.44M filtered orders rows. The merge phase (42.87 ms) is expensive due to merging 64 thread-local aggregation maps.
- **Assessment:** Passes but slow. At 357 ms, over budget for a TPC-H Q3 at SF10. Optimization attempts that tried a zone-map shortcut on lineitem regressed correctness. The correct approach is to use the ZoneMapIndex utility (which uses actual block min/max values) rather than a manual binary search that assumes sorted data.

---

### Q6 — SUCCESS ✅
- **Final timing:** 83.15 ms (iter_10; best accepted = 76.03 ms at iter_5)
- **Validation:** PASS in all iterations except **iter_3** (1/11 = 9% failure rate)
- **Timing breakdown (final):** zone_map_prune 0.03 ms · main_scan 23.78 ms · output 0.11 ms · total 83.26 ms
- **Root cause of failure (iter_3):** Revenue mismatch by 0.266% (~$3.27M off on a ~$1.23B total). The failure was `actual=1226841856.13` vs `expected=1230113636.01`. This pattern matches a boundary filter error — likely the optimizer adjusted a date threshold or BETWEEN bound slightly, admitting or excluding a small fraction of rows. Specific error: off by ~$3.27M out of $1.23B (0.266%), consistent with a 1-day shift in the `l_shipdate < '1995-01-01'` upper bound (date epoch off by ±1) or a marginal `l_discount` or `l_quantity` threshold change.
- **Optimization trajectory:** Started at 105.45 ms → improved to 76.03 ms (iter_5) → final accepted 83.15 ms. Zone-map pruning (`zone_map_prune: 0.03 ms`) was highly effective, reducing the main scan to 23.78 ms.
- **Assessment:** Strong result. Zone-map exploitation was exemplary (0.03 ms prune overhead). The single correctness regression was quickly recovered.

---

### Q9 — NOT EVALUATED ❌ (no execution_results.json)
- **Status:** Binary compiled and ran successfully (results CSV exists at `iter_0/results/Q9.csv`), but `execution_results.json` was never written — the pipeline appears to have terminated or skipped the evaluation step. No optimization iterations were run (empty `optimization_history.json`).
- **Output CSV check:** CSV has 175 rows (25 nations × 7 years), well-formed format (`nation,o_year,sum_profit`). Sample: `ALGERIA,1998,19598.19`.
- **Code analysis:** The implementation reads `part.tbl` directly for LIKE '%green%' filtering because the p_name binary column uses a non-injective hash encoding (multiple names can share same code). This is correct behavior per C6 experience entry.
- **Amount formula:** Uses two accumulator arrays (`tl_agg_a` for `ep*(100-disc)`, `tl_agg_b` for `supplycost*qty`), then computes `(a - b*100) / 10000.0`. This correctly handles the asymmetric scale factors per C17: term1 is scale² (scale 10000), term2 is scale¹ (scale 100), so multiplying term2 by 100 to normalize to scale 10000 before subtracting is correct.
- **Nation output:** Uses hardcoded `TPCH_NATION_NAMES` rather than loading a dictionary — correct since TPC-H nation names are fixed.
- **Assessment:** Implementation appears correct; missing evaluation is a pipeline issue, not a code issue. Results cannot be formally classified without validation output.

---

### Q18 — NOT EVALUATED ❌ (no execution_results.json)
- **Status:** Binary compiled and ran successfully (results CSV exists at `iter_0/results/Q18.csv`), but `execution_results.json` was never written. No optimization iterations were run (empty `optimization_history.json`).
- **Output CSV check:** Has 100 rows, well-formed (`c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty`). Top row: `Customer#001287812,1287812,42290181,1997-11-26,558289.17,318.00`. `sum_qty` values are all `> 300` (range 302–318), consistent with `HAVING SUM(l_quantity) > 300`.
- **Threshold correctness:** Uses `val > 300` (not 30000) — correct per C8/C16 (l_quantity scale_factor=1).
- **c_name generation:** `sprintf("Customer#%09d", c_custkey)` — correct TPC-H standard format; avoids loading c_name dictionary.
- **o_totalprice output:** Formatted as `tp_whole.tp_frac` (two decimal places) after dividing by 100 — correct for scale_factor=100.
- **Assessment:** Implementation looks correct; missing evaluation is a pipeline issue. Results cannot be formally classified without validation output.

---

## Cross-Query Patterns

### 1. Incorrect Zone-Map Usage on Non-Sorted Columns (Q3 failures)
Iters 6, 7, 9 all failed because a custom block-skip binary search assumed lineitem was sorted by `l_shipdate`. The correct ZoneMapIndex utility in `mmap_utils.h` uses actual stored block min/max values and does not require the data to be globally sorted. Manual "binary search on last-element-of-block" is invalid unless the column is physically sorted.

### 2. Q9/Q18 Pipeline Truncation
Both Q9 and Q18 produced result CSVs but no `execution_results.json`. This suggests the orchestrator exited before the evaluation step (e.g., timeout at first generation, or a pipeline error in the evaluator subprocess). These queries should be re-run with evaluation enabled.

### 3. Optimization Instability Under Correctness Pressure (Q3)
Q3 had a 27% iteration failure rate during optimization. Architecturally complex refactors (partitioned HTs, custom zone-map skips) introduced correctness regressions that were not caught until validation. The optimizer should preserve at minimum a "correctness checkpoint" — once passing, only accept changes that also pass validation.

### 4. date_utils.h Compiler Warning (Recurring)
All Q3 iterations emit a compiler warning from `date_utils.h:52`: `'%02d' directive writing between 2 and 4 bytes into a region of size between 0 and 4`. This is a false positive (the buffer is always large enough for valid dates), but it appears in every Q3 compile and may confuse automated error detection. The buffer in `epoch_days_to_date_str` should be sized explicitly to silence this warning.

### 5. Timing Overhead Structure
Q1: ~70 ms startup overhead beyond actual compute (109 ms total vs 40 ms main_scan).
Q6: ~60 ms overhead (83 ms total vs 24 ms main_scan).
Q3: much smaller relative overhead — build/scan dominate at 289 ms of 357 ms total.
The startup cost (mmap initialization, OS page faults) is significant at SF10 and should be budgeted in performance targets.

---

## Summary Table

| Query | Classification | Final Time | Iterations | Val Failures | Key Issue |
|-------|---------------|-----------|------------|--------------|-----------|
| Q1    | SUCCESS       | 108.22 ms | 11         | 0/11         | None      |
| Q3    | SLOW          | 357.34 ms | 11         | 3/11         | Incorrect zone-map skip on unsorted lineitem l_shipdate |
| Q6    | SUCCESS       | 83.15 ms  | 11         | 1/11         | Single boundary regression in optimization (recovered) |
| Q9    | NOT EVALUATED | —         | 0          | N/A          | Pipeline dropped evaluation step |
| Q18   | NOT EVALUATED | —         | 0          | N/A          | Pipeline dropped evaluation step |
