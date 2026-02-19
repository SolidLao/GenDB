# Retrospective Summary — TPC-H Run 2026-02-19T09-21-47

## Overall Results

| Query | Classification | Final Timing (ms) | Iters | Root Issues |
|-------|---------------|-------------------|-------|-------------|
| Q1    | SUCCESS       | 47.53             | 3     | None (clean convergence) |
| Q3    | SUCCESS*      | 248.54            | 6     | 4× timeout (C9), then P6 output overhead |
| Q6    | SUCCESS       | 20.15             | 1     | None (first-shot pass) |
| Q9    | SUCCESS       | 137.98            | 1     | None (first-shot pass) |
| Q18   | SUCCESS       | 232.16            | 6     | P8 subquery bottleneck in dim_filter |

**Total: 5 SUCCESS, 0 SLOW, 0 FAILED**

\* Q3 passed on the final iteration but had 4 consecutive 300 s timeout crashes before the fix.

---

## Per-Query Analysis

### Q1 — Pricing Summary (SUCCESS, 47.53 ms)
- Clean 3-iteration convergence: 222 ms → 111 ms → 47 ms.
- Bottleneck was `main_scan` (~47 ms); no correctness issues.
- No warnings in final iterations.

### Q3 — Shipping Priority (SUCCESS*, 248.54 ms)
**Early failures (iter_0 – iter_3):** All four runs exited with `code null` after exactly 300 s.
Signature: compiled fine, infinite loop at runtime → classic **C9** (open-addressing hash table
at 100% load factor). The optimizer needed 4 full timeout cycles (~20 min wasted) before
it resized the table correctly in iter_5.

**Final iteration timing breakdown:**
```
dim_filter:   13.00 ms
build_joins: 111.44 ms   ← large hash join, now correctly sized
main_scan:   120.94 ms
output:      108.72 ms   ← anomalously high for LIMIT 10 (P6 pattern)
total:       357.26 ms   (wall) / 248.54 ms (recorded)
```
`output` at 108 ms for a LIMIT 10 query is the residual issue — strong indicator of a full
`std::sort` over a large intermediate vector instead of `std::partial_sort` (P6).

### Q6 — Forecasting Revenue Change (SUCCESS, 20.15 ms)
- First-shot pass. All phases trivially fast. Zero issues.

### Q9 — Product Type Profit Measure (SUCCESS, 137.98 ms)
- First-shot pass.
- `dim_filter` (71 ms) > `main_scan` (65 ms) — reasonable for a 5-table join with
  LIKE-style part filter; well balanced.

### Q18 — Large Volume Customer (SUCCESS, 232.16 ms)
**Trajectory:** 1066 ms → 1417 ms (iter_1 **regressed**) → 328 ms → 232 ms.

`dim_filter` dominated throughout:

| Iter | dim_filter | % of total |
|------|-----------|-----------|
| 0    | 1054 ms   | 99%       |
| 1    | 1406 ms   | 99% (worse) |
| 4    | 318 ms    | 97%       |
| 5    | 222 ms    | 96%       |

The subquery `WHERE o_orderkey IN (SELECT o_orderkey FROM lineitem GROUP BY o_orderkey
HAVING SUM(l_quantity) > 300)` drove the entire cost — a textbook **P8** pattern
(lineitem scanned once for the subquery aggregation, then again for the main join).
The optimizer eventually collapsed this into a single pass, but the iter_1 regression
suggests the optimizer tried an intermediate strategy that was worse (possibly
`std::unordered_map` → P1 on a large build side).

`dim_filter` still accounts for 96% of runtime at 232 ms; further work could reduce
this with a better-sized open-addressing hash map for the subquery aggregation.

---

## Recurring Patterns

### 1. C9 Dominates Timeout Failures
Q3's 4× timeout (300 s × 4 = ~20 min wasted) was entirely due to hash table
infinite-loop. C9 is already in the experience base, but the DBA Stage A check
was insufficient to prevent 4 bad iterations. Detection needs to be more aggressive:
*any* query with a multi-table join that touches lineitem (6 M rows at SF=1) must
pre-verify every open-addressing hash table capacity.

### 2. Output-Phase Overhead for ORDER BY + LIMIT
Q3's `output` phase at 108 ms (43% of scan time) is disproportionate for 10 result
rows. P6 (full sort vs. partial_sort) exists in the experience base but was not
caught pre-generation.

### 3. dim_filter Regression Before Improvement (Q18 iter_1)
The optimizer made Q18 **slower** on iter_1 before recovering. This suggests the
optimization loop does not guard against regressions in the `dim_filter` phase
when replacing a subquery strategy.

### 4. `date_utils.h` sprintf Overflow Warning (Recurring)
Compile warnings about sprintf buffer overflow appeared in Q3 and Q18. Not a
correctness failure in this run, but it is a latent buffer-overflow risk that
produces noise in every compilation and could cause silent corruption on
longer date strings.

---

## Wasted Compute Summary

| Cause | Queries Affected | Wasted Iterations | Est. Wasted Time |
|-------|-----------------|-------------------|-----------------|
| C9 hash table infinite loop | Q3 | 4 | ~20 min |
| P8 subquery double-scan | Q18 | 2 (iter_1 regress + iter_2/3 skipped) | ~5 min |
| P6 output sort | Q3 | residual overhead | ~0.1 s/run |
