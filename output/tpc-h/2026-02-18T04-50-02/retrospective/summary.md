# TPC-H Retrospective — Run 2026-02-18T04-50-02

**Workload:** TPC-H · Scale Factor 10 · Model: haiku · Max iterations: 10
**Queries:** Q1, Q3, Q6, Q9, Q18
**Overall:** 2 SUCCESS · 1 SLOW · 1 FAILED (never recovered) · 1 SLOW (corrected, but still slow)

---

## Per-Query Classification

### Q1 — Pricing Summary Report
**Classification: SLOW** (correct + fast best iteration, but optimization regressed)

| Iter | Timing (ms) | Validation | Notes |
|------|------------|------------|-------|
| 0    | 121        | pass       | Baseline — correct |
| 1    | 136        | pass       | No improvement |
| 2    | 134        | pass       | No improvement |
| 3    | 4842       | **FAIL**   | Massive regression — scan ~4.8 s, all 4 rows wrong across 5 cols |
| 4    | skipped    | skipped    | Compile/skip after regression |
| 5    | 116        | pass       | Recovered — best so far |
| 6    | 254        | **FAIL**   | Row 2 wrong across 5 cols (+2.89%) |
| 7    | **112**    | pass       | **Best kept** (bestCppPath) |
| 8    | 130        | **FAIL**   | Row 2 wrong across 5 cols (+2.89%) — same as iter_6 |
| 9    | 104        | **FAIL**   | Row 2 wrong across 5 cols (+2.89%) — faster but incorrect |
| 10   | 133        | **FAIL**   | Row 2 wrong across 5 cols (+2.89%) — same failure |

**Final best: iter_7 @ 112 ms (correct)**

**Root cause of recurring failure (iters 6, 8, 9, 10):**
The constant `~2.89%` excess on row 2 across all five aggregated columns (sum_qty, sum_base_price,
sum_disc_price, sum_charge, count_order) is consistent and uniform. The relative diff of 2.894%
applied to ~29M rows suggests the date threshold `l_shipdate <= threshold` was computed with a
slightly wrong cutoff epoch value (too large by approximately 843K rows / 29M ≈ 2.9%). This is a
C7-style **date arithmetic error**: the optimizer appears to have used `1998-12-01 - 90` via a
wrong formula (e.g., subtracting 90 from the Julian day incorrectly or off-by-one). The iter_7
solution (which is correct) hardcodes epoch day `10471` in the comment, which is correct.
Later iterations reverted to a computed threshold that was wrong by ~1 day.

**Root cause of catastrophic failure (iter_3):**
Scan time exploded to 4788 ms and all values are ~4500× too large. This indicates the date filter
was removed or inverted — the code scanned *all* 60 M rows without the `l_shipdate` upper-bound
filter, multiplying all aggregates by the full-table row count. Classic C7 date threshold
regression during an optimization attempt that accidentally broke the filter predicate.

**Performance observation:** main_scan dominates at 52–62 ms. The ~110–120 ms total is reasonable
for SF-10 at 60 M lineitem rows. No further meaningful speedup is available without SIMD
intrinsics or a persistent buffer pool.

---

### Q3 — Shipping Priority
**Classification: FAILED** (never produced a correct result in any iteration)

| Iter | Timing (ms) | Validation | Notes |
|------|------------|------------|-------|
| 0    | 1235       | **FAIL**   | Baseline — revenue ~0.1% of expected, wrong orderkeys, wrong dates |
| 1    | 649        | **FAIL**   | Same pattern — revenue off by 99.8%, wrong orderkeys |
| 2    | skipped    | skipped    | Skipped |
| 3    | 1441       | **FAIL**   | Revenue off by exactly 99% (100× too small) |
| 4    | 281        | **FAIL**   | Revenue off by 99.7%, wrong orderkeys AND wrong dates |
| 5–10 | skipped   | skipped    | All skipped after repeated failures |

**Final best: iter_0 @ 1235 ms (still failing)** — run.json confirms `bestCppPath = iter_0`.

**Root cause analysis:**

*Revenue error (all iterations):*
- iter_3: revenue actual = expected / 100.0 exactly (99% off, rel_diff_pct=99 uniformly).
  This is a **C5 / scale double-division** bug: the code divides by 100 twice. Correct formula:
  `revenue = l_extendedprice * (100 - l_discount) / 10000` (one division), but iter_3 divided
  by 10000 AND then by 100 again, yielding `/1,000,000` total.
- iters 0, 1, 4: revenue is ~0.1–0.2% of expected (off by ~99.8%). This is far worse than a
  simple ×100 error. The actual values (e.g., 760, 1199, 2726) look like revenue was computed as
  raw `l_extendedprice * l_discount` (subtraction applied as multiplication) or the discount term
  was not inverted: `l_extendedprice * l_discount / 10000` instead of
  `l_extendedprice * (100 - l_discount) / 10000`. At a mean discount of ~5%, this produces
  ~5% of the real revenue, which matches the ~0.1–0.2% scale (since extprice itself is small
  at these top-10 rows).

*Order key / date errors (iters 0, 1, 4):*
- All 10 output rows have wrong `l_orderkey` and wrong `o_orderdate`. Expected dates cluster
  around 1995-01 to 1995-03 (just before 1995-03-15 threshold); actual dates are also near
  March 1995 but not the same rows. This is a **wrong sort ordering / wrong aggregation key**:
  the GROUP BY key does not correctly identify (l_orderkey, o_orderdate, o_shippriority), so the
  aggregation produces a different set of top-10 rows.
- Combined with the near-zero revenue values in failing iterations, the top-10 selection by
  `ORDER BY revenue DESC` picks completely different rows.

*Performance observation (when it ran):*
- iter_4 at 281 ms shows the architecture (dim_filter 11ms, build_joins 144ms, main_scan 33ms,
  aggregation_merge 6ms) is on-track for the target. `build_joins` (orders scan + hash build)
  dominates. When correctness is fixed, target is ~300 ms.
- iter_0 at 1235 ms: `build_joins` = 272 ms + `aggregation_merge` = 113 ms — the global merge
  of thread-local CompactHashMaps is expensive. This suggests C9 (capacity overflow during merge)
  may be inflating the aggregation step.

---

### Q6 — Forecasting Revenue Change
**Classification: SUCCESS** (correct + fast, solved at iter_0 with no optimization needed)

| Iter | Timing (ms) | Validation | Notes |
|------|------------|------------|-------|
| 0    | **22**     | pass       | **Only iteration run** |

**Final best: iter_0 @ 22 ms (correct)**

Zero optimization iterations were needed. The initial generation was correct and fast.
Timing breakdown: zone_map_prune 0.04 ms, main_scan 14 ms, total 22 ms.
This is excellent — zone-map pruning on `l_shipdate` combined with the tight `l_discount`
BETWEEN filter and `l_quantity < 24` filter reduced the effective scan significantly.

---

### Q9 — Product Type Profit Measure
**Classification: SLOW** (correct in best iteration, but optimizer struggled with dim_filter cost)

| Iter | Timing (ms) | Validation | Notes |
|------|------------|------------|-------|
| 0    | 425        | **FAIL**   | Baseline fail — sum_profit ~153× too large (C5 scale error ×100) |
| 1    | 350        | pass       | First correct result |
| 2    | 1465       | **FAIL**   | Regression — dim_filter 1465 ms → P1/C9: large hash table bottleneck |
| 3    | 361        | pass       | Recovered |
| 4    | 416        | pass       | Slightly slower |
| 5    | 338        | pass       | Improving |
| 6    | 297        | pass       | Improving |
| 7    | 275        | pass       | Improving |
| 8    | **268**    | pass       | **Best kept** (bestCppPath) |
| 9    | 227        | **FAIL**   | Faster but wrong — sum_profit ~75% of expected (scale division error) |
| 10   | 280        | pass       | Recovered to correct but slower |

**Final best: iter_8 @ 268 ms (correct)**

**Root cause of baseline failure (iter_0):**
`sum_profit` actual = expected × ~153. With `actual / expected ≈ 153`, this is consistent with
a factor-of-100 error on both the `l_extendedprice*(1-l_discount)` term AND the
`ps_supplycost*l_quantity` term — i.e., neither was divided back to scale¹, leaving both at
scale². This is a C5 bug. Experience entry C5 covers this; the fix (divide by 10000 once) was
found by iter_1.

**Root cause of iter_2 regression (1465 ms):**
`dim_filter` exploded to 1465 ms (vs 430 ms at iter_0). This is the phase building hash maps
for partsupp and orders. The optimizer introduced a change that eliminated the split-thread
OMP optimization and reverted to an `std::unordered_map` or oversized structure (P1 pattern),
dramatically slowing the hash-build phase.

**Root cause of iter_9 failure (sum_profit ~75% of expected):**
`actual / expected ≈ 0.246`. This is a **over-division** error: dividing by 10000 when the
formula already partially divided. The optimizer introduced an extra `/100` step on one of the
two terms, leaving one term at scale¹ and the other at scale², so the difference was off.

**Performance observation:**
`dim_filter` dominates at 268–280 ms in best iterations (building partsupp 8M + orders 15M
hash maps, plus LIKE-filter over 2M part rows). The fused single-OMP-region approach in iter_8
reduces this from 430 ms to 268 ms. The `main_scan` (lineitem probe, 60M rows) runs in only
36 ms showing good parallelism. Further gains require prefiltered part index (only green-part keys).

---

### Q18 — Large Volume Customer
**Classification: SLOW** (correct in best iterations, subquery_precompute dominates)

| Iter | Timing (ms) | Validation | Notes |
|------|------------|------------|-------|
| 0    | 4057       | pass       | Baseline — `subquery_precompute` = 3740 ms |
| 1    | 530        | pass       | Huge improvement — subquery 180 ms |
| 2    | **435**    | pass       | **Best kept** (bestCppPath) — subquery 179 ms |
| 3    | 2403       | **FAIL**   | Wrong results — 98/100 rows differ; subquery 2204 ms |
| 4    | 2326       | **FAIL**   | Same wrong results as iter_3 — subquery 2095 ms |
| 5–9  | skipped    | skipped    | Skipped after consecutive failures |
| 10   | 4180       | pass       | Final attempt — correct but reverted to 3961 ms subquery |

**Final best: iter_2 @ 435 ms (correct)**

**Root cause of baseline slowness (iter_0, 4057 ms):**
`subquery_precompute` = 3740 ms. This is the two-pass lineitem scan approach without
parallelism — reading 60M rows twice (once for GROUP BY SUM, once for the qualifying set).
This matches P8 (materializing full lineitem twice without a thread-parallel hash build).

**Root cause of iter_2 improvement (435 ms):**
`subquery_precompute` drops to 179 ms — the optimizer parallelized the subquery computation
using OMP, building the `CompactHashMap<orderkey→sum_qty>` in a single parallel pass and then
filtering qualifying orders. This is the correct fix for P8.

**Root cause of iters 3–4 failures (wrong rows, 98/100 differ):**
The `c_name`, `c_custkey`, `o_orderkey`, `o_orderdate`, and `o_totalprice` are all wrong.
The pattern is identical across iter_3 and iter_4 — the same 98 rows are wrong, and the
`sum_qty` output is scaled differently (e.g., expected 322.00 actual 303.00). This strongly
suggests a **C8 threshold scale error**: the optimizer introduced `HAVING SUM(l_quantity) > 300`
without scaling (using `> 300` instead of `> 30000`), admitting a completely different — and
much larger — set of qualifying orders, then TOP-100 by `o_totalprice DESC` picks different rows.
The subquery runtime exploded to 2000–2200 ms confirming the larger intermediate set.
This is exactly the C8 pattern from the experience base.

**Root cause of iter_10 regression (4180 ms, correct):**
The optimizer abandoned the parallel hash-build and reverted to a sequential two-pass approach.
`subquery_precompute` = 3961 ms — essentially back to the iter_0 baseline. This suggests the
optimizer could not successfully build on the iter_2 correct+fast solution when trying to
further optimize.

**Performance observation:**
iter_2's 435 ms is still above the target of ~200 ms. The remaining ~179 ms in
`subquery_precompute` is dominated by a parallel lineitem GROUP BY pass. With P8's recommended
two explicit sequential mmap passes and a pre-sized CompactHashMap (avoiding C9), it should
reach ~200 ms. The orders_scan (18 ms) and dim_filter (4 ms) phases are negligible.

---

## Cross-Query Patterns

### Pattern A: Scale² Output Not Fixed Under Optimization Pressure
Seen in Q1 (iters 6, 8, 9, 10), Q9 (iter_0, iter_9), Q3 (all iterations), Q18 (iters 3–4).
The optimizer tends to introduce decimal scale bugs when restructuring the hot computation path.
The experience entries C5, C8, C10 exist but are not preventing recurrence — likely because
the optimizer reverts to "simpler" arithmetic that accidentally removes division steps.

### Pattern B: Date Threshold Regression Under Optimization
Seen in Q1 (iter_3 catastrophic, iters 6/8/9/10 off-by-~1-day).
When optimizing the scan boundary (zone-map threshold computation), the optimizer recomputes
the epoch-day cutoff and gets it wrong. The correct value (10471) was found early but later
iterations recomputed it with a slightly wrong formula.

### Pattern C: Optimization Locks In Failures → Cascading Skips
Seen in Q3 (iterations 5–10 all skipped) and Q18 (iterations 5–9 all skipped).
After 2–3 consecutive failing iterations, the pipeline enters a skip loop where `validation="skipped"`.
This prevents recovery. Q3 was stuck from iter_5 onward despite iter_4 being only 281 ms
(a good performance result, just wrong). The optimizer needed to continue from a correct
baseline but had no passing iteration to roll back to after iter_0 → all subsequent fails.

### Pattern D: dim_filter Phase Bottleneck for Multi-Table Queries
Q9: dim_filter 268–430 ms; Q18: (subquery_precompute) 179–3961 ms.
Hash-building multi-million-row dimension tables is the dominant cost for complex queries.
Sequential phases for partsupp (8M), orders (15M), and lineitem subquery (60M) cannot be
adequately parallelized without careful OMP design.

### Pattern E: date_utils.h sprintf Buffer Overflow Warning (Q3, Q18)
Every iteration of Q3 and Q18 emits:
```
warning: '__builtin___sprintf_chk' may write a terminating nul past the end of the destination
   std::sprintf(buf, "%04d-%02d-%02d", y, m, d);
   note: output between 11 and 17 bytes into a destination of size 11 (or 16)
```
The `epoch_days_to_date_str` function in `date_utils.h` declares `char buf[11]` but the
format string `%04d-%02d-%02d` can write up to 10 chars + null = 11, which is tight.
GCC detects that the `%02d` for month can go negative (range [-128,127] on int8_t), producing
a minus sign and making the output longer. This does not cause a runtime crash in practice
(values are always positive dates) but is a latent unsafe code pattern and produces noisy warnings.

---

## Final Result Table

| Query | Best Iter | Best Time (ms) | Final Validation | Classification |
|-------|-----------|---------------|-----------------|----------------|
| Q1    | iter_7    | 112           | pass            | **SLOW** (regressed in later iters) |
| Q3    | iter_0    | 1235          | FAIL            | **FAILED** |
| Q6    | iter_0    | 22            | pass            | **SUCCESS** |
| Q9    | iter_8    | 268           | pass            | **SLOW** |
| Q18   | iter_2    | 435           | pass            | **SLOW** |

- **SUCCESS:** 1 (Q6)
- **SLOW (correct):** 3 (Q1, Q9, Q18)
- **FAILED:** 1 (Q3)
- **Total queries:** 5
