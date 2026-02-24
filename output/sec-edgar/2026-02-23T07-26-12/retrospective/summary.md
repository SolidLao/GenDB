# Retrospective: sec-edgar / 2026-02-23T07-26-12

## Overall Results

| Query | Final Status | Iterations | Final Timing | Root Cause / Notes |
|-------|-------------|------------|-------------|---------------------|
| Q1    | ✅ SUCCESS  | 0          | 39.8 ms     | Passed first-time, no optimization needed |
| Q2    | ✅ SUCCESS  | 1          | 42.6 ms     | iter_0 (93ms) → iter_1 (42ms) via data_loading removal |
| Q3    | ❌ FAILED   | 5          | ~126 ms     | C29 precision_loss (never fixed, all 6 iters fail) |
| Q4    | ✅ SUCCESS  | 5          | 121.2 ms    | Steady optimization 267ms → 121ms |
| Q6    | 🟡 SLOW     | 5          | 1012.5 ms   | 3 correctness failures before passing; agg_merge still 385ms |
| Q24   | ✅ SUCCESS  | 3          | 319.7 ms    | Bloom filter + thread-local agg eliminated 167ms merge overhead |

**Summary: 4 SUCCESS, 1 SLOW, 1 FAILED** (6 queries total)

---

## Q1 — SUCCESS (39.8 ms)

Single iteration. All phases fast: data_loading 15ms, main_scan 10ms, aggregation_merge 0.9ms.
No further optimization was attempted (timing already excellent).

---

## Q2 — SUCCESS (42.6 ms)

- **iter_0 (93ms → baseline):** data_loading dominated at 53ms; build_joins negligible (0.08ms).
- **iter_1 (42.6ms):** Eliminated data_loading cost via mmap caching (data_loading dropped to 0.39ms).
  Output phase still 21ms (acceptable for this query).

---

## Q3 — FAILED (all 6 iterations)

**Root cause: C29 — double precision insufficient for SUM(value) at ~10¹³ magnitude.**

The `num.value` column stores IEEE 754 doubles. For sums at magnitude 10¹³ (ULP ≈ 0.004),
accumulated round-off error causes last-digit mismatch (±0.01–0.05) in the final SUM output.

The storage_design.json **explicitly annotates** the value column with a C29 WARNING:
> "SUM must accumulate as int64_t cents: iv=llround(v*100); sum+=iv; output=sum/100."

Despite this, the code generator applied **Kahan-Neumaier compensated summation in long double**
(iter_5 header says so). C29 explicitly states "FP compensation (Kahan/Neumaier) does NOT fix this."
This explains why all 6 iterations failed with identical `precision_loss` patterns.

**Key diagnostic signal:**
- `mismatch_patterns: { "precision_loss": 34, "string_mismatch": 27, "value_mismatch": 9 }` at iter_0
- String mismatches = row ordering issue (likely comes from values being slightly wrong → different sort order)
- abs_diff: 0.01–0.05, magnitude: 10¹³, pattern: precision_loss — classic C29

**Fix:** Accumulate as `int64_t cents`: for each double `v`, compute `iv = llround(v * 100.0)`
then `sum += iv`. Output as `(sum/100).(abs(sum%100))`. Values up to 10¹³ → cents up to 10¹⁵
which is within double's exact integer range (2⁵³ ≈ 9×10¹⁵), so llround recovers exact cent values.

**Optimizer failure mode:** Optimizer was aware of precision issues but chose the wrong fix (Kahan
vs int64_t). All 5 optimization iterations were wasted on Kahan variants rather than switching
to the prescribed approach. Note iter_5 regressed to 884ms (Kahan approach apparently tried
an inefficient accumulation path, main_scan ballooned to 839ms).

---

## Q4 — SUCCESS (121.2 ms after 5 iterations)

Clean, steady optimization trajectory:

| Iter | Timing | Dominant Phase | Optimization Applied |
|------|--------|---------------|----------------------|
| 0    | 266.8ms | data_loading 55ms, main_scan 55ms, agg_merge 22ms | Baseline |
| 1    | 254.4ms | main_scan 90ms | data_loading eliminated |
| 2    | 269.8ms | data_loading 60ms | Regression — reverted direction |
| 3    | 217.3ms | data_loading 62ms, build_joins 48ms | Subquery optimization |
| 4    | 159.1ms | data_loading 56ms, build_joins 12ms | Pre-filtered subquery |
| 5    | 121.2ms | main_scan 26ms, agg_merge 9ms | data_loading cached |

Iter 2 was a regression (~6% worse) that was correctly reverted (P12). iter_4 introduced
`build_filtered_sub` (0.14ms) to pre-filter the subquery join, halving build_joins from 48ms to 12ms.

---

## Q6 — SLOW (1012.5 ms final, correctness achieved at iter_3)

### Correctness Failures

**iter_0 (10.8s, FAILED):** Massive mismatch — 521 string mismatches in 200 rows.
Wrong grouping and wrong ordering. Root cause: incorrect ORDER BY or GROUP BY logic,
possibly sorting on wrong column. The main_scan took 10.1s — suspect hash probe degeneracy.

**iter_1 (51.8s, FAILED):** Only 1 row wrong, but **aggregation_merge consumed 46,800ms**.
This is a catastrophic P17 violation: `std::unordered_map` single shared merge under 64 threads.
Merging 64 thread-local unordered_maps into a shared map is O(N × hash_lookup_cost) with contention,
creating a 46-second bottleneck for what should be <10ms. The 1 remaining wrong row
(HANRYU HOLDINGS, INC. — a rare entry) suggests a tie-break ordering ambiguity.

**iter_2 (1.5s, FAILED):** 2 rows wrong, both with **exactly 50% factor off** in value and count.
  - Expected: 5,821,367,926,000 / count 210 → Actual: 2,910,683,963,000 / count 105 (exactly ½)
This is C15: hash map key missing one GROUP BY dimension. When two distinct groups hash to the
same key (because key is incomplete), their sums merge, doubling the true count/value for
overlapping groups — but here we see halving, suggesting the query is counting each data point
in TWO groups but only one is output. Actually: the exact 50% off in BOTH value and count means
the data that should go to this group is being split into 2 groups (correct + incorrect extra).
Only the extra (wrong) group passes the HAVING/filter threshold. Fix was adding the missing key dimension.

### Performance After Correctness Fixed (iter_3+)

| Iter | Timing  | data_loading | build_joins | main_scan | agg_merge |
|------|---------|-------------|-------------|-----------|-----------|
| 3    | 1650ms  | 47ms        | 206ms       | 268ms     | 901ms     |
| 4    | 1143ms  | 54ms        | 121ms       | 261ms     | 501ms     |
| 5    | 1012ms  | 46ms        | 119ms       | 270ms     | 385ms     |

**Remaining bottleneck: aggregation_merge (385ms) — 38% of total runtime.**

The iter_5 code uses occupied-slot tracking + parallel sort-merge:
1. Collect ~total_occ entries from 64 thread-local hash maps (sequential pass over tracked indices)
2. `__gnu_parallel::sort` all collected entries (~2M entries, ~64MB total)
3. Sequential reduce: merge consecutive equal-key entries

With AGG_CAP=2M slots per thread × 64 threads = 128M potential slots (4GB), even with
occupied-slot filtering, the parallel sort over 2M 32-byte records (64MB) dominates.
Additionally, `build_joins` at 119ms (building the 1.73M-row pre IS hash table) and
`main_scan` at 270ms (probing 39M num rows) remain significant.

**Path to improvement:**
- Reduce AGG_CAP to next_pow2(group_cardinality × 2) per thread (P23-analog for aggregation)
- Use a two-phase tree-merge: merge 8 threads → 8 partial results in parallel, then merge 8 → 1
- The pre IS hash table build could use a pre-built index if available (P11)

---

## Q24 — SUCCESS (319.7 ms after 3 optimization iterations)

Excellent result with effective use of P21 (bloom filter) and P17/P20 (thread-local aggregation).

| Iter | Timing | build_bloom | main_scan | agg_merge | Key Change |
|------|--------|-------------|-----------|-----------|------------|
| 0    | 509ms  | 140ms       | 127ms     | 167ms     | Baseline |
| 1    | 504ms  | 140ms       | 130ms     | 166ms     | Minor tweaks |
| 2    | 403ms  | 71ms        | 96ms      | 168ms     | Bloom optimization |
| 3    | 322ms  | 67ms        | 193ms     | 1ms       | Thread-local agg → merge ≈0 |
| 4    | 330ms  | 67ms        | 206ms     | 1ms       | Slight regression (reverted) |
| 5    | 320ms  | 67ms        | 188ms     | 1ms       | Minor tuning |

**P17/P20 win at iter_3:** aggregation_merge dropped from 167ms → 1ms. Classic pattern.
**Build_bloom at 67ms** — still the second-largest phase. Further reduction possible with
parallel bloom fill or smaller bloom (if false positive rate acceptable).

---

## Recurring Failure Patterns

1. **C29 (precision_loss in SUM)** — Q3 failed all 6 iterations. Code generator ignored the
   explicit C29 WARNING in storage_design.json and used Kahan instead of int64_t cents.
   **This is the single most impactful failure of the run.**

2. **P17/P20 (shared aggregation map) violation** — Q6 iter_1: 46.8s aggregation_merge.
   Thread-local aggregation must be the default; any shared merge path is catastrophic at scale.

3. **C15 (incomplete GROUP BY key in hash map)** — Q6 iter_2: exact 50% factor mismatch
   in both value and count. Diagnostic: exactly N× off in ALL aggregate columns simultaneously.
