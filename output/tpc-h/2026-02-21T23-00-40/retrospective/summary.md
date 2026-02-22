# TPC-H Retrospective — 2026-02-21T23-00-40
**Workload:** TPC-H · **Scale Factor:** SF10 · **Queries:** Q1, Q3, Q6, Q9, Q18

---

## Per-Query Classification

| Query | Final Status | Best Hot Time | Iterations | Notes |
|-------|-------------|--------------|------------|-------|
| Q1    | ✅ SUCCESS   | 88 ms        | 0 (iter_0 final) | Correct, no optimization triggered |
| Q3    | ✅ SUCCESS   | 150 ms (iter_3) | 5 | iter_1 zero-rows; iter_5 wrong-rows; oscillation after best |
| Q6    | ✅ SUCCESS   | 7 ms (iter_1)  | 1 | Excellent; mmap optimization cut 59 ms → 7 ms |
| Q9    | ✅ SUCCESS   | 187 ms       | 0 (iter_0 final) | Correct, no optimization triggered |
| Q18   | ✅ SUCCESS   | 513 ms (iter_4) | 4 | iter_2 signal-killed; iter_3 regression; recovered at iter_4 |

**Overall:** 5/5 queries delivered correct final results. All intermediate failures were
self-corrected by the optimizer. No query ended in a permanently failed state.

---

## Q1 — iter_0 (SUCCESS, 88 ms hot)

**Validation:** ✅ pass — 4/4 rows
**Timing breakdown (hot):**
- data_loading: 48 ms (54%)
- main_scan: 34 ms (38%)
- output: 1 ms

**Notes:**
- No optimization iterations recorded; first attempt was accepted as-is.
- Compiler emitted a `[-Wclass-memaccess]` warning: `memset(tl_agg, 0, sizeof(tl_agg))` on a
  non-trivial struct `ThreadAgg`. While zeroing to 0 is safe here, this is a latent C20 risk if
  any sentinel is introduced later.
- data_loading (48 ms) dominates. Could benefit from mmap prefetch tuning (P13/P14) but is
  acceptable at this timing level.

---

## Q3 — Best: iter_3 (SUCCESS, 150 ms hot)

**Iteration trajectory:**

| Iter | Hot (ms) | Valid | Dominant Phase |
|------|---------|-------|----------------|
| iter_0 | 8,584 | ✅   | aggregation_merge: **3,263 ms** |
| iter_1 | 5,279 | ❌ (0 rows) | agg_init: 635 ms; main_scan: 13 ms |
| iter_2 | 319   | ✅   | subquery_precompute: 54 ms; main_scan: 29 ms |
| iter_3 | **149** | ✅  | main_scan: 66 ms; aggregation_merge: 22 ms |
| iter_4 | 215   | ✅   | main_scan: **162 ms** (regression) |
| iter_5 | 1,243 | ❌ (wrong rows) | build_joins: 87 ms; main_scan: **1,103 ms** |

**Failure analysis — iter_1 (zero rows):**
- `dim_filter` time collapsed to 0 ms (from 9.8 ms in iter_0), suggesting the optimizer
  introduced an always-false predicate or completely removed the filter stage.
- 0 rows returned despite 10 expected → filter logic inversion pattern.
- Likely cause: optimizer changed a date comparison (`<` vs `<=`, or wrong constant) that
  eliminated all qualifying rows.

**Failure analysis — iter_5 (wrong rows, correct count):**
- Row count still 10, but `l_orderkey`, `revenue`, and `o_orderdate` all differ from expected.
- main_scan ballooned from 66 ms (iter_3 best) to 1,103 ms — 17× regression.
- build_joins grew from 23 ms to 87 ms simultaneously.
- Indicates the optimizer restructured the join or probe side in a way that produced wrong row
  assignments (wrong join key mapping). Suspect C15 (missing GROUP BY dimension) or join
  predicate regression.
- This is also a P12 oscillation event: optimizer had already accepted iter_4 (regression from
  149 ms → 215 ms) and then pushed further to iter_5, yielding an incorrect result.

**Root cause of iter_0 bottleneck (aggregation_merge: 3,263 ms):**
- Per-thread partial aggregates were merged using an expensive sequential scan over all partial
  groups. Likely std::unordered_map used for thread-local aggregation (P1), producing large maps
  that serialized during merge.
- After fix in iter_2 (aggregation_merge dropped to 22 ms), this validates the diagnosis.

---

## Q6 — Best: iter_1 (SUCCESS, 7 ms hot)

**Validation:** ✅ pass — 1/1 rows both iterations
**Timing breakdown (hot):**

| Iter | Hot (ms) | data_loading | main_scan |
|------|---------|-------------|-----------|
| iter_0 | 59 | 47 ms | 10 ms |
| iter_1 | **7** | 0.2 ms | 4–10 ms |

**Notes:**
- iter_1 eliminated data_loading (47 ms → 0.2 ms) — optimizer correctly shifted to mmap
  without file re-read (data stays OS-cached / columns memory-mapped persistently).
- 88% wall-clock reduction in one optimization step. Clean success.
- No correctness concerns.

---

## Q9 — iter_0 (SUCCESS, 187 ms hot)

**Validation:** ✅ pass — 175/175 rows
**Timing breakdown (hot):**
- data_loading: 122 ms (65%)
- dim_filter: 31 ms (17%)
- main_scan: 36 ms (19%)
- aggregation_merge: 0.02 ms

**Notes:**
- No optimization iterations; optimizer accepted iter_0 directly.
- data_loading dominates at 65% of runtime (122/187 ms). Q9 involves 6 tables
  (LINEITEM, ORDERS, SUPPLIER, PART, PARTSUPP, NATION) — large I/O footprint is expected, but
  zone-map-guided prefetch or selective madvise could reduce this further (P13).
- 187 ms is correct and competitive for a 6-table star-schema join at SF10.

---

## Q18 — Best: iter_4 (SUCCESS, 513 ms hot)

**Iteration trajectory:**

| Iter | Hot (ms) | Valid | Dominant Phase |
|------|---------|-------|----------------|
| iter_0 | 2,299 | ✅   | build_joins: **1,766 ms** |
| iter_1 | 612   | ✅   | dim_filter: 141 ms; build_joins: 386 ms |
| iter_2 | —     | ❌ (signal kill) | No timing output — process killed at 2 s |
| iter_3 | 775   | ✅   | dim_filter: **573 ms** (regression) |
| iter_4 | **513** | ✅  | dim_filter: 256 ms; build_joins: 156 ms |

**Failure analysis — iter_2 (signal kill):**
- Process killed after ~2 s with zero `[TIMING]` lines in stdout.
- This is the C23 pattern exactly: hash table infinite loop before first timed phase.
- The optimizer likely resized or restructured a hash table for the LINEITEM-based subquery
  (large cardinality), causing capacity overflow and unbounded probing (C9/C20).
- Optimizer correctly skipped this iteration (`validation: "skipped"`) and retried.

**iter_3 regression:**
- dim_filter jumped from 141 ms (iter_1 best) to 573 ms — 4× slowdown on the filter phase.
- build_joins improved (73 ms) but the dim_filter regression dominated.
- P12 oscillation: optimizer accepted iter_3 despite worse total time (775 ms > 612 ms iter_1).

**iter_4 recovery:**
- Balanced optimization: dim_filter 256 ms, build_joins 156 ms, total 513 ms — new best.
- 4.5× end-to-end speedup vs iter_0 baseline.

---

## Cross-Query Patterns

### 1. P12 Optimizer Oscillation (Q3, Q18)
Both Q3 and Q18 exhibited multi-step regression after finding their best iteration:
- Q3: best at iter_3 (149 ms) → iter_4 regresses (215 ms) → iter_5 fails (wrong results)
- Q18: best at iter_1 (612 ms) → iter_2 crashes → iter_3 regresses (775 ms) → iter_4 recovers

**Pattern:** The optimizer did not revert to best-known-good after detecting regression, allowing
two additional compile-run-validate cycles (one wasted in Q3, one crash in Q18) before landing.

### 2. C23 Hash Table Crash (Q18 iter_2)
One out of five queries triggered the C23 pattern (signal kill, zero timing output). The fix
(C9/C20: proper power-of-2 sizing + std::fill sentinel init) is already documented but was not
consistently applied by the code generator in the new join structure attempted at iter_2.

### 3. Zero-Row Filter Inversion (Q3 iter_1)
A new failure pattern not yet in the experience base: optimizer removed or inverted a filter
(dim_filter time → 0 ms), yielding 0 output rows. Distinct from C13 (date constant regression)
because it affected the existence of the filter stage itself, not just the threshold value.

### 4. Aggregation Merge Bottleneck at iter_0 (Q3)
Q3 iter_0's aggregation_merge phase consumed 3,263 ms — 38× the final optimized value.
This is a systematic P1/P3 issue: thread-local aggregation using std::unordered_map with
expensive sequential merge. A pre-allocated composite-key hash table resolves this in one step.

### 5. data_loading Dominates Cold Start (Q1, Q9)
Q1 (48 ms / 88 ms total) and Q9 (122 ms / 187 ms total) both have data_loading as the dominant
phase. Neither query triggered optimization. Selective `madvise(MADV_WILLNEED)` on qualifying
blocks (P13) could cut these by 30–50% on a cold start.
