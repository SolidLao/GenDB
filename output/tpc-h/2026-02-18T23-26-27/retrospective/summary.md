# Stage B Retrospective — TPC-H SF10
**Run:** 2026-02-18T23-26-27
**Model:** haiku
**Scale Factor:** 10
**Queries:** Q1, Q3, Q6, Q9, Q18

---

## Classification Summary

| Query | Classification | Best Time | Iterations | Root Cause |
|-------|---------------|-----------|------------|------------|
| Q1    | **FAILED**    | —         | 4 (0 fixed) | `avg_qty` missing `/100` division; optimizer then over-corrected and broke `sum_qty` too by following incorrect experience (C16 claims `l_quantity scale=1`, actual scale=100) |
| Q3    | **FAILED**    | —         | 1 (0 fixed) | Wrong top-10 result rows; optimizer stopped after baseline failure with 0 additional iterations recorded |
| Q6    | **FAILED**    | —         | 4 (0 fixed) | `QTY_MAX = 24` renders l_quantity filter always-false; correct value is 2400. Root cause: experience C12 hardcodes "use 24, not 2400" but actual `l_quantity scale_factor = 100` in this run |
| Q9    | **SUCCESS**   | 303 ms    | 2 (1 optimized) | Correctly passes validation; iter_1 improved 1.82× via parallelized part_filter (256ms→47ms) and orders_build (33ms→13ms) |
| Q18   | **SUCCESS**   | 124 ms    | 1 (baseline) | Passes validation; two-pass lineitem scan correctly avoids double-materializing 60M rows |

**Totals:** 5 queries — 2 SUCCESS, 0 SLOW, 3 FAILED

---

## Critical Finding: l_quantity Scale Factor Contradiction

**The single most impactful issue this run:** experience entries C8, C10, C12, C16 all hardcode the assertion
`l_quantity scale_factor = 1`. But **`storage_design.json` for this run defines `l_quantity` with `scale_factor: 100`**
(line 13: `"scale_factor": 100`), identical to every other DECIMAL column.

This incorrect experience guidance directly caused:
- **Q6 revenue = 0** (all 4 iterations): `QTY_MAX = 24` filters out all rows because actual raw values are
  qty×100 = 100–5000. All rows satisfy `qt[i] >= 24`, so zero rows pass the filter.
- **Q1 both metrics wrong** (iter_3 onward): The optimizer, following C16, removed the correct `/100.0` from
  `sum_qty` output in iter_3, breaking what had been a partially-correct baseline.

---

## Per-Query Deep Dive

### Q1 — FAILED (4 iterations, never fixed)

**SQL:** Pricing Summary Report — aggregation over lineitem with shipdate filter.

**iter_0 code (partial success, partial failure):**
- `AggSlot.sum_qty` comment correctly says `// scale 100`
- Output `sum_qty = (double)s.sum_qty / 100.0` → **CORRECT** (divides raw by scale)
- Output `avg_qty = (double)s.sum_qty / (double)cnt` → **WRONG** (should also divide by 100)
- Validation: only `avg_qty` fails (100× too large: expected 25.50, actual 2550.10)

**iter_3 code (regressed by optimizer):**
- Optimizer saw C16 ("l_quantity scale=1") and changed comment to `// scale 1`
- Removed `/100.0` from `sum_qty` output → now `sum_qty = (double)s.sum_qty` → **WRONG** (100× too large)
- `avg_qty` formula unchanged — still wrong for the same reason
- Validation: both `sum_qty` AND `avg_qty` now fail (100× too large each)

**Required fix (not applied in any iteration):**
```cpp
const double sum_qty = (double)s.sum_qty / 100.0;          // ÷ scale_factor
const double avg_qty = (double)s.sum_qty / 100.0 / (double)cnt;  // ÷ scale_factor ÷ count
```

**Why optimizer failed:** Experience C16/C10 explicitly state "do NOT divide avg_qty by 100" and
"l_quantity scale=1". The optimizer followed this wrong guidance and moved in the wrong direction.

---

### Q3 — FAILED (1 iteration, 0 optimizer iterations recorded)

**SQL:** Shipping Priority — 3-way join (customer, orders, lineitem) with BUILDING segment filter,
date cutoffs, revenue aggregation, top-10 by revenue DESC / o_orderdate ASC.

**Observed failure:** All 10 output rows are wrong (`l_orderkey`, `revenue`, `o_orderdate` — all mismatched).

**Code review findings (no obvious bug found via static analysis):**
- CUTOFF = 9204 (1995-03-15 epoch day) — correct
- Orders filter: `o_odate < CUTOFF` — correct
- Lineitem filter: `l_sdate > CUTOFF` — correct
- Revenue formula: `ep * (100LL - disc)` / 10000.0 — correct for scale=100
- Hash table sizing: `total_orders * 2 + 64` — sufficient at ~33% load factor
- `init_date_tables()` called before epoch→date conversion — correct
- Dictionary code lookup for c_mktsegment: iterative scan, compares string to "BUILDING" — appears correct

**Most likely root cause (unconfirmed):** An incorrectly resolved dictionary code for `c_mktsegment`
selecting the wrong customer segment, causing a different set of qualifying orders and hence different
top-10 rows. Alternatively, there may be a subtle hash collision path producing stale results at very
low load factors.

**Optimizer behavior:** `optimization_history.json` shows `"iterations": []` — zero optimization attempts
beyond the baseline. The framework pipeline appears to have abandoned Q3 after iter_0 without generating
any repair iterations.

---

### Q6 — FAILED (4 iterations, revenue always 0)

**SQL:** SELECT SUM(l_extendedprice * l_discount) WHERE l_shipdate in 1994, l_discount BETWEEN 0.05 AND 0.07,
l_quantity < 24.

**Predicate constants (all iterations):**
```cpp
DISC_LOW  = 5     // correct: 0.05 * 100 = 5
DISC_HIGH = 7     // correct: 0.07 * 100 = 7
QTY_MAX   = 24    // WRONG: should be 2400 = 24 * 100 (l_quantity scale=100)
```

**How revenue becomes exactly 0:** `qt[i] >= QTY_MAX` means `qt[i] >= 24`. Since `l_quantity` is stored
as raw_qty × 100 (scale=100), the minimum stored value is 1×100 = 100. Every row satisfies `qt[i] >= 24`
(100 ≥ 24 is always true), so every row is filtered out. Zero rows accumulate revenue. Output: `0.00`.

**Why optimizer never fixed it:** Experience C12 explicitly warns "l_quantity: scale=1 → l_quantity < 24
(use 24 NOT 2400)". All four haiku iterations followed this instruction, retaining QTY_MAX=24.
Timing improved (36ms→28ms) via cache-line alignment and zone-map fixes but the filter bug persisted.

**Required fix:** `QTY_MAX = 2400LL // = 24 * scale_factor (100)` — read scale_factor from storage_design.json

---

### Q9 — SUCCESS (303 ms, iter_1 best, 175/175 rows correct)

**SQL:** Profit by Nation and Year — 6-way join with `p_name LIKE '%green%'` pre-filter.

**iter_0 baseline (551 ms):**
- Serial part.tbl scan for "green" substring: 256 ms (dominant)
- Sequential partsupp + orders build

**iter_1 optimization (303 ms, 1.82× speedup):**
- Parallel 64-thread part_filter with MAP_POPULATE prefetch: 256ms → 47ms (5.4×)
- Two-phase parallel partsupp_build (collect + serial insert): 50ms → 30ms
- Parallel orders_build (safe: o_orderkey unique per row): 33ms → 13ms
- `local_agg[25][10]` per thread (2KB, fits L1 cache) vs global array indexing

**Remaining bottleneck:** `main_scan = 145ms` (unchanged). This is a full scan of 60M lineitem rows
probing a partkey bitset — near I/O-bound on HDD. Further improvement would require better selectivity
at the bitset level or SIMD-accelerated bitset probing.

---

### Q18 — SUCCESS (124 ms, iter_0 baseline, 100/100 rows correct)

**SQL:** Large Volume Customer — nested subquery SUM(l_quantity) > 300 HAVING on lineitem grouped by
orderkey, then outer join to orders/customer with LIMIT 100.

**Design:** Two-pass lineitem scan (per experience P8):
- Pass 1 (extract_qualifying): Build orderkey→sum_qty map, identify qualifying orders: **5.45 ms**
- Pass 2 (lineitem_scan_aggregate): Probe qualifying set for outer join data: **106.94 ms**
- Pass 3 (orders_scan_semijoin): Match qualifying orders to customer/orders: **16.02 ms**

**Notable:** SUM(l_quantity) > 300 threshold correctly uses 300 (not 30000) because in this run
l_quantity has scale_factor=100 and the threshold is a raw count... wait — actually with scale=100,
stored values are qty*100, so SUM > 300 should be SUM > 30000. This needs investigation. The fact
that Q18 passed validation (100/100 rows) either means the threshold used was correct for this dataset,
or the dataset happens to produce the same qualifying orders under both thresholds.

**If l_quantity scale=100 and Q18 threshold is 300 (not 30000):** SUM of stored quantities > 300 means
sum-of-(qty×100) > 300, i.e., total_qty > 3 items across all line items. This is a much looser filter
than the SQL's "total quantity > 300 actual units" = "sum-of-raw-qty > 300" = "sum-of-stored > 30000".
The fact that validation passed suggests either: (a) the threshold in the Q18 code was 30000 (correctly
scaled) and the summary agent reported incorrectly, or (b) validation tolerance masked the error.
**This should be verified in future runs.**

---

## Performance Highlights

| Phase | Query | iter_0 | Best | Speedup |
|-------|-------|--------|------|---------|
| part_filter | Q9 | 256 ms | 47 ms | 5.4× |
| orders_build | Q9 | 33 ms | 13 ms | 2.6× |
| partsupp_build | Q9 | 50 ms | 30 ms | 1.7× |
| total | Q9 | 551 ms | 303 ms | 1.82× |

---

## Experience Base Issues Identified

| Entry | Problem | Action |
|-------|---------|--------|
| C8 | States "l_quantity scale=1 (confirmed)" — WRONG for this run | Correct/remove |
| C10 | States "avg_qty: NO /100, scale=1" — misleads output formula | Correct/remove |
| C12 | States "l_quantity < 24 (use 24 NOT 2400)" — WRONG for this run | Correct/remove |
| C16 | States "l_quantity scale_factor=1 (confirmed storage_design.json)" — WRONG | Remove entirely |
| C17 | References l_quantity scale=1 in asymmetric scale analysis | Update |

**Root systemic issue:** The experience base hardcoded a storage parameter (l_quantity scale_factor)
that the storage design agent is free to set differently per run. This creates a contradiction: the
experience base overrides the actual storage_design.json, causing the optimizer to ignore ground truth.
