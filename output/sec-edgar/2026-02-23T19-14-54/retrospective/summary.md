# Retrospective Summary: sec-edgar 2026-02-23T19-14-54

## Overall Results

| Query | Status   | Best Timing | Iters | Root Cause / Notes                                 |
|-------|----------|------------|-------|----------------------------------------------------|
| Q1    | SUCCESS  | 47ms       | 2     | Fast across all phases                             |
| Q2    | SUCCESS  | 92ms       | 6     | iter_2 regressed (190ms); iter_3 reverted to best |
| Q3    | SUCCESS  | 111ms      | 2     | iter_0 failed: TOP-N tiebreaker missing; fixed     |
| Q4    | FAILED   | —          | 6     | Persistent 0-rows + CSV column-shift; never fixed  |
| Q6    | FAILED   | —          | 6     | Persistent ordering bug; compile error in iter_4   |
| Q24   | SLOW     | 192ms      | 6     | Correct; aggregation_merge (60ms) + main_scan (75ms) remain dominant |

- **queries_total**: 6
- **queries_success**: 3 (Q1, Q2, Q3)
- **queries_slow**: 1 (Q24)
- **queries_failed**: 2 (Q4, Q6)

---

## Per-Query Analysis

### Q1 — SUCCESS ✓
- **Best**: iter_1 at **47ms** (hot)
- Phase breakdown: data_loading 0.67ms, main_scan 5.2ms, aggregation_merge 0.9ms, output 0.24ms
- 1 optimization improved from 58ms → 47ms (19% speedup). All phases negligible.

### Q2 — SUCCESS ✓
- **Best**: iter_3 at **92ms** (hot)
- Phase breakdown (iter_3): data_loading 31.6ms, main_scan 37.8ms, aggregation_merge 11.3ms
- iter_2 (190ms) attempted a multi-pass restructure (`build_maxval_map` 60ms + `probe_and_collect` 37ms) — regressed and was correctly discarded per P12.
- iter_3 reverted to single-pass; iter_4 and iter_5 explored further but produced regressions then partial recovery. Best was iter_3.
- data_loading ~30ms is 32% of runtime; P16 zone-map-guided prefetch could help if columns are sparse.

### Q3 — SUCCESS ✓ (after 1 fix)
- **Best**: iter_1 at **111ms** (hot)
- **iter_0 FAILED**: 98/100 rows wrong — name and cik columns mismatched. Expected TOP-N entities in wrong order (e.g., row 2 expected FEDERAL NATIONAL MORTGAGE ASSOCIATION but got HSBC HOLDINGS PLC).
- Root cause: Missing stable sort tiebreaker in the final ORDER BY. When two entities have tied aggregate values, the sort is non-deterministic, producing wrong ordering from position K+1 onward. **(New C33)**
- iter_1 fixed the comparator; validation passed. Single-iteration recovery.

### Q4 — FAILED ✗ (all 6 iterations)
- **Never passed validation across 6 iterations.**

**Bug 1 — Filter elimination (C25), iter_0, 1, 3, 4, 5:**
- rows_actual=0, rows_expected=500. The optimizer repeatedly introduced or failed to fix a filter that eliminated all rows. The condition (likely a date or string filter on a pre-filter dimension) was incorrectly set.
- iter_3 and iter_4 show the optimizer tried different approaches but kept returning to 0-row results without a passing baseline to revert to.

**Bug 2 — CSV column-shift, iter_2:**
- rows_actual=500 but all 500 rows wrong. Columns `tlabel` and `stmt` are shifted:
  - Expected: `tlabel="Equity, Including Portion Attributable to Noncontrolling Interest"`, `stmt="EQ"`
  - Actual: `tlabel="Equity"`, `stmt="Including Portion Attributable to Noncontrolling Interest"`
- Root cause: string column values containing commas were output **without double-quote wrapping** in the CSV printf. The validator's CSV parser splits at the unescaped comma, producing a column offset for every row where `tlabel` contains a comma. **(New C31)**
- Fix: always `printf("\"%s\"", str)` for all dictionary-decoded or string output columns.

### Q6 — FAILED ✗ (all 6 iterations)
- **Never passed validation across 6 iterations.**

**Bug 1 — Persistent TOP-N ordering (iter_0–3, 5):**
- rows_actual=200 (correct count), but 97–98/200 rows differ. Rows 0–17 are correct; rows 18+ are the wrong entities.
- The consistent break point and same error entities across all 5 iterations confirms a **deterministic but wrong sort comparator**. The `std::partial_sort` uses `sum_cents DESC` as the sole key; when multiple entities have the same sum_cents, their relative order is non-deterministic vs. the reference. **(C33 applies)**
- Fix: append a stable secondary key (e.g., `cik` or `entity_name` lexicographic) to the comparator.

**Bug 2 — Compile error, iter_4:**
- `is_mask`, `is_cap`, `is_ht` referenced inside main_scan loop body but never declared in that scope.
- Root cause: incomplete refactoring of pre-built index usage. The index file (`is_hash_raw`) was mmap'd correctly, but the **header parsing block** (extracting `is_cap`, computing `is_mask = is_cap - 1`, and casting `is_ht` pointer) was omitted. **(C27 freq increment)**
- iter_5 fixed this by adding the three declarations before the loop.

**Bug 3 — aggregation_merge 300ms+:**
- aggregation_merge cost: 300–310ms across ALL passing iterations (iter_0–3, 5).
- This is the P17/P20 anti-pattern: a shared aggregation map is being merged sequentially across threads. Thread-local merge was never applied.
- This is simultaneously a correctness blocker (fails to pass validation) AND a performance issue. Even if correctness were fixed, 300ms aggregation_merge is unacceptable.

### Q24 — SLOW ✓
- **Best**: iter_4 at **192ms** (hot). Passes validation on all 6 iterations.
- Phase breakdown (iter_4): data_loading 31ms (16%), main_scan 75ms (39%), aggregation_merge 60ms (31%), sort_topk 3ms
- Big jump at iter_2: 613ms → 205ms (3× speedup). No further meaningful improvement (iters 3–5 plateau at 192–202ms).
- **Remaining bottlenecks**:
  - `aggregation_merge` 60ms: thread-local merge may be applied (P20) but AGG_CAP likely oversized per P25. Size AGG_CAP to actual group cardinality.
  - `main_scan` 75ms: likely LLC cache thrashing from over-large hash tables during probe phase; apply P23 + P26 bloom pre-filter.

---

## Dominant Failure Patterns (Ranked by Impact)

### 1. TOP-N Ordering Non-Determinism — NEW C33
**Queries**: Q3 iter_0, Q6 all iters
**Symptom**: Row count correct; first K rows correct; rows K+1…N are wrong entities permuted among themselves.
**Root cause**: `std::sort` / `std::partial_sort` with only one sort key (e.g., `sum_cents DESC`). Tied values produce non-deterministic ordering.
**Fix**: Always add a unique, stable tiebreaker (cik, entity_id, adsh, or lexicographic name) as the LAST comparator key.

### 2. CSV Comma-in-String Column Shift — NEW C31
**Queries**: Q4 iter_2
**Symptom**: Correct row count; all string columns wrong by exactly one column offset.
**Root cause**: `printf("%s", str)` when `str` contains commas → CSV parser splits on comma → all subsequent columns shift.
**Fix**: Always quote-wrap: `printf("\"%s\"", str)` for all string/dictionary output columns.

### 3. Filter Elimination (C25) — freq 2→3
**Queries**: Q4 (5/6 iters)
**Symptom**: rows_actual=0, rows_expected>0.
**Fix**: Revert to last passing filter logic. Without a passing baseline, the optimizer has no safe anchor.

### 4. Pre-Built Index Incomplete Header Parse (C27) — freq 1→2
**Queries**: Q6 iter_4
**Symptom**: Compile error 'is_mask/is_cap/is_ht not declared in this scope'.
**Fix**: All 5 elements of index usage must be present atomically: file mmap, header parse (cap, mask, ht pointer), hash function, probe loop. Missing header parse → undeclared variables.

### 5. aggregation_merge Bottleneck (P17/P20) — freq 4→5 / 2→3
**Queries**: Q6 (300ms), Q24 (60ms)
**Fix**: Thread-local per-key reduction by default. Never use shared aggregation map. Apply P23 to size AGG_CAP to actual group cardinality.
