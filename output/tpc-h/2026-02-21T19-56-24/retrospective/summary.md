# TPC-H Retrospective — 2026-02-21T19-56-24
**Workload:** TPC-H SF10 | **Queries:** Q1, Q3, Q6, Q9, Q18

## Overall Results

| Query | Classification | Best Timing | Best Iter | Final Iter Timing | Validation Failures |
|-------|---------------|-------------|-----------|-------------------|---------------------|
| Q1    | SUCCESS       | 427 ms      | iter_4    | 475 ms (iter_5)   | None                |
| Q3    | SUCCESS       | 414 ms      | iter_3    | 3512 ms (iter_5)  | None                |
| Q6    | SUCCESS       | 101 ms      | iter_1    | 152 ms (iter_5)   | None                |
| Q9    | SLOW          | 1197 ms     | iter_4    | 2120 ms (iter_5)  | None                |
| Q18   | SLOW          | 1139 ms     | iter_4    | compile fail (iter_5) | iter_1 runtime fail, iter_5 compile fail |

**Total: 3 SUCCESS, 2 SLOW, 0 FAILED**

All queries that ran produced correct results (validation always passed when run completed).

---

## Per-Query Analysis

### Q1 — SUCCESS (427 ms)

**Iteration timeline:**

| Iter | Total   | data_loading | dim_filter | main_scan | output  |
|------|---------|-------------|------------|-----------|---------|
| 0    | 8628 ms | 8578 ms     | 13 ms      | 36 ms     | 1 ms    |
| 1    | 943 ms  | 904 ms      | 10 ms      | 30 ms     | 586 ms  |
| 2    | 444 ms  | 78 ms       | 25 ms      | 342 ms    | 2 ms    |
| 3    | 495 ms  | 93 ms       | 25 ms      | 377 ms    | 0.3 ms  |
| 4 ✓  | 427 ms  | 59 ms       | 25 ms      | 343 ms    | 0.6 ms  |
| 5    | 475 ms  | 75 ms       | 25 ms      | 375 ms    | 0.3 ms  |

**Key findings:**
- **iter_0:** data_loading=8578 ms — full `madvise(MADV_WILLNEED)` on entire LINEITEM column files (cold start, P13-pattern). Fixed in iter_1.
- **iter_1:** data_loading improved to 904 ms but output jumped to 586 ms — likely slow string formatting that was fixed in iter_2.
- **iter_2→4:** data_loading stabilized at 60–90 ms; main_scan stabilized at ~340–375 ms.
- **Oscillation (P12):** Best was iter_4 (427 ms). iter_3 and iter_5 both regressed (~475–495 ms). main_scan variance is ~32 ms — likely from micro-optimization attempts changing vector loop structure.
- **Bottleneck at best:** main_scan dominates (343 ms / 427 ms total = 80%). This is inherent for a full-LINEITEM scan with 7 arithmetic aggregates at SF10 (~60M rows). dim_filter cost of 25 ms is stable but perhaps improvable with better zone-map usage.

---

### Q3 — SUCCESS (414 ms)

**Iteration timeline:**

| Iter | Total   | data_loading | dim_filter | build_joins | main_scan | agg_merge | sort_topk |
|------|---------|-------------|------------|-------------|-----------|-----------|-----------|
| 0    | 6144 ms | 1292 ms     | 10 ms      | 81 ms       | 98 ms     | 1678 ms   | 29 ms     |
| 1    | 1476 ms | 1150 ms     | 4 ms       | 122 ms      | 103 ms    | 28 ms     | 9 ms      |
| 2    | 525 ms  | 208 ms      | 12 ms      | 118 ms      | 96 ms     | 26 ms     | 9 ms      |
| 3 ✓  | 414 ms  | 208 ms      | 13 ms      | 96 ms       | 28 ms     | 33 ms     | 7 ms      |
| 4    | 412 ms  | 130 ms      | 13 ms      | 87 ms       | 23 ms     | 32 ms     | 6 ms      |
| 5    | 3512 ms | 145 ms      | 12 ms      | 85 ms       | 33 ms     | 1039 ms   | 6 ms      |

**Key findings:**
- **iter_0:** aggregation_merge=1678 ms — likely std::unordered_map or std::map for grouping (P1/P7). Fixed in iter_1.
- **iter_0→2:** data_loading dominating (1150–1292 ms); resolved by iter_2 after madvise optimization.
- **iter_3:** main_scan dropped from 96 ms to 28 ms — a 3.4x improvement from better join probe strategy.
- **Severe oscillation (P12):** iter_5 aggregation_merge exploded from 33 ms → 1039 ms (31x!), total 414→3512 ms (8.5x regression). The optimizer introduced a slower aggregation strategy (possibly reverting to std::map or a lock-contending parallel map). This was the worst oscillation in this run. Since improved=false, the best (iter_3/4 at 414 ms) is the delivered result.
- **Bottleneck at best:** build_joins (96 ms) + data_loading (208 ms) are the next targets. build_joins at 96 ms for CUSTOMER+ORDERS hash tables is reasonable; data_loading may still have LINEITEM not fully prefetch-optimized.

---

### Q6 — SUCCESS (101 ms)

**Iteration timeline:**

| Iter | Total   | data_loading | main_scan | output  |
|------|---------|-------------|-----------|---------|
| 0    | 2095 ms | 1962 ms     | 9 ms      | 21 ms   |
| 1 ✓  | 101 ms  | 45 ms       | 42 ms     | 0.3 ms  |
| 2    | 129 ms  | 64 ms       | 53 ms     | 0.2 ms  |
| 3    | 147 ms  | 114 ms      | 8 ms      | 0.4 ms  |
| 4    | 171 ms  | 137 ms      | 12 ms     | 1.4 ms  |
| 5    | 152 ms  | 118 ms      | 8 ms      | 0.4 ms  |

**Key findings:**
- **iter_0:** data_loading=1962 ms (madvise WILLNEED on all LINEITEM columns cold). Fixed in iter_1.
- **iter_0 output=21 ms:** Unusually high for a single-row aggregate output; likely CSV flushing overhead. Fixed in iter_1.
- **Persistent oscillation (P12):** iter_1 achieved 101 ms (the best). Every subsequent iter was strictly worse (29%→46%→70%→51% regressions). The optimizer never recovered to iter_1's baseline. This is the clearest P12 example in this run — 4 consecutive regressions with no recovery.
- **Bottleneck at best:** iter_1 is balanced: data_loading=45 ms, main_scan=42 ms. main_scan=42 ms for a filtered LINEITEM scan (date range + discount + quantity) at 60M rows suggests no zone-map skip was applied. If zone maps cover shipdate, applying block-skip could halve main_scan.
- **data_loading trend iter_2→5:** 64→114→137→118 ms — the optimizer inexplicably kept increasing prefetch scope after iter_1, wasting I/O bandwidth.

---

### Q9 — SLOW (1197 ms)

**Iteration timeline:**

| Iter | Total   | data_loading | dim_filter | build_joins | index_load | main_scan | output |
|------|---------|-------------|------------|-------------|------------|-----------|--------|
| 0    | 7336 ms | 0 ms        | 41 ms      | 565 ms      | —          | 50 ms     | 2 ms   |
| 1    | 3655 ms | 0 ms        | 31 ms      | 245 ms      | —          | 49 ms     | 2 ms   |
| 2    | 2025 ms | 0 ms        | 36 ms      | 223 ms      | 1418 ms    | 44 ms     | 6 ms   |
| 3    | 3227 ms | 0 ms        | 39 ms      | 332 ms      | 2404 ms    | 30 ms     | 1 ms   |
| 4 ✓  | 1197 ms | 77 ms       | 24 ms      | 206 ms      | 654 ms     | 30 ms     | 1 ms   |
| 5    | 2120 ms | 49 ms       | 13 ms      | 1806 ms     | 0 ms       | 32 ms     | 14 ms  |

**Key findings:**
- **Large unaccounted time in iter_0–1 (P14/P15):** iter_0 phases sum to ~658 ms, but total=7336 ms — **~6679 ms of aggregation/groupby was completely uninstrumented**. The optimizer had no diagnostic signal for 91% of runtime. Q9 has a complex `GROUP BY nation, year` aggregation over LINEITEM+PARTSUPP+ORDERS+SUPPLIER+PART+NATION — none of this was wrapped in GENDB_PHASE in early iterations. This is a severe P14 gap.
- **Index strategy instability:** After iter_2 introduced an index_load phase, its cost swung wildly: 1418 ms → 2404 ms → 654 ms → 0 ms (dropped entirely in iter_5). The optimizer kept changing which index to use, causing the index_load variance to drive regressions. When index_load was dropped in iter_5, build_joins exploded from 206 ms to 1806 ms — the optimizer eliminated the index but forgot to substitute a runtime-built alternative efficiently.
- **build_joins ceiling at ~200–565 ms:** Even at best, build_joins=206 ms for SUPPLIER+PART+NATION hash tables is expensive. With SF10 PART=2M rows, this suggests a non-optimal hash table implementation.
- **data_loading=0 in iters 0–3:** No explicit data_loading phase — all I/O was hidden inside other phases (P14).
- **Oscillation (P12):** iter_3 regressed from 2025→3227 ms; iter_5 regressed from 1197→2120 ms.

---

### Q18 — SLOW (1139 ms)

**Iteration timeline:**

| Iter | Total    | data_loading | subquery_pre | main_scan | Outcome      |
|------|----------|-------------|-------------|-----------|--------------|
| 0    | 7485 ms  | 341 ms      | 329 ms      | 6704 ms   | Pass         |
| 1    | —        | 333 ms      | 294 ms      | —         | **RUN FAIL** |
| 2    | 1173 ms  | 629 ms      | 332 ms      | 126 ms    | Pass         |
| 3    | 1229 ms  | 70 ms       | 383 ms      | 727 ms    | Pass         |
| 4 ✓  | 1139 ms  | 49 ms       | 440 ms      | 589 ms    | Pass         |
| 5    | —        | —           | —           | —         | **COMPILE FAIL** |

**Key findings:**
- **iter_0:** main_scan=6703 ms — O(n²) nested-loop join between LINEITEM subquery results and ORDERS+LINEITEM. Fixed in iter_2 with hash-based lookup.
- **iter_1 runtime failure (new pattern — C25):** Generated code opened `lineitem_orderkey_positions.bin` which does not exist in the index layout. This is different from C22 (path construction error) — the optimizer *hallucinated* an index file name not present in the Query Guide's Index Layouts. stderr: `No such file or directory`. This caused a skipped validation and wasted one optimization cycle.
- **iter_5 compile failure (new pattern — C26):** `ord_raw` was not declared in scope at line 310. The optimizer declared the mmap pointer inside a conditional block but referenced it in cleanup code outside the block. Three related symbols had this scope error. A wasted cycle at the end of optimization.
- **subquery_precompute growing (383→440 ms, iter_3→4):** The `GROUP BY l_orderkey HAVING SUM(l_quantity) > 300` subquery processes ~60M LINEITEM rows. Cost increased 15% from iter_3 to iter_4 while main_scan improved. This suggests the optimizer traded subquery efficiency for main-scan efficiency — a valid trade but the subquery remains a bottleneck at 39% of total time.
- **data_loading spike at iter_2 (629 ms):** Possibly due to loading both LINEITEM and ORDERS column files with madvise. Resolved in iter_3–4.

---

## Cross-Cutting Patterns

### Pattern A: Optimizer Oscillation (P12) — Universal
All 5 queries showed timing regressions after reaching the best iteration:
- Q1: iter_3 regressed 16% from iter_2 best; iter_5 regressed 11% from iter_4 best
- Q3: iter_5 regressed **8.5×** from iter_3 best (414→3512 ms) — agg_merge blowup
- Q6: iters 2–5 ALL regressed from iter_1; never recovered (4 consecutive regressions)
- Q9: iter_3 regressed 59% from iter_2; iter_5 regressed 77% from iter_4
- Q18: iter_3 regressed 5% from iter_2; iter_5 compile-failed

P12 is documented in experience.md but the revert logic is clearly not triggering fast enough. The Q6 and Q3 cases show that once a regression path begins, the optimizer cannot self-correct.

### Pattern B: Cold-Start data_loading Dominance
Every query's iter_0 was dominated by data_loading (8578 ms, 1962 ms, 1292 ms). The first iteration is always a throwaway. This is expected but confirms P13/P14 guidance is not yet in the initial code-generation template.

### Pattern C: Q9 Uninstrumented Aggregation (6.7 seconds invisible)
Q9's iters 0–1 had 90% of runtime invisible to the optimizer. No GENDB_PHASE for the aggregation/sort phases. The optimizer was targeting the wrong phases (build_joins) while the real bottleneck (aggregation) was unobserved. P14 covers data_loading but not aggregation/sort.

### Pattern D: Hallucinated Index File (Q18 iter_1)
The optimizer invented `lineitem_orderkey_positions.bin` — an index that was never built. The Query Guide explicitly lists available indexes; the code generator referenced a non-existent one.

### Pattern E: Variable Scope in Generated Cleanup Code (Q18 iter_5)
When an optimizer refactors code to conditionally use an index, variables declared inside the conditional block are later referenced in unconditional cleanup code (munmap/close). Three related symbols had this error.

---

## Proposals Summary

| ID  | Type             | Priority | Description                                           |
|-----|-----------------|----------|-------------------------------------------------------|
| C25 | correctness      | HIGH     | Hallucinated index file names not in Query Guide      |
| C26 | correctness      | HIGH     | Variable scope error in generated cleanup code        |
| P15 | performance      | HIGH     | Missing GENDB_PHASE for aggregation/sort phases       |
| P12u| performance      | MEDIUM   | Strengthen P12 oscillation revert — immediate revert  |
| P16 | performance      | MEDIUM   | Index strategy instability in multi-join queries      |
