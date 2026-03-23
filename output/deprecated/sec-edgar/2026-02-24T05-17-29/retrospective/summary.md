# Run Retrospective — sec-edgar 2026-02-24T05-17-29

## Overall Results

| Query | Class   | Best (ms) | Best Iter | Iters Used | Dominant Bottleneck (best iter)          |
|-------|---------|-----------|-----------|------------|------------------------------------------|
| Q1    | SUCCESS | 17        | 0         | 1          | None — trivial, passed immediately       |
| Q2    | SUCCESS | 98        | 5         | 6          | build_joins 32ms + data_loading 28ms     |
| Q3    | SUCCESS | 58        | 2         | 3          | main_scan 20ms + aggregation_merge 6ms   |
| Q4    | SUCCESS | 124       | 2         | 6          | data_loading 34ms + main_scan 37ms       |
| Q6    | SLOW    | 188       | 5         | 6          | main_scan 140ms (memory-bandwidth bound) |
| Q24   | SUCCESS | 95        | 0         | 1          | data_loading 39ms                        |

**Correctness: 100% clean — zero validation failures across all 23 executions.**

---

## Per-Query Analysis

### Q1 — SUCCESS (17ms, iter_0)
- Correct and fast on first generation. 12 groups (stmt × rfile), simple COUNT/AVG aggregation.
- `data_loading=0.07ms`, `main_scan=4ms`, `aggregation_merge=1.6ms`. No iteration needed.

### Q2 — SUCCESS (98ms, iter_5)
**Optimization path:**
- iter_0/1: `aggregation_merge=67ms` (shared map across 64 threads — high contention).
- iter_2: Thread-local aggregation applied → `aggregation_merge=19ms` (-72%), BUT `build_joins=34ms` appeared (new subquery phase). Net: 155ms → 101ms.
- iter_4: Regression — `aggregation_merge` jumped back to 87ms (optimizer reverted to shared map).
- iter_5: Thread-local re-applied → best 98ms.

**Key lesson:** Thread-local aggregation delivered 72% merge reduction. However the optimizer oscillated — iter_4 reverted the thread-local pattern, then iter_5 fixed it. Build_joins is now the co-bottleneck at 32ms alongside data_loading at 28ms.

### Q3 — SUCCESS (58ms, iter_2)
**Optimization path:**
- iter_0: `main_scan=515ms` — catastrophic (likely single-threaded sequential scan over 39M num rows).
- iter_1: Parallelism + `dim_filter` pre-filter stage → 72ms (-86%). `main_scan=25ms`, `aggregation_merge=9ms`.
- iter_2: Further improved to 58ms — `main_scan=20ms`, `aggregation_merge=6ms`.

**Key lesson:** This query had the largest single-iteration speedup (7.4×). The iter_0 single-threaded scan was the only major inefficiency; once parallelized, performance is good.

### Q4 — SUCCESS (124ms, iter_2)
**Optimization path:**
- iter_0: 175ms — `data_loading=41ms`, `main_scan=57ms`, `aggregation_merge=25ms`.
- iter_1: 182ms (regression) — `sort_aggregate=78ms` replaced hash aggregation (wrong direction).
- iter_2: 124ms (best) — hash aggregation with thread-local maps, AGG_CAP=4096, `aggregation_merge=13ms`.
- iter_3: 136ms (regression) — `pre_build_qual` filter phase added, main_scan overhead increased.
- iter_4: 128ms — back near iter_2 best.
- iter_5: 150ms (regression) — `aggregation_merge=59ms` spike (shared map reintroduced).

**Remaining bottleneck:** `data_loading` is stubbornly 34-41ms across ALL iterations. This is the multi-index mmap overhead (sub_adsh_hash + tag_pair_hash + pre_triple_hash + 5 num columns). P27 concurrent madvise was applied but disk I/O on HDD limits the floor.

### Q6 — SLOW (188ms, iter_5)
**Optimization path:**
- iter_0: `aggregation_merge=350ms` (catastrophic shared-map contention, 50K groups × 64 threads).
- iter_1: Thread-local aggregation → 314ms (`aggregation_merge=13ms`, but `data_loading=150ms` emerged as bottleneck).
- iter_2: 364ms regression — main_scan worsened to 128ms.
- iter_3: Bloom filter attempted (`build_bloom=19ms`) — reduced main_scan to 87ms but data_loading still 134ms; net 346ms.
- iter_4: 306ms — `data_loading=94ms` (partial improvement).
- iter_5: **188ms (best)** — `data_loading=31ms` via parallel dict loading across 5 OpenMP sections for uom/name/stmt/tag/plabel dicts. `aggregation_merge=4ms`. `main_scan=140ms` is now the sole bottleneck.

**Remaining bottleneck:** `main_scan=140ms` — this is the 5-table join (num→sub, num→pre, pre→tag) via pre-built hash indexes, 39M num rows, 50K group combinations. The random-access pattern into pre_triple_hash index (9.6M rows) is LLC cache thrashing on HDD. Further gains require zone-map skip or column pruning.

**Key discovery:** Parallel dict loading (`#pragma omp parallel sections num_threads(5)`) for 5 dict files reduced sequential dict I/O (~70ms) to effectively parallel → data_loading 132ms → 31ms. This is a new reusable optimization pattern (see P29).

### Q24 — SUCCESS (95ms, iter_0)
- Correct and fast on first generation. `data_loading=39ms` (HDD cold), `main_scan=33ms`, `aggregation_merge=3ms`.
- No optimization iterations needed. Thread-local aggregation was applied from the start (5K groups).

---

## Cross-Query Patterns

### Correctness
- **Zero failures.** No date bugs (C1/C7/C11), no hash table overflow (C9/C24), no dict decode errors (C2/C18), no GROUP BY key omissions (C15/C30), no sentinel issues (C20).
- C29 (int64_t cents for large-magnitude doubles) was apparently handled correctly in all queries — no precision failures reported.

### Performance Patterns Observed
1. **aggregation_merge domination at scale** (Q6 iter_0: 350ms, Q2 iter_0: 67ms) — shared maps with 50K and 80K groups at 64 threads are catastrophic. Thread-local merge is mandatory (P17/P20).
2. **Optimizer oscillation on aggregation_merge** (Q2 iter_4: 87ms, Q4 iter_5: 59ms after achieving 19ms and 13ms respectively) — once thread-local merge validates, subsequent iters must not revert to shared map.
3. **Parallel dict loading** (Q6 iter_5: 132ms → 31ms) — queries referencing 3+ dictionary columns benefit from concurrent dict loading. New pattern P29.
4. **HDD data_loading floor** — data_loading 28-41ms is consistently the floor for queries hitting multiple large index files. P27 concurrent madvise helps but HDD bandwidth is the hard limit.
5. **Single-threaded scan penalty** (Q3 iter_0: 515ms → 25ms) — parallelism is mandatory on the 39M-row num table.

---

## Proposals Summary
See proposals.json for structured entries. Key proposals:
- **P29**: Parallel dict loading for queries with 3+ dict columns (observed 70-100ms savings).
- **C34**: Lock-in rule — once thread-local aggregation proves best, optimizer must not revert (behavioral guard).
- Increment P17 freq (Q6 and Q2 wins), P27 freq (Q6 iter_5), P16 freq (Q6 data_loading targeted).
