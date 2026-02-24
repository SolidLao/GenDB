# Stage B Retrospective — sec-edgar 2026-02-23T02-55-41

## Run Overview
| Query | Classification | iter_0 (ms) | Final (ms) | Best (ms) | Validation | Primary Bottleneck |
|-------|---------------|-------------|------------|-----------|------------|-------------------|
| Q1    | **SUCCESS**   | 134.9       | 37.9       | 37.9      | pass (iter_1) | — |
| Q2    | **SUCCESS**   | 123.9       | 98.6       | 77.2      | pass (all)    | build_joins (38ms) — mild regression from best |
| Q3    | **FAILED**    | 345.2       | 351.8      | N/A       | fail (all 5)  | Floating-point precision in double accumulation |
| Q4    | **SUCCESS**   | 3225.0      | 141.5      | 141.5     | pass (all)    | — (22.8x improvement via pre-built index) |
| Q6    | **SLOW**      | 831.5       | 546.5      | 546.5     | pass (all)    | main_scan (298ms) + build_joins (60ms) |
| Q24   | **SLOW**      | 52176.2     | 61858.3    | 22714.2   | pass (all)    | build_joins dominates (19–65s) |

**Meta:** 6 queries — 3 SUCCESS, 2 SLOW, 1 FAILED

---

## Per-Query Analysis

### Q1 — SUCCESS ✓
- **iter_0→iter_1**: 134.9ms → 37.9ms (3.6x improvement)
- `aggregation_merge`: 49.1ms → 1.7ms — thread-local per-key reduction applied (P17)
- `main_scan`: 41.5ms → 15.9ms — scan tightened, likely SIMD or early filter
- No further optimization iterations run (already fast)

### Q2 — SUCCESS ✓ (mild regression from best)
- **iter_0→best(iter_3)**: 123.9ms → 77.2ms; final (iter_5): 98.6ms
- iter_3 had `data_loading` ~0ms (cached/madvised) vs iter_0 77ms — great improvement
- iter_4 regression: `build_joins` jumped from 10ms → 151ms (restructure backfired); iter_5 recovered to 38ms
- P12 tracking caught the regression and partially recovered but not to best
- Final is still 21% worse than best iter_3

### Q3 — FAILED ✗ (persistent, all 5 iterations)
- **Root cause: double-precision overflow for large SEC-EDGAR financial values**
- The `value.bin` column stores financial data as IEEE 754 double. Values up to ~2×10¹⁵
  (e.g. `200008366377752.72`) require **17 significant digits** — beyond double's 15.95-digit
  capacity. When accumulated and printed with `%.2f`, the last digit is non-deterministic
  depending on summation order.
- Kahan summation was applied (iter_1+) but does NOT fix the problem — Kahan reduces
  catastrophic cancellation, not absolute magnitude precision loss.
- The 3 failing rows are always the same large-value companies (10X CAPITAL ~2×10¹⁴,
  AMERICAN INT'L ~7×10¹³). Off-by-one in the last decimal place (`0.66` vs `0.72`,
  `0.83` vs `0.84`, `0.19` vs `0.20`).
- **Fix needed**: accumulate using `int64_t` with an implicit scale factor (e.g., multiply
  by 100 before accumulation, keep as integer sum), then convert to string for output with
  `printf("%" PRId64 ".%02" PRId64, sum/100, sum%100)`. This preserves exact cents.

### Q4 — SUCCESS ✓ (excellent)
- **iter_0→iter_3**: 3225ms → 141.5ms (22.8x improvement)
- iter_0 build_joins: 2779ms — brute-force hash build from raw table
- iter_2: pre-built mmap index applied → build_joins 2779ms → 20ms (P11 triggered)
- iter_3: further tuning → 247ms → 141ms
- Textbook P11 success case

### Q6 — SLOW (34% improvement, still above target)
- **iter_0→iter_5**: 831ms → 546ms
- `aggregation_merge`: 320ms → 3ms (P17 applied, huge win)
- `build_joins`: 116ms → 60ms (moderate improvement)
- `main_scan`: 111ms → 298ms — **main_scan grew** as build_joins shrank (work shifted)
- `output`: 7ms → 35ms — output phase grew, suspect large result set serialization
- Remaining bottleneck: main_scan at ~300ms. Zone-map skip or predicate pushdown
  may help. output at 35ms is also surprising — check if materializing too many rows.

### Q24 — SLOW (best 22.7s, final regressed to 61.9s)
- **iter_0→iter_2**: 52176ms → 22714ms (2.3x improvement); final iter_5: 61858ms (regression)
- `build_joins` is 85-96% of total runtime in every iteration (19-65s out of 22-68s total)
- Root cause: PRE hash table at CAP=2^25=33M slots. PreSlot has multiple fields →
  table footprint ~500-700MB → severe LLC thrashing during parallel CAS build
- iter_4/5 regression: build_joins jumped from ~19s back to 59-65s — optimizer changed
  hash table structure or parallelism that hurt CAS throughput
- No pre-built index for this join type was used (P11 not applied here)
- **Fix needed**: Use a pre-built mmap hash index for the PRE table (P11), or partition
  the build phase by hash prefix to reduce per-thread working set to L3-cache size

---

## Cross-Query Patterns

| Pattern | Queries | Observation |
|---------|---------|-------------|
| P17 (thread-local aggregation_merge) | Q1, Q6 | aggregation_merge 49ms→2ms, 320ms→3ms — always apply |
| P11 (pre-built join index) | Q4 | build_joins 2779ms→20ms; Q24 still needs this |
| P12 regression tracking | Q2, Q24 | best was found mid-run but not preserved at end |
| C29 (double precision SEC-EDGAR) | Q3 | New class of failure — int64_t accumulation needed |
| Large hash table cache thrash | Q24 | 33M-slot table → 60s build; need P11 or partitioned build |
