# TPC-H Retrospective — 2026-02-19T23-31-08

## Run Configuration
- **Workload**: TPC-H (Q1, Q3, Q6, Q9, Q18)
- **Scale Factor**: SF-10 (lineitem: 60M rows, orders: 15M, customer: 1.5M, part: 2M, partsupp: 8M)
- **Hardware**: 64-core CPU, AVX-512, HDD, 376 GB RAM

---

## Per-Query Classification

### Q1 — `SUCCESS` ✅
| Iteration | Timing | Validation |
|-----------|--------|------------|
| iter_0    | 249 ms | pass       |
| iter_4    | 57 ms  | pass (best)|
| iter_5    | 68 ms  | pass       |

**Result**: Correct. Improved 3.7× over baseline.
**Dominant phase (final)**: `main_scan` 67.7 ms — vectorized scan of 60 M lineitem rows.
**Note**: iter_4 was the best (57 ms), iter_5 regressed slightly to 68 ms but still well within acceptable range.

---

### Q3 — `SLOW` 🐢
| Iteration | Timing   | Validation |
|-----------|----------|------------|
| iter_0    | 1752 ms  | pass       |
| iter_1    | 799 ms   | pass (first improvement) |
| iter_4    | 798 ms   | pass       |
| iter_5    | 786 ms   | pass (best)|

**Result**: Correct. 2.2× improvement over baseline but plateaued near 786 ms.
**Phase breakdown (iter_5)**:
- `dim_filter`: 105 ms ← regression vs iter_1 (35 ms), 3× slower
- `orders_scan`: 171 ms
- `build_joins`: 113 ms
- `build_bloom`: 23 ms
- `main_scan`: 262 ms (improved from 359 ms in iter_1)
- `part_aggregate`: 32 ms

**Bottleneck**: `dim_filter` regressed from 35 ms → 105 ms between iter_1 and iter_5 while `main_scan` improved proportionally. The optimizer shifted work without net total benefit. The three heaviest phases (`dim_filter` + `orders_scan` + `main_scan`) sum to 538 ms, leaving little room for the remaining budget.

---

### Q6 — `SUCCESS` ✅
| Iteration | Timing  | Validation |
|-----------|---------|------------|
| iter_0    | 170 ms  | pass       |
| iter_1    | 22 ms   | pass (best)|

**Result**: Correct. 7.8× improvement in one iteration.
**Key fix**: `mmap_columns` phase dropped from 101 ms → 0.23 ms (eliminated redundant data copy, P2 pattern).
**Dominant phase (final)**: `main_scan` 11 ms — branch-free vectorized scan with zone-map skip on `l_shipdate`.
**Stopped at iter_1**: No further iterations needed; performance is near-optimal.

---

### Q9 — `SLOW` 🐢
| Iteration | Timing   | Validation |
|-----------|----------|------------|
| iter_0    | 1808 ms  | pass       |
| iter_1    | 607 ms   | pass (best)|
| iter_2    | 627 ms   | pass       |
| iter_3    | 673 ms   | pass       |
| iter_5    | 870 ms   | pass       |

**Result**: Correct. Best was 607 ms at iter_1. Final (iter_5) **regressed to 870 ms** — 43% slower than the best found.
**Phase regression (iter_1 → iter_5)**:
| Phase           | iter_1  | iter_5  | Change  |
|-----------------|---------|---------|---------|
| build_part_bitset | 56 ms | 77 ms  | +37%    |
| build_partsupp  | 103 ms  | 112 ms  | +9%     |
| build_orders    | 35 ms   | 67 ms   | +91%    |
| main_scan       | 338 ms  | 508 ms  | +50%    |

**Root cause of regression**: The optimizer continued exploring for 4 iterations after finding the best solution at iter_1. Later iterations introduced changes (likely different parallelism strategies or hash table sizing for `build_orders` and `main_scan`) that degraded performance. The framework re-executed the final iteration's code (iter_5) rather than the best iteration's code (iter_1).

---

### Q18 — `FAILED` ❌
| Iteration | Timing   | Validation |
|-----------|----------|------------|
| iter_0    | TIMEOUT (300 s) | skipped |
| iter_1    | TIMEOUT (300 s) | skipped |
| iter_2    | TIMEOUT (300 s) | skipped |
| iter_3    | TIMEOUT (300 s) | skipped |
| iter_4    | TIMEOUT (300 s) | skipped |
| iter_5    | TIMEOUT (300 s) | skipped |

**Result**: Failed on every iteration. Never produced output. Validation always skipped.

**Root cause — EMPTY_KEY / memset sentinel mismatch (new pattern)**:

The generated code defines `EMPTY_KEY = INT32_MIN` (`0x80000000`) but initializes hash table slots with `memset(keys, 0x80, sizeof(keys))`, which fills each 4-byte slot with `0x80808080` (= `-2139062144`), NOT `INT32_MIN` (`0x80000000` = `-2147483648`).

```cpp
static constexpr int32_t EMPTY_KEY = INT32_MIN;   // 0x80000000

void init() {
    memset(keys, 0x80, sizeof(keys));  // sets each slot to 0x80808080 ≠ INT32_MIN !
}

inline void insert(int32_t key, double qty) {
    uint32_t s = hash32(key) & MASK;
    // keys[s] starts as 0x80808080; EMPTY_KEY = 0x80000000
    // keys[s] != EMPTY_KEY is ALWAYS TRUE for un-touched slots
    // → infinite probe loop on first insert into any empty table
    while (keys[s] != EMPTY_KEY && keys[s] != key) s = (s + 1) & MASK;
    ...
}
```

The probe loop can never find an empty slot, spinning forever. This affects all three hash map types used in Q18 (`LocalMap`, `MergeMap`, `QualSet`). The first call to `LocalMap::insert()` in Phase 1 (parallel lineitem scan) causes every thread to loop infinitely → 300 s timeout.

**Fix**: Use `std::fill(keys, keys + CAP, INT32_MIN)` instead of `memset(0x80)`, OR change `EMPTY_KEY` to `(int32_t)0x80808080`. The `memset(0x80)` trick only works correctly when the sentinel value is exactly `(int32_t)0x80808080`.

**Plan quality note**: The Q18 plan.json described a correct, well-reasoned algorithm using hash aggregation + index lookups. The logic was sound; only the hash table initialization was broken. Without the bug, this query should execute in ~130 ms per the plan's estimate.

---

## Summary Table

| Query | Status  | Final Timing | Best Timing | Correctness | Key Issue |
|-------|---------|-------------|-------------|-------------|-----------|
| Q1    | SUCCESS | 68 ms       | 57 ms       | ✅          | —         |
| Q3    | SLOW    | 786 ms      | 786 ms      | ✅          | dim_filter phase regression during opt |
| Q6    | SUCCESS | 22 ms       | 22 ms       | ✅          | —         |
| Q9    | SLOW    | 870 ms      | 607 ms      | ✅          | Optimizer regression after iter_1 |
| Q18   | FAILED  | TIMEOUT     | —           | ❌          | EMPTY_KEY ≠ memset(0x80) sentinel → ∞ loop |

- **Queries total**: 5
- **SUCCESS**: 2 (Q1, Q6)
- **SLOW**: 2 (Q3, Q9)
- **FAILED**: 1 (Q18)

---

## Recurring Patterns

### Pattern 1: Optimizer Continues Past Best Solution (Q9, Q3, Q1)
The optimizer kept iterating after finding the optimal or near-optimal solution:
- Q9: Best at iter_1 (607 ms), final at iter_5 (870 ms) — 43% regression
- Q3: Plateaued at 786 ms after iter_1; no meaningful improvement in iters 3-5
- Q1: Best at iter_4 (57 ms), slightly worse at iter_5 (68 ms)

The framework should lock in the best-performing iteration as the final result instead of always using the last iteration's binary.

### Pattern 2: Phase-Level Optimization Without Net Benefit (Q3)
The optimizer improved `main_scan` (359 ms → 262 ms) but simultaneously degraded `dim_filter` (35 ms → 105 ms). Total time barely moved (~799 ms → ~786 ms). Without tracking total time as the optimization objective, individual phase improvements can cancel each other out.

### Pattern 3: memset Sentinel Mismatch → Infinite Loop at t=0 (Q18)
A `memset(buf, 0x80, n)` initialization pattern combined with `EMPTY_KEY = INT32_MIN` is a latent infinite-loop bug. This variant is distinct from C9 (capacity overflow): the loop occurs immediately at 0% load factor because the sentinel byte pattern is wrong. All three hash structures in Q18 shared this bug.
