# TPC-H Run Retrospective — 2026-02-20T05-23-51

## Overall Results

| Query | Status  | Final Timing | Iterations Used |
|-------|---------|-------------|-----------------|
| Q1    | SUCCESS | 49.82 ms    | 2               |
| Q3    | FAILED  | N/A         | 5 (all fail)    |
| Q6    | SUCCESS | 67.24 ms    | 5               |
| Q9    | SUCCESS | 100.87 ms   | 5               |
| Q18   | SUCCESS | 128.45 ms   | 2               |

**4/5 queries succeeded. 1 query (Q3) failed in every iteration.**

---

## Per-Query Analysis

### Q1 — SUCCESS (49.82 ms)

**Optimization path:** 153.75 ms → 50.67 ms → 49.82 ms

**Root cause of iter_0 slowness:** `mmap_columns` cost 108.53 ms in iter_0 — classic P2 pattern
(copying mmap'd data into a vector). Fixed in iter_1; mmap_columns dropped to 0.10 ms.

**Remaining bottleneck:** `main_scan` (~49.6 ms). This is a full scan of the lineitem table at
SF10 and is unlikely to be reduced further without zone-map pruning or SIMD vectorization.

---

### Q3 — FAILED (all 5 iterations)

**Failure log:**

| Iteration | Failure Mode                             | Duration   |
|-----------|------------------------------------------|------------|
| iter_0    | Process killed by signal (crash)         | 530 ms     |
| iter_1    | Timeout                                  | 300,179 ms |
| iter_2    | Missing files (`sf10.gendb` path wrong)  | 138 ms     |
| iter_3    | Timeout                                  | 300,113 ms |
| iter_4    | Timeout                                  | 300,115 ms |

**Root cause 1 — Infinite loop (iter_0, iter_1, iter_3, iter_4):**
Signal kill (530 ms) followed by three 300 s timeouts is the hallmark of an infinite loop in hash
table probing. Two known causes:
- **C9:** Open-addressing hash table without resize or max-probe limit hits 100% load and loops
  forever. Q3 joins CUSTOMER (~1.5M rows at SF10) × ORDERS (~15M rows) × LINEITEM (~60M rows);
  any hash table sized for the wrong cardinality overflows.
- **C20:** `memset(buf, 0x80, n)` on int32_t/int64_t keys sets each byte to 0x80, producing
  0x80808080 per slot — not INT32_MIN. Probing never finds the sentinel and loops forever.

The optimizer could not escape this failure because no [TIMING] output was ever produced —
there was no signal to guide the next iteration.

**Root cause 2 — Wrong database path (iter_2):**
Generated code hard-coded `sf10.gendb` as the database directory name. This path does not exist;
the actual database lives under a different naming convention. The code generator inferred the
directory name from the scale factor rather than using the canonical path from the Query Guide.
This is a new failure class not previously documented in the experience base.

---

### Q6 — SUCCESS (67.24 ms)

**Optimization path:** 196.06 ms → 93.65 ms → 69.04 ms → **152.40 ms (regression)** → 72.41 ms → 67.24 ms

**Key improvement:** main_scan reduced from 32.74 ms to 26.00 ms. Large initial overhead (196 ms
total vs ~27 ms scan) was eliminated by iter_1 — likely fixed column loading or removed an
unnecessary sort.

**Oscillation in iter_3:** Timing jumped from 69.04 ms back to 152.40 ms, then recovered to
72.41 ms in iter_4 and 67.24 ms in iter_5. The optimizer wasted one iteration. There is no
zone_map_load [TIMING] entry for Q6 in any iteration — mmap_columns is also absent, suggesting
Q6 only touches one column group.

---

### Q9 — SUCCESS (100.87 ms)

**Optimization path:** 334.02 ms → 318.91 ms → 208.55 ms → 167.51 ms → **220.38 ms (regression)** → 100.87 ms

**Key improvements:**
- `build_joins`: 53.11 ms → 20.41 ms (better join key strategy)
- `build_ordermap`: 150.72 ms → 27.06 ms (O(n log n) sort replaced with O(n) hash, likely)
- `main_scan`: 50.21 ms → 46.12 ms (modest)

**Oscillation in iter_4:** Jumped from 167.51 ms to 220.38 ms (added `build_ps_compact` at
32.84 ms unnecessarily), recovered to 100.87 ms in iter_5 with a cleaner approach.

**Compile warnings:** Uninitialized variable warnings appeared in every iteration but did not
affect correctness. Sloppy code generation pattern that should be fixed upstream.

**Remaining bottleneck:** `main_scan` (46.12 ms) and `build_ordermap` (27.06 ms) are co-dominant.

---

### Q18 — SUCCESS (128.45 ms)

**Optimization path:** 308.5 ms → 182.1 ms → 128.45 ms

**Key improvements:**
- `phase1_subquery`: 226.00 ms → 85.87 ms (~2.6× improvement)
- `phase2_main`: 82.17 ms → 42.16 ms (~2× improvement)

**Compile warnings:** `date_utils.h sprintf warning` appeared in all 3 iterations. Cosmetic; did
not affect correctness.

**Remaining bottleneck:** `phase1_subquery` (85.87 ms) is still dominant. A pre-built hash
index on `l_orderkey` (if available in the Query Guide) could eliminate this cost.

---

## Cross-Query Patterns

### Pattern 1: Optimizer Oscillation
Both Q6 (iter_2→iter_3: 69 ms → 152 ms) and Q9 (iter_3→iter_4: 168 ms → 220 ms) showed a
one-step regression before recovering. The optimizer is not preserving the best-performing
validated code version and re-applying it when a new attempt regresses. This cost each query
one wasted iteration.

### Pattern 2: Q3 Hash Table Infinite Loop — Unresolved Across All 4 Crash/Timeout Iterations
The optimizer received zero timing data from all crash/timeout iterations and had no diagnostic
signal. A specific escape heuristic is needed: after 2 consecutive crashes or timeouts, the
optimizer must assume C9/C20 and audit all hash tables before generating the next iteration.

### Pattern 3: Wrong Hardcoded Database Path
iter_2's use of `sf10.gendb` is a new bug class. The code generator constructed the database
root from scale factor instead of using the canonical path. Must be documented and checked
in Stage A.

### Pattern 4: P2 Fix Happens Quickly
Q1's 108 ms mmap_columns cost was fixed in a single iteration. The P2 experience entry is
working well as a known pattern.
