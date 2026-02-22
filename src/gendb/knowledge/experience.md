# GenDB Experience Base
# Updated by DBA agent. Checked by Code Inspector.

## Correctness Issues — Critical (always check)

### C1: Custom Date Conversion Functions
- Detect: Any function computing year from days/365 or manual month/day arithmetic
- Impact: Wrong dates at year/month boundaries (validation failure)
- Fix: #include "date_utils.h", use gendb::epoch_days_to_date_str()

### C2: Hardcoded Dictionary Codes
- Detect: Integer literals compared against dictionary-encoded columns without loading _dict.txt
- Impact: Wrong filter results
- Fix: Load dictionary at runtime, find code by value

### C7: Interval Arithmetic on DATE Literals
- Detect: DATE + INTERVAL computed as epoch_day ± N*365 or ± N*30
- Impact: Off-by-one errors at leap years/month boundaries
- Fix: Use gendb::add_years/add_months/add_days from date_utils.h

### C9: Open-Addressing Hash Table Capacity Overflow
- Detect: Open-addressing hash table without resize or max-probe limit
- Impact: Infinite loop at 100% load factor
- Fix: Power-of-2 sizing with ≤50% load factor and max probe count. Size for union cardinality when merging. For thread-local hash tables: size each thread's map for the FULL estimated key cardinality (not cardinality/nthreads). With dynamic scheduling, any thread may encounter all groups.

### C11: init_date_tables() Must Be Called Before extract_year/extract_month/extract_day
- Detect: extract_year() etc. called without preceding init_date_tables()
- Impact: All date extractions yield 0
- Fix: Call gendb::init_date_tables() once at top of main()

### C15: Missing GROUP BY Dimension in Aggregation
- Detect: Hash map key missing a dimension from SQL GROUP BY clause
- Impact: Wrong aggregation groups, fewer output rows
- Fix: Ensure hash map key includes ALL GROUP BY columns

### C19: Zone-Map Block Skip on Unsorted Column
- Detect: Binary search or block-skip on unsorted column (e.g., unsorted numeric columns)
- Impact: Silently skips qualifying rows → wrong results
- Fix: Only apply zone-map skip to columns with zone-map indexes per Query Guide

### C20: memset() Sentinel Mismatch on Multi-Byte Types
- Detect: `memset(buf, 0x80, n)` or similar non-zero/non-0xFF memset on int32_t/int64_t arrays
- Impact: memset sets each BYTE to 0x80, producing 0x80808080 per int32_t — NOT INT32_MIN (0x80000000). Causes infinite loops in hash table probing (sentinel never matches).
- Fix: Use `std::fill(keys, keys + CAP, EMPTY_KEY)` or a for-loop. NEVER use memset for non-zero/non-0xFF sentinels on multi-byte types.

## Correctness Issues — Data-Dependent

### C4: Incorrect NOT EXISTS Semantics
- Detect: NOT EXISTS checking if >1 late supplier total, instead of checking other suppliers
- Impact: Wrong semi-join results
- Fix: For (orderkey, suppkey): check if any OTHER supplier Z!=suppkey has the condition

### C6: LIKE Filter on Dictionary-Encoded String Column
- Detect: LIKE pattern against dictionary-encoded column
- Impact: Wrong results if comparing integer codes directly
- Fix: Load dict, find matching codes, filter rows by code membership

### C13: Date Threshold Recomputation During Optimization
- Detect: Optimizer changed a date constant that was correct in a previous passing iteration
- Impact: Wrong date ranges, incorrect row counts
- Fix: Preserve original date constants from passing iteration

### C18: Dictionary-Encoded Column Output — Decode Code to String
- Detect: Output prints int16_t dictionary code instead of string value
- Impact: Wrong output (integer codes instead of strings)
- Fix: Look up code in loaded dictionary: `dict[code].c_str()`

## Correctness Issues — Conditional (int64_t DECIMAL encoding only, rare since v26 double switch)

### C3: Decimal Scale Factor (when using int64_t DECIMAL encoding)
- Detect: Any DECIMAL column constant in code without matching the Query Guide's Column Reference
- Applies when: storage_design.json uses int64_t with scale_factor for DECIMAL columns
- Impact: Wrong filters (revenue=0), wrong aggregates (100x off), wrong HAVING thresholds
- Fix: Every DECIMAL threshold, AVG divisor, and HAVING comparison must use scale_factor
  from the Query Guide. The Column Reference shows exact SQL → C++ conversions.

### C5: Scale-Squared Output (when using int64_t DECIMAL encoding)
- Detect: SUM or direct output of (col_a * col_b) where both are int64_t with scale_factor
- Applies when: two DECIMAL columns are multiplied, both stored as int64_t
- Impact: Result is scale× too large in final output
- Fix: Divide by scale after each multiplication step

### C14: Revenue/Discount Formula (when using int64_t DECIMAL encoding)
- Detect: Changed arithmetic in revenue = ep * (scale - disc) / scale pattern
- Applies when: DECIMAL columns stored as int64_t with scale_factor
- Impact: Revenue off by 100x or inverted discount
- Fix: Keep canonical pattern: ep * (scale_factor - discount_column) / scale_factor

### C17: Asymmetric Scale Factors in Multi-Column Expressions (when using int64_t DECIMAL encoding)
- Detect: Expression like `a*b - c*d` where columns have different scale factors
- Applies when: multiple DECIMAL columns with int64_t encoding are combined
- Impact: Wrong result if scale divisions are inconsistent
- Fix: Each product term needs exactly ONE /scale division

### C22: Hardcoded Database Path Constructed from Scale Factor
- Detect: Generated code opens files from a path like `sf10.gendb` or `sf1.gendb` constructed by concatenating 'sf' + scale_factor + '.gendb'
- Impact: All fopen/mmap calls fail with "No such file or directory" if the actual DB uses a different naming convention
- Fix: Use the exact database root path from the Query Guide's Data Layout section. Never derive the path from the scale factor string.

### C23: Repeated Crash/Timeout With No Timing Output — Hash Table Infinite Loop
- Detect: Query crashes (signal kill) or times out in ≥2 consecutive iterations with zero [TIMING] lines in stdout
- Impact: Infinite loop before any timed phase completes; optimizer receives no diagnostic signal and cannot self-correct
- Fix: Suspect C9 or C20. Audit every open-addressing hash table: (1) capacity = next_power_of_2(2 * max_expected_entries) for ≤50% load; (2) EMPTY_KEY set with std::fill — never memset for non-zero sentinels on multi-byte types. At SF10: ORDERS ~15M rows, LINEITEM ~60M rows.

### C24: Unbounded Hash Table Probing Loop
- Detect: `while (keys[h] != EMPTY && keys[h] != k) h = (h+1) & mask;` without probe count limit
- Impact: Infinite loop if table reaches 100% load due to wrong cardinality estimate
- Fix: Replace with bounded for-loop: `for (uint32_t p = 0; p < cap; ++p) { ... h = (h+1) & mask; }` with abort() on exhaustion. Or use Robin Hood distance tracking.

## Performance Issues

### P11: Not Using Pre-Built Hash Indexes
- Detect: Building runtime hash map for a join key when Query Guide lists a pre-built hash index
- Impact: Wastes 100-500ms on runtime hash table construction for large tables (>1M rows)
- Fix: mmap the pre-built index file instead. Zero build cost. Check Query Guide's Index Layouts section.

### P1: std::unordered_map for Large Hash Tables
- Detect: std::unordered_map or std::unordered_set with >1000 expected entries
- Impact: 2-5x slower than open-addressing
- Fix: Custom open-addressing hash table with power-of-2 sizing, 50% max load factor

### P2: Copying mmap'd Data into Vectors
- Detect: mmap's file, copies to vector, munmaps
- Impact: 500ms+ wasted on large tables
- Fix: mmap() + reinterpret_cast<T*> for zero-copy access

### P3: Sorting for Grouping
- Detect: std::sort on >1M elements followed by linear grouping
- Impact: O(n log n) vs O(n) hash-based grouping
- Fix: Hash-based aggregation with custom open-addressing hash map

### P4: Multiple Passes Over Same Large Table
- Detect: Same binary file mmap'd/opened multiple times
- Impact: 2-3x I/O cost
- Fix: Single-pass with multiple output structures

### P5: Manual Timing Blocks
- Detect: #ifdef GENDB_PROFILE with manual timing
- Fix: Use GENDB_PHASE("name") from timing_utils.h

### P6: Full Sort for LIMIT Queries
- Detect: std::sort + .resize(k) when k <= 100
- Fix: std::partial_sort or custom min-heap. O(n log k).

### P7: std::map with std::pair Keys for Composite Joins
- Detect: std::map<pair<int32_t,int32_t>, V> or unordered_map with pair key
- Fix: Custom open-addressing hash table with composite key struct

### P8: Double-Materializing Large Table for Subquery + Join
- Detect: Two separate scans of the same large table for subquery + outer join
- Fix: Two explicit passes: Pass 1 build hash map, Pass 2 probe qualifying orders

### P9: Three-Column INT32 GROUP BY Without Composite Key
- Detect: GROUP BY with 3+ int32_t fields using std::unordered_map<tuple>
- Fix: Custom open-addressing hash table with multi-key struct

### P10: Ignoring Zone-Map Index — Full Scan Instead of Block-Skip
- Detect: Full column scan without consulting zone_map index when range predicate exists
- Fix: Read zone map per Query Guide layout, skip non-qualifying blocks. row_offset is ROW index.

### P12: Optimizer Oscillation — Regression Not Caught Before Emitting Next Iteration
- Detect: An optimization iteration produces timing >20% worse than the current best (e.g., Q6 iter_3: 152 ms vs best 69 ms; Q9 iter_4: 220 ms vs best 168 ms)
- Impact: One wasted generate-compile-run cycle per oscillation event
- Fix: Track best validated (timing_ms, source) pair. If next iteration regresses >20% from best, revert to best-known source and apply a different optimization direction instead of emitting the regressed version.

### P13: Full Column madvise(WILLNEED) When Zone Maps Show <50% Blocks Qualify
- Detect: `madvise(MADV_WILLNEED)` on entire column file when zone maps exist and filter selectivity is low
- Impact: 2-5x unnecessary I/O on cold start — reads non-qualifying blocks from disk
- Fix: Load zone maps first, compute qualifying block ranges, issue `madvise(MADV_WILLNEED)` only on qualifying byte ranges

### P14: No Explicit data_loading Phase — I/O Mixed With Computation
- Detect: No `GENDB_PHASE("data_loading")` in generated code; mmap/madvise calls scattered across dim_filter, build_joins, main_scan
- Impact: Optimizer cannot identify I/O as the bottleneck; cold-start regressions invisible in timing breakdown
- Fix: Add `GENDB_PHASE("data_loading")` as Phase 0 before dim_filter. Consolidate all mmap + madvise prefetch calls there.

### P15: Thread-Local Aggregation Merge Using std::unordered_map — O(n·t) Sequential Merge
- Detect: aggregation_merge phase consumes >500 ms in a multi-threaded GROUP BY query; each thread holds a separate std::unordered_map of partial aggregates that are merged sequentially
- Impact: Q3 iter_0: aggregation_merge 3,263 ms (38% of total). After fix: 22 ms (99% reduction).
- Fix: Use a single shared open-addressing hash table with per-slot spinlocks or CAS updates, OR pre-allocate one global hash table and have all threads probe-and-update with atomic operations. Never merge t separate unordered_maps sequentially.

### P16: data_loading Dominates Unoptimized Queries — Missed madvise Opportunity
- Detect: Query exits after iter_0 with no optimization attempted AND data_loading phase > 40% of hot runtime (e.g., Q1: 55%, Q9: 65%)
- Impact: Leaves 30-50% runtime on the table for I/O-bound queries; zone-map-guided prefetch not applied
- Fix: Always attempt at least one optimization targeting I/O when data_loading > 40% of hot runtime: (1) add zone-map-guided selective `madvise(MADV_WILLNEED)` on qualifying byte ranges only; (2) verify GENDB_PHASE("data_loading") is Phase 0 (P14).

## Correctness Issues — Optimizer Behavioral

### C25: Filter Stage Elimination Causing Zero-Row Output
- Detect: rows_actual == 0 with rows_expected > 0, AND dim_filter phase timing collapses to ~0 ms from a non-zero value in a prior passing iteration
- Impact: All output silently lost; optimizer may misinterpret as a "fast" result
- Fix: Treat rows_actual == 0 with rows_expected > 0 as a definitive correctness failure. When dim_filter time collapses to 0 ms, the filter predicate was eliminated or inverted — revert to the previous passing iteration's filter logic. Never accept a zero-row result as a performance win.

### C26: Join Restructuring Regression — Correct Row Count, Wrong Row Values
- Detect: rows_actual == rows_expected but column_failures exist across join key, aggregated value, and date columns simultaneously. main_scan timing is 5-20x higher than best passing iteration; build_joins also regresses.
- Impact: Q3 iter_5: wrong l_orderkey, revenue, o_orderdate despite correct row count (10/10). Caused by incorrect join key mapping after restructure.
- Fix: When an optimization modifies join build/probe structure and produces correct count but wrong values, suspect missing composite join key column (C15 variant) or probe-side predicate changed. Revert build_joins and main_scan logic to the last passing iteration. A simultaneous regression in both build_joins AND main_scan timing is a strong signal of join logic breakage.
