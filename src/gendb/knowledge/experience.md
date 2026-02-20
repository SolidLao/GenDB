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
- Fix: Power-of-2 sizing with ≤50% load factor and max probe count. Size for union cardinality when merging.

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
