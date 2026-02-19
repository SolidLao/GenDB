# GenDB Experience Base
# Updated by DBA agent. Checked by Code Inspector.

## Correctness Issues

### C1: Custom Date Conversion Functions
- Detect: Any function computing year from days/365 or manual month/day arithmetic
- Impact: Wrong dates at year/month boundaries (validation failure)
- Fix: #include "date_utils.h", use gendb::epoch_days_to_date_str()

### C2: Hardcoded Dictionary Codes
- Detect: Integer literals compared against dictionary-encoded columns without loading _dict.txt
- Impact: Wrong filter results
- Fix: Load dictionary at runtime, find code by value

### C3: Decimal Scale Factor (when using int64_t DECIMAL encoding)
- Detect: Any DECIMAL column constant in code without matching the Query Guide's Column Reference
- Applies when: storage_design.json uses int64_t with scale_factor for DECIMAL columns
- Impact: Wrong filters (revenue=0), wrong aggregates (100x off), wrong HAVING thresholds
- Fix: Every DECIMAL threshold, AVG divisor, and HAVING comparison must use scale_factor
  from the Query Guide. The Column Reference shows exact SQL → C++ conversions.

### C4: Incorrect NOT EXISTS Semantics
- Detect: NOT EXISTS checking if >1 late supplier total, instead of checking other suppliers
- Impact: Wrong semi-join results
- Fix: For (orderkey, suppkey): check if any OTHER supplier Z!=suppkey has the condition

### C5: Scale-Squared Output (when using int64_t DECIMAL encoding)
- Detect: SUM or direct output of (col_a * col_b) where both are int64_t with scale_factor
- Applies when: two DECIMAL columns are multiplied, both stored as int64_t
- Impact: Result is scale× too large in final output
- Fix: Divide by scale after each multiplication step

### C6: LIKE Filter on Dictionary-Encoded String Column
- Detect: LIKE pattern against dictionary-encoded column
- Impact: Wrong results if comparing integer codes directly
- Fix: Load dict, find matching codes, filter rows by code membership

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

### C13: Date Threshold Recomputation During Optimization
- Detect: Optimizer changed a date constant that was correct in a previous passing iteration
- Impact: Wrong date ranges, incorrect row counts
- Fix: Preserve original date constants from passing iteration

### C14: Revenue/Discount Formula (when using int64_t DECIMAL encoding)
- Detect: Changed arithmetic in revenue = ep * (scale - disc) / scale pattern
- Applies when: DECIMAL columns stored as int64_t with scale_factor
- Impact: Revenue off by 100x or inverted discount
- Fix: Keep canonical pattern: ep * (scale_factor - discount_column) / scale_factor

### C15: Missing GROUP BY Dimension in Aggregation
- Detect: Hash map key missing a dimension from SQL GROUP BY clause
- Impact: Wrong aggregation groups, fewer output rows
- Fix: Ensure hash map key includes ALL GROUP BY columns


### C18: Dictionary-Encoded Column Output — Decode Code to String
- Detect: Output prints int16_t dictionary code instead of string value
- Impact: Wrong output (integer codes instead of strings)
- Fix: Look up code in loaded dictionary: `dict[code].c_str()`

### C17: Asymmetric Scale Factors in Multi-Column Expressions (when using int64_t DECIMAL encoding)
- Detect: Expression like `a*b - c*d` where columns have different scale factors
- Applies when: multiple DECIMAL columns with int64_t encoding are combined
- Impact: Wrong result if scale divisions are inconsistent
- Fix: Each product term needs exactly ONE /scale division

### C19: Zone-Map Block Skip on Unsorted Column
- Detect: Binary search or block-skip on unsorted column (e.g., unsorted numeric columns)
- Impact: Silently skips qualifying rows → wrong results
- Fix: Only apply zone-map skip to columns with zone-map indexes per Query Guide

## Performance Issues

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
