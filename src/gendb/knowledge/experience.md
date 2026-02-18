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

### C3: Decimal Scale Factor Mismatch
- Detect: Comparing raw threshold against scaled column (e.g., > 300 when column is scale 100)
- Impact: Wrong filter selectivity
- Fix: threshold_scaled = threshold * scale_factor

### C4: Incorrect NOT EXISTS Semantics
- Detect: NOT EXISTS checking if >1 late supplier total, instead of checking other suppliers
- Impact: Wrong semi-join results
- Fix: For (orderkey, suppkey): check if any OTHER supplier Z!=suppkey has the condition

## Performance Issues

### P1: std::unordered_map for Large Hash Tables
- Detect: std::unordered_map or std::unordered_set with >1000 expected entries
- Impact: 2-5x slower than open-addressing
- Fix: #include "hash_utils.h", use gendb::CompactHashMap/CompactHashSet

### P2: Copying mmap'd Data into Vectors
- Detect: Function that mmap's file, copies to vector, munmaps
- Impact: 500ms+ wasted on large tables
- Fix: #include "mmap_utils.h", use gendb::MmapColumn<T>

### P3: Sorting for Grouping
- Detect: std::sort on >1M elements followed by linear grouping
- Impact: O(n log n) vs O(n) hash-based grouping
- Fix: Use hash-based aggregation with CompactHashMap

### P4: Multiple Passes Over Same Large Table
- Detect: Same binary file mmap'd/opened multiple times
- Impact: 2-3x I/O cost
- Fix: Single-pass with multiple output structures

### P5: Manual Timing Blocks
- Detect: #ifdef GENDB_PROFILE with manual start/end timing
- Impact: Code bloat, easy to break, inconsistent names
- Fix: #include "timing_utils.h", use GENDB_PHASE("name")

### P6: Full Sort for LIMIT Queries
- Detect: std::sort on result set followed by .resize(k) or result.erase(result.begin()+k, result.end()), when k <= 100
- Impact: O(n log n) on potentially millions of result rows; wasteful when n >> k
- Fix: #include "hash_utils.h", use gendb::TopKHeap<Row>(k, cmp); call heap.push(row) per result row; call heap.sorted() once for output. O(n log k).
- Queries: Q3 (LIMIT 10), Q18 (LIMIT 100)

### P8: Q18 — Materializing Full lineitem Twice (Subquery + Outer Join)
- Detect: Two separate mmap/open loops over lineitem.bin in Q18: one for the subquery SUM(l_quantity)>300, one for the outer join l_orderkey=o_orderkey
- Impact: 60M-row scan × 2 = 120M reads on HDD; ~2× unnecessary I/O (dominant cost on disk-based system)
- Fix: Combine into two explicit passes: Pass 1 — single lineitem scan to build CompactHashMap<int32_t,int64_t> orderkey→sum_qty, then build CompactHashSet<int32_t> qualifying_orders (where sum_qty>30000). Pass 2 — second lineitem scan probing qualifying_orders. Do NOT attempt a single-pass accumulation that requires buffering all 60M rows — two sequential passes on mmap'd files are cheaper than RAM-buffering.
- Queries: Q18

### P7: std::map or std::unordered_map with std::pair Keys for Composite Joins
- Detect: std::map<std::pair<int32_t,int32_t>, V> or std::unordered_map with pair key (won't compile without custom hash)
- Impact: Either compile error or O(log n) tree traversal on 8M partsupp rows
- Fix: #include "hash_utils.h", use gendb::CompactHashMapPair<V>; keys are gendb::Key32Pair{partkey, suppkey}
- Queries: Q9 (partsupp join on ps_partkey + ps_suppkey, lineitem join on l_partkey + l_suppkey)

## Correctness Issues (continued)

### C5: Scale-Squared Output for Product of Two Decimal Columns
- Detect: SUM or direct output of (col_a * col_b) where both col_a and col_b are stored as int64_t with scale_factor=100
- Impact: Result is 100× too large (scale 10000 instead of 100) in final output
- Fix: When multiplying two scale-100 columns, divide result by 100 before accumulating into output sum, OR divide the final SUM by 100. Never divide by 100 twice.
- Affected expressions:
  - Q1: l_extendedprice*(1-l_discount) → scale²; *(1+l_tax) → scale³ — chain: ((ep*(scale-disc))/scale * (scale+tax))/scale
  - Q6: l_extendedprice*l_discount → scale², divide SUM by 10000 for output
  - Q9: l_extendedprice*(scale-l_discount)/scale - ps_supplycost*l_quantity/scale → both terms are scale²/scale = scale¹ after one division
  - Q3: l_extendedprice*(scale-l_discount)/scale → scale¹
- Canonical pattern: compute intermediate as int64_t, divide by scale (100) after each multiplication step to stay in scale¹

### C6: LIKE Filter on Dictionary-Encoded String Column
- Detect: p_name LIKE '%green%' (or any substring LIKE) against a dictionary-encoded column
- Impact: If naively scanning decoded strings per row, correct but slow; if comparing integer codes directly without decoding, wrong results
- Fix: At load time, iterate dictionary entries and collect all codes where value.find("green") != string::npos; then filter rows by code membership (integer compare). Do NOT compare raw code integers against the pattern string.
- Queries: Q9 (p_name LIKE '%green%')

### C7: Interval Arithmetic on DATE Literals
- Detect: DATE 'YYYY-MM-DD' + INTERVAL 'N' YEAR/MONTH/DAY computed inline as epoch_day ± N*365 or ± N*30
- Impact: Off-by-one errors at leap years (YEAR), wrong month lengths (MONTH), or boundary drift (DAY via flat offset is correct for DAY only)
- Fix: #include "date_utils.h"; use gendb::add_years(epoch, N) / gendb::add_months(epoch, N) / gendb::add_days(epoch, N)
- Affected queries:
  - Q1: DATE '1998-12-01' - INTERVAL '90' DAY → add_days(date_str_to_epoch_days("1998-12-01"), -90)
  - Q6: DATE '1994-01-01' + INTERVAL '1' YEAR → add_years(date_str_to_epoch_days("1994-01-01"), 1)

### C9: CompactHashMap Capacity Overflow
- Detect: CompactHashMap/Set sized for per-thread partition count, then used to merge all thread-local maps (total distinct keys >> initial capacity)
- Impact: Infinite loop at 100% load factor (insert never finds empty slot); uint8_t dist overflow at 255 causes incorrect Robin Hood displacement
- Fix: Auto-resize is now built into hash_utils.h (rehash at 75% load). Still pre-size for expected total distinct keys when possible to avoid unnecessary rehashes. When merging N thread-local maps, size the target map for the union cardinality, not per-thread cardinality.
- Queries: Q18 (merging ~15M orderkeys from thread-local maps into global map sized for 300K)

### C8: Subquery Threshold Not Scaled (Q18 HAVING)
- Detect: HAVING SUM(l_quantity) > 300 where l_quantity is stored as int64_t scale_factor=100
- Impact: Filter passes orders with SUM(raw) > 300 instead of SUM(quantity) > 300; admits orders with as little as 3.0 total quantity — result set will be millions of rows, not ~10K
- Fix: Compare against 300 * 100 = 30000 (scaled threshold); threshold_scaled = 300LL * scale_factor

### C10: AVG Output of a Single Scaled Decimal Column
- Detect: AVG(col) where col is int64_t with scale_factor=100 (e.g., AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount) in Q1)
- Impact: AVG = SUM(raw)/COUNT gives a value 100× too large; printing as a float without dividing by scale gives wrong answer
- Fix: Output (double)sum_raw / count / 100.0 — divide by count first, then by scale, OR divide sum by 100 before the count division. Never omit the /scale step for AVG.
- Pattern: `double avg_qty = (double)sum_qty_raw / count / 100.0;`
- Queries: Q1 (avg_qty, avg_price, avg_disc)

### C11: init_date_tables() Must Be Called Before Any extract_year/extract_month/extract_day
- Detect: Any call to gendb::extract_year(), gendb::extract_month(), or gendb::extract_day() without a preceding gendb::init_date_tables() at program startup
- Impact: YEAR_TABLE/MONTH_TABLE/DAY_TABLE are zero-initialized; all rows yield year=0, month=0, day=0 → wrong GROUP BY keys and wrong EXTRACT output
- Fix: Call gendb::init_date_tables() once at the top of main() (or query entry point) before any date extraction. The function is idempotent; calling it multiple times is safe.
- Queries: Q9 (EXTRACT(YEAR FROM o_orderdate) used as group-by key)

### C12: BETWEEN on Scaled Decimal — Both Bounds Must Be Scaled
- Detect: col BETWEEN lo AND hi where col is int64_t scale_factor=100 and lo/hi are floating-point literals (e.g., l_discount BETWEEN 0.05 AND 0.07)
- Impact: BETWEEN 0.05 AND 0.07 against raw int64_t always false (raw values are 5–7 for 0.05–0.07); zero rows pass filter
- Fix: Scale both bounds: lo_scaled = (int64_t)round(lo * 100), hi_scaled = (int64_t)round(hi * 100). For Q6: l_discount >= 5 AND l_discount <= 7; l_quantity < 2400 (not < 24).
- Queries: Q6 (l_discount BETWEEN 0.05 AND 0.07; l_quantity < 24)
