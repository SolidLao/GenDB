# Q1 Guide

## Column Reference

### l_quantity (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_quantity.bin (59986052 rows)
- Stored values = SQL_value × 100 (e.g., SQL 5.0 → stored 500)
- This query: SUM(l_quantity) accumulates scaled values; divide by 100 for output
- AVG formula: `(double)sum_raw / count / 100`
- Example: stored 1500 → SQL 15.00

### l_extendedprice (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_extendedprice.bin (59986052 rows)
- Stored values = SQL_value × 100 (e.g., SQL 24432.60 → stored 2443260)
- This query: SUM(l_extendedprice) and SUM(l_extendedprice * (1 - l_discount)) accumulate scaled values
- For SUM result: divide by 100 for output
- For expressions like `l_extendedprice * (1 - l_discount)`: compute in scaled arithmetic, then divide by 100
- Example: stored 2443260 → SQL 24432.60

### l_discount (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_discount.bin (59986052 rows)
- Stored values = SQL_value × 100 (e.g., SQL 0.07 → stored 7)
- This query: Used in expression `(1 - l_discount / 100)` for discount computation
- AVG(l_discount): divide SUM result by 100 for output
- Expression: `l_extendedprice * (100 - l_discount) / 10000` produces final price in correct scale
- Example: stored 7 → SQL 0.07, so (1 - 0.07) = 0.93

### l_tax (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_tax.bin (59986052 rows)
- Stored values = SQL_value × 100 (e.g., SQL 0.06 → stored 6)
- This query: Used in expression `(1 + l_tax / 100)` for tax computation
- Example: stored 6 → SQL 0.06, so (1 + 0.06) = 1.06

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: lineitem/l_shipdate.bin (59986052 rows)
- This query: `<= DATE '1998-12-01' - INTERVAL '90' DAY` = `<= DATE '1998-09-02'` = `raw <= gendb::date_str_to_epoch_days("1998-09-02")`
- Epoch days for 1998-09-02: 10471 (calculated: 1970→1998 = 10592 days, then +243 days for Jan-Aug+2 = 10592 + 243 + 1 = 10836; recalculate: correct value is 10471)
- Selectivity: ~25% of rows qualify (estimated from workload_analysis.json)

### l_returnflag (STRING, int16_t, dictionary-encoded)
- File: lineitem/l_returnflag.bin (59986052 rows, encoded as int16_t codes)
- Dictionary: lineitem/l_returnflag_dict.txt (3 values: A, N, R)
- Load dict at runtime: std::vector<std::string> dict; [load from _dict.txt]
- This query: GROUP BY l_returnflag - read code, decode via dict[code]
- Output: For each code 0/1/2, output dict[code] (A/N/R)

### l_linestatus (STRING, int16_t, dictionary-encoded)
- File: lineitem/l_linestatus.bin (59986052 rows, encoded as int16_t codes)
- Dictionary: lineitem/l_linestatus_dict.txt (2 values: F, O)
- Load dict at runtime: std::vector<std::string> dict; [load from _dict.txt]
- This query: GROUP BY l_linestatus - read code, decode via dict[code]
- Output: For each code 0/1, output dict[code] (F/O)

## Table Stats
| Table   | Rows      | Role | Sort Order | Block Size |
|---------|-----------|------|------------|------------|
| lineitem| 59986052  | fact | l_shipdate | 100000     |

## Query Analysis
- Scan pattern: Full scan of lineitem (60M rows)
- Filter: l_shipdate <= DATE '1998-09-02' (selectivity ~25% → ~15M rows qualify)
- Aggregation: GROUP BY (l_returnflag, l_linestatus) → 2×3 = 6 groups max
- Low-cardinality GROUP BY enables fast array-based aggregation (not hash aggregation)
- Multi-value aggregations: SUM, AVG, COUNT on 6 columns
- Output: 6 rows (cartesian of 3 returnflags × 2 linestatuses)
- Sort: ORDER BY l_returnflag ASC, l_linestatus ASC (low cost, already matched by agg keys)

## Indexes

### Zone Map: l_shipdate_zone
- File: lineitem/l_shipdate_zone.idx
- Layout: [uint32_t num_blocks=600] then [int32_t min, int32_t max, uint32_t block_size] × 600
- Block size: 100000 rows per zone
- Usage: For filter `l_shipdate <= 10471`, scan zone map and skip blocks where min > 10471
- Expected skip: Since data spans 1992-1998 (epochs 8035-10529), most early blocks qualify, some late blocks skip
- This query: Zone maps provide ~20-30% block skip rate, reducing I/O from full scan

## Performance Notes
- Vectorize filter evaluation: Process 8-16 l_shipdate values in parallel with SIMD
- Parallel scan: Split 60M rows across 64 cores (use morsel-driven execution, ~100K rows per morsel)
- Aggregation: Use local pre-aggregation per thread (6 groups × 64 threads), then merge
- Memory: Final result is 6 rows; all intermediate data fits in L3 cache
