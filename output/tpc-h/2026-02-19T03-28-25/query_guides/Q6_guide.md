# Q6 Guide

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: lineitem/l_shipdate.bin (59986052 rows)
- This query: Filter `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'`
  - DATE '1994-01-01' → epoch 8401
  - DATE '1995-01-01' → epoch 8766 (1 year later)
  - Condition: `raw >= 8401 AND raw < 8766`
- Selectivity: ~16% of rows (estimated from workload_analysis.json)
- Zone map: lineitem/l_shipdate_zone.idx (600 blocks) can skip blocks where max < 8401 or min >= 8766

### l_discount (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_discount.bin (59986052 rows)
- Stored values = SQL_value × 100 (e.g., SQL 0.07 → stored 7)
- This query: Filter `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01` = `BETWEEN 0.05 AND 0.07`
  - Scaled thresholds: BETWEEN 5 AND 7 (in stored representation)
  - Condition: `raw >= 5 AND raw <= 7`
- Selectivity: ~34% of rows (estimated from workload_analysis.json)
- Cardinality: 11 distinct discount values (0.00-0.10 in 0.01 increments)

### l_quantity (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_quantity.bin (59986052 rows)
- Stored values = SQL_value × 100 (e.g., SQL 15 → stored 1500)
- This query: Filter `l_quantity < 24`
  - Scaled threshold: `raw < 2400`
  - Condition: `raw < 2400`
- Selectivity: ~47% of rows (estimated from workload_analysis.json)
- Cardinality: 50 distinct quantity values (1-50)

### l_extendedprice (DECIMAL, int64_t, scale_factor=100)
- File: lineitem/l_extendedprice.bin (59986052 rows)
- Stored values = SQL_value × 100
- This query: SUM(l_extendedprice * l_discount) accumulates in scaled arithmetic
- Formula: `sum_scaled = sum(l_extendedprice_code[i] * l_discount_code[i] / 100)` (maintain precision)
- Output: Divide final sum by 100 for display
- Example computation: extendedprice=1234567, discount=7 → (1234567 × 7) / 100 = 86419.69 (SQL)

## Table Stats
| Table   | Rows      | Role | Sort Order   | Block Size |
|---------|-----------|------|--------------|------------|
| lineitem| 59986052  | fact | l_shipdate   | 100000     |

## Query Analysis
- Scan pattern: Full scan of lineitem (60M rows)
- Filter 1: l_shipdate in [8401, 8766) → selectivity 16% (~9.6M rows)
- Filter 2: l_discount in [5, 7] → selectivity 34% (~3.3M of filtered rows)
- Filter 3: l_quantity < 2400 → selectivity 47% (~1.55M of doubly-filtered rows)
- Combined selectivity: 0.16 × 0.34 × 0.47 ≈ 2.5% (~1.5M rows qualify)
- Aggregation: Single SUM(expression) → 1 output row
- No GROUP BY, ORDER BY, or LIMIT

## Indexes

### Zone Map: lineitem/l_shipdate_zone.idx
- 600 blocks, block_size=100K rows
- Layout: [uint32_t num_blocks=600] [min, max, block_size]×600
- Usage: For filter `raw >= 8401 AND raw < 8766`:
  - Skip blocks where max < 8401 (all values before 1994-01-01)
  - Skip blocks where min >= 8766 (all values from 1995-01-01 onward)
- Expected skip: ~60-70% of blocks (data spans 1992-1998, only 1994 Q1 qualifies)
- Estimated blocks to scan: ~180-240 blocks (30-40% of 600)

## Performance Notes
- Multi-predicate filtering: Vectorize all three filters in single pass over lineitem
- Branch-free filtering: Use `(shipdate >= 8401) & (shipdate < 8766) & (discount >= 5 & discount <= 7) & (quantity < 2400)` with AND operations
- SIMD: Process 8-16 rows per SIMD operation for filter evaluation
- Parallel scan: Split 60M rows across 64 cores (~1M rows per core with morsels)
- Aggregation: Partial sum per thread (thread-local accumulator), then global reduction using atomic or mutex
- Memory: Final result is 1 value; all computation fits in registers/L1 cache
- I/O: Zone map eliminates ~60% of blocks from access; effective scan size ~9.6M rows after filter 1
