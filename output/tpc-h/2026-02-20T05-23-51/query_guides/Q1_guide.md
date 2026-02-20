# Q1 Guide — Pricing Summary Report

## Query
```sql
SELECT l_returnflag, l_linestatus,
    SUM(l_quantity), SUM(l_extendedprice),
    SUM(l_extendedprice*(1-l_discount)), SUM(l_extendedprice*(1-l_discount)*(1+l_tax)),
    AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes)
- Encoding: days since 1970-01-01. Formula: Howard Hinnant proleptic Gregorian.
- Self-test: parse("1970-01-01") = 0
- Date range in data: first=9568 (≈1996-03), last=10143 (≈1997-10) [natural file order]
- **This query**: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
  - 1998-12-01 = epoch 10561; minus 90 days = **10471** (which is 1998-09-02)
  - C++ predicate: `l_shipdate[row] <= 10471`
  - Estimated selectivity: 98.8% of rows qualify

### l_returnflag (STRING, int8_t, raw_char encoding)
- File: `lineitem/l_returnflag.bin` (59,986,052 × 1 byte)
- Encoding: raw ASCII character stored as int8_t. NO dict file.
  - 'A' = 65 (int8_t), 'N' = 78, 'R' = 82
- No dict file. Compare using C char literals: `col[row] == 'A'`
- GROUP BY output: cast back to char for display: `(char)col[row]`
- Distinct values: 3 ('A', 'N', 'R')

### l_linestatus (STRING, int8_t, raw_char encoding)
- File: `lineitem/l_linestatus.bin` (59,986,052 × 1 byte)
- Encoding: raw ASCII character stored as int8_t. NO dict file.
  - 'F' = 70 (int8_t), 'O' = 79
- Compare: `col[row] == 'F'` or `col[row] == 'O'`
- GROUP BY output: `(char)col[row]`
- Distinct values: 2 ('F', 'O')

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes)
- Stored as native double. Values match SQL directly. Range: 1.0–50.0
- This query: SUM and AVG. Access: `double* qty = (double*)mmap(...)`

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes)
- Stored as native double. Values match SQL directly.
- This query: SUM, SUM×(1-discount), SUM×(1-discount)×(1+tax), AVG
- Verified non-zero: first=33078.9, last=16131.1

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes)
- Stored as native double. Range: 0.00–0.10 (11 distinct values)
- This query: used in expression `(1 - l_discount)` and AVG(l_discount)
- Verified non-zero: first=0.04, last=0.02

### l_tax (DECIMAL, double)
- File: `lineitem/l_tax.bin` (59,986,052 × 8 bytes)
- Stored as native double. Range: 0.00–0.08
- This query: used in expression `(1 + l_tax)` for sum_charge

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| lineitem | 59,986,052 | fact | none       | 100,000    |

## Query Analysis
- **Access pattern**: Full scan of lineitem (98.8% selectivity on l_shipdate → nearly all rows pass)
- **Filter**: `l_shipdate <= 10471` — zone map can skip blocks whose min > 10471 (last ~1.2% of data by date). Since data is in natural (l_orderkey) order, each block spans wide date range → zone map provides minimal pruning. Expect ~99% of blocks to be read.
- **Aggregation**: 4 output groups (combinations of l_returnflag × l_linestatus). Use a small 4-slot accumulator array keyed by (returnflag, linestatus) pair.
  - Composite key: `int key = (int)(rflag_byte) * 256 + (int)(lstatus_byte)` or use 2D array
  - Better: `int idx = (rflag=='R'?2:rflag=='N'?1:0)*2 + (lstatus=='O'?1:0)` for 6 possible slots
- **Parallelism**: Divide lineitem rows into 64 chunks. Each thread computes local partial aggregates (6 slots). Merge thread-local results in O(6×64) final step.
- **SIMD opportunity**: Load l_shipdate, l_extendedprice, l_discount, l_tax in 4/8-wide AVX2/AVX-512 vectors. Filter with vectorized comparison, accumulate sums with masked adds.
- **Combined selectivity**: ~98.8% of 60M rows = ~59.3M rows contribute to output.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][{int32_t min, int32_t max, uint32_t block_size} × 600]`
- Block i covers rows [i×100000 .. (i+1)×100000)
- Usage: mmap file, iterate 600 entries. Skip block b if `zm[b].max < lower_bound OR zm[b].min >= upper_bound`
- **This query**: Skip block if `zm[b].max > 10471` is FALSE (i.e., skip if all dates > 10471)
  - Condition to SKIP: `zm[b].min > 10471` (all rows in block have shipdate > cutoff)
  - Since data is in natural order (not sorted by shipdate), blocks span wide date ranges → very few blocks skipped (~1-2%)
  - Still worth checking: iterate zone map first, collect qualifying block offsets, then load column data

### No other indexes are used for Q1.
(Q1 is a single-table scan; hash indexes on l_orderkey are irrelevant here)
