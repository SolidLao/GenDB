# Q1 Guide — Pricing Summary Report

## Column Reference

### l_returnflag (STRING, int8_t, dictionary-encoded)
- File: lineitem/l_returnflag.bin (59986052 rows × 1 byte)
- Dictionary: lineitem/l_returnflag_dict.txt → `["A","N","R"]` (line 0="A", 1="N", 2="R")
- Filtering: no filter on this column; used as GROUP BY key
- Output: decode code to string via `dict[code]`
- GROUP BY: since only 3 values, use `code` (0/1/2) directly as group index dimension 0

### l_linestatus (STRING, int8_t, dictionary-encoded)
- File: lineitem/l_linestatus.bin (59986052 rows × 1 byte)
- Dictionary: lineitem/l_linestatus_dict.txt → `["F","O"]` (line 0="F", 1="O")
- Filtering: no filter; used as GROUP BY key
- Output: decode code to string via `dict[code]`
- GROUP BY: 2 values; use `code` (0/1) directly as group index dimension 1
- Combined GROUP BY key: `group_idx = returnflag_code * 2 + linestatus_code` → 6 possible groups (0..5)
  Use a flat array of 6 accumulators — no hash table needed

### l_quantity (DECIMAL, double)
- File: lineitem/l_quantity.bin (59986052 rows × 8 bytes)
- Stored as native double — values match SQL directly (e.g., 17.0, 36.0)
- This query: appears in SUM(l_quantity) and AVG(l_quantity); no filter
- Zone map: indexes/lineitem_quantity_zonemap.bin (600 blocks, not used for Q1 filter)

### l_extendedprice (DECIMAL, double)
- File: lineitem/l_extendedprice.bin (59986052 rows × 8 bytes)
- Stored as native double — values match SQL directly
- This query: SUM(l_extendedprice), SUM(l_extendedprice*(1-l_discount)), AVG(l_extendedprice)
- No filter on this column

### l_discount (DECIMAL, double)
- File: lineitem/l_discount.bin (59986052 rows × 8 bytes)
- Stored as native double — values in [0.00, 0.10], e.g. 0.04, 0.09
- This query: used in SUM/AVG expressions; no filter
- Zone map: indexes/lineitem_discount_zonemap.bin (not used for Q1 filter)

### l_tax (DECIMAL, double)
- File: lineitem/l_tax.bin (59986052 rows × 8 bytes)
- Stored as native double — values in [0.00, 0.08]
- This query: used in SUM(l_extendedprice*(1-l_discount)*(1+l_tax)); no filter

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: lineitem/l_shipdate.bin (59986052 rows × 4 bytes)
- **lineitem is sorted by l_shipdate** → zone maps are highly effective
- Range in data: ~8035 (1992-01-01) to ~10591 (1998-12-31)
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
  - `1998-12-01 - 90 days = 1998-09-02`
  - Epoch: `parse_date("1998-09-02")` = 10471
  - C++ predicate: `l_shipdate[i] <= 10471`
- Zone map: indexes/lineitem_shipdate_zonemap.bin
  - Skip blocks where `block_min > 10471`
  - Since lineitem sorted by shipdate, only the last few blocks (with min > 10471) can be skipped
  - Selectivity 0.988 → ~98.8% of rows qualify → zone map skips ≈ 7 trailing blocks out of 600

## Table Stats
| Table    | Rows       | Role | Sort Order   | Block Size |
|----------|------------|------|--------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate   | 100,000    |

## Query Analysis
- **Join pattern**: none — single table scan
- **Filters**:
  - `l_shipdate <= 10471` (epoch of 1998-09-02), selectivity 0.988 → ~59.3M rows qualify
- **Combined selectivity**: 0.988 → essentially full scan
- **Aggregation**: 6-group (l_returnflag × l_linestatus), flat 6-element accumulator array
  - Per group accumulate: count, sum_qty, sum_baseprice, sum_disc_price, sum_charge, sum_disc
  - After scan: compute AVG = sum/count
- **Output**: 6 rows max, ORDER BY l_returnflag ASC, l_linestatus ASC
  - Sort codes (A=0,N=1,R=2) × (F=0,O=1) already produce correct ASCII order

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: indexes/lineitem_shipdate_zonemap.bin
- Layout: `[uint32_t num_blocks=600]` then per block: `[int32_t min, int32_t max, uint32_t block_nrows]` (12 bytes each)
- Total file size: 4 + 600×12 = 7204 bytes (fits in L1 cache)
- Usage: mmap file, read num_blocks, iterate. For each block b:
  - `skip if block_max[b] < lower_bound` (no lower bound here — just upper)
  - `skip if block_min[b] > 10471`
  - row_offset = sum of block_nrows[0..b-1] (row index into column arrays)
  - Access columns as `col[row_offset .. row_offset + block_nrows[b])`
- This query: predicates prune blocks where `block_min > 10471`; since data sorted by shipdate, only the rightmost ~7 blocks qualify for skipping. Minimal benefit, but effectively free to check.
- **Key optimization**: skip zone-map overhead since selectivity=0.988. Just scan all 60M rows using SIMD branch-free comparison, accumulate into 6-slot array.

## Implementation Notes
- Load 4 columns as int8_t (returnflag, linestatus — 1 byte each) + double arrays
- Tight loop: check `l_shipdate[i] <= 10471`, if true: `group = returnflag[i]*2 + linestatus[i]`, accumulate
- Parallelize: split 60M rows into 64 chunks; each thread maintains a local 6-slot accumulator; merge at end
- Use `-fopenmp` or `std::thread` + per-thread accumulators
- Expected throughput: ~3-4 GB/s memory bandwidth → ~600M-800M rows/s → < 1s for 60M rows
- Date threshold for quick reference: `1998-09-02` = epoch 10471
