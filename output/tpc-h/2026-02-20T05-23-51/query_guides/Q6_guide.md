# Q6 Guide — Forecasting Revenue Change

## Query
```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate <  DATE '1994-01-01' + INTERVAL '1' YEAR
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24;
```

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes)
- Encoding: days since 1970-01-01.
- **This query** (two-sided range):
  - `l_shipdate >= DATE '1994-01-01'` → `l_shipdate[row] >= 8766`
    - Epoch(1994-01-01): 24 years from 1970, leap years 1972,76,80,84,88,92 = 6 → 24×365+6 = **8766**
  - `l_shipdate < DATE '1995-01-01'` → `l_shipdate[row] < 9131`
    - Epoch(1995-01-01): 8766 + 365 = **9131** (1994 is not a leap year)
  - Combined: `8766 <= l_shipdate[row] < 9131` (exactly 1994 calendar year, 365 days)
  - Selectivity: ~8% of rows (1 year out of ~7 year range 1992-1998)

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes)
- Stored as native double. Range: 0.00–0.10
- **This query**: `l_discount BETWEEN 0.05 AND 0.07`
  - C++: `l_discount[row] >= 0.05 && l_discount[row] <= 0.07`
  - Use exact literals: `0.05` and `0.07` (no scale conversion needed for double)
  - Selectivity: ~10% of rows (3 out of 11 distinct discount values: 0.05, 0.06, 0.07)

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes)
- Stored as native double. Range: 1.0–50.0 (50 distinct integer-valued doubles)
- **This query**: `l_quantity < 24`
  - C++: `l_quantity[row] < 24.0`
  - Selectivity: ~46% of rows (23 out of 50 distinct values)

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes)
- Stored as native double. Values match SQL directly.
- **This query**: Used only in aggregate: `SUM(l_extendedprice * l_discount)`
- Combined filter selectivity: ~8% × ~10% × ~46% ≈ **0.37%** of rows (~222K rows contribute)

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| lineitem | 59,986,052 | fact | none       | 100,000    |

## Query Analysis
- **Access pattern**: Filtered scan of lineitem, single scalar aggregate output
- **Three filters applied together** (branch-free in tight loop for best SIMD throughput):
  1. Date range: `ship >= 8766 && ship < 9131`
  2. Discount range: `disc >= 0.05 && disc <= 0.07`
  3. Quantity: `qty < 24.0`
- **Aggregation**: Single scalar SUM. No GROUP BY. Accumulate in double.
- **Combined selectivity**: ~0.37% → ~222K rows qualify. Most work is in filtering.
- **Parallelism**: 64 threads × local double accumulator → reduce sum at end.
- **SIMD strategy**: Load 8 doubles at a time (AVX-512). Apply all 3 filters as bitmask. Masked multiply and add.
- **Column load order**: Load l_shipdate first (cheapest filter at 8%). If block passes zone map, load disc+qty+price together.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][{int32_t min, int32_t max, uint32_t block_size} × 600]`
- Block i covers rows [i×100000 .. (i+1)×100000)
- row_offset is a ROW index, not byte offset. Byte offset into column file = row_idx × sizeof(type).
- **This query** — critical optimization: skip block b if `zm[b].max < 8766 OR zm[b].min >= 9131`
  - Since data is in natural order (not sorted by date), blocks span wide date ranges
  - Each block likely contains dates from 1992–1998, so zone map prunes few blocks
  - Still: for any block where min > 9131 or max < 8766 (unlikely but possible at file extremes), skip entirely
- **Expected pruning**: minimal (~1-5% blocks skipped) due to unsorted data
- **Usage pattern**:
  ```cpp
  auto* zm = (ZMEntry*)mmap_zone_map;
  uint32_t nb = zm_header.num_blocks;
  for (uint32_t b = 0; b < nb; b++) {
      if (zm[b].max < 8766 || zm[b].min >= 9131) continue; // skip block
      uint32_t row_start = b * 100000;
      uint32_t row_end   = row_start + zm[b].block_size;
      // process rows [row_start, row_end)
  }
  ```
