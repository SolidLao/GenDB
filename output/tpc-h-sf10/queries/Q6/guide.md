# Q6 Guide — Forecasting Revenue Change

## Column Reference

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows of int32_t)
- This query: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'`
  → C++: `l_shipdate[i] >= days_from_civil(1994, 1, 1) && l_shipdate[i] < days_from_civil(1995, 1, 1)`
- Selectivity: ~0.157

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: `l_discount BETWEEN 0.05 AND 0.07` → C++: `l_discount[i] >= 0.05 && l_discount[i] <= 0.07`
- Selectivity: ~0.274; also used in SUM computation

### l_quantity (decimal, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows of double)
- This query: `l_quantity < 24` → C++: `l_quantity[i] < 24.0`
- Selectivity: ~0.457

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `SUM(l_extendedprice * l_discount)`

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| lineitem | 59,986,052 | fact | l_orderkey | 65536      |

## Query Analysis
- **Pattern**: Single-table scan, 3 filters, single scalar aggregate
- **Combined selectivity**: ~0.157 * 0.274 * 0.457 ≈ 0.0197 → ~1.18M rows qualify
- **Strategy**: Sequential scan of 4 columns. Evaluate filters in selectivity order:
  1. `l_shipdate` range (0.157) — most selective, check first
  2. `l_discount` range (0.274)
  3. `l_quantity < 24` (0.457)
  4. If all pass: accumulate `l_extendedprice * l_discount` into a single double
- **Output**: Single scalar value
- **Optimization**: Use zone maps to skip blocks outside the date range. Can also SIMD-vectorize the accumulation loop.

## Indexes

### lineitem_shipdate_zm (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Meta: `storage/indexes/lineitem_l_shipdate_zonemap_meta.txt`
- Layout: `struct { int32_t min_val; int32_t max_val; }` — 8 bytes per entry
- Block size: 65536 rows → 916 blocks
- Usage: For block `b`, skip if `zm[b].max_val < lower_bound` OR `zm[b].min_val >= upper_bound`. Expect ~85% of blocks skipped since only 1 year of ~6.5 years of data qualifies.
