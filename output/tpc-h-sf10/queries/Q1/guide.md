# Q1 Guide — Pricing Summary Report

## Column Reference

### l_shipdate (date, int32_t, days_since_epoch_1970)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows of int32_t)
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
  → C++: `l_shipdate[i] <= days_from_civil(1998, 9, 2)`
- Date encoding: `days_from_civil(y,m,d)` as defined in ingest.cpp

### l_returnflag (char1, int8_t, raw_char)
- File: `storage/lineitem/l_returnflag.bin` (59,986,052 rows of int8_t)
- This query: GROUP BY column → C++: `(int8_t)` stores ASCII char ('A','N','R')

### l_linestatus (char1, int8_t, raw_char)
- File: `storage/lineitem/l_linestatus.bin` (59,986,052 rows of int8_t)
- This query: GROUP BY column → C++: `(int8_t)` stores ASCII char ('F','O')

### l_quantity (decimal, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows of double)
- This query: `SUM(l_quantity)`, `AVG(l_quantity)`

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `SUM(l_extendedprice)`, revenue calculations

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: revenue calculations, `AVG(l_discount)`

### l_tax (decimal, double, raw)
- File: `storage/lineitem/l_tax.bin` (59,986,052 rows of double)
- This query: `SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))`

## Table Stats
| Table    | Rows       | Role | Sort Order  | Block Size |
|----------|------------|------|-------------|------------|
| lineitem | 59,986,052 | fact | l_orderkey  | 65536      |

## Query Analysis
- **Pattern**: Single-table scan with date filter, group by two 1-byte columns, 8 aggregates
- **Filter**: `l_shipdate <= threshold` — selectivity ~0.986 (nearly all rows pass)
- **Group by**: (l_returnflag, l_linestatus) — only ~4 distinct groups (3 returnflag * 2 linestatus, but only 4 actually occur)
- **Aggregation**: Use a small fixed-size array or hash map keyed by `(returnflag, linestatus)` pair.
  Since both are single chars, combine into a 16-bit key: `((uint16_t)(uint8_t)returnflag << 8) | (uint8_t)linestatus`
- **Output**: 4 rows sorted by (l_returnflag ASC, l_linestatus ASC)
- **Approach**: Sequential scan of all 7 columns. With ~98.6% selectivity, nearly all rows participate. Avoid branch mispredictions by always accumulating and masking.

## Indexes

### lineitem_shipdate_zm (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Meta: `storage/indexes/lineitem_l_shipdate_zonemap_meta.txt`
- Layout: array of `struct { int32_t min_val; int32_t max_val; }` — 8 bytes per entry
- Block size: 65536 rows → `num_blocks = ceil(59986052 / 65536) = 916 blocks`
- Usage: For each block `b`, if `zm[b].min_val > threshold`, skip block entirely.
  With ~98.6% selectivity, most blocks will pass — zone map provides marginal benefit for this query but can skip the last few blocks near the date boundary.
