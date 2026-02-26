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

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate | 100,000    |

Number of blocks = ceil(59,986,052 / 100,000) = 600

## Query Analysis
- **Single-table scan** on lineitem (no joins).
- **Filters** (combined selectivity ~1.7% of all rows pass):
  - `l_shipdate BETWEEN 8766 AND 9130` → ~14% rows
  - `l_discount BETWEEN 0.05 AND 0.07` → ~27% rows
  - `l_quantity < 24` → ~46% rows
- **Zone map** eliminates most blocks: only blocks covering 1994 rows need scanning.
- **Output**: single scalar SUM — no GROUP BY, no ORDER BY.

## Date Thresholds
```
DATE '1994-01-01'                        → date_to_days(1994,1,1) = 8766
DATE '1994-01-01' + INTERVAL '1' YEAR   → date_to_days(1995,1,1) = 9131
```
C++ predicates:
```cpp
l_shipdate[i] >= 8766 && l_shipdate[i] < 9131
```

## Column Reference

### l_shipdate (date_filter, int32_t, days_since_epoch_1970)
- File: `lineitem/l_shipdate.bin` — int32_t[59,986,052]
- Sorted ascending (table sort order = l_shipdate)
- Predicate: `l_shipdate[i] >= 8766 && l_shipdate[i] < 9131`

### l_discount (measure_filter, double, raw)
- File: `lineitem/l_discount.bin` — double[59,986,052]
- Predicate: `l_discount[i] >= 0.05 && l_discount[i] <= 0.07`
- Also used in: `l_extendedprice * l_discount` accumulation

### l_quantity (measure, double, raw)
- File: `lineitem/l_quantity.bin` — double[59,986,052]
- Predicate: `l_quantity[i] < 24.0`

### l_extendedprice (measure, double, raw)
- File: `lineitem/l_extendedprice.bin` — double[59,986,052]
- Used in: `SUM(l_extendedprice * l_discount)`

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout (from build_indexes.cpp `build_zone_map`):
  ```
  uint64_t  n_blocks               // = 600
  { int32_t min_val; int32_t max_val; } [n_blocks]
  ```
- Usage for Q6: scan zone map to find the contiguous run of blocks that overlaps [8766, 9131):
  ```cpp
  uint64_t n_blocks;
  fread(&n_blocks, 8, 1, f);
  size_t first_block = SIZE_MAX, last_block = 0;
  for (size_t b = 0; b < n_blocks; b++) {
      int32_t mn, mx;
      fread(&mn, 4, 1, f);
      fread(&mx, 4, 1, f);
      // Block overlaps [8766, 9131) if mx >= 8766 && mn < 9131
      if (mx >= 8766 && mn < 9131) {
          if (b < first_block) first_block = b;
          last_block = b;
      }
  }
  // Only scan rows [first_block*100000 .. (last_block+1)*100000)
  ```
- Since table is sorted by l_shipdate, the qualifying blocks form a contiguous range. This eliminates the vast majority of the 59.9M rows — only ~1994 date values are in range (~8.4M rows), but zone map narrows it further to only blocks touching 1994.

## Aggregation Strategy
Single accumulator:
```cpp
double revenue = 0.0;
// For each row i in qualifying block range:
if (l_shipdate[i] >= 8766 && l_shipdate[i] < 9131 &&
    l_discount[i] >= 0.05 && l_discount[i] <= 0.07 &&
    l_quantity[i]  < 24.0) {
    revenue += l_extendedprice[i] * l_discount[i];
}
```
Read columns in block-sized chunks (100,000 rows) to maximize cache reuse.
