# Q6 Guide

## SQL
```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24;
```

## Column Reference

### l_shipdate (date, int32_t, days_since_epoch)
- File: `lineitem/l_shipdate.bin` (~60M rows, 4 bytes each)
- Encoding: Howard Hinnant's `days_from_civil` — int32_t days since 1970-01-01
- This query: `WHERE l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'`
- Date constants:
  ```cpp
  // days_from_civil(1994, 1, 1):
  // yr=1994, mn=1, dy=1 → yr -= (1<=2)=1 → yr=1993
  // era = (1993>=0 ? 1993 : 1993-399)/400 = 4, yoe = 1993-1600=393
  // doy = (153*(1+9)+2)/5 + 1 - 1 = (153*10+2)/5 = 1532/5 = 306
  // doe = 393*365 + 393/4 - 393/100 + 306 = 143445+98-3+306 = 143846
  // result = 4*146097 + 143846 - 719468 = 584388+143846-719468 = 8766
  int32_t date_19940101 = 8766;

  // days_from_civil(1995, 1, 1):
  // yr=1995, mn=1, dy=1 → yr -= 1 → yr=1994
  // era=4, yoe=1994-1600=394
  // doy = (153*10+2)/5 + 0 = 306
  // doe = 394*365 + 394/4 - 394/100 + 306 = 143810+98-3+306 = 144211
  // result = 4*146097 + 144211 - 719468 = 584388+144211-719468 = 9131
  int32_t date_19950101 = 9131;
  ```
- C++ filter: `l_shipdate[i] >= 8766 && l_shipdate[i] < 9131`
- Selectivity: ~15.1% of lineitem rows

### l_discount (measure, double, native_binary)
- File: `lineitem/l_discount.bin` (~60M rows, 8 bytes each)
- This query: `WHERE l_discount BETWEEN 0.05 AND 0.07` AND in `SUM(l_extendedprice * l_discount)`
- C++ filter: `l_discount[i] >= 0.05 && l_discount[i] <= 0.07`
- Selectivity: ~27.3% of lineitem rows

### l_quantity (measure, double, native_binary)
- File: `lineitem/l_quantity.bin` (~60M rows, 8 bytes each)
- This query: `WHERE l_quantity < 24`
- C++ filter: `l_quantity[i] < 24.0`
- Selectivity: ~46% of lineitem rows

### l_extendedprice (measure, double, native_binary)
- File: `lineitem/l_extendedprice.bin` (~60M rows, 8 bytes each)
- This query: in `SUM(l_extendedprice * l_discount)`

## Table Stats

| Table    | Rows        | Role | Sort Order | Block Size |
|----------|-------------|------|------------|------------|
| lineitem | ~59,986,052 | fact | l_orderkey | 100,000    |

## Query Analysis

### Pattern
Single-table scan with compound filter, scalar aggregation (no GROUP BY).

### Filters (compound selectivity)
- `l_shipdate >= 8766 AND l_shipdate < 9131`: ~15.1%
- `l_discount BETWEEN 0.05 AND 0.07`: ~27.3%
- `l_quantity < 24.0`: ~46%
- Combined (assuming independence): ~15.1% × 27.3% × 46% ≈ 1.89% → ~1.13M qualifying rows
- Only need to accumulate a single double: `SUM(l_extendedprice * l_discount)`

### Recommended Execution Strategy
1. **Zone map pre-filter on shipdate**: Use `lineitem_shipdate_zonemap` to identify blocks
   where `max_date >= 8766 AND min_date < 9131`. Skip blocks entirely outside this range.
   With ~600 blocks, roughly 15% (~90 blocks, ~9M rows) will overlap the date range.

2. **Scan qualifying blocks**: For each non-skipped block, scan l_shipdate, l_discount,
   l_quantity, l_extendedprice. Apply all 3 filters, accumulate sum.

3. **No grouping needed** — single scalar result.

### Optimization
- The zone map is highly effective here: skip ~85% of blocks → read only ~9M rows instead of 60M.
- Within qualifying blocks, the shipdate filter is the most selective individual predicate.
  Apply it first to minimize work on other columns.
- Can vectorize with SIMD (AVX-512): 8 doubles per vector, do comparisons and conditional accumulation.
- Each thread accumulates a local sum; final merge is a simple addition.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout (from build_indexes.cpp):
  ```
  Byte 0-3:   uint32_t num_blocks
  Byte 4-7:   uint32_t block_size (= 100000)
  Byte 8+:    struct { int32_t min_date; int32_t max_date; }[num_blocks]
  ```
  Each ZoneEntry is 8 bytes.
- Loading pattern:
  ```cpp
  FILE* zf = fopen("indexes/lineitem_shipdate_zonemap.bin", "rb");
  uint32_t num_blocks, block_size;
  fread(&num_blocks, 4, 1, zf);
  fread(&block_size, 4, 1, zf);
  struct ZoneEntry { int32_t min_date; int32_t max_date; };
  std::vector<ZoneEntry> zones(num_blocks);
  fread(zones.data(), sizeof(ZoneEntry), num_blocks, zf);
  fclose(zf);

  // For each block b:
  // Skip if zones[b].max_date < 8766 (all dates before range)
  // Skip if zones[b].min_date >= 9131 (all dates after range)
  // Otherwise: scan rows [b*block_size, min((b+1)*block_size, nrows))
  ```

## Performance Notes
- Zone map reduces I/O from ~2.2GB (4 columns × 60M rows) to ~330MB (~15% of blocks).
- With zone map, this becomes a fast scan of ~9M rows with simple arithmetic.
- 4 columns for qualifying blocks: l_shipdate(34MB) + l_discount(69MB) + l_quantity(69MB) +
  l_extendedprice(69MB) ≈ 241MB — fits well in memory, sequential read.
- With 64 cores: assign each qualifying block to a thread. ~90 blocks / 64 cores = ~1.4 blocks/core.
- Expected runtime: very fast (sub-second with warm cache).
