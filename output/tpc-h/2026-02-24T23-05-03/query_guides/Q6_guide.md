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
| lineitem | 59,986,052 | fact | (none)     | 65536      |

## Column Reference

### l_shipdate (date, int32_t — days_since_epoch_1970)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes = 239,944,208 bytes)
- Encoding: Howard Hinnant algorithm; 1970-01-01 → 0; values confirmed > 3000
- This query: `l_shipdate >= lo AND l_shipdate < hi` (BETWEEN year range)
- **C1/C7**: Use `date_utils.h`:
  ```cpp
  gendb::init_date_tables();  // C11: call once at top of main()
  int32_t lo = gendb::date_to_epoch("1994-01-01");
  int32_t hi = gendb::add_years(lo, 1);  // 1995-01-01; NEVER use lo + 365
  ```
- Filter selectivity: ~15.4% (1 year out of ~7-year range)

### l_discount (measure, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; values ∈ [0.00, 0.10] with 11 distinct values
- This query: `l_discount BETWEEN 0.05 AND 0.07` (0.06-0.01 = 0.05, 0.06+0.01 = 0.07)
- Filter selectivity: ~27.3%
- **Floating-point comparison**: use tolerance or inclusive integer-scaled check:
  ```cpp
  // Precise: compare as doubles — 0.05 and 0.07 are stored exactly in TPC-H
  l_discount >= 0.05 && l_discount <= 0.07
  ```

### l_quantity (measure, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; values ∈ [1, 50] (integer-valued but stored as double)
- This query: `l_quantity < 24`
- Filter selectivity: ~46.0%

### l_extendedprice (measure, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; max individual value ~104,949
- This query: used in SUM(l_extendedprice * l_discount) — scalar aggregate
- **C35 / C29 analysis**: SUM(ep * disc) is a two-column derived expression (NOT single-column SUM).
  C29 does NOT apply. Use `long double` accumulation (C35):
  ```cpp
  long double revenue = 0.0L;
  revenue += (long double)ep * disc;
  ```
  Combined filter selectivity ~2.0% → ~1.2M rows contribute

## Query Analysis
- **Access pattern**: Sequential scan of lineitem, 4 columns
- **Filters**: shipdate (zone-map guided), discount (inline), quantity (inline)
- **Combined selectivity**: ~2.0% → ~1.2M qualifying rows out of 60M
- **Aggregation**: single scalar sum → no GROUP BY, no hash table needed
- **Zone map**: l_shipdate zone map enables skipping ~85% of blocks (non-1994 year)
- **Parallelism**: OpenMP parallel reduction over blocks; merge = single sum addition

## Indexes

### l_shipdate_zone_map (zone_map on l_shipdate)
- File: `lineitem/indexes/l_shipdate_zone_map.bin`
- Layout: `[uint32_t num_blocks][Block*]` where `struct Block { int32_t mn, mx; uint32_t cnt; }`
- Block size: 65536 rows → `num_blocks = ceil(59986052 / 65536) = 916` blocks
- Each Block record: 12 bytes (int32_t min + int32_t max + uint32_t count)
- File size: 4 + 916×12 = 10,996 bytes
- **Usage**: skip block b entirely if `blocks[b].mx < lo || blocks[b].mn >= hi`
  (block cannot overlap [1994-01-01, 1995-01-01))
- With 15.4% selectivity, ~85% of blocks can potentially be skipped → major I/O win on HDD
- Access pattern:
  ```cpp
  size_t zm_sz;
  const uint32_t* zm_raw = (const uint32_t*)mmap_ro(zm_file, zm_sz);
  uint32_t num_blocks = zm_raw[0];
  struct ZMBlock { int32_t mn, mx; uint32_t cnt; };
  const ZMBlock* blocks = (const ZMBlock*)(zm_raw + 1);

  for (uint32_t b = 0; b < num_blocks; b++) {
      if (blocks[b].mx < lo || blocks[b].mn >= hi) continue; // skip block
      size_t row_start = (size_t)b * 65536;
      size_t row_end   = row_start + blocks[b].cnt;
      // scan [row_start, row_end) with inline discount/quantity filters
  }
  ```

## Scalar Aggregation Pattern
```cpp
long double revenue = 0.0L;
// Per-thread local accumulator; merge with atomic or barrier:
#pragma omp parallel reduction(+:revenue)
{
    long double local_rev = 0.0L;
    // ... zone-map-guided block loop ...
    // per row:
    if (sd[row] >= lo && sd[row] < hi &&
        disc[row] >= 0.05 && disc[row] <= 0.07 &&
        qty[row]  < 24.0) {
        local_rev += (long double)ep[row] * disc[row];
    }
    revenue += local_rev;
}
printf("%.2Lf\n", revenue);  // or cast to double for output
```

## Date Constant Summary
| SQL Expression                              | C++ Pattern                                              |
|---------------------------------------------|----------------------------------------------------------|
| DATE '1994-01-01'                           | `gendb::date_to_epoch("1994-01-01")`                     |
| DATE '1994-01-01' + INTERVAL '1' YEAR       | `gendb::add_years(gendb::date_to_epoch("1994-01-01"), 1)` |

## Filter Constant Summary
| SQL Expression            | C++ Comparison                    |
|---------------------------|-----------------------------------|
| l_discount BETWEEN 0.05 AND 0.07 | `disc >= 0.05 && disc <= 0.07` |
| l_quantity < 24           | `qty < 24.0`                      |
