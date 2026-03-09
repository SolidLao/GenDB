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
| Table    | Rows       | Role      | Sort Order     | Block Size |
|----------|------------|-----------|----------------|------------|
| lineitem | 59,986,052 | fact/scan | l_shipdate ASC | 100,000    |

## Column Reference

### l_shipdate (`DATE`, `int32_t`, days_since_epoch)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes = ~229 MB)
- Encoding: days since 1970-01-01 (civil calendar, `date_to_days()` function in ingest.cpp)
- Table is **sorted by this column ASC** — zone map provides maximum benefit here
- This query: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR`
  - Low bound:  **1994-01-01**
    - y=1994, m=1, d=1 → (m≤2): y=1993, m=13
    - era=4, yoe=393, doy=(153×10+2)/5+0=306, doe=143846
    - **lo = 4×146097 + 143846 − 719468 = 8766**
  - High bound: **1995-01-01** (= 1994-01-01 + 365 days in non-leap year 1994)
    - y=1995, m=1, d=1 → (m≤2): y=1994, m=13
    - era=4, yoe=394, doy=306, doe=144211
    - **hi = 4×146097 + 144211 − 719468 = 9131**
  - C++ filter: `l_shipdate[i] >= 8766 && l_shipdate[i] < 9131`
- Selectivity: ~1.9% — zone map skips ~98.1% of blocks (most powerful filter in workload)

### l_discount (`DECIMAL(15,2)`, `int8_t`, int8_hundredths)
- File: `lineitem/l_discount.bin` (59,986,052 × 1 byte = ~57 MB)
- Encoding: value × 100 stored as int8_t; range 0–10 (representing 0.00–0.10)
- This query: `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01` = `BETWEEN 0.05 AND 0.07`
  - In encoded form: `l_discount[i] >= 5 && l_discount[i] <= 7`
  - **No floating-point conversion needed** — filter entirely in integer domain
- Selectivity after shipdate filter: ~21% (3 out of 11 distinct values: 5, 6, 7)

### l_quantity (`DECIMAL(15,2)`, `int8_t`, int8_integer_value)
- File: `lineitem/l_quantity.bin` (59,986,052 × 1 byte = ~57 MB)
- Encoding: actual integer value 1–50 stored directly as int8_t (no scale factor)
- This query: `l_quantity < 24`
  - C++ filter: `l_quantity[i] < (int8_t)24`
  - **No decoding needed** — integer comparison on raw stored byte
- Selectivity: ~47% (values 1–23 pass out of 1–50)

### l_extendedprice (`DECIMAL(15,2)`, `double`, plain_double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes = ~458 MB)
- Encoding: IEEE-754 double, value as-is
- This query: `SUM(l_extendedprice * l_discount)` — revenue accumulation
  - Decode discount at use: `double disc = l_discount[i] * 0.01;`
  - Revenue contribution: `l_extendedprice[i] * disc`
  - Accumulate into a single `double revenue = 0.0`

## Query Analysis

### Combined Selectivity
- l_shipdate BETWEEN: ~1.9%
- l_discount BETWEEN 5 AND 7 (given shipdate passes): ~21%
- l_quantity < 24 (given above): ~47%
- Combined: ~1.9% × 21% × 47% ≈ **0.19%** of all rows contribute to revenue

### Execution Strategy
Because lineitem is sorted by l_shipdate ASC, the zone map isolates rows to a
contiguous region of blocks. This is Q6's dominant optimization.

```
Total rows: 59,986,052
After zone map (1.9%): ~1,140,000 rows in qualifying blocks
After all 3 filters:   ~113,700 rows → each contributes to revenue sum
```

**Recommended inner loop** (after zone-map block selection):
```cpp
double revenue = 0.0;
// Only scan rows in blocks where zone.min_date < 9131 && zone.max_date >= 8766
for (size_t i = row_start; i < row_end; i++) {
    int32_t sd   = l_shipdate[i];
    if (sd < 8766 || sd >= 9131) continue;   // shipdate filter
    int8_t  disc = l_discount[i];
    if (disc < 5 || disc > 7) continue;      // discount filter (int8, no decode)
    int8_t  qty  = l_quantity[i];
    if (qty >= 24) continue;                 // quantity filter (int8, no decode)
    revenue += l_extendedprice[i] * (disc * 0.01);
}
```

### Parallelization
- Divide the qualifying block range across 64 threads
- Each thread accumulates a local `double` sum; reduce at end
- Since blocks are contiguous in the zone-map range, thread partitioning is trivial

## Indexes

### l_shipdate_zone_map (zone_map on l_shipdate)
- File: `lineitem/l_shipdate_zone_map.bin`
- Layout:
  ```
  Byte [0..3]:   uint32_t num_blocks  (= ceil(59986052 / 100000) = 600)
  Byte [4..7]:   uint32_t block_size  (= 100,000)
  Byte [8..]:    ZoneEntry[num_blocks]
                   struct ZoneEntry { int32_t min_date; int32_t max_date; }
                   sizeof = 8 bytes; total = 600 × 8 = 4800 bytes
  ```
- Total file size: 8 + 4800 = **4808 bytes** (fits entirely in L1 cache)
- **Usage for Q6** — this is the critical index:
  ```cpp
  // Load zone map:
  FILE* zf = fopen(".../lineitem/l_shipdate_zone_map.bin", "rb");
  uint32_t num_blocks, block_size;
  fread(&num_blocks, 4, 1, zf);
  fread(&block_size, 4, 1, zf);
  struct ZE { int32_t mn, mx; };
  std::vector<ZE> zones(num_blocks);
  fread(zones.data(), sizeof(ZE), num_blocks, zf);
  fclose(zf);

  const int32_t LO = 8766;  // date_to_days("1994-01-01")
  const int32_t HI = 9131;  // date_to_days("1995-01-01")

  for (uint32_t b = 0; b < num_blocks; b++) {
      // Skip block if entirely outside [LO, HI)
      if (zones[b].mx < LO || zones[b].mn >= HI) continue;
      size_t row_start = (size_t)b * block_size;
      size_t row_end   = std::min(row_start + block_size, (size_t)total_rows);
      // inner loop here
  }
  ```
- **Q6 effectiveness**: ~98.1% of blocks skipped (only ~11–12 of 600 blocks contain
  any 1994 dates, since lineitem spans 1992–1998 sorted ascending)
- Since data is sorted, qualifying blocks form a contiguous window — can find start/end
  block with binary search on zone min/max for further speedup
