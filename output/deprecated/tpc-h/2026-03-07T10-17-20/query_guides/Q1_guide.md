# Q1 Guide — Pricing Summary Report

## Query
```sql
SELECT l_returnflag, l_linestatus,
    SUM(l_quantity), SUM(l_extendedprice),
    SUM(l_extendedprice * (1 - l_discount)),
    SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)),
    AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount),
    COUNT(*)
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

## Table Stats
| Table    | Rows       | Role | Sort Order   | Block Size |
|----------|------------|------|--------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate ↑ | 100,000    |

## Column Reference

### l_shipdate (date, int32_t — days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes)
- Filter: `l_shipdate <= DATE '1998-12-01' - 90 days`
- The threshold date is **1998-09-02**. Compute at query startup using the same
  algorithm as `ingest.cpp::parse_date`:
  ```cpp
  // parse_date("1998-09-02")
  int y=1998, m=9, d=2;
  // m > 2: no adjustment
  int A = y / 100;                          // 19
  int B = 2 - A + A/4;                     // 2-19+4 = -13
  int jdn = (int)(365.25*(y+4716))         // (int)(365.25*6714) = 2451100
           + (int)(30.6001*(m+1))          // (int)(30.6001*10)  = 306
           + d + B - 1524;                 // +2 -13 -1524
  // jdn = 2449871  →  days_since_epoch = jdn - 2440588 = 9283
  const int32_t kQ1ShipdateMax = 9283;
  ```
- Selectivity: ~98% of rows pass (almost the entire table)

### l_returnflag (categorical_3, int8_t — dict encoded)
- File: `lineitem/l_returnflag.bin` (59,986,052 × 1 byte)
- Encoding from `ingest.cpp::encode_returnflag`:
  ```cpp
  // A→0, N→1, R→2
  (c == 'A') ? 0 : (c == 'N') ? 1 : 2
  ```
- Load the mapping at query startup by reading this hardcoded switch.
  **Important:** dict codes are in alphabetical order (A < N < R ↔ 0 < 1 < 2),
  so ORDER BY l_returnflag ASC on the raw int8_t values produces the correct
  alphabetical output order without any re-mapping.
- This query: GROUP BY key (3 distinct values)

### l_linestatus (categorical_2, int8_t — dict encoded)
- File: `lineitem/l_linestatus.bin` (59,986,052 × 1 byte)
- Encoding from `ingest.cpp::encode_linestatus`:
  ```cpp
  // F→0, O→1
  (c == 'F') ? 0 : 1
  ```
- **Important:** F < O alphabetically ↔ 0 < 1, so ORDER BY l_linestatus ASC
  on the raw int8_t also produces correct alphabetical output order.
- This query: GROUP BY key (2 distinct values)

### l_quantity (decimal_15_2, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes)
- This query: SUM and AVG accumulator

### l_extendedprice (decimal_15_2, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes)
- This query: SUM and AVG accumulator; also multiplicand in disc_price and charge

### l_discount (decimal_15_2, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes)
- This query: AVG accumulator; multiplied in `extendedprice * (1 - discount)`

### l_tax (decimal_15_2, double)
- File: `lineitem/l_tax.bin` (59,986,052 × 8 bytes)
- This query: multiplied in `... * (1 + tax)`

## Indexes

### l_shipdate_zone_map (zone_map on l_shipdate)
- File: `lineitem/l_shipdate_zone_map.bin`
- Layout (all int32_t, packed):
  ```
  [0]           int32_t  num_blocks        // = ceil(59986052 / 100000) = 600
  [1]           int32_t  block_size        // = 100000
  [2 .. N+1]    int32_t  min[num_blocks]   // min l_shipdate in block b
  [N+2 .. 2N+1] int32_t  max[num_blocks]   // max l_shipdate in block b
  ```
  Because `lineitem` is **sorted by l_shipdate ascending**, `min[b] = col[b*100000]`
  and `max[b] = col[min(b*100000+99999, nrows-1)]`.
- Usage for Q1: skip block `b` entirely when `min[b] > kQ1ShipdateMax`.
  Since 98% of rows pass, most blocks will NOT be skipped — the zone map mainly
  confirms that a suffix of blocks (near the end of the date range) can be skipped.
  The primary benefit is avoiding reads of the final ~2% of blocks.
- Skip condition:
  ```cpp
  if (zone_min[b] > kQ1ShipdateMax) continue; // entire block is past threshold
  ```
  No lower-bound skip is needed since the filter is only an upper bound.

## Query Analysis

### Execution Strategy
1. Load zone map header (num_blocks=600, block_size=100000).
2. For each block b in [0, num_blocks):
   - If `zone_min[b] > 9283`: skip (entire block is after threshold).
   - Else: load block slice of all 7 columns (l_shipdate, l_returnflag,
     l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax) and iterate rows.
3. Per row: if `l_shipdate[i] <= 9283` → accumulate into group `(l_returnflag[i], l_linestatus[i])`.
4. Aggregation state: fixed array of 6 slots (3 returnflag × 2 linestatus).
   Key: `int key = l_returnflag[i] * 2 + l_linestatus[i]` → direct index [0..5].
   ```cpp
   struct Q1Group {
       double sum_qty, sum_base_price, sum_disc_price, sum_charge;
       double sum_qty_for_avg, sum_price_for_avg, sum_disc_for_avg;
       int64_t count;
   } groups[6] = {};
   ```
5. After scan: compute AVG = sum / count per group.
6. Output: 4 groups max, ordered by (l_returnflag ASC, l_linestatus ASC).
   Sort the 6 slots by key (already in ascending order by construction) and emit
   only non-zero groups.

### Decoding for Output
To print the original string values from dict codes:
```cpp
// returnflag: code → char
const char rf_chars[] = {'A', 'N', 'R'};   // index = dict code
// linestatus: code → char
const char ls_chars[] = {'F', 'O'};         // index = dict code
```
Do NOT hardcode these values — derive them from the encoding functions in ingest.cpp
(encode_returnflag: A=0,N=1,R=2; encode_linestatus: F=0,O=1).

### Performance Notes
- ~98% of rows pass the date filter; zone map skips only the trailing blocks.
- Aggregation into 6 fixed slots fits in a few cache lines — no hash map needed.
- AVX-512 SIMD vectorization opportunity: process 8 doubles per cycle for the
  sum/multiply operations on l_extendedprice, l_discount, l_tax.
- Hardware: 64 cores, AVX-512 available (avx512f avx512dq avx512bw avx512vl).
