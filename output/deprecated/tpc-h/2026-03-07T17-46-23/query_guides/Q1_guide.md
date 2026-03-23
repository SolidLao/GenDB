# Q1 Guide

## SQL
```sql
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity) AS sum_qty, SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price,
       AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

## Column Reference

### l_shipdate (date, int32_t, days_since_epoch)
- File: `lineitem/l_shipdate.bin` (~60M rows, 4 bytes each)
- Encoding: Howard Hinnant's `days_from_civil` — days since 1970-01-01
- This query: `WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
- Date constant: 1998-09-02. Compute at compile time using same algorithm:
  ```cpp
  // days_from_civil(1998, 9, 2):
  // yr=1998, mn=9, dy=2 → yr -= (9<=2)=0 → yr=1998
  // era = 1998/400 = 4, yoe = 1998-1600=398
  // doy = (153*(9-3)+2)/5 + 2 - 1 = (153*6+2)/5 + 1 = 920/5 + 1 = 184+1 = 185
  // doe = 398*365 + 398/4 - 398/100 + 185 = 145270+99-3+185 = 145551
  // result = 4*146097 + 145551 - 719468 = 584388+145551-719468 = 10471
  int32_t threshold = 10471; // 1998-09-02
  ```
- C++ filter: `l_shipdate[i] <= 10471`
- Selectivity: ~98.6% of lineitem rows pass (from workload analysis)

### l_returnflag (flag, uint8_t, raw_char_as_byte)
- File: `lineitem/l_returnflag.bin` (~60M rows, 1 byte each)
- Encoding: raw ASCII character stored as uint8_t (e.g., 'A'=65, 'N'=78, 'R'=82)
- This query: GROUP BY key + output column
- 3 distinct values: 'A', 'N', 'R'

### l_linestatus (flag, uint8_t, raw_char_as_byte)
- File: `lineitem/l_linestatus.bin` (~60M rows, 1 byte each)
- Encoding: raw ASCII character stored as uint8_t (e.g., 'F'=70, 'O'=79)
- This query: GROUP BY key + output column
- 2 distinct values: 'F', 'O'

### l_quantity (measure, double, native_binary)
- File: `lineitem/l_quantity.bin` (~60M rows, 8 bytes each)
- This query: `SUM(l_quantity)`, `AVG(l_quantity)` — accumulate in double

### l_extendedprice (measure, double, native_binary)
- File: `lineitem/l_extendedprice.bin` (~60M rows, 8 bytes each)
- This query: `SUM(l_extendedprice)`, `AVG(l_extendedprice)`, and in expressions

### l_discount (measure, double, native_binary)
- File: `lineitem/l_discount.bin` (~60M rows, 8 bytes each)
- This query: `AVG(l_discount)`, and in `(1 - l_discount)` expressions

### l_tax (measure, double, native_binary)
- File: `lineitem/l_tax.bin` (~60M rows, 8 bytes each)
- This query: in `(1 + l_tax)` expression for sum_charge

## Table Stats

| Table    | Rows       | Role | Sort Order    | Block Size |
|----------|------------|------|---------------|------------|
| lineitem | ~59,986,052 | fact | l_orderkey    | 100,000    |

## Query Analysis

### Pattern
Single-table scan of lineitem with date filter, grouped aggregation, ordered output.

### Filter
- `l_shipdate <= 10471` (1998-09-02): passes ~98.6% of rows (~59.1M rows)
- Very high selectivity — nearly a full table scan

### Grouping
- Group key: `(l_returnflag, l_linestatus)` — 2 uint8_t values
- Estimated groups: ~4–6 (3 returnflag values × 2 linestatus values, not all combos exist)
- Optimal: use a small flat array indexed by `(returnflag * 256 + linestatus)` or a compact
  hash map. With only 4–6 groups, a 256×256 or smaller direct-index array is fastest.
- Alternative: encode key as `uint16_t key = ((uint16_t)returnflag << 8) | linestatus`
  and use an array of size 65536 (512KB — fits in L2 cache).

### Aggregation per group
- Accumulators needed: `sum_qty`, `sum_base_price`, `sum_disc_price`, `sum_charge`,
  `sum_discount`, `count` (6 doubles + 1 int64_t)
- `avg_qty = sum_qty / count`, `avg_price = sum_base_price / count`, `avg_disc = sum_discount / count`
- Per-row compute: `disc_price = extendedprice * (1.0 - discount)`, `charge = disc_price * (1.0 + tax)`

### Output ordering
- ORDER BY l_returnflag ASC, l_linestatus ASC
- Only 4–6 result rows — sort is trivial

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t num_blocks
  Byte 4-7:   uint32_t block_size (= 100000)
  Byte 8+:    array of { int32_t min_date; int32_t max_date; } per block
  ```
  Each ZoneEntry is 8 bytes (two int32_t).
- Total entries: ceil(59986052 / 100000) = 600 blocks
- Usage: For each block b, if `zones[b].min_date > 10471`, skip entire block (all rows > threshold).
  Since selectivity is 98.6%, only the last ~8 blocks might be skippable.
- **Net benefit for Q1 is minimal** since almost all rows pass. The zone map may skip a few
  trailing blocks. A simple full scan may be equally fast.

## Performance Notes
- This is a CPU-bound aggregation over ~60M rows. Key bottleneck is memory bandwidth.
- Column files total: 7 columns × 60M rows ≈ l_shipdate(228MB) + l_returnflag(57MB) +
  l_linestatus(57MB) + l_quantity(457MB) + l_extendedprice(457MB) + l_discount(457MB) +
  l_tax(457MB) ≈ 2.2 GB total read.
- With 64 cores and HDD: parallelize the scan across row ranges. Each thread processes a
  chunk and maintains local accumulators, then merge at the end.
- SIMD (AVX-512) can accelerate the date comparison and arithmetic.
