# Q1 Guide — Pricing Summary Report

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59986052 rows)
- Sorted column — lineitem is sorted by l_shipdate ascending. Zone map available.
- Encoding: int32_t days since 1970-01-01. `parse_date("1970-01-01") == 0`.
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
  - Threshold date: 1998-09-02 → epoch **10471**
  - C++ filter: `raw_shipdate <= 10471`
- Zone map file: `indexes/lineitem_shipdate_zonemap.bin`
  - num_blocks=600; block 0: [8036,8066] (1992-01-02 to 1992-02-01); block 599: [10533,10561]
  - Skip blocks where `block_min > 10471` (only the very last few blocks; ~98% of rows pass, so zone map has minimal benefit here but skip tail blocks)

### l_returnflag (STRING, uint8_t, byte_pack)
- File: `lineitem/l_returnflag.bin` (59986052 rows, uint8_t)
- Lookup: `lineitem/l_returnflag_lookup.txt` — load as `std::vector<std::string>`
  - Code 0 = "A", Code 1 = "N", Code 2 = "R"  (alphabetical assignment)
- This query: GROUP BY l_returnflag → use raw uint8_t code as group key; decode to string for output via `lookup[code]`
- Aggregate into per-code accumulators (3 distinct codes)

### l_linestatus (STRING, uint8_t, byte_pack)
- File: `lineitem/l_linestatus.bin` (59986052 rows, uint8_t)
- Lookup: `lineitem/l_linestatus_lookup.txt`
  - Code 0 = "F", Code 1 = "O"  (alphabetical assignment)
- This query: GROUP BY l_linestatus → use raw uint8_t code as group key; decode to string for output

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59986052 rows, 8 bytes each)
- Stored as native double. Values match SQL directly (e.g., 17.0, 36.0).
- This query: `SUM(l_quantity)`, `AVG(l_quantity)` — accumulate directly as double

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59986052 rows, 8 bytes each)
- Stored as native double. Values match SQL directly (e.g., 81304.65).
- This query: `SUM(l_extendedprice)`, `SUM(l_extendedprice*(1-l_discount))`,
  `SUM(l_extendedprice*(1-l_discount)*(1+l_tax))`, `AVG(l_extendedprice)`

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59986052 rows, 8 bytes each)
- Stored as native double. Values in [0.00, 0.10].
- This query: `SUM(l_extendedprice*(1-l_discount))`, `AVG(l_discount)`

### l_tax (DECIMAL, double)
- File: `lineitem/l_tax.bin` (59986052 rows, 8 bytes each)
- Stored as native double.
- This query: `SUM(l_extendedprice*(1-l_discount)*(1+l_tax))`

## Table Stats
| Table    | Rows     | Role | Sort Order  | Block Size |
|----------|----------|------|-------------|------------|
| lineitem | 59986052 | fact | l_shipdate  | 100000     |

## Query Analysis
- **Join pattern**: Single table scan, no joins.
- **Filter**: `l_shipdate <= 10471` (epoch for 1998-09-02).
  - Estimated selectivity: 98% — almost all rows qualify.
  - Zone maps: can skip the very last few blocks (where block_min > 10471); ~2 blocks skipped out of 600.
  - Main strategy: full columnar scan with SIMD vectorized filter.
- **Aggregation**: GROUP BY (l_returnflag, l_linestatus) → at most 3×2=6 groups.
  - Use a fixed 6-slot accumulator array indexed by `returnflag_code * 2 + linestatus_code`.
  - Accumulate: sum_qty, sum_base_price, sum_disc_price, sum_charge, count, and partial sums for AVG.
  - AVG = sum / count computed at the end.
- **Output**: 6 rows (or fewer), ORDER BY l_returnflag ASC, l_linestatus ASC.
- **Parallelism**: Morsel-driven (64 threads), each thread processes a chunk of blocks.
  Per-thread local accumulators; merge at the end.
- **Combined selectivity**: ~98% of 60M rows ≈ 58.8M rows processed.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout:
  ```
  uint32_t num_blocks  (= 600)
  [int32_t min, int32_t max] × 600   (8 bytes per block)
  ```
- Block i covers rows [i*100000, min((i+1)*100000, 59986052)).
- row_offset is a ROW index (not byte offset). Access column value at `col_data[row_idx]`.
- This query: iterate blocks; if `block_min > 10471` → skip entire block.
  - Since lineitem is sorted by l_shipdate and threshold is 10471 (1998-09-02), only the last ~2 blocks (with dates in Sep–Dec 1998) can be skipped; not a large win for Q1 specifically.
- Probe pattern:
  ```cpp
  const int32_t* shipdate = mmap_col<int32_t>(db + "/lineitem/l_shipdate.bin");
  for (uint32_t b = 0; b < num_blocks; b++) {
      if (zone_min[b] > 10471) continue;  // skip block
      uint32_t start = b * 100000, end = std::min(start + 100000, N);
      for (uint32_t i = start; i < end; i++) {
          if (shipdate[i] <= 10471) { /* process row i */ }
      }
  }
  ```
