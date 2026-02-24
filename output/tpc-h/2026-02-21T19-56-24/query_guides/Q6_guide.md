# Q6 Guide — Forecasting Revenue Change

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59986052 rows)
- Sorted column — lineitem is sorted by l_shipdate ascending. Zone map available.
- This query: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR`
  - 1994-01-01 → epoch **8766**
  - 1995-01-01 → epoch **9131**
  - C++ filter: `raw_shipdate >= 8766 && raw_shipdate < 9131`
- Zone map: `indexes/lineitem_shipdate_zonemap.bin`
  - Skip blocks where `block_max < 8766` OR `block_min >= 9131`
  - Data range 1992-01-02 (8036) to 1998-12-01 (10561); 1994 is ~2 years in.
  - Estimated: ~14.5% of rows qualify → ~85% of 600 blocks can be skipped.
  - Expected: ~87 qualifying blocks (~500+ blocks skipped). Massive I/O reduction on HDD.

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59986052 rows)
- Stored as native double. Values in [0.00, 0.10] with 11 distinct values.
- This query: `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01` → `l_discount BETWEEN 0.05 AND 0.07`
  - C++ filter: `l_discount >= 0.05 && l_discount <= 0.07`
  - Estimated selectivity: ~27% of rows (distinct values 0.05, 0.06, 0.07 out of 11 possible)
  - Note: use `>= 0.05 - 1e-9 && <= 0.07 + 1e-9` if floating-point edge cases are a concern, but since TPC-H discount values are exact 2-decimal representable fractions (0.00–0.10 step 0.01), direct comparison is safe.

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- Stored as native double. Values in [1.0, 50.0], 50 distinct integer values.
- This query: `l_quantity < 24`
  - C++ filter: `l_quantity < 24.0`
  - Estimated selectivity: ~46% of rows

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- Stored as native double. Values match SQL directly.
- This query: `SUM(l_extendedprice * l_discount) AS revenue` — accumulate over qualifying rows.

## Table Stats
| Table    | Rows     | Role | Sort Order | Block Size |
|----------|----------|------|------------|------------|
| lineitem | 59986052 | fact | l_shipdate | 100000     |

## Query Analysis
- **Join pattern**: Single table scan, no joins.
- **Filters** (applied in order for efficiency):
  1. `l_shipdate >= 8766 AND l_shipdate < 9131` — selectivity 14.5%, zone maps skip ~85% of blocks
  2. `l_discount BETWEEN 0.05 AND 0.07` — selectivity 27% of remaining rows
  3. `l_quantity < 24.0` — selectivity 46% of remaining rows
  - Combined selectivity: ~14.5% × 27% × 46% ≈ **1.8%** of all rows (≈ 1.08M qualifying rows)
- **Aggregation**: Single scalar output `SUM(l_extendedprice * l_discount)`. No GROUP BY.
- **Execution strategy**:
  - Load zone map first; identify qualifying block ranges [first_block, last_block].
  - For qualifying blocks only: mmap and scan l_shipdate (primary filter), then l_discount, l_quantity, l_extendedprice.
  - Fuse all filters and accumulation in a single vectorized loop (AVX-512 capable).
  - Reduce partial sums from 64 threads.
- **Output**: Single double value. Trivial.
- **This is the most I/O-efficient query**: zone maps reduce HDD reads to ~15% of lineitem data.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout:
  ```
  uint32_t num_blocks  (= 600)
  [int32_t min, int32_t max] × 600  (total file = 4 + 600*8 = 4804 bytes)
  ```
- row_offset is a ROW index, not byte offset. Access column i via `col_data[row_idx]`.
- This query: skip block b if `zone_max[b] < 8766 OR zone_min[b] >= 9131`
  - Qualifying block range corresponds to rows with shipdate in 1994 only.
  - Load the entire zone map (4.7 KB) into L1 cache, then iterate.
  - Skip pattern:
    ```cpp
    for (uint32_t b = 0; b < 600; b++) {
        if (zone_max[b] < 8766 || zone_min[b] >= 9131) continue;  // skip block
        uint32_t row_start = b * 100000;
        uint32_t row_end   = std::min(row_start + 100000u, 59986052u);
        // scan rows [row_start, row_end) with all three filters
    }
    ```
  - Since data is sorted, qualifying blocks form a contiguous range. Once `zone_min[b] >= 9131`, all subsequent blocks can also be skipped (break early).
