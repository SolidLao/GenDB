# Q6 Guide — Forecasting Revenue Change

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows, **sorted ascending**)
- Encoding: days since epoch. `parse_date("1970-01-01")` = 0.
- This query:
  - `l_shipdate >= DATE '1994-01-01'` → `raw >= 8766`
    - Epoch: `YEAR_DAYS[24] + 0 + 0` = `8766`
  - `l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR` = `< DATE '1995-01-01'` → `raw < 9131`
    - Epoch: `YEAR_DAYS[25] + 0 + 0` = `9131`
  - Combined: `raw >= 8766 AND raw < 9131`

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows)
- Stored as native double. Values in `[0.00, 0.10]` with 11 distinct values.
- This query: `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01` = `BETWEEN 0.05 AND 0.07`
  - C++ filter: `raw >= 0.05 && raw <= 0.07`
  - Use epsilon-safe comparison: `raw >= 0.05 - 1e-9 && raw <= 0.07 + 1e-9`

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows)
- Stored as native double. Values in `[1.0, 50.0]` with 50 distinct values.
- This query: `l_quantity < 24` → C++ filter: `raw < 24.0`

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows)
- Stored as native double. No scaling needed.
- This query: `SUM(l_extendedprice * l_discount) AS revenue`
  - Accumulate: `revenue += l_extendedprice[i] * l_discount[i]` for qualifying rows.

---

## Table Stats
| Table    | Rows       | Role | Sort Order   | Block Size |
|----------|------------|------|--------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate ↑ | 100,000    |

---

## Query Analysis
- **Access pattern**: Highly selective scan of lineitem with 3 independent filters.
- **Filters and selectivities** (from workload analysis):
  - `l_shipdate >= 8766 AND l_shipdate < 9131`: combined ~14% of rows qualify (1994 date range)
  - `l_discount BETWEEN 0.05 AND 0.07`: ~27% of rows qualify (3 of 11 discount values)
  - `l_quantity < 24`: ~48% of rows qualify (values 1-23 out of 1-50)
  - **Combined selectivity**: ~0.14 × 0.27 × 0.48 ≈ **0.018** (only ~1.1M rows match all 3 filters)
- **Zone map effectiveness** (critical for this query):
  - `l_shipdate` zone map skips ~85% of blocks:
    - Blocks with `max < 8766` → fully before 1994 → skip
    - Blocks with `min >= 9131` → fully after 1994 → skip
    - Only ~90 of 600 blocks need to be scanned at all
  - `l_discount` zone map: all blocks contain the full range [0.00, 0.10] → **no blocks skippable**
  - `l_quantity` zone map: all blocks contain range [1, 50] → **no blocks skippable**
  - **Use only l_shipdate zone map** for block skipping; evaluate discount and quantity as row-level filters.
- **Aggregation**: Single output row (`SUM(l_extendedprice * l_discount)`).
- **Parallelism**: Partition the ~90 qualifying blocks among threads. Each thread computes local sum.
  Final result = sum of thread-local sums.
- **No joins, no subqueries, no GROUP BY, no ORDER BY**.

---

## Indexes

### l_shipdate_zonemap (zone_map on l_shipdate) — PRIMARY OPTIMIZATION
- File: `lineitem/indexes/l_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600]` then `[double min_val, double max_val, uint32_t row_count, uint32_t _pad]` per block = 24 bytes/entry
- `row_offset` of block `i` = `i * 100000` (row index, NOT byte offset). Access column data as `col_ptr[row_offset]`.
- This query: skip block `i` if `entry[i].max_val < 8766.0 OR entry[i].min_val >= 9131.0`
  - This skips ~510 of 600 blocks (85% skip rate) — critical for Q6 performance.
  - Since data is sorted, can binary-search for first block with `max >= 8766`, and last block with `min < 9131`.

### l_discount_zonemap (zone_map on l_discount)
- File: `lineitem/indexes/l_discount_zonemap.bin`
- Layout: same 24-byte ZoneMapEntry per block.
- This query: theoretically `min > 0.07 OR max < 0.05` could skip blocks, but since l_discount
  is uniformly distributed within blocks (data sorted by shipdate, not discount), **every block
  spans [0.00, 0.10] → no blocks skippable**. Do NOT use for block skipping in Q6.

### l_quantity_zonemap (zone_map on l_quantity)
- File: `lineitem/indexes/l_quantity_zonemap.bin`
- Layout: same 24-byte ZoneMapEntry per block.
- This query: `l_quantity < 24` could skip blocks where `min >= 24.0`, but since data is not
  sorted by quantity, **all blocks span [1, 50] → no blocks skippable**. Do NOT use.

---

## Optimized Execution Plan
1. Load l_shipdate zone map (14 KB). Identify ~90 qualifying blocks via zone map skip.
2. For each qualifying block (up to 100,000 rows):
   - Load l_shipdate chunk, filter rows where `8766 <= val < 9131` → candidate bitmask
   - For candidates: load l_discount, filter `0.05 <= val <= 0.07`
   - For remaining: load l_quantity, filter `val < 24.0`
   - For final survivors: load l_extendedprice, accumulate `revenue += price * discount`
3. Sum across all threads. Output single row.
