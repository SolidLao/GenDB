# Q6 Guide — Forecasting Revenue Change

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows × 4 bytes, **sorted ascending**)
- This query: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR`
  = `l_shipdate >= 8766 AND l_shipdate < 9131`
  - epoch('1994-01-01') = 8766
  - epoch('1995-01-01') = 9131  (1994 is not a leap year: 8766 + 365 = 9131)
- Zone map: since lineitem is sorted by l_shipdate, rows in [8766, 9131) form a contiguous range of blocks
  - Estimated selectivity: 14.3% → ~86 of 600 blocks qualify → skip 514 blocks (86% skip rate!)
  - This is the most impactful optimization for Q6

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Stored as native double; range [0.00, 0.10]
- This query: `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01` = `l_discount BETWEEN 0.05 AND 0.07`
  - In C++: `l_discount[i] >= 0.05 && l_discount[i] <= 0.07`
  - Note: floating-point BETWEEN — use `>= 0.05 - 1e-10 && <= 0.07 + 1e-10` to handle FP edges
  - Estimated selectivity: ~20% of rows (discount has 11 distinct values: 0.00..0.10)
    Values 0.05, 0.06, 0.07 = 3 of 11 ≈ 27.3% but combined with shipdate filter

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- This query: `l_quantity < 24` → `l_quantity[i] < 24.0`
  - Estimated selectivity: ~55%

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- This query: output = `SUM(l_extendedprice * l_discount)` — compute product and accumulate

## Table Stats
| Table    | Rows       | Role | Sort Order   | Block Size |
|----------|------------|------|--------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate ↑ | 100,000    |

## Query Analysis
- **Join pattern**: Single table scan — no joins
- **Filters** (all on lineitem, applied conjunctively):
  1. `l_shipdate >= 8766 AND l_shipdate < 9131` → selectivity ~14.3% — zone-map very effective
  2. `l_discount BETWEEN 0.05 AND 0.07` → selectivity ~27% (of rows passing date filter)
  3. `l_quantity < 24.0` → selectivity ~55% (of rows passing date filter)
  - Combined selectivity: ~2% of total lineitem rows pass all filters
- **Aggregation**: Single scalar SUM → no GROUP BY, returns one row
- **Output**: single row with `revenue` = SUM(l_extendedprice * l_discount)
- **Optimization**: branch-free SIMD-style multi-predicate filtering. All three predicates can be evaluated on registers. Accumulate product conditionally.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600]` then 600 × `[int32_t min_val, int32_t max_val, uint32_t row_count]`
- Total file: 4 + 600 × 12 = 7,204 bytes (fits in L1 cache)
- Skip logic for Q6: skip block b if `zone.max_val < 8766 OR zone.min_val >= 9131`
  - Keep only blocks that OVERLAP [8766, 9131)
  - Since data is sorted, qualifying blocks form a contiguous run
  - Binary search for first block with `max_val >= 8766` and last block with `min_val < 9131`
  - row_offset for block b = b × 100,000 (row index); use `zone.row_count` for boundary blocks
- **Expected skip**: ~86% of blocks skipped → only ~86 blocks (8.6M rows) need to be read
  - Further filtered by discount+quantity → ~172K rows actually contribute to SUM
- Access: `const int32_t* shipdate = reinterpret_cast<const int32_t*>(mmap_ptr)` etc.

## Execution Strategy
1. Load zone map (7KB, fits in L1 cache)
2. Find qualifying block range [first_block, last_block] via binary search on zone min/max
3. For each qualifying block (or partial end blocks):
   - mmap l_shipdate, l_discount, l_quantity, l_extendedprice for the block range
   - 64-thread parallel scan: each thread processes a morsel within the qualifying range
   - Per row: `if (sd >= 8766 && sd < 9131 && disc >= 0.05 && disc <= 0.07 && qty < 24.0) revenue += price * disc`
   - Branch-free vectorization friendly: compute mask, conditional add
4. Reduce thread-local revenues → final SUM
5. Output: `SELECT SUM(...) AS revenue` → single value
