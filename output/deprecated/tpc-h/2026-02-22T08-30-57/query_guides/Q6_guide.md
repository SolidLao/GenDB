# Q6 Guide — Forecasting Revenue Change

## SQL
```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate <  DATE '1994-01-01' + INTERVAL '1' YEAR  -- = '1995-01-01'
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01      -- [0.05, 0.07]
  AND l_quantity < 24;
```

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows × 4 bytes)
- lineitem is **sorted by l_shipdate** — zone map is maximally effective for this range query.
- This query: `>= '1994-01-01'` → `raw >= 8766`; `< '1995-01-01'` → `raw < 9131`
- Epoch derivation: 1994-01-01 = 24 years from 1970 (with leap years) = 8766; 1995-01-01 = 9131
- Range [8766, 9131) spans 365 days (1994 was not a leap year)
- Zone map file: `indexes/lineitem_shipdate_zonemap.bin`
  - Estimated selectivity: 15.6% of rows (only 1994 rows pass)
  - Since sorted, all qualifying rows form a contiguous segment → **binary search** to find first/last block

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values 0.00..0.10 match SQL directly.
- This query: `BETWEEN 0.05 AND 0.07` → `raw >= 0.05 && raw <= 0.07`
- Zone map file: `indexes/lineitem_discount_zonemap.bin`
  - Format: `[uint32_t num_blocks=600][double min, double max per block]`
  - Skip block if `block_max < 0.05 || block_min > 0.07`
  - But since lineitem is sorted by shipdate (not discount), discount zone maps have little skipping power within the date range. Use only for the date-filtered candidate blocks.
- Selectivity: ~28.2% of rows within date range

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values 1.0..50.0 match SQL directly.
- This query: `l_quantity < 24` → `raw < 24.0`
- Zone map file: `indexes/lineitem_quantity_zonemap.bin`
  - Format: `[uint32_t num_blocks=600][double min, double max per block]`
  - Skip block if `block_min >= 24.0`
  - Selectivity: ~46.9% of rows within date range

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values match SQL directly.
- This query: output `SUM(l_extendedprice * l_discount)` — multiply and accumulate for qualifying rows

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate | 100,000    |

## Query Analysis
- **Pattern**: Pure single-table scan with 3 filters + single scalar aggregate. No joins, no grouping.
- **Filters and selectivities**:
  - `l_shipdate` range [8766, 9131): ~15.6% selectivity
  - `l_discount BETWEEN 0.05 AND 0.07`: ~28.2% within date range
  - `l_quantity < 24`: ~46.9% within date range
  - **Combined selectivity**: ~2.1% of 60M rows ≈ 1.26M qualifying rows
- **Aggregation**: single scalar `SUM(l_extendedprice * l_discount)` — no grouping
- **Output**: 1 row, no ORDER BY
- **Strategy**:
  1. Use shipdate zone map to identify first and last qualifying block via binary search (sorted data).
  2. Within those blocks, apply all 3 predicates in a vectorized inner loop.
  3. AVX512 masked multiply-accumulate: load 8 doubles, mask by (shipdate in range) AND (discount in range) AND (quantity < 24), conditionally accumulate `ep * disc`.
  4. 64 threads partition the qualifying block range; each accumulates a local sum. Final reduce.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][int32_t min, int32_t max per block]`
- Block b covers rows `[b*100000 .. min((b+1)*100000, 59986052))`
- `row_offset` is ROW index, not byte offset
- Usage:
  - Binary search for first block where `block_max >= 8766` (start of 1994)
  - Binary search for last block where `block_min < 9131` (end of 1994)
  - Skip all blocks outside this range (sorted data ⟹ ~84% of blocks pruned)
  - For boundary blocks (first and last), apply per-row date check
  - For interior blocks where `block_min >= 8766 && block_max < 9131`: date predicate always true, skip date check in inner loop

### lineitem_quantity_zonemap (zone_map on l_quantity)
- File: `indexes/lineitem_quantity_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][double min, double max per block]`
- Usage: For candidate blocks (within date range), skip if `block_min >= 24.0`
- This query: since quantity is not sorted within date-range blocks, provides limited but non-zero pruning

### lineitem_discount_zonemap (zone_map on l_discount)
- File: `indexes/lineitem_discount_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][double min, double max per block]`
- Usage: For candidate blocks, skip if `block_max < 0.05 || block_min > 0.07`
- This query: ~11 distinct discount values (0.00..0.10 in 0.01 steps); blocks likely cover [0.00, 0.10] fully → minimal pruning within date range
