# Q6 Guide — Forecasting Revenue Change

## Query
```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24;
```

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows × 4 bytes)
- **lineitem is physically sorted by l_shipdate ascending**
- This query: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'`
  - Lower bound: `raw >= 8766`
    - Derivation: YEAR_START[1994-1970=24] = **8766**
  - Upper bound: `raw < 9131`
    - Derivation: YEAR_START[1995-1970=25] = **9131**
- Zone map skips all blocks where `block_max < 8766` OR `block_min >= 9131`
- Since data is sorted: leading blocks (early dates) are skipped, trailing blocks (late dates) are skipped. ~15% of blocks overlap the 1994 window.

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Stored as native double — values match SQL directly. No scaling needed.
- This query: `l_discount BETWEEN 0.05 AND 0.07` → C++ `ld >= 0.05 && ld <= 0.07`
  - 11 distinct values (0.00, 0.01, ..., 0.10); three qualify: 0.05, 0.06, 0.07
  - Selectivity: ~27% of rows in date range

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- Stored as native double — values match SQL directly. No scaling needed.
- This query: `l_quantity < 24` → C++ `lq < 24.0`
  - Range 1–50; values 1–23 qualify (~46% of distinct values). Selectivity: ~46%

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- Stored as native double — values match SQL directly. No scaling needed.
- Used in: `SUM(l_extendedprice * l_discount)` for qualifying rows only.

## Table Stats
| Table    | Rows       | Role | Sort Order  | Block Size |
|----------|------------|------|-------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate↑ | 100,000    |

## Query Analysis
- **Access pattern**: full scan of lineitem with highly selective combined predicates
- **Filters and selectivities** (from workload analysis):
  - `l_shipdate BETWEEN 1994-01-01 AND 1995-01-01`: 15% selectivity → ~9,000,000 rows
  - `l_discount BETWEEN 0.05 AND 0.07`: 15% selectivity → further ~1,350,000 rows
  - `l_quantity < 24`: 85% of discount-filtered rows → ~1,148,000 rows
  - **Combined: ~1.9% of 60M rows = ~1,148,000 qualifying rows**
- **Aggregation**: single scalar SUM — no GROUP BY, result is one row. Accumulate into a single `double`.
- **Output**: one row, no ORDER BY.
- **Critical optimization**: zone map on l_shipdate eliminates ~85% of blocks (only ~90 of 600 blocks overlap 1994). Within qualifying blocks, fuse all three predicates in a single branch-free or early-exit inner loop. SIMD-friendly (all double columns, packed 64-bit).
- **Predicate ordering for best performance**: apply l_shipdate range first (from zone map), then l_discount (3 valid values), then l_quantity. Load l_extendedprice only for qualifying rows.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate) — CRITICAL FOR Q6
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600]` then per block: `[int32_t min, int32_t max, uint32_t block_size]` (12 bytes each)
- Total file size: 7,204 bytes
- **Skip logic for Q6**: skip block if `block_max < 8766 OR block_min >= 9131`
- Since data is sorted by l_shipdate: early blocks (all < 8766) and late blocks (all ≥ 9131) are skipped entirely. Only the ~90 blocks spanning the 1994 range need to be scanned. **This reduces I/O by ~85%.**
- `row_offset` is the ROW index of the first row in the block (= block_index × 100,000 for full blocks). Access column data as `col[row_offset + i]` for i in [0, block_size).
- mmap the zone map file; iterate all 600 entries; process only blocks where `block_max >= 8766 AND block_min < 9131`.
