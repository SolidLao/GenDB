# Q6 Guide — Forecasting Revenue Change

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: lineitem/l_shipdate.bin (59,986,052 rows × 4 bytes)
- **lineitem is sorted by l_shipdate** → zone map provides massive block-skip for this narrow date range
- Range in data: ~8035 (1992-01-01) to ~10591 (1998-12-31); total 7-year span
- This query:
  - `l_shipdate >= DATE '1994-01-01'` → `l_shipdate[i] >= 8766`
    - `parse_date("1994-01-01")`: year=1994, leaps(1970..1993)=6; 365×24+6 = **8766**
  - `l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR` = `DATE '1995-01-01'` → `l_shipdate[i] < 9131`
    - `parse_date("1995-01-01")`: year=1995, leaps(1970..1994)=6; 365×25+6 = **9131**
  - C++ predicate: `l_shipdate[i] >= 8766 && l_shipdate[i] < 9131`
- Selectivity: 0.161 (16.1% of rows in the 1-year window)
- Zone map usage: **critical**. Since lineitem is sorted by shipdate:
  - Skip blocks where `block_max < 8766` (before the window) OR `block_min >= 9131` (after)
  - The 1-year window covers ~1/7 of 7 years → ~86% of 600 blocks can be skipped
  - Only ~84 blocks (out of 600) need to be scanned at all

### l_discount (DECIMAL, double)
- File: lineitem/l_discount.bin (59,986,052 rows × 8 bytes)
- Stored as native double — values in {0.00, 0.01, 0.02, ..., 0.10} (11 distinct values)
- This query: `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01` = `l_discount BETWEEN 0.05 AND 0.07`
  - C++ predicate: `l_discount[i] >= 0.05 && l_discount[i] <= 0.07`
  - Note: floating-point safe since TPC-H discount values are exactly representable (2 decimal places)
- Selectivity: 0.015 (very selective — only 3 out of 11 discount values qualify: 0.05, 0.06, 0.07)
- Zone map: indexes/lineitem_discount_zonemap.bin
  - Format: double min/max per block; can skip blocks where `block_max < 0.05 OR block_min > 0.07`
  - However, discount is NOT sorted → most blocks span [0.00, 0.10] and cannot be skipped
  - Zone map on discount provides minimal pruning here; focus on shipdate zone map

### l_quantity (DECIMAL, double)
- File: lineitem/l_quantity.bin (59,986,052 rows × 8 bytes)
- Stored as native double — values in {1.0, 2.0, ..., 50.0}
- This query: `l_quantity < 24` → C++ `l_quantity[i] < 24.0`
- Selectivity: 0.469 (46.9% of rows qualify for quantity < 24)
- Zone map: indexes/lineitem_quantity_zonemap.bin
  - Skip blocks where `block_min >= 24.0`; but quantity is not sorted → minimal pruning
  - zone map provides ~0% pruning here since all blocks span [1..50]

### l_extendedprice (DECIMAL, double)
- File: lineitem/l_extendedprice.bin (59,986,052 rows × 8 bytes)
- Stored as native double — values match SQL directly
- This query: used only in `SUM(l_extendedprice * l_discount)` for qualifying rows
- No filter on this column

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate | 100,000    |

## Query Analysis
- **Join pattern**: none — single table scan with 3 predicates
- **Filters** (apply in this order for efficiency):
  1. `l_shipdate >= 8766 AND l_shipdate < 9131` (selectivity 0.161) — MOST SELECTIVE on zone maps
  2. `l_discount BETWEEN 0.05 AND 0.07` (selectivity 0.015) — extremely selective per row
  3. `l_quantity < 24.0` (selectivity 0.469) — moderate
  - Combined: 0.161 × 0.015 × 0.469 ≈ **0.11%** of all lineitem rows qualify
- **Aggregation**: single scalar `SUM(l_extendedprice * l_discount)` — no grouping
- **Output**: single row

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate) — PRIMARY OPTIMIZATION
- File: indexes/lineitem_shipdate_zonemap.bin
- Layout: `[uint32_t num_blocks=600]` then per block: `[int32_t min, int32_t max, uint32_t block_nrows]` (12 bytes each)
- Total file size: 7204 bytes — fits entirely in L1 cache; zero overhead after initial load
- Usage: mmap file, read header, iterate 600 entries. For each block b:
  - `skip if block_max[b] < 8766`   (block entirely before 1994)
  - `skip if block_min[b] >= 9131`  (block entirely after 1995)
  - Otherwise: scan rows `[row_offset .. row_offset + block_nrows[b])` for all 3 predicates
- row_offset is ROW index (not byte offset); use as `col[row_offset + j]` for j in [0, block_nrows)
- This query: lineitem sorted by shipdate → only the ~84 blocks whose shipdate range overlaps [8766, 9131) need scanning → **~86% block skip rate**
- Expected rows to scan after zone-map pruning: ~8.4M rows instead of 60M

### lineitem_discount_zonemap (zone_map on l_discount)
- File: indexes/lineitem_discount_zonemap.bin
- Layout: `[uint32_t num_blocks=600]` then per block: `[double min, double max, uint32_t block_nrows]` (20 bytes each)
- Note: double min/max (8 bytes each) unlike shipdate (int32_t, 4 bytes each)
- Usage: `skip if block_max[b] < 0.05 OR block_min[b] > 0.07`
- This query: discount not sorted → nearly all blocks span [0.00, 0.10] → minimal pruning; use as secondary check only after shipdate pruning

### lineitem_quantity_zonemap (zone_map on l_quantity)
- File: indexes/lineitem_quantity_zonemap.bin
- Layout: `[uint32_t num_blocks=600]` then per block: `[double min, double max, uint32_t block_nrows]` (20 bytes each)
- Usage: `skip if block_min[b] >= 24.0`
- This query: quantity not sorted → no pruning; do not bother loading this index for Q6

## Implementation Notes
- **Primary strategy**: zone-map on l_shipdate to skip 86% of blocks
- **Within qualifying blocks**: vectorized (SIMD) evaluation of all 3 predicates simultaneously
  - Load 4 doubles at a time (AVX2: 256-bit = 4×64-bit)
  - Compare shipdate, discount, quantity; AND together; conditionally accumulate extprice × discount
- **Parallelism**: divide the ~84 qualifying blocks across 64 threads; each thread accumulates partial sum; merge at end
- Date thresholds for quick reference: `1994-01-01` = 8766, `1995-01-01` = 9131 (difference = 365, 1994 not leap)
- Discount BETWEEN: use `>= 0.05 && <= 0.07` (not `>= 0.05 - 1e-9` — TPC-H values are exact)
