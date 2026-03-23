# Q6 Guide — Forecasting Revenue Change

## Column Reference

### l_shipdate (date, int32_t, days_since_epoch_1970)
- File: `storage/lineitem/l_shipdate.bin` (59986052 rows × 4 bytes)
- This query: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'`
  → C++ `l_shipdate[i] >= 8766 && l_shipdate[i] < 9131`
- Date encoding: 1994-01-01 = 8766, 1995-01-01 = 9131 (days since epoch 1970-01-01)
- Selectivity: ~0.15 → ~9.0M rows in range

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59986052 rows × 8 bytes)
- This query: `l_discount BETWEEN 0.05 AND 0.07`
  → C++ `l_discount[i] >= 0.05 && l_discount[i] <= 0.07`
- Selectivity: ~0.27
- Also used in aggregation: `SUM(l_extendedprice * l_discount)`

### l_quantity (measure, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59986052 rows × 8 bytes)
- This query: `l_quantity < 24` → C++ `l_quantity[i] < 24.0`
- Selectivity: ~0.46

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59986052 rows × 8 bytes)
- This query: `SUM(l_extendedprice * l_discount)` — revenue computation

## Table Stats
| Table    | Rows       | Role | Sort Order | Block Size |
|----------|------------|------|------------|------------|
| lineitem | 59,986,052 | fact | l_orderkey | 65536      |

## Query Analysis

### Pattern
Single-table scan with 3 conjunctive filters, scalar aggregation (no GROUP BY), single output value.

### Filter Selectivities
- l_shipdate range: ~0.15
- l_discount range: ~0.27
- l_quantity threshold: ~0.46
- Combined (assuming independence): ~0.15 × 0.27 × 0.46 ≈ 0.019 → ~1.1M qualifying rows

### Aggregation
- No GROUP BY — single scalar SUM
- Accumulate: `revenue += l_extendedprice[i] * l_discount[i]` for qualifying rows

### Output
- Single row, single column: `revenue`

## Indexes

### lineitem_l_shipdate_zonemap (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout:
  - Header: `uint64_t num_blocks` (= ceil(59986052 / 65536) = 916 blocks), `uint32_t block_size` (= 65536)
  - Body: `int32_t[num_blocks * 2]` — (min_date, max_date) per block
- Usage: For each block `b`:
  - Skip if `zonemap[b*2] >= 9131` (min_date >= upper bound, all too late)
  - Skip if `zonemap[b*2+1] < 8766` (max_date < lower bound, all too early)
  - Process otherwise
- At 15% selectivity, ~85% of blocks can be skipped — significant speedup

## Recommended Execution Strategy
1. Load zone map
2. Load all 4 columns: l_shipdate, l_discount, l_quantity, l_extendedprice
3. For each block, check zone map to skip blocks outside date range
4. For qualifying blocks, scan rows with 3 conjunctive filters. Apply cheapest filter first (shipdate range has lowest selectivity at 0.15, test it first).
5. For passing rows: `revenue += l_extendedprice[i] * l_discount[i]`
6. Output single value
7. Consider SIMD vectorization for the scan — all columns are contiguous doubles/int32s
