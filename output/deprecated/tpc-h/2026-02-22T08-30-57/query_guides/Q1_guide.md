# Q1 Guide — Pricing Summary Report

## SQL
```sql
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice),
       SUM(l_extendedprice*(1-l_discount)),
       SUM(l_extendedprice*(1-l_discount)*(1+l_tax)),
       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY  -- = '1998-09-02'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

## Column Reference

### l_returnflag (STRING, int8_t, raw ASCII)
- File: `lineitem/l_returnflag.bin` (59,986,052 rows × 1 byte)
- Stored as raw ASCII char value: `'A'=65`, `'N'=78`, `'R'=82`
- 3 distinct values. Use as group key directly (compare by value).
- This query: group by `l_returnflag` → use `int8_t` value as key; output as `char` cast to string

### l_linestatus (STRING, int8_t, raw ASCII)
- File: `lineitem/l_linestatus.bin` (59,986,052 rows × 1 byte)
- Stored as raw ASCII char value: `'F'=70`, `'O'=79`
- 2 distinct values. Combined with l_returnflag → at most 6 unique groups.
- This query: group by `l_linestatus` → composite group key with l_returnflag

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows × 4 bytes)
- lineitem is **sorted by l_shipdate** — the zone map is maximally effective.
- Date range in data: 1992-01-02 (8036) to 1998-12-01 (10561)
- This query: `l_shipdate <= '1998-09-02'` → `raw <= 10471`
- Zone map file: `indexes/lineitem_shipdate_zonemap.bin`
  - Format: `[uint32_t num_blocks=600][int32_t min, int32_t max per block]`
  - Skip block if `block_min > 10471` — only last few blocks qualify for skipping (~1.3% saved)
  - Selectivity ≈ 98.7%: almost all rows pass; zone map skips ~8 blocks at tail

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values match SQL directly (1.0..50.0).
- This query: `SUM(l_quantity)`, `AVG(l_quantity)` — accumulate as `double`

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values match SQL directly.
- This query: 4 different aggregates involving l_extendedprice — compute all in one pass

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values: 0.00..0.10 matching SQL directly.
- This query: `SUM(l_extendedprice*(1-l_discount))`, `AVG(l_discount)`

### l_tax (DECIMAL, double)
- File: `lineitem/l_tax.bin` (59,986,052 rows × 8 bytes)
- Stored as native double. Values match SQL directly.
- This query: `SUM(l_extendedprice*(1-l_discount)*(1+l_tax))`

## Table Stats
| Table    | Rows       | Role | Sort Order  | Block Size |
|----------|------------|------|-------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate  | 100,000    |

## Query Analysis
- **Pattern**: Pure single-table scan with filter + aggregation. No joins.
- **Filter**: `l_shipdate <= 10471` (98.7% selectivity — nearly full scan)
- **Group by**: (l_returnflag, l_linestatus) — at most 6 unique groups (A/N/R × F/O in practice)
- **Aggregation**: 10 aggregates per group (4 SUM, 3 SUM for AVG numerators, 1 AVG direct, COUNT)
  - Maintain per-group: sum_qty, sum_base, sum_disc_price, sum_charge, count — derive AVGs at end
- **Output**: 6 rows ordered by (l_returnflag ASC, l_linestatus ASC). No LIMIT.
- **Strategy**: Single-pass columnar scan across 64 threads. Each thread maintains a local 6-slot accumulator. Merge thread-local accumulators at end.
- **Zone map pruning**: lineitem sorted by shipdate; ~8 of 600 blocks (tail of 1998) can be skipped.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][int32_t min, int32_t max per block]`
- Block b covers rows `[b*100000 .. min((b+1)*100000, 59986052))`
- `row_offset` is ROW index, not byte offset
- Usage: mmap file, read `num_blocks`, iterate blocks. Skip if `block_min > 10471`.
- This query: filter is `<= 10471`; skip block if `block_min > 10471`.
  Blocks near the end of sorted lineitem (late 1998 rows) will be skipped. ~1.3% pruning.
- Most blocks pass — process sequentially in large chunks; AVX512 vectorized inner loop.

### lineitem_quantity_zonemap (zone_map on l_quantity)
- File: `indexes/lineitem_quantity_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][double min, double max per block]`
- This query: NOT used (no quantity predicate in Q1).

### lineitem_discount_zonemap (zone_map on l_discount)
- File: `indexes/lineitem_discount_zonemap.bin`
- Layout: `[uint32_t num_blocks=600][double min, double max per block]`
- This query: NOT used (no discount predicate in Q1).

## Performance Notes
- At 98.7% selectivity, the bottleneck is memory bandwidth (60M rows × ~40 bytes of hot columns).
- Load l_shipdate, l_returnflag, l_linestatus, l_quantity, l_extendedprice, l_discount, l_tax in columnar loops.
- AVX512 can process 8 doubles per instruction. Fuse filter + accumulate in a single vectorized pass.
- 64-thread parallel scan: assign each thread ~938K rows (59.9M / 64). Merge 64 × 6 accumulators.
- Group encoding: use `(rf_val << 8) | ls_val` as map key (or direct 6-slot array: index by {A,N,R}×{F,O}).
