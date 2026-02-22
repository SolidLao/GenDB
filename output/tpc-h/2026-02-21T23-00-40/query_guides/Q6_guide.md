# Q6 Guide — Forecasting Revenue Change

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: lineitem/l_shipdate.bin (59986052 rows)
- Stored as int32_t = days since 1970-01-01.
- Column is sorted ascending. Zone map available for block pruning.
- This query:
  - `l_shipdate >= DATE '1994-01-01'` → `raw >= 8766`
    (derivation: 24 years × 365 + 6 leap years = 8766)
  - `l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR` → `raw < 9131`
    (derivation: 25 years × 365 + 6 leap years = 9131; 1995-01-01)
  - Combined: `raw >= 8766 && raw < 9131`
- Zone map usage: skip blocks where `block_max < 8766` OR `block_min >= 9131`.
  Estimated 15.7% of rows qualify on shipdate range alone → ~84% blocks skippable.

### l_discount (DECIMAL, uint8_t, byte_pack compression)
- File: lineitem/l_discount.bin (59986052 rows)
- Compression: byte_pack. code = round(double_value × 100). Lookup: lineitem/l_discount_lookup.bin (256 doubles).
- Actual value: `disc_lut[discount_code]`  (code=5 → 0.05, code=7 → 0.07)
- This query: `l_discount BETWEEN 0.06-0.01 AND 0.06+0.01` = `BETWEEN 0.05 AND 0.07`
  → integer filter: `discount_code >= 5 && discount_code <= 7`
  No floating-point comparison needed — pure integer range check.
- Revenue: SUM(l_extendedprice * l_discount) → `extprice * disc_lut[disc_code]`

### l_quantity (DECIMAL, uint8_t, byte_pack compression)
- File: lineitem/l_quantity.bin (59986052 rows)
- Compression: byte_pack. code = round(double_value). code IS the integer quantity (1–50).
- This query: `l_quantity < 24` → `quantity_code < 24`
  Pure integer comparison — no lookup needed for filtering.
- quantity_code values are exact integers (TPC-H l_quantity is always integer 1–50).

### l_extendedprice (DECIMAL, double)
- File: lineitem/l_extendedprice.bin (59986052 rows)
- Stored as native double. Values match SQL directly; no scaling needed.
- This query: contributes to SUM(l_extendedprice * l_discount).
  Revenue = `extprice * disc_lut[disc_code]` for qualifying rows.

## Table Stats

| Table    | Rows     | Role | Sort Order | Block Size |
|----------|----------|------|------------|------------|
| lineitem | 59986052 | fact | l_shipdate | 100000     |

## Query Analysis

- **Join pattern**: None — single table scan on lineitem.
- **Filters** (combined selectivity ~1.9%):
  1. `l_shipdate >= 8766 && l_shipdate < 9131` (selectivity ~15.7%) — use zone maps to prune
  2. `discount_code >= 5 && discount_code <= 7` (further reduces qualifying rows)
  3. `quantity_code < 24` (further reduces qualifying rows)
- **Aggregation**: Single global SUM (no GROUP BY). Scalar result.
- **Strategy**: Zone-map-guided scan to skip ~84% of lineitem blocks by shipdate range.
  Within qualifying blocks, apply all three predicates with branch-free integer comparisons
  before loading l_extendedprice (late materialization).
- **Output**: One row — the revenue sum.
- **Optimization hint**: This is the most selective query (1.9%). Zone-map pruning is critical.
  After skipping via zone maps, use AVX-512 SIMD to test shipdate range across a block,
  then apply discount and quantity filters. Accumulate partial sums per thread, merge at end.
  Byte-packed discount and quantity enable integer comparisons — faster than float comparisons.

## Indexes

### shipdate_zonemap (zone_map on l_shipdate)
- File: lineitem/indexes/shipdate_zonemap.bin
- Layout: `[uint32_t num_blocks=600]` then `[int32_t min, int32_t max, uint32_t start_row]` × 600
- Each block covers 100,000 rows of the (sorted) lineitem table.
- Usage: mmap file, read num_blocks, for each block:
  - Skip if `block_max < 8766` (all rows before 1994)
  - Skip if `block_min >= 9131` (all rows 1995 or later; since sorted, break here)
  - Otherwise scan rows in range [start_row, start_row + block_size)
- row_offset is a ROW index, not byte offset. Access columns as `l_shipdate[start_row]`, etc.
- This query: ~84% of blocks skipped → reads only ~9.5M rows instead of 60M.
  Critical for HDD performance (2–5× I/O reduction).
