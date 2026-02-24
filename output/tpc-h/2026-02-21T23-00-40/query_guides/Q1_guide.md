# Q1 Guide — Pricing Summary Report

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: lineitem/l_shipdate.bin (59986052 rows)
- Stored as int32_t = days since 1970-01-01. Self-test: parse_date("1970-01-01") == 0.
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
  → `raw <= 10471`  (10471 = epoch days for 1998-09-02)
  Derivation: 1998-12-01 = 10561; 10561 - 90 = 10471.
- Column is sorted ascending. Zone map available: skip blocks where block_min > 10471.
  Since 98% of rows qualify, zone maps skip very few blocks — full scan is appropriate.

### l_returnflag (STRING, int8_t, raw ASCII char)
- File: lineitem/l_returnflag.bin (59986052 rows)
- Stored as int8_t = raw ASCII value. Values: A=65, N=78, R=82.
- This query: GROUP BY l_returnflag → group on the raw int8_t value.
  Output: cast back to char for display: `(char)raw_val`
- Only 3 distinct values → 3 possible groups (combined with l_linestatus: 6 max groups).

### l_linestatus (STRING, int8_t, raw ASCII char)
- File: lineitem/l_linestatus.bin (59986052 rows)
- Stored as int8_t = raw ASCII value. Values: F=70, O=79.
- This query: GROUP BY l_linestatus → group on raw int8_t value.
  Output: cast back to char for display: `(char)raw_val`
- Only 2 distinct values → combined with l_returnflag: at most 6 groups.

### l_quantity (DECIMAL, uint8_t, byte_pack compression)
- File: lineitem/l_quantity.bin (59986052 rows)
- Compression: byte_pack. code = round(double_value). Lookup: lineitem/l_quantity_lookup.bin (256 doubles).
- Load lookup: `double qty_lut[256]; /* mmap or fread 256*8 bytes */`
- Actual value: `qty_lut[quantity_code]`   (e.g., code=17 → qty_lut[17] = 17.0)
- This query: SUM/AVG l_quantity → accumulate `qty_lut[code]` per group.

### l_extendedprice (DECIMAL, double)
- File: lineitem/l_extendedprice.bin (59986052 rows)
- Stored as native double. Values match SQL directly; no scaling needed.
- This query: SUM(l_extendedprice) → sum doubles per group.
  Also: SUM(l_extendedprice * (1-l_discount)) and SUM(l_extendedprice * (1-l_discount) * (1+l_tax)).

### l_discount (DECIMAL, uint8_t, byte_pack compression)
- File: lineitem/l_discount.bin (59986052 rows)
- Compression: byte_pack. code = round(double_value * 100). Lookup: lineitem/l_discount_lookup.bin (256 doubles).
- Actual value: `disc_lut[discount_code]`  (e.g., code=4 → disc_lut[4] = 0.04)
- This query: (1 - l_discount) → `1.0 - disc_lut[discount_code]`
  SUM(extprice*(1-disc)): `extprice * (1.0 - disc_lut[disc_code])`
  AVG(l_discount): accumulate `disc_lut[disc_code]` per group.

### l_tax (DECIMAL, uint8_t, byte_pack compression)
- File: lineitem/l_tax.bin (59986052 rows)
- Compression: byte_pack. code = round(double_value * 100). Lookup: lineitem/l_tax_lookup.bin (256 doubles).
- Actual value: `tax_lut[tax_code]`  (e.g., code=2 → tax_lut[2] = 0.02)
- This query: (1 + l_tax) → `1.0 + tax_lut[tax_code]`
  SUM(extprice*(1-disc)*(1+tax)): `extprice * (1.0 - disc_lut[dc]) * (1.0 + tax_lut[tc])`

## Table Stats

| Table    | Rows     | Role | Sort Order  | Block Size |
|----------|----------|------|-------------|------------|
| lineitem | 59986052 | fact | l_shipdate  | 100000     |

## Query Analysis

- **Join pattern**: None — single table scan on lineitem.
- **Filter**: `l_shipdate <= 10471` (98% selectivity — almost all rows qualify).
  Zone maps can skip trailing blocks where block_min > 10471, but gain is minimal.
  Effective strategy: full parallel scan using morsel-driven parallelism across 64 threads.
- **Aggregation**: GROUP BY (l_returnflag, l_linestatus) — at most 6 groups. Use a 6-slot
  array keyed on (returnflag_byte, linestatus_byte). Very low cardinality → no hash table needed.
- **Aggregates computed per group**:
  - sum_qty:        SUM(qty_lut[qcode])
  - sum_base_price: SUM(extprice)
  - sum_disc_price: SUM(extprice * (1 - disc_lut[dcode]))
  - sum_charge:     SUM(extprice * (1 - disc_lut[dcode]) * (1 + tax_lut[tcode]))
  - avg_qty:        sum_qty / count
  - avg_price:      sum_base_price / count
  - avg_disc:       SUM(disc_lut[dcode]) / count
  - count_order:    COUNT(*)
- **Output**: 6 rows max, ORDER BY l_returnflag ASC, l_linestatus ASC.
- **Combined selectivity**: ~98% of 59,986,052 ≈ 58.8M rows processed.
- **Optimization hint**: AVX-512 SIMD scan; operator fusion (filter+accumulate in one pass);
  parallel partial aggregates per thread, merge at end. Byte-packed columns (quantity, discount,
  tax) reduce I/O by ~3× vs. storing as double.

## Indexes

### shipdate_zonemap (zone_map on l_shipdate)
- File: lineitem/indexes/shipdate_zonemap.bin
- Layout: `[uint32_t num_blocks=600]` then `[int32_t min, int32_t max, uint32_t start_row]` × 600
- Each block covers 100,000 rows. block_min and block_max are the min/max l_shipdate in that block.
- Usage: mmap file, read num_blocks, iterate blocks.
  Skip block where `block_min > 10471` (since column is sorted, once block_min > threshold,
  all subsequent blocks also exceed threshold — can break early).
- row_offset is ROW index, not byte offset. Access column as `col[start_row]` through `col[start_row + block_size - 1]`.
- This query: With 98% selectivity, only the very last few blocks (shipdate near 1998-11-27) may
  be skipped. Zone map provides minimal gain for Q1 — full scan is usually optimal.
