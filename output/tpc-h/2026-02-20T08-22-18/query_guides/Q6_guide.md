# Q6 Guide — Forecasting Revenue Change

```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24
```

---

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59986052 rows × 4 bytes = 240 MB)
- Sorted ascending (lineitem sort key). `parse_date("1970-01-01")=0`.
- **This query:** `>= DATE '1994-01-01'` AND `< DATE '1994-01-01' + INTERVAL '1' YEAR`
  - `parse_date("1994-01-01")` = 365×24 + 6_leaps_1970-1993 + 0 + 0 = **8766**
    (leaps in [1970,1993]: 1972,1976,1980,1984,1988,1992 = 6)
  - `parse_date("1995-01-01")` = 365×25 + 6_leaps_1970-1994 + 0 + 0 = **9131**
    (1994 is not a leap year: same 6 leaps)
  - C++ filter: `raw_shipdate >= 8766 && raw_shipdate < 9131`
- **Zone map:** skip block if `block_max < 8766` OR `block_min >= 9131`.
  With sorted data, find the contiguous range of blocks fully within [8766,9131).

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Range: 0.00 to 0.10 (11 distinct values in 0.01 increments).
- **This query:** `BETWEEN 0.06 - 0.01 AND 0.06 + 0.01` = `BETWEEN 0.05 AND 0.07`
  - C++ filter: `disc >= 0.05 && disc <= 0.07`
  - Note: floating-point exact comparison. TPC-H discounts are multiples of 0.01; using
    `disc >= 0.04999 && disc <= 0.07001` is safer to avoid fp rounding edge cases.
  - Expected selectivity for l_discount filter alone: ~3 of 11 distinct values ≈ 27%.
  - Combined with l_shipdate range: workload estimates ~8% of all lineitem rows.

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Range: 1.00 to 50.00 (50 distinct values).
- **This query:** `l_quantity < 24` → C++: `qty < 24.0`
  - Selectivity: ~47% of rows (quantities 1-23 out of 1-50). Workload analysis: 0.48.

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Values match SQL directly.
- **This query:** `SUM(l_extendedprice * l_discount)` — multiply and accumulate.
  C++: `revenue += ep * disc` for each qualifying row.

---

## Table Stats

| Table    | Rows     | Role | Sort Order   | Block Size |
|----------|----------|------|--------------|------------|
| lineitem | 59986052 | fact | l_shipdate ↑ | 100000     |

---

## Query Analysis
- **Access pattern:** Single-table scan, scalar aggregation (no GROUP BY). Simplest query.
- **Filters (in optimal evaluation order):**
  1. `l_shipdate BETWEEN 8766 AND 9130` — selectivity ~1/7 years ≈ 14.3%. Zone map prunes
     blocks outside [8766,9131). With sorted lineitem, this is a contiguous range of ~85 blocks
     (out of 600). All other ~515 blocks can be completely skipped.
  2. `l_discount BETWEEN 0.05 AND 0.07` — applies to qualifying rows. ~27% pass.
  3. `l_quantity < 24.0` — applies to qualifying rows. ~48% pass.
  - Combined filter selectivity for the ~8M rows in date range: ~27% × 48% ≈ 13% → ~1M rows contribute.
- **Aggregation:** Single scalar `double revenue = 0.0`. No hash table.
- **Output:** Single row with revenue value.
- **Optimization:** After zone-map block selection, vectorized filter evaluation on 3 columns
  simultaneously. SIMD (AVX-512) can process 8 doubles per instruction. Tight inner loop:
  check shipdate, discount, quantity; multiply and accumulate for qualifying rows.
  Expected bottleneck: memory bandwidth for ~85 blocks × 100K rows × 4 columns read.

---

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout:
  ```
  uint32_t num_blocks   // = 600
  per block (12 bytes):
    int32_t  min_val    // min l_shipdate in block (equals block's first row since sorted)
    int32_t  max_val    // max l_shipdate in block (equals block's last row)
    uint32_t num_rows   // rows in block (100000, or 86052 for last)
  ```
  Block b row range: [b*100000, b*100000 + num_rows).
  `row_offset` is a ROW index. Access column value as `col_ptr[row_idx]` (not byte offset).
- **This query:** Q6 is the PRIMARY beneficiary of this zone map.
  Skip block if `block_max < 8766 OR block_min >= 9131`.
  Only blocks where `block_min <= 9130 AND block_max >= 8766` need processing.
  Approximately 85 of 600 blocks qualify (~14%). The other 515 blocks (~8.6M rows) are
  entirely skipped without reading any column data. This is the biggest single optimization for Q6.
- **Implementation pattern:**
  ```cpp
  uint32_t nb = *(uint32_t*)zm_ptr;
  struct ZMBlock { int32_t mn, mx; uint32_t nr; };
  auto* blocks = (const ZMBlock*)(zm_ptr + 4);
  const int32_t LO = 8766, HI = 9131;

  for (uint32_t b = 0; b < nb; b++) {
      if (blocks[b].mx < LO) continue;   // block entirely before range
      if (blocks[b].mn >= HI) break;     // sorted → all subsequent blocks also after range
      uint32_t row_start = b * 100000;
      uint32_t row_end   = row_start + blocks[b].nr;
      // scan rows [row_start, row_end) checking all 3 predicates
  }
  ```
