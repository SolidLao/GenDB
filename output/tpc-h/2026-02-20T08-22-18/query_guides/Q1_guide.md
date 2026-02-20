# Q1 Guide — Pricing Summary Report

```sql
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice),
       SUM(l_extendedprice*(1-l_discount)), SUM(l_extendedprice*(1-l_discount)*(1+l_tax)),
       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus
```

---

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59986052 rows × 4 bytes = 240 MB)
- Stored as int32_t days since 1970-01-01. No scaling. `parse_date("1970-01-01")=0`.
- Range in data: 8036 (1992-01-02) to 10591 (1998-12-31).
- **This query:** `<= DATE '1998-12-01' - INTERVAL '90' DAY` = `<= '1998-09-02'`
  - `parse_date("1998-09-02")` = 365×28 + 7_leaps_1970-1997 + 243_days_Jan-Aug + 1 = **10471**
  - C++ filter: `raw_shipdate <= 10471`
- **Zone map skip:** block qualifies for skipping when `block_min > 10471`.
  Since lineitem is sorted ASC by l_shipdate, once `block_min > 10471` all subsequent blocks also skip.

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Values match SQL directly (e.g., SQL `l_quantity = 17.0` → C++ `17.0`).
- No scaling needed. `SUM(l_quantity)` = `std::accumulate` or vectorized loop.
- This query: used in `SUM(l_quantity)`, `AVG(l_quantity)` aggregations.

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Values match SQL directly.
- This query: `SUM(l_extendedprice)`, `SUM(l_extendedprice*(1-l_discount))`,
  `SUM(l_extendedprice*(1-l_discount)*(1+l_tax))`, `AVG(l_extendedprice)`.
- C++ arithmetic: `ep * (1.0 - disc)` and `ep * (1.0 - disc) * (1.0 + tax)`.

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Range: 0.00 to 0.10 (11 distinct values).
- This query: `SUM(l_extendedprice*(1-l_discount))`, `AVG(l_discount)`.

### l_tax (DECIMAL, double)
- File: `lineitem/l_tax.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Range: 0.02 to 0.08.
- This query: `SUM(l_extendedprice*(1-l_discount)*(1+l_tax))`.

### l_returnflag (STRING, int16_t, dictionary-encoded)
- File: `lineitem/l_returnflag.bin` (59986052 rows × 2 bytes = 120 MB)
- Dictionary: `lineitem/l_returnflag_dict.txt`
  - Format: `code=value` per line, e.g., `0=A\n1=N\n2=R`
  - Codes are assigned in first-encounter order by parsing threads; always load dict at runtime.
- **Loading pattern:**
  ```cpp
  // load dict: code → char
  std::unordered_map<int16_t, std::string> rf_dict;
  std::ifstream df("lineitem/l_returnflag_dict.txt");
  std::string line;
  while (std::getline(df, line)) {
      size_t eq = line.find('=');
      rf_dict[(int16_t)std::stoi(line.substr(0, eq))] = line.substr(eq+1);
  }
  // reverse: find code for a given string (if filtering)
  // for GROUP BY: use int16_t code directly as aggregation key, decode only at output
  ```
- This query: `GROUP BY l_returnflag` → group by int16_t code, decode at output for ORDER BY / output.
- 3 distinct values; at most 3 codes. Output ORDER BY l_returnflag means sort 3 decoded strings.

### l_linestatus (STRING, int16_t, dictionary-encoded)
- File: `lineitem/l_linestatus.bin` (59986052 rows × 2 bytes = 120 MB)
- Dictionary: `lineitem/l_linestatus_dict.txt` (format: `code=value`, e.g., `0=F\n1=O`)
- Loading pattern: same as l_returnflag_dict above.
- This query: `GROUP BY l_linestatus` → group by int16_t code, decode at output. 2 distinct values.

---

## Table Stats

| Table    | Rows     | Role | Sort Order  | Block Size |
|----------|----------|------|-------------|------------|
| lineitem | 59986052 | fact | l_shipdate ↑ | 100000    |

---

## Query Analysis
- **Access pattern:** Single-table full scan of lineitem (no joins).
- **Filter:** `l_shipdate <= 10471` — estimated selectivity 75% (workload analysis).
  ~45M of 60M rows qualify. Zone map prunes blocks with `block_min > 10471`.
  With sorted data and 600 blocks, approximately 150 trailing blocks (25%) can be skipped.
- **Aggregation:** GROUP BY (l_returnflag, l_linestatus) — at most 3×2=6 groups, but TPC-H
  produces exactly 4 distinct (rf, ls) combinations. Use direct 6-slot array or tiny hash table.
- **Output:** 4 rows, ORDER BY l_returnflag, l_linestatus (just sort 4 result rows after aggregation).
- **Combined selectivity:** ~75% of 60M = ~45M rows processed.
- **Optimization:** Vectorized (SIMD AVX-512) scan+filter on l_shipdate, fused accumulation
  into 4 group accumulators. No materialization needed.

---

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout:
  ```
  uint32_t num_blocks     // = 600 (ceil(59986052/100000))
  per block (12 bytes each):
    int32_t  min_val      // min l_shipdate in block (= first row's value, since sorted)
    int32_t  max_val      // max l_shipdate in block (= last row's value)
    uint32_t num_rows     // = 100000 except last block = 86052
  ```
  Block b starts at row offset `b * 100000`.
- **Usage:**
  ```cpp
  // mmap the zone map
  const char* zm_data = (const char*)mmap(...);
  uint32_t num_blocks = *(uint32_t*)zm_data;
  struct ZMBlock { int32_t mn, mx; uint32_t nr; };
  const ZMBlock* blocks = (const ZMBlock*)(zm_data + 4);

  for (uint32_t b = 0; b < num_blocks; b++) {
      if (blocks[b].mn > 10471) break; // sorted → all remaining blocks also skip
      uint32_t row_start = b * 100000;
      uint32_t row_end   = row_start + blocks[b].nr;
      // process rows [row_start, row_end)
  }
  ```
  - `row_offset` is a ROW index, not a byte offset. Access column as `col_ptr[row_idx]`.
- **This query:** Skips trailing ~25% of blocks (those with min_val > 10471). ~150 blocks × 100000
  rows = ~15M rows skipped without touching column data. Effective I/O reduction ~25%.
