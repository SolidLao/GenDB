# Q18 Guide

## SQL
```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, SUM(l_quantity) AS sum_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100;
```

## Column Reference

### l_orderkey (FK, int32_t, native_binary)
- File: `lineitem/l_orderkey.bin` (~60M rows, 4 bytes each)
- This query: GROUP BY key in subquery, join key `o_orderkey = l_orderkey`

### l_quantity (measure, double, native_binary)
- File: `lineitem/l_quantity.bin` (~60M rows, 8 bytes each)
- This query: `SUM(l_quantity)` in subquery (HAVING > 300) and outer query

### o_orderkey (PK, int32_t, native_binary)
- File: `orders/o_orderkey.bin` (~15,000,000 rows, 4 bytes each)
- This query: join key, GROUP BY key, output column

### o_custkey (FK, int32_t, native_binary)
- File: `orders/o_custkey.bin` (~15,000,000 rows, 4 bytes each)
- This query: join key `c_custkey = o_custkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `orders/o_orderdate.bin` (~15,000,000 rows, 4 bytes each)
- This query: GROUP BY key, output column, ORDER BY

### o_totalprice (measure, double, native_binary)
- File: `orders/o_totalprice.bin` (~15,000,000 rows, 8 bytes each)
- This query: GROUP BY key, output column, ORDER BY (primary sort key)

### c_custkey (PK, int32_t, native_binary)
- File: `customer/c_custkey.bin` (~1,500,000 rows, 4 bytes each)
- This query: join key `c_custkey = o_custkey`, GROUP BY key, output column

### c_name (attribute, varlen_string, offsets_plus_data)
- Files: `customer/c_name_offsets.bin` (int64_t[nrows+1]) + `customer/c_name_data.bin`
- Offsets: int64_t array. `name[i]` spans bytes `[offsets[i], offsets[i+1])` in data file.
- This query: GROUP BY key, output column
- Only accessed for final result rows (~100 rows at most)

## Table Stats

| Table    | Rows        | Role      | Sort Order | Block Size |
|----------|-------------|-----------|------------|------------|
| lineitem | ~59,986,052 | fact      | l_orderkey | 100,000    |
| orders   | ~15,000,000 | fact      | o_orderkey | 100,000    |
| customer | ~1,500,000  | dimension | c_custkey  | 100,000    |

## Query Analysis

### Pattern
Subquery-driven: first find "large" orders (SUM(l_quantity) > 300 per orderkey),
then join those orders with customer and lineitem for final output.

### Recommended Execution Strategy

#### Phase 1: Subquery — Find large orders
Scan lineitem to compute `SUM(l_quantity)` grouped by `l_orderkey`.
- Lineitem is sorted by l_orderkey → rows for same orderkey are contiguous.
- Use `lineitem_orderkey_index` to know (start, count) for each orderkey, or simply
  scan sequentially since the table is sorted by l_orderkey.
- **Sequential scan approach** (preferred — avoids 60M random accesses):
  ```cpp
  // Lineitem is sorted by l_orderkey. Scan and accumulate:
  size_t i = 0;
  while (i < li_nrows) {
      int32_t ok = l_orderkey[i];
      double sum_q = 0.0;
      while (i < li_nrows && l_orderkey[i] == ok) {
          sum_q += l_quantity[i];
          i++;
      }
      if (sum_q > 300.0) {
          // ok is a qualifying orderkey
          qualifying_orderkeys.push_back(ok);
          // Can also store sum_q for later use
      }
  }
  ```
- Alternatively, use the `lineitem_orderkey_index` to iterate orderkeys:
  ```cpp
  for (int32_t ok = 0; ok <= max_orderkey; ok++) {
      auto [start, count] = li_index[ok];
      if (count == 0) continue;
      double sum_q = 0.0;
      for (uint32_t j = start; j < start + count; j++) sum_q += l_quantity[j];
      if (sum_q > 300.0) qualifying.push_back({ok, sum_q});
  }
  ```
- Selectivity: ~0.1% → ~15,000 qualifying orderkeys (from workload analysis)
- Data read: l_orderkey (228MB) + l_quantity (457MB) = 685MB

#### Phase 2: Join qualifying orders with orders and customer
For each qualifying orderkey:
1. Use `orders_orderkey_lookup[ok]` → orders row index → get o_custkey, o_orderdate, o_totalprice
2. Use `customer_custkey_lookup[o_custkey]` → customer row index → get c_name, c_custkey

Since o_orderkey determines (o_custkey, o_orderdate, o_totalprice, c_name, c_custkey),
the GROUP BY is effectively by o_orderkey.

#### Phase 3: Re-aggregate lineitem for qualifying orders
For each qualifying orderkey, recompute `SUM(l_quantity)` (or reuse from Phase 1).
Using `lineitem_orderkey_index`: `index[ok]` → `{start, count}` → sum l_quantity[start..start+count).

#### Phase 4: Top-100 output
- Sort qualifying results by `o_totalprice DESC, o_orderdate ASC`.
- LIMIT 100 — use a partial sort or priority queue.
- Only ~15,000 qualifying orders → full sort is cheap.
- For final output, decode c_name for the top 100 rows:
  ```cpp
  // c_name for customer row r:
  int64_t off_start = c_name_offsets[r];
  int64_t off_end = c_name_offsets[r + 1];
  std::string name(c_name_data + off_start, off_end - off_start);
  ```

## Indexes

### lineitem_orderkey_index (dense_range on l_orderkey)
- File: `indexes/lineitem_orderkey_index.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t max_orderkey
  Byte 4+:    struct { uint32_t start; uint32_t count; }[max_orderkey + 1]
  ```
  Each RangeEntry is 8 bytes (two uint32_t).
- Sentinel: `{0, 0}` means no lineitem rows for that orderkey
- Usage in Phase 1: Can iterate all orderkeys to compute per-orderkey quantity sums.
  Or use sequential scan since lineitem is sorted by l_orderkey.
- Usage in Phase 3: For qualifying orderkeys, `index[ok]` gives exact row range for re-aggregation.

### orders_orderkey_lookup (dense_lookup on o_orderkey)
- File: `indexes/orders_orderkey_lookup.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t max_orderkey
  Byte 4+:    int32_t[max_orderkey + 1] — lookup[orderkey] = row_index, or -1
  ```
- Sentinel: `-1` means no order with that orderkey
- Usage: For each qualifying orderkey, `lookup[ok]` → row index in orders columns.

### customer_custkey_lookup (dense_lookup on c_custkey)
- File: `indexes/customer_custkey_lookup.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t max_custkey
  Byte 4+:    int32_t[max_custkey + 1] — lookup[custkey] = row_index, or -1
  ```
- Sentinel: `-1` means no customer with that custkey
- Usage: For each qualifying order, `lookup[o_custkey]` → customer row index → c_name.

## Performance Notes
- Phase 1 dominates cost: scanning 60M rows of l_orderkey (228MB) and l_quantity (457MB).
- With 64 cores: partition the scan by row ranges. Each thread identifies qualifying orderkeys.
  Merge results (small — ~15K orderkeys).
- Phase 2/3: Only ~15K qualifying orders × O(1) index lookups = negligible.
- Phase 4: Sort 15K entries — negligible.
- c_name is varlen but only accessed for ~100 final rows — no performance concern.
- Total expected read: ~685MB for Phase 1, ~trivial for Phase 2/3.
- Can combine Phase 1 and Phase 3: store sum_qty during the subquery scan to avoid re-scanning.
