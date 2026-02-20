# Q18 Guide — Large Volume Customer

```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
       SUM(l_quantity) AS sum_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300
)
AND c_custkey = o_custkey
AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100
```

---

## Column Reference

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59986052 rows × 4 bytes = 240 MB)
- Stored as int32_t. Foreign key into orders (15M unique values in lineitem).
- **Subquery:** `GROUP BY l_orderkey HAVING SUM(l_quantity) > 300` — scan lineitem, accumulate
  quantity per order key, collect those with sum > 300. Expected ~100K qualifying orders.
- **Outer query:** join `o_orderkey = l_orderkey` for qualifying orders.

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Range: 1.00 to 50.00.
- **Subquery:** `SUM(l_quantity)` per l_orderkey → accumulate `qty[row]` per order group.
  `HAVING SUM(l_quantity) > 300.0` → C++ threshold: `sum > 300.0`.
- **Outer query:** `SUM(l_quantity)` per (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice).
  Only for lineitem rows whose l_orderkey is in the qualifying set.

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15000000 rows × 4 bytes = 60 MB)
- Primary key, orders sorted by o_orderdate. Stored as int32_t.
- This query: filter `o_orderkey IN subquery_result_set`. Probe qualifying_orderkeys hash set.

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15000000 rows × 4 bytes = 60 MB)
- Foreign key into customer. Join key: `c_custkey = o_custkey`.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15000000 rows × 4 bytes = 60 MB)
- Sorted ascending. Range: 8035 to 10441.
- This query: output column and secondary ORDER BY key `ORDER BY o_totalprice DESC, o_orderdate ASC`.
  **Output formatting:** convert int32_t epoch days back to YYYY-MM-DD string:
  ```cpp
  std::string format_date(int32_t d) {
      // Compute year from epoch days (adjust from year-start offsets)
      int y = 1970 + d/365;
      auto yr_start = [](int y)->int32_t {
          int y1=y-1; return 365*(y-1970)+(y1/4-y1/100+y1/400)-477;
      };
      while (yr_start(y+1) <= d) y++;
      while (yr_start(y)   >  d) y--;
      int rem = d - yr_start(y);
      bool leap = (y%4==0&&y%100!=0)||y%400==0;
      static const int md[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
      int mo = 1;
      while (rem >= md[mo] + (mo==2&&leap)) { rem -= md[mo]+(mo==2&&leap); mo++; }
      int day = rem + 1;
      char buf[12]; snprintf(buf,12,"%04d-%02d-%02d",y,mo,day);
      return buf;
  }
  ```

### o_totalprice (DECIMAL, double)
- File: `orders/o_totalprice.bin` (15000000 rows × 8 bytes = 120 MB)
- Stored as native double. Range: up to ~500,000.
- This query: output column and primary ORDER BY key `ORDER BY o_totalprice DESC`.
  No arithmetic needed; just output and compare for top-100 selection.

### o_shippriority (INTEGER, int32_t) — NOT used in Q18, skip.

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1500000 rows × 4 bytes = 6 MB)
- Primary key. Join key `c_custkey = o_custkey`. For qualifying orders, look up customer name.
- Build hash map `c_custkey → (row_index or c_name)` for O(1) lookup.

### c_name (STRING, char[26], fixed-char)
- File: `customer/c_name.bin` (1500000 rows × 26 bytes = 39 MB)
- Stored as null-terminated fixed-width char[26] (stride=26). NOT dictionary-encoded.
- This query: output column in SELECT and GROUP BY.
- **Access pattern:**
  ```cpp
  const char* cnames = (const char*)mmap("customer/c_name.bin", ...);
  // For customer row r: name string starts at cnames + r*26 (null-terminated, max 25 chars)
  std::string name(cnames + r*26); // or strndup(cnames + r*26, 25)
  ```
- Build hash map `c_custkey → (row_index_in_customer)` when ingesting, then access name via
  `cnames + customer_row * 26`.

### c_mktsegment (int16_t, dictionary) — NOT used in Q18, skip.

---

## Table Stats

| Table    | Rows     | Role      | Sort Order    | Block Size |
|----------|----------|-----------|---------------|------------|
| lineitem | 59986052 | fact      | l_shipdate ↑  | 100000     |
| orders   | 15000000 | fact      | o_orderdate ↑ | 100000     |
| customer | 1500000  | dimension | none          | 100000     |

---

## Query Analysis
- **Subquery pattern:** `IN (SELECT ... GROUP BY l_orderkey HAVING SUM > 300)` →
  Semi-join: materialize the inner subquery result to a hash set, then probe the outer query.
  - Inner: full scan lineitem (60M) → aggregate qty per l_orderkey (hash aggregation, 15M groups).
    Collect l_orderkeys with sum > 300. Expected ~100K qualifying order keys.
    Memory: 15M groups × ~16B each = ~240MB hash table. Fits in RAM.
  - Qualifying hash set size: ~100K entries. Very compact.
- **Outer query join order:**
  1. Filter orders: `o_orderkey IN qualifying_set` (15M orders → ~100K pass).
     For qualifying orders, collect (o_orderkey, o_custkey, o_orderdate, o_totalprice).
  2. Join orders→customer: probe c_custkey → customer row for c_name. (100K lookups, tiny.)
  3. Scan lineitem again (or use l_orderkey hash index): find all lineitem rows for qualifying orders.
     For each, sum l_quantity per group key.
- **Output:** GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice) → ~100K groups.
  Top-100 by o_totalprice DESC, o_orderdate. Use partial sort / max-heap of 100 elements.
- **Note on zone maps for Q18:** The orders zone map does NOT help since there's no date filter
  on orders. The subquery scans all of lineitem (full scan of 60M rows). No date predicate → all
  600 zone-map blocks must be read.
- **Subquery strategy from workload analysis:** "Materialize inner SELECT to hash set, probe with outer"
  (`estimated_inner_result: 100000, outer_rows: 15000000, inner_rows: 1500000`).

---

## Indexes

### lineitem_orderkey_hash (multi-value hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout:
  ```
  uint32_t num_unique   // = 15000000 unique l_orderkey values
  uint32_t ht_capacity  // = 33554432 (2^25, ~45% load factor)
  uint32_t num_rows     // = 59986052
  Slot ht[33554432]     // each: {int32_t key (INT32_MIN=empty), uint32_t offset, uint32_t count}
  uint32_t positions[59986052]  // lineitem row indices grouped by l_orderkey value
  ```
  Average ~4 lineitem rows per order (60M / 15M = 4).
- **Hash function:** `slot = (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> (64 - 25))`
- **Lookup:** hash key K → probe ht → (offset, count) → `positions[offset..offset+count-1]`.
  `row_offset` in positions is a ROW index. Access column as `col_ptr[row_idx]`.
- **Subquery usage:** Instead of building a full hash aggregation table for 15M unique keys
  at runtime, use this index to efficiently iterate per-key position groups:
  ```cpp
  // Iterate all unique keys in hash table
  for (uint32_t slot = 0; slot < ht_capacity; slot++) {
      if (ht[slot].key == INT32_MIN) continue;
      double sum_q = 0.0;
      for (uint32_t i = ht[slot].offset; i < ht[slot].offset + ht[slot].count; i++)
          sum_q += l_quantity[positions[i]];
      if (sum_q > 300.0) qualifying.insert(ht[slot].key);
  }
  ```
  Benefit: avoids building a 15M-entry runtime hash table. The pre-built index replaces it.
- **Outer query usage:** After finding qualifying_orderkeys, for each `ok` in qualifying_set,
  look up positions and scan `l_quantity[positions[i]]` only for qualifying lineitem rows (~100K orders × 4 rows = ~400K accesses).
