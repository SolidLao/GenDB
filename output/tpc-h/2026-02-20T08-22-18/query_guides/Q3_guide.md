# Q3 Guide — Shipping Priority

```sql
SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10
```

---

## Column Reference

### c_mktsegment (STRING, int16_t, dictionary-encoded)
- File: `customer/c_mktsegment.bin` (1500000 rows × 2 bytes = 3 MB)
- Dictionary: `customer/c_mktsegment_dict.txt` (format: `code=value`, 5 distinct values:
  AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY — codes assigned at parse time)
- **Loading and filter pattern:**
  ```cpp
  std::ifstream df("customer/c_mktsegment_dict.txt");
  std::string line;
  int16_t building_code = -1;
  while (std::getline(df, line)) {
      size_t eq = line.find('=');
      if (line.substr(eq+1) == "BUILDING")
          building_code = (int16_t)std::stoi(line.substr(0, eq));
  }
  // Filter: keep rows where c_mktsegment[i] == building_code
  ```
- This query: filter `c_mktsegment = 'BUILDING'` → `mktseg_code[row] == building_code`.
  Selectivity: ~20% → ~300K qualifying customers.

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1500000 rows × 4 bytes = 6 MB)
- Stored as int32_t. Values match SQL directly. Range: 1 to 1,500,000.
- This query: join key `c_custkey = o_custkey`. Build hash set of qualifying c_custkeys after
  applying c_mktsegment filter.

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15000000 rows × 4 bytes = 60 MB)
- Stored as int32_t. Foreign key into customer.
- This query: probe qualifying customer set, filter `o_custkey IN qualifying_customers`.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15000000 rows × 4 bytes = 60 MB)
- File is sorted ascending by o_orderdate (sort_order = ["o_orderdate"]).
- Range in data: 8035 (1992-01-01) to 10441 (1998-08-02).
- **This query:** `o_orderdate < DATE '1995-03-15'`
  - `parse_date("1995-03-15")` = 365×25 + 6_leaps_1970-1994 + 59_days_Jan-Feb + 14 = **9204**
  - C++ filter: `raw_orderdate < 9204`
- **Zone map skip:** block qualifies for skipping if `block_min >= 9204` (all dates in block ≥ 1995-03-15).
  Estimated selectivity 30% → ~70% of rows (blocks from 1995+ onwards) are skipped.

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15000000 rows × 4 bytes = 60 MB)
- Join key `l_orderkey = o_orderkey`. After applying date and customer filters on orders,
  build hash map: `o_orderkey → (o_orderdate, o_shippriority)` for qualifying orders.

### o_shippriority (INTEGER, int32_t)
- File: `orders/o_shippriority.bin` (15000000 rows × 4 bytes = 60 MB)
- Stored as int32_t. All values = 0 in TPC-H SF10 (1 distinct value).
- This query: output column in GROUP BY and SELECT.

### o_totalprice (DECIMAL, double) — not used in Q3, skip.

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59986052 rows × 4 bytes = 240 MB)
- Stored as int32_t. Foreign key into orders.
- This query: probe qualifying orders hash map by l_orderkey.

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59986052 rows × 4 bytes = 240 MB)
- Sorted ascending (lineitem sort key).
- **This query:** `l_shipdate > DATE '1995-03-15'` → `raw_shipdate > 9204`
- **Zone map skip:** block qualifies for skipping if `block_max <= 9204` (all dates ≤ 1995-03-15).
  Estimated 50% of lineitem rows qualify → ~50% of blocks can be skipped.

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Values match SQL directly.
- This query: `SUM(l_extendedprice*(1-l_discount))` → `ep * (1.0 - disc)` per qualifying row.

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59986052 rows × 8 bytes = 480 MB)
- Stored as native double. Range: 0.00 to 0.10.
- This query: used in revenue computation `l_extendedprice*(1-l_discount)`.

---

## Table Stats

| Table    | Rows     | Role      | Sort Order   | Block Size |
|----------|----------|-----------|--------------|------------|
| customer | 1500000  | dimension | none         | 100000     |
| orders   | 15000000 | fact      | o_orderdate ↑ | 100000    |
| lineitem | 59986052 | fact      | l_shipdate ↑ | 100000     |

---

## Query Analysis
- **Join pattern:**
  1. Scan customer (1.5M), filter c_mktsegment='BUILDING' → ~300K rows → build hash set on c_custkey.
  2. Scan orders (15M), apply zone map to skip blocks with o_orderdate >= 9204 (~70% skip),
     then filter `o_custkey IN custkey_set` AND `o_orderdate < 9204`. Build hash map:
     `o_orderkey → (o_orderdate, o_shippriority)` for ~900K qualifying orders (30% × 50% = 15% × 15M).
  3. Scan lineitem (60M), use zone map to skip blocks with l_shipdate <= 9204 (~50% skip),
     then probe orders hash map by `l_orderkey`. Filter `l_shipdate > 9204`.
     Group by (l_orderkey, o_orderdate, o_shippriority), accumulate revenue.
- **Filters:** c_mktsegment=BUILDING (20%), o_orderdate<9204 (30%), l_shipdate>9204 (50%).
  Combined lineitem selectivity estimate: ~10% of lineitem rows contribute to output.
- **Aggregation:** ~100K groups. Hash aggregation on (l_orderkey, o_orderdate, o_shippriority).
- **Output:** Top-10 by revenue DESC, o_orderdate ASC. Use partial sort / top-10 heap.
- **Subquery:** None.

---

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout:
  ```
  uint32_t num_blocks   // = 600
  per block (12 bytes):
    int32_t  min_val    // min l_shipdate in block
    int32_t  max_val    // max l_shipdate in block
    uint32_t num_rows   // = 100000 (last block = 86052)
  ```
  Block b covers rows [b*100000, b*100000 + num_rows).
  `row_offset` is ROW index, not byte offset.
- **This query:** `l_shipdate > 9204` → skip block if `block_max <= 9204`.
  With ascending sort, all early blocks (dates before 1995-03-15) can be skipped.
  ~50% of 600 blocks (300 blocks × 100K rows = 30M rows) skipped → significant I/O reduction.

### orders_orderdate_zonemap (zone_map on o_orderdate)
- File: `indexes/orders_orderdate_zonemap.bin`
- Layout:
  ```
  uint32_t num_blocks   // = 150
  per block (12 bytes):
    int32_t  min_val    // min o_orderdate in block (= first row, sorted)
    int32_t  max_val    // max o_orderdate in block (= last row, sorted)
    uint32_t num_rows   // = 100000
  ```
  Block b covers rows [b*100000, b*100000 + 100000). `row_offset` is ROW index.
- **This query:** `o_orderdate < 9204` → skip block if `block_min >= 9204`.
  ~70% of orders blocks have dates >= 1995-03-15 and are fully skipped.

### lineitem_orderkey_hash (multi-value hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout:
  ```
  uint32_t num_unique   // = 15000000 unique l_orderkey values
  uint32_t ht_capacity  // = 33554432 (next power-of-2 above 30M, ~45% load)
  uint32_t num_rows     // = 59986052
  Slot ht[ht_capacity]  // each Slot: {int32_t key (INT32_MIN=empty), uint32_t offset, uint32_t count}
  uint32_t positions[59986052]  // row indices grouped by key
  ```
- **Hash function:** `slot = (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> (64 - 25))`
  (capacity=33554432=2^25, so shift=64-25=39)
- **Lookup:** Given key K, compute slot, probe linearly until `ht[slot].key == K` or `ht[slot].key == INT32_MIN`.
  Then `positions[ht[slot].offset .. ht[slot].offset + ht[slot].count - 1]` are all lineitem row indices for K.
  `row_offset` in positions is a ROW index, not byte offset. Access as `col_ptr[row_idx]`.
- **This query:** Optional — can probe lineitem by qualifying o_orderkey values instead of full scan.
  For each qualifying order key, look up lineitem rows directly. Trades scan for random access;
  may be beneficial if orders selectivity after filtering is very low (<1%).
