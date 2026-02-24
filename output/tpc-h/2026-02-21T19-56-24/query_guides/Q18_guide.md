# Q18 Guide — Large Volume Customer

## Column Reference

### c_name (STRING, char[25], fixed-width)
- File: `customer/c_name.bin` (1500000 × 25 = 37.5MB; each record is exactly 25 null-padded bytes)
- This query: `SELECT c_name` — output column only (not a filter or join key)
- Access: `const char* name = customer_c_name_data + (row_idx * 25);`
- For output: read 25 bytes; null-terminated; TPC-H names are like "Customer#000000001"

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1500000 rows)
- This query: `c_custkey = o_custkey` join condition; also in SELECT output and GROUP BY
- Index: `indexes/customer_custkey_hash.bin` (single-value PK hash)

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15000000 rows)
- This query: `o_orderkey IN (subquery)` filter; also in SELECT output and GROUP BY
- The IN subquery selects orderkeys where `SUM(l_quantity) > 300`.

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15000000 rows)
- This query: join condition `o_custkey = c_custkey`

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15000000 rows)
- Sorted column (orders sorted by o_orderdate).
- This query: SELECT o_orderdate (output column); also ORDER BY o_orderdate ASC (secondary sort)
- Decode for display: `epoch_days_to_string(o_orderdate[row_idx])` — reconstruct "YYYY-MM-DD"

### o_totalprice (DECIMAL, double)
- File: `orders/o_totalprice.bin` (15000000 rows)
- Stored as native double. Values match SQL directly.
- This query: SELECT o_totalprice (output); ORDER BY o_totalprice DESC (primary sort)

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59986052 rows)
- This query:
  - **Subquery**: GROUP BY l_orderkey, HAVING SUM(l_quantity) > 300 → collect qualifying orderkeys
  - **Outer query**: join condition `o_orderkey = l_orderkey`
- Index: `indexes/lineitem_orderkey_hash.bin` (multi-value hash, 15M unique keys)

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- Stored as native double.
- This query:
  - **Subquery**: `SUM(l_quantity)` per l_orderkey group, filter `> 300`
  - **Outer query**: `SUM(l_quantity) AS sum_qty` for the GROUP BY output

## Table Stats
| Table    | Rows     | Role      | Sort Order  | Block Size |
|----------|----------|-----------|-------------|------------|
| lineitem | 59986052 | fact      | l_shipdate  | 100000     |
| orders   | 15000000 | fact      | o_orderdate | 100000     |
| customer | 1500000  | dimension | none        | 100000     |

## Query Analysis
- **Subquery (decorrelate first)**:
  - `SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`
  - Strategy: full scan of `lineitem/l_orderkey.bin` + `lineitem/l_quantity.bin`
  - Use sort-based grouping: sort positions by l_orderkey, accumulate SUM(l_quantity) per group.
    Alternative: hash aggregation on l_orderkey (15M unique keys → hash table with 15M entries).
  - Selectivity: ~0.004% of orders qualify → ~600 qualifying orderkeys. Build a small hash set.
  - Estimated qualifying orderkeys: ~600 (orders with total quantity > 300).
- **Outer query execution**:
  1. Scan `orders/o_orderkey.bin` (15M rows): filter by qualifying orderkey hash set (~600 keys).
     Very selective: ~600 out of 15M orders qualify.
     Also load o_custkey, o_orderdate, o_totalprice for qualifying rows.
  2. For each qualifying order, look up customer by o_custkey → get c_name, c_custkey.
  3. Scan `lineitem/l_orderkey.bin` + `lineitem/l_quantity.bin`: filter by qualifying orderkey set.
     ~600 orderkeys × avg 4 lineitems = ~2400 lineitem rows.
  4. Group by (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice); SUM(l_quantity).
  5. ORDER BY o_totalprice DESC, o_orderdate ASC; output top 100.
- **Selectivities**:
  - Subquery qualifying orderkeys: ~600 (~0.004% of 15M orders)
  - Outer orders matching: ~600 rows (extremely selective)
  - Lineitem rows for those orders: ~2400 rows
- **Aggregation**: ~600 groups (one per qualifying order). Trivial.
- **Output**: Up to 100 rows after sorting. Small sort.
- **Key insight**: After decorrelating the subquery, this query touches very few rows in the outer joins. The subquery itself requires a full scan of lineitem (60M rows) to compute per-orderkey sums, but only 2 columns: l_orderkey and l_quantity.

## Indexes

### lineitem_orderkey_hash (multi-value hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout:
  ```
  uint32_t num_positions  (= 59986052)
  uint32_t num_unique     (= 15000000)
  uint32_t capacity       (= 33554432)
  uint32_t positions[59986052]    // row indices sorted by l_orderkey
  MvEntry[33554432]: { int32_t key; uint32_t offset; uint32_t count; }
  // empty slot: key == INT32_MIN (-2147483648)
  ```
- row_idx is the ROW index into lineitem column files.
- This query (outer query): for each of the ~600 qualifying orderkeys, use this index to find their lineitem rows and compute SUM(l_quantity).
  ```cpp
  uint32_t slot = (key * 2654435761ULL >> 32) & (capacity - 1);
  while (entries[slot].key != INT32_MIN && entries[slot].key != key) slot = (slot+1) & mask;
  if (entries[slot].key == key) {
      uint32_t off = entries[slot].offset, cnt = entries[slot].count;
      for (uint32_t j = 0; j < cnt; j++) {
          uint32_t row = positions[off + j];
          sum_qty += l_quantity[row];
      }
  }
  ```

### orders_orderkey_hash (single-value hash on o_orderkey, PK)
- File: `indexes/orders_orderkey_hash.bin`
- Layout:
  ```
  uint32_t num_rows  (= 15000000)
  uint32_t capacity  (= 33554432)
  SvEntry[33554432]: { int32_t key; uint32_t row_idx; }
  // empty: key == INT32_MIN
  ```
- This query: after collecting qualifying orderkeys from subquery, use to fetch order details (o_custkey, o_orderdate, o_totalprice) by orderkey.

### customer_custkey_hash (single-value hash on c_custkey, PK)
- File: `indexes/customer_custkey_hash.bin`
- Layout:
  ```
  uint32_t num_rows  (= 1500000)
  uint32_t capacity  (= 4194304)
  SvEntry[4194304]: { int32_t key; uint32_t row_idx; }
  // empty: key == INT32_MIN
  ```
- This query: look up customer by o_custkey → get row_idx → load c_name[row_idx*25..+25] and c_custkey[row_idx].

### orders_custkey_hash (multi-value hash on o_custkey)
- File: `indexes/orders_custkey_hash.bin`
- Layout:
  ```
  uint32_t num_positions  (= 15000000)
  uint32_t num_unique     (= 999982)
  uint32_t capacity       (= 2097152)
  uint32_t positions[15000000]
  MvEntry[2097152]: { int32_t key; uint32_t offset; uint32_t count; }
  // empty: key == INT32_MIN
  ```
- This query: alternative plan — scan qualifying customers and look up their orders (not needed if using orders_orderkey_hash after subquery, but available if plan changes).
