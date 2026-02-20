# Q18 Guide — Large Volume Customer

## Column Reference

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 rows × 4 bytes)
- This query: join key `c_custkey = o_custkey`; also output column

### c_name (STRING, offsets_data encoding)
- Files:
  - `customer/c_name_offsets.bin`: uint32_t[1,500,001] — offsets[i] = byte start of name i
  - `customer/c_name_data.bin`: concatenated raw chars
- Access string i: `char* s = name_data + name_offsets[i]; size_t len = name_offsets[i+1] - name_offsets[i];`
- This query: output column in SELECT — retrieve string for qualifying rows via late materialization
- Example name format: "Customer#000000001" (18 chars)

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 rows × 4 bytes)
- This query: join key `o_orderkey = l_orderkey`; part of GROUP BY and output

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 rows × 4 bytes)
- This query: join key `c_custkey = o_custkey`

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 rows × 4 bytes)
- This query: output column and secondary sort key (ORDER BY o_orderdate ASC)
- Format for output: convert epoch days back to "YYYY-MM-DD" string

### o_totalprice (DECIMAL, double)
- File: `orders/o_totalprice.bin` (15,000,000 rows × 8 bytes)
- Stored as native double
- This query: output column and primary sort key (ORDER BY o_totalprice DESC)

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 rows × 4 bytes, sorted by l_shipdate)
- This query (subquery): `SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`
  — aggregate ALL lineitem rows by l_orderkey, keep orderkeys with total qty > 300
- This query (outer): join key `o_orderkey = l_orderkey`

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- This query (subquery): `HAVING SUM(l_quantity) > 300` — threshold for large orders
  - Compare accumulated quantity as double: `sum_qty > 300.0`
- This query (outer): `SUM(l_quantity)` per group — output column sum_qty

## Table Stats
| Table    | Rows       | Role      | Sort Order   | Block Size |
|----------|------------|-----------|--------------|------------|
| lineitem | 59,986,052 | fact      | l_shipdate ↑ | 100,000    |
| orders   | 15,000,000 | fact      | none         | 100,000    |
| customer | 1,500,000  | dimension | none         | 100,000    |

## Query Analysis
- **Subquery**: `SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300`
  - Hash aggregate ALL 60M lineitem rows by l_orderkey → 15M groups → filter to those with qty > 300
  - Very few orders have total quantity > 300 (rare, high selectivity filter)
  - Result: a small hash set of qualifying l_orderkey values (estimated << 1% of 15M orders)
- **Outer query join chain**: `customer ⋈ orders ⋈ lineitem` filtered by qualifying o_orderkey from subquery
  - `o_orderkey IN (subquery result)` — hash semi-join, probe orders against qualifying set
  - `c_custkey = o_custkey` — probe customer hash table
  - `o_orderkey = l_orderkey` — probe lineitem hash index
- **Aggregation**: GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)
  - Each group corresponds to one order (since o_orderkey is PK of orders)
  - SUM(l_quantity) per order
- **Output**: TOP-100 by o_totalprice DESC, o_orderdate ASC
- **Subquery optimization**: semi-join pattern — execute subquery once, build hash set, probe in outer query

## Indexes

### lineitem_orderkey_hash (multivalue hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout:
  ```
  [uint32_t cap=33554432]
  [uint32_t n_pos=59986052]
  [cap × {int32_t key, uint32_t offset, uint32_t count}]  (12 bytes each, MHEntry)
  [59986052 × uint32_t positions]                          (4 bytes each)
  ```
- Total file size: 4 + 4 + 33554432×12 + 59986052×4 ≈ 613 MB
- Empty slot: `entry.key == INT32_MIN` (0x80000000 = -2147483648)
- Hash function: `uint32_t slot = (uint32_t)(((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL) >> 32) & (cap-1)`
- Linear probe: advance `slot = (slot + 1) & (cap - 1)` until `entry.key == key` (found) or `entry.key == INT32_MIN` (not found)
- On match: `positions[entry.offset .. entry.offset + entry.count)` → row indices into sorted lineitem arrays
- Each `position` is a ROW INDEX (not byte offset). Access: `l_quantity[position]`, `l_extendedprice[position]`
- Usage in Q18 subquery: NOT used — subquery scans ALL lineitem rows sequentially and does in-memory hash aggregation
- Usage in Q18 outer query: After building qualifying order set, use this index to look up lineitem rows per qualifying o_orderkey; or do a sequential scan of lineitem filtered by the qualifying orderkey set

### orders_orderkey_hash (unique hash on o_orderkey)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t cap=33554432]` then 33554432 × `[int32_t key, uint32_t row_idx]` (8 bytes each, UHEntry)
- Empty slot: `entry.key == INT32_MIN`; not-found: probe hits empty slot
- Lookup: `row_idx = orders_hash.lookup(o_orderkey)` → row index into orders arrays
- Usage in Q18: After subquery identifies qualifying l_orderkey values, probe this hash to get orders row, then join to customer

### customer_custkey_hash (unique hash on c_custkey)
- File: `indexes/customer_custkey_hash.bin`
- Layout: `[uint32_t cap=4194304]` then 4194304 × `[int32_t key, uint32_t row_idx]` (8 bytes each)
- Empty slot: `entry.key == INT32_MIN`
- Usage in Q18: Given o_custkey from qualifying orders, look up customer row to get c_name

## Execution Strategy
1. **Subquery phase** (full lineitem scan + hash aggregation):
   - Sequentially scan l_orderkey.bin and l_quantity.bin (60M rows, 480MB total)
   - Build in-memory hash map: `orderkey → sum_qty` (15M entries with ~0.6 load factor)
   - After scan, iterate map and collect orderkeys where `sum_qty > 300.0` → small hash set
2. **Order filter phase**:
   - Scan o_orderkey.bin (15M rows), filter to those in qualifying set from step 1
   - For qualifying orders: look up o_orderdate, o_totalprice (already in memory from orders scan)
   - Build set of qualifying (o_orderkey, o_custkey, o_orderdate, o_totalprice)
3. **Customer join phase**:
   - For each qualifying order, probe customer_custkey_hash on o_custkey → get customer row_idx → load c_name, c_custkey
4. **Lineitem aggregation phase**:
   - Scan l_orderkey.bin + l_quantity.bin again for qualifying orderkeys
   - SUM(l_quantity) per qualifying o_orderkey
5. **Final result**:
   - Assemble (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum_qty) tuples
   - Top-100 by o_totalprice DESC, o_orderdate ASC using partial sort (bounded priority queue)
   - For output: format o_orderdate as "YYYY-MM-DD" string from epoch days; decode c_name from offsets
