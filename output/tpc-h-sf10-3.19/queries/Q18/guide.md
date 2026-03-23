# Q18 Guide — Large Volume Customer

## Column Reference

### l_orderkey (fk, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59986052 rows × 4 bytes)
- This query: Subquery GROUP BY key, JOIN key `o_orderkey = l_orderkey`

### l_quantity (measure, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59986052 rows × 8 bytes)
- This query: Subquery `SUM(l_quantity) > 300`, outer `SUM(l_quantity)` per group

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15000000 rows × 4 bytes)
- This query: `o_orderkey IN (subquery)`, JOIN key, GROUP BY key

### o_custkey (fk, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15000000 rows × 4 bytes)
- This query: JOIN key `c_custkey = o_custkey`

### o_orderdate (date, int32_t, days_since_epoch_1970)
- File: `storage/orders/o_orderdate.bin` (15000000 rows × 4 bytes)
- This query: GROUP BY key, ORDER BY key (secondary, ASC), SELECT output

### o_totalprice (measure, double, raw)
- File: `storage/orders/o_totalprice.bin` (15000000 rows × 8 bytes)
- This query: GROUP BY key, ORDER BY key (primary, DESC), SELECT output

### c_custkey (pk, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1500000 rows × 4 bytes)
- This query: JOIN key `c_custkey = o_custkey`, GROUP BY key, SELECT output

### c_name (text, varlen string)
- Files: `storage/customer/c_name_offsets.bin` (1500001 × 4 bytes), `storage/customer/c_name_data.bin`
- Varlen format: offsets are `uint32_t[N+1]`, string `i` spans `[offsets[i], offsets[i+1])` in data file
- This query: GROUP BY key, SELECT output — only needed for final result rows

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey | 65536      |
| customer | 1,500,000  | dimension | c_custkey  | 65536      |

## Query Analysis

### Subquery Pattern
```sql
SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
```
- Groups: ~15M unique orderkeys
- Very selective HAVING: ~0.001 → ~15K qualifying orderkeys (orders with total quantity > 300)

### Join Pattern
- orders filtered by subquery result: `o_orderkey IN (...)`
- orders → customer: `c_custkey = o_custkey`
- orders → lineitem (again): `o_orderkey = l_orderkey` for final SUM(l_quantity)

### Aggregation
- GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice): ~57 effective groups
- Each (o_orderkey) uniquely determines (o_orderdate, o_totalprice, c_custkey, c_name), so the effective group key is o_orderkey
- SUM(l_quantity) per group

### Output
- ORDER BY o_totalprice DESC, o_orderdate ASC
- LIMIT 100

## Indexes

### lineitem_l_orderkey_grouped (sorted_grouped on l_orderkey)
- File: `storage/indexes/lineitem_l_orderkey_grouped.bin`
- Layout:
  - Header: `uint64_t num_entries` (= max_orderkey + 1)
  - Body: interleaved `uint32_t[num_entries * 2]` — `[start0, count0, start1, count1, ...]`
  - For orderkey `k`: `start = body[k*2]`, `count = body[k*2+1]`
  - If count == 0, no lineitems for that orderkey
- Usage pattern for subquery: Iterate all valid orderkeys using the grouped index. For each orderkey with count > 0, sum l_quantity for rows [start, start+count). If sum > 300, add to qualifying set.
- This avoids a full scan + hash aggregation — just iterate the index entries.

### orders_o_orderkey_lookup (dense_pk_array on o_orderkey)
- File: `storage/indexes/orders_o_orderkey_lookup.bin`
- Layout:
  - Header: `uint64_t num_entries` (= max_orderkey + 1)
  - Body: `int32_t[num_entries]` where `arr[orderkey] = row_id`, `-1` if missing
- Usage: For qualifying orderkeys, look up orders row to get o_custkey, o_orderdate, o_totalprice.

## Recommended Execution Strategy
1. **Subquery — find large orders**: Load the `lineitem_l_orderkey_grouped` index. Load `l_quantity`. For each orderkey `k` in [0, num_entries):
   - If `count = grouped[k*2+1]` is 0, skip
   - Sum `l_quantity[start..start+count)` where `start = grouped[k*2]`
   - If sum > 300.0, record orderkey `k` and sum into a qualifying set
   - Expected: ~15K qualifying orderkeys
2. **Join with orders**: For each qualifying orderkey, use `orders_o_orderkey_lookup` to get the orders row_id. Read o_custkey, o_orderdate, o_totalprice.
3. **Join with customer**: For each qualifying order, look up customer by c_custkey. Customer is sorted by c_custkey, so use direct index: `c_custkey` values are dense 1-based, so row_id = c_custkey - 1 (verify, or build a small lookup).
4. **Build result tuples**: (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice, sum_qty)
5. **Top-100**: Sort by o_totalprice DESC, o_orderdate ASC. Take first 100.
6. **Output c_name**: Only decode varlen c_name for the final ≤100 result rows.
