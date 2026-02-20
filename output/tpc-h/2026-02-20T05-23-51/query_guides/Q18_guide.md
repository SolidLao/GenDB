# Q18 Guide — Large Volume Customer

## Query
```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
       SUM(l_quantity) AS sum_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
)
AND c_custkey = o_custkey
AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate
LIMIT 100;
```

## Column Reference

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes)
- **This query**:
  - Phase 1 (subquery): GROUP BY l_orderkey, compute SUM(l_quantity), keep where SUM > 300
  - Phase 2 (main query): join `o_orderkey = l_orderkey` for qualifying orders
- Index: `indexes/lineitem_orderkey_hash.bin`

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes)
- Stored as native double. Range 1.0–50.0.
- **Phase 1 (subquery)**: SUM per l_orderkey. Accumulate per order. Keep orders with sum > 300.0
- **Phase 2 (main query)**: SUM(l_quantity) per group in outer query output
- C++ comparison: `sum_per_order > 300.0`

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes)
- **This query**: Filter `o_orderkey IN subquery_result_set`. Output column.
- Index: `indexes/orders_orderkey_hash.bin` (PK, 15M unique, cap=33,554,432)

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15,000,000 × 4 bytes)
- **This query**: Join key `c_custkey = o_custkey`
- Index: `indexes/orders_custkey_hash.bin`
  - Layout: `[uint32_t num_positions=15M][uint32_t positions[15M]][uint32_t cap=2,097,152][HEntry32 × 2097152]`
  - 999,982 unique customer keys found in orders

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes)
- **This query**: Output column only (GROUP BY, ORDER BY). No filter predicate.
- To retrieve date for display: value is epoch days. Convert to YYYY-MM-DD using inverse formula.

### o_totalprice (DECIMAL, double)
- File: `orders/o_totalprice.bin` (15,000,000 × 8 bytes)
- Stored as native double. Values match SQL directly.
- **This query**: Output column (GROUP BY, ORDER BY DESC). No filter.
- Verified non-zero: first=186600, last=369545

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1,500,000 × 4 bytes)
- **This query**: Join key `c_custkey = o_custkey`. Output column.
- Index: `indexes/customer_custkey_hash.bin` (PK, 1.5M unique, cap=4,194,304)

### c_name (STRING, char[26], fixed_char26 encoding)
- File: `customer/c_name.bin` (1,500,000 × 26 bytes = 39 MB)
- Encoding: null-padded fixed-width 26-byte char array. Row i at byte offset `i × 26`.
- Access: `const char* name = c_name_col + row_idx * 26;`
- **This query**: Output column (SELECT c_name, GROUP BY c_name)
- Values like "Customer#000000001" (18 chars, null-padded to 26)

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| lineitem | 59,986,052 | fact      | none       | 100,000    |
| orders   | 15,000,000 | fact      | none       | 100,000    |
| customer | 1,500,000  | dimension | none       | 100,000    |

## Query Analysis
- **Subquery type**: IN (SELECT ... GROUP BY ... HAVING) — hash semi-join
  - Decorrelate: compute the subquery result as a hash set of qualifying l_orderkey values
- **Recommended two-phase execution**:
  1. **Phase 1** (subquery): Full scan of lineitem. Aggregate SUM(l_quantity) per l_orderkey using hash map. Collect orderkeys where sum > 300 into a hash set (`qualified_orders`). Expected: very few orders qualify (~1% or less, since average order qty is ~4×25=100, needing all 7+ lineitems with qty=50)
  2. **Phase 2** (main query): Scan orders table. For each row, check `o_orderkey IN qualified_orders`. For qualifying orders, look up customer via c_custkey. Scan lineitem again (or use lineitem_orderkey_hash) to compute SUM(l_quantity) per qualifying order.
- **Alternative Phase 2**: Use `lineitem_orderkey_hash` to directly access lineitem rows for qualifying orders without a full scan.
- **Aggregation**: GROUP BY (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice). Since o_orderkey is unique per order, this is effectively GROUP BY o_orderkey. Expect ~few hundred qualifying orders (very selective HAVING clause).
- **Output**: Top 100 by o_totalprice DESC, o_orderdate ASC → partial sort
- **Selectivity**: HAVING SUM(l_quantity) > 300 is very selective (~0.01% of orders). Expected groups: ~few hundred.

## Indexes

### lineitem_orderkey_hash (hash on l_orderkey, multi-value)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout:
  ```
  [uint32_t num_positions = 59,986,052]
  [uint32_t positions[59986052]]
  [uint32_t capacity = 33,554,432]
  [struct HEntry32 {int32_t key; uint32_t offset; uint32_t count;} × 33554432]
  // Empty slot: key == INT32_MIN
  ```
- Hash: `(uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & (cap-1)`
- **Phase 1 usage**: NOT needed — Phase 1 does a full scan of lineitem (all rows needed for aggregation)
- **Phase 2 usage**: For each qualifying orderkey k, lookup rows and sum l_quantity:
  ```cpp
  uint32_t h = hash32(k) & (cap-1);
  while (ht[h].key != INT32_MIN && ht[h].key != k) h = (h+1) & (cap-1);
  // Access positions[ht[h].offset .. ht[h].offset+count-1] to get lineitem row indices
  ```
- row_offset is ROW index. Byte offset in l_quantity.bin = row_idx × 8

### orders_orderkey_hash (hash on o_orderkey, single-value PK)
- File: `indexes/orders_orderkey_hash.bin`
- Layout: `[uint32_t num_positions=15M][positions[15M]][uint32_t cap=33554432][HEntry32 × 33554432]`
- **This query**: Not the primary access path. Alternatively, scan orders sequentially and check each o_orderkey against the qualified_orders hash set.

### customer_custkey_hash (hash on c_custkey, single-value PK)
- File: `indexes/customer_custkey_hash.bin`
- Layout: `[uint32_t num_positions=1500000][positions[1500000]][uint32_t cap=4194304][HEntry32 × 4194304]`
- **This query**: For qualifying orders, look up customer row: `c_custkey = o_custkey`
  - Probe with o_custkey to find row in customer table → read c_name[row×26]
- Hash: same multiply-shift hash32 function

### orders_custkey_hash (hash on o_custkey, multi-value)
- File: `indexes/orders_custkey_hash.bin`
- Layout: `[uint32_t num_positions=15M][positions[15M]][uint32_t cap=2097152][HEntry32 × 2097152]`
- 999,982 unique customer keys. This index is less useful for Q18 (join goes order→customer, not customer→order).
