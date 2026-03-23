# Q18 Guide — Large Volume Customer

## Query
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
ORDER BY o_totalprice DESC, o_orderdate ASC
LIMIT 100;
```

## Table Stats
| Table    | Rows       | Role           | Sort Order     | Block Size |
|----------|------------|----------------|----------------|------------|
| lineitem | 59,986,052 | fact (2 passes) | l_shipdate ASC | 100,000    |
| orders   | 15,000,000 | dimension      | (none)         | 100,000    |
| customer | 1,500,000  | dimension      | (none)         | 100,000    |

## Column Reference

### l_orderkey (`INTEGER`, `int32_t`, plain)
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes = ~229 MB)
- This query (Pass 1): GROUP BY l_orderkey, aggregate SUM(l_quantity) > 300
- This query (Pass 2): join key → orders, final GROUP BY key and output column
- Key range: 1 to ~60,000,000 (TPC-H SF10)

### l_quantity (`DECIMAL(15,2)`, `int8_t`, int8_integer_value)
- File: `lineitem/l_quantity.bin` (59,986,052 × 1 byte = ~57 MB)
- Encoding: integer values 1–50 stored directly as int8_t (no scale)
- This query (Pass 1): `SUM(l_quantity) > 300` per l_orderkey group
  - Accumulate: `int32_t qty_sum[orderkey]` (max per group: 7 rows × 50 = 350, fits int32)
  - Filter condition: `qty_sum > 300`
- This query (Pass 2): `SUM(l_quantity)` in final aggregation → output `sum_qty`
- Selectivity of subquery: ~0.0042% of orderkeys qualify (≈2,520 qualifying orderkeys)

### o_orderkey (`INTEGER`, `int32_t`, plain)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes = ~57 MB)
- This query: checked against the qualifying-orderkey set from Pass 1; GROUP BY key; output

### o_custkey (`INTEGER`, `int32_t`, plain)
- File: `orders/o_custkey.bin` (15,000,000 × 4 bytes = ~57 MB)
- This query: join key → customer (c_custkey = o_custkey)

### o_orderdate (`DATE`, `int32_t`, days_since_epoch)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes = ~57 MB)
- Encoding: days since 1970-01-01
- This query: GROUP BY key, ORDER BY o_orderdate ASC (secondary sort), output column
- Output: decode to YYYY-MM-DD string for display only

### o_totalprice (`DECIMAL(15,2)`, `double`, plain_double)
- File: `orders/o_totalprice.bin` (15,000,000 × 8 bytes = ~114 MB)
- Encoding: IEEE-754 double
- This query: GROUP BY key, ORDER BY o_totalprice DESC (primary sort), output column

### c_custkey (`INTEGER`, `int32_t`, plain)
- File: `customer/c_custkey.bin` (1,500,000 × 4 bytes = ~5.7 MB)
- This query: join key (o_custkey = c_custkey); GROUP BY key; output column

### c_name (`VARCHAR(25)`, `char[26]`, fixed_26)
- File: `customer/c_name.bin` (1,500,000 × 26 bytes = ~37 MB)
- Encoding: null-padded fixed 26-byte records (up to 25 chars + 1 null byte, zero-initialized)
- This query: GROUP BY key (part of composite group key), output column
- C++ comparison for grouping: `memcmp(a_name, b_name, 26)` or use custkey as proxy
  (since c_name is 1:1 with c_custkey, keying on c_custkey is sufficient)
- C++ output: `printf("%.25s", c_name[row].data())`

## Query Analysis

### Two-Pass Execution

**Pass 1 — Subquery: find orderkeys with total quantity > 300**
```cpp
// Scan lineitem/l_orderkey.bin and lineitem/l_quantity.bin together
// Build accumulator: unordered_map or dense array keyed by l_orderkey
// Dense array approach (60M slots × 2 bytes = 120MB, fits in RAM):
std::vector<int32_t> qty_by_order(60000001, 0);
for (size_t i = 0; i < N_lineitem; i++)
    qty_by_order[l_orderkey[i]] += (int32_t)l_quantity[i];

// Find qualifying orderkeys:
std::unordered_set<int32_t> big_orders;
for (int32_t okey = 1; okey <= 60000000; okey++)
    if (qty_by_order[okey] > 300) big_orders.insert(okey);
// Expected: ~2,520 qualifying orderkeys
```
- Alternatively use a bitset of size 60M (7.5 MB) for fast membership test

**Pass 2 — Main query join and aggregation**

1. **Filter orders**: scan `o_orderkey.bin` → keep rows where `big_orders.count(o_orderkey[j]) > 0`
   - Use dense array lookup: `bool is_big[60000001]; is_big[okey] = (qty_by_order[okey] > 300);`
   - Only ~2,520 matching orders rows (from 15M) — very selective

2. **Join orders → customer**: for each qualifying order row j:
   ```cpp
   int32_t cust_row = customer_by_custkey[o_custkey[j]];  // O(1) dense array
   // Access c_name[cust_row], c_custkey[cust_row]
   ```

3. **Pass 2 lineitem scan**: scan lineitem again for matching l_orderkey:
   ```cpp
   for (size_t i = 0; i < N_lineitem; i++) {
       if (!is_big[l_orderkey[i]]) continue;
       // accumulate sum_qty per (l_orderkey) group
       final_qty[l_orderkey[i]] += (int32_t)l_quantity[i];
   }
   ```
   - Since `is_big[]` is a dense boolean array, the check is a single memory read per row

4. **Aggregate**: at most ~2,520 output groups (≈ number of qualifying orderkeys)

5. **Sort**: `partial_sort` or `nth_element` + sort for top 100 by (o_totalprice DESC, o_orderdate ASC)

### Group Key Note
The GROUP BY is on (c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice).
Since `o_orderkey` uniquely determines all other join columns in a valid TPC-H database,
use `o_orderkey` alone as the group key in the accumulator. Carry the other group-by
values as payload alongside the running sum.

### Memory Layout for Final Result
```cpp
struct Q18Result {
    char     c_name[26];    // from customer/c_name.bin via c_custkey
    int32_t  c_custkey;
    int32_t  o_orderkey;
    int32_t  o_orderdate;   // days_since_epoch; decode for output
    double   o_totalprice;
    int32_t  sum_qty;
};
std::vector<Q18Result> results;  // ~2520 entries
```

## Indexes

### orders_by_orderkey (dense_array on o_orderkey)
- File: `orders/orders_by_orderkey.bin`
- Layout: flat `int32_t` array, **60,000,001 entries** (indices 0..60,000,000)
  - `array[o_orderkey] = row_index` into orders column files
  - Sentinel: `-1` for unused slots
  - File size: 60,000,001 × 4 = **240 MB**
- Built by `build_dense_index()` with `max_key = 60000000`
- **Usage for Q18**:
  - Pass 1: not used (direct scan of lineitem)
  - Pass 2: after building `big_orders` set, look up order metadata for qualifying orderkeys:
    ```cpp
    for each qualifying okey in big_orders:
        int32_t orow = orders_by_orderkey[okey];
        // access o_custkey[orow], o_orderdate[orow], o_totalprice[orow]
    ```
  - More efficiently: during the orders column scan, flag matching rows directly
    using `is_big[]` bitset, avoiding the reverse lookup entirely

### customer_by_custkey (dense_array on c_custkey)
- File: `customer/customer_by_custkey.bin`
- Layout: flat `int32_t` array, **1,500,001 entries** (indices 0..1,500,000)
  - `array[c_custkey] = row_index` into customer column files
  - Sentinel: `-1` for unused slots
  - File size: 1,500,001 × 4 = **~5.7 MB**
- Built by `build_dense_index()` with `max_key = 1500000`
- **Usage for Q18**: O(1) orders→customer join
  ```cpp
  int32_t cust_row = customer_by_custkey[o_custkey[j]];
  // access c_name[cust_row] (char[26]), c_custkey[cust_row] (int32_t)
  ```
  - Only called for ~2,520 qualifying orders — negligible cost

### Implicit: qty_by_order dense array (constructed at query runtime — NOT a stored index)
- Constructed by Pass 1: `std::vector<int32_t> qty_by_order(60000001, 0)`
- Size: 60,000,001 × 4 = **~229 MB** in working memory
- This is a runtime data structure, not stored in gendb — built from lineitem columns each run
- After Pass 1: convert to `std::vector<bool> is_big(60000001)` (7.5 MB) for fast membership test in Pass 2
