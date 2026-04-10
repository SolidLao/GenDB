# Q18 Guide — Large Volume Customer

## Column Reference

### c_custkey (PK, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`, GROUP BY, output

### c_name (string, varlen)
- File: `storage/customer/c_name.bin` (offsets), `storage/customer/c_name_data.bin`
- This query: GROUP BY, output

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: IN subquery, join key, GROUP BY, output

### o_custkey (FK, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows of int32_t)
- This query: GROUP BY, output, ORDER BY (secondary)

### o_totalprice (decimal, double, raw)
- File: `storage/orders/o_totalprice.bin` (15,000,000 rows of double)
- This query: GROUP BY, output, ORDER BY (primary, DESC)

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: subquery GROUP BY, join key

### l_quantity (decimal, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows of double)
- This query: `SUM(l_quantity)` in both subquery (HAVING > 300) and outer query

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| customer | 1,500,000  | dimension | c_custkey  | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |

## Query Analysis
- **Pattern**: Subquery finds orderkeys where SUM(l_quantity) > 300, then join customer → orders → lineitem for those orders
- **Strategy**:
  1. Compute SUM(l_quantity) per l_orderkey: scan lineitem, accumulate per orderkey. Use lineitem_orderkey_idx or sequential scan (lineitem is sorted by l_orderkey, so runs of same orderkey are contiguous).
     - Dense array approach: `double qty_sum[max_orderkey+1]` — but max_orderkey=60M → 480MB, may be too large.
     - Alternative: Since lineitem is sorted by l_orderkey, process runs: accumulate quantity per run, emit when orderkey changes.
  2. Collect qualifying orderkeys where sum > 300 (very few — maybe ~57 orders at SF10)
  3. For each qualifying order: look up o_custkey, o_orderdate, o_totalprice via orders_pk_idx. Look up c_name via customer_pk_idx.
  4. Re-scan lineitem for qualifying orders (use lineitem_orderkey_idx) to get final SUM(l_quantity) per (orderkey group).
  5. Output: ORDER BY o_totalprice DESC, o_orderdate ASC, LIMIT 100

## Indexes

### lineitem_orderkey_idx (dense_range on l_orderkey)
- File: `storage/indexes/lineitem_orderkey_idx.bin`
- Meta: `storage/indexes/lineitem_orderkey_idx_meta.txt`
- Layout: `struct { uint32_t start; uint32_t count; }` indexed by orderkey, 8 bytes per entry
- Usage: For each orderkey, quickly sum l_quantity for its lineitem rows. Also for step 4, retrieve lineitem rows for qualifying orders.
- Sentinel: `{0, 0}` = no rows

### orders_pk_idx (dense_pk on o_orderkey)
- File: `storage/indexes/orders_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by orderkey → row_id, sentinel -1
- Usage: Look up order attributes for qualifying orderkeys

### customer_pk_idx (dense_pk on c_custkey)
- File: `storage/indexes/customer_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by custkey → row_id, sentinel -1
- Usage: Look up customer name for qualifying orders
