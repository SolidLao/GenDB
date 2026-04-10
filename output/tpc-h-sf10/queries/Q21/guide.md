# Q21 Guide — Suppliers Who Kept Orders Waiting

## Column Reference

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: join key `s_suppkey = l1.l_suppkey`

### s_name (string, varlen)
- File: `storage/supplier/s_name.bin` (offsets), `storage/supplier/s_name_data.bin`
- This query: GROUP BY, output, ORDER BY (secondary)

### s_nationkey (FK, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows of int32_t)
- This query: `s_nationkey = n_nationkey` → filter to SAUDI ARABIA

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: join key `o_orderkey = l1.l_orderkey`, also EXISTS/NOT EXISTS subqueries use l_orderkey

### l_suppkey (FK, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows of int32_t)
- This query: join key `s_suppkey = l1.l_suppkey`, also `l2.l_suppkey <> l1.l_suppkey` and `l3.l_suppkey <> l1.l_suppkey`

### l_commitdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_commitdate.bin` (59,986,052 rows of int32_t)
- This query: `l1.l_receiptdate > l1.l_commitdate` (main), `l3.l_receiptdate > l3.l_commitdate` (NOT EXISTS)

### l_receiptdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_receiptdate.bin` (59,986,052 rows of int32_t)
- This query: date comparison with commitdate

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: join key `o_orderkey = l1.l_orderkey`

### o_orderstatus (char1, int8_t, raw_char)
- File: `storage/orders/o_orderstatus.bin` (15,000,000 rows of int8_t)
- This query: `o_orderstatus = 'F'` → C++: `o_orderstatus[row] == 'F'`
- Selectivity: ~0.33

### n_nationkey (PK, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)

### n_name (string, uint8_t, dictionary)
- File: `storage/nation/n_name.bin` (25 rows of uint8_t)
- Dict: `storage/nation/n_name_dict.bin`
- This query: `n_name = 'SAUDI ARABIA'` → find SAUDI ARABIA nationkey

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| supplier | 100,000    | dimension | s_suppkey  | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey | 65536      |
| nation   | 25         | dimension | n_nationkey| 65536      |

## Query Analysis
- **Pattern**: Complex semi-join and anti-semi-join on lineitem (self-join), nation filter, order status filter
- **Conditions**:
  - l1: a lineitem row from a Saudi Arabian supplier, where `l_receiptdate > l_commitdate` (supplier was late)
  - EXISTS l2: same order has at least one lineitem from a different supplier
  - NOT EXISTS l3: no other supplier on that order was also late
  - Order must have status 'F' (fulfilled)
- **Strategy**:
  1. Find SAUDI ARABIA nationkey → build set of Saudi supplier suppkeys (bitset, ~4K suppliers)
  2. Scan lineitem: find rows where `l_suppkey` is Saudi AND `l_receiptdate > l_commitdate` → candidate (orderkey, suppkey) pairs
  3. For each candidate orderkey:
     - Use **lineitem_orderkey_idx** to get all lineitems for that order
     - Check EXISTS: any lineitem with different suppkey?
     - Check NOT EXISTS: no other lineitem with different suppkey AND `l_receiptdate > l_commitdate`?
     - Check o_orderstatus == 'F' via orders_pk_idx
  4. Count qualifying rows per s_suppkey → GROUP BY s_name
  5. Output: ORDER BY numwait DESC, s_name ASC, LIMIT 100

## Indexes

### lineitem_orderkey_idx (dense_range on l_orderkey)
- File: `storage/indexes/lineitem_orderkey_idx.bin`
- Meta: `storage/indexes/lineitem_orderkey_idx_meta.txt`
- Layout: `struct { uint32_t start; uint32_t count; }` indexed by orderkey, 8 bytes per entry
- Usage: For each candidate orderkey, retrieve all lineitems to check EXISTS and NOT EXISTS conditions. Since lineitem is sorted by l_orderkey, the rows `[start, start+count)` are contiguous.
- Sentinel: `{0, 0}` = no rows

### orders_pk_idx (dense_pk on o_orderkey)
- File: `storage/indexes/orders_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by orderkey → row_id, sentinel -1
- Usage: Check `o_orderstatus[orders_pk_index[orderkey]] == 'F'`

### supplier_pk_idx (dense_pk on s_suppkey)
- File: `storage/indexes/supplier_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by suppkey → row_id, sentinel -1
- Usage: Look up s_name for final output
