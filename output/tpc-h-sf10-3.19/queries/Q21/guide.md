# Q21 Guide — Suppliers Who Kept Orders Waiting

## Column Reference

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows)
- This query: `s_suppkey = l1.l_suppkey` → join key

### s_name (text, varlen string)
- Files: `storage/supplier/s_name_offsets.bin`, `storage/supplier/s_name_data.bin`
- This query: GROUP BY s_name, ORDER BY, output

### s_nationkey (fk, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows)
- This query: `s_nationkey = n_nationkey` → filter to SAUDI ARABIA

### n_nationkey (pk, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)

### n_name (text, varlen string)
- Files: `storage/nation/n_name_offsets.bin`, `storage/nation/n_name_data.bin`
- This query: `n_name = 'SAUDI ARABIA'`

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows)
- This query: `o_orderkey = l1.l_orderkey` → join key

### o_orderstatus (flag, int8_t, char_as_byte)
- File: `storage/orders/o_orderstatus.bin` (15,000,000 rows)
- This query: `o_orderstatus = 'F'` → C++ `o_orderstatus[i] == 'F'` (== 70)
- Selectivity: ~49%

### l_orderkey (fk, int32_t, raw) — l1, l2, l3
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: `l1.l_orderkey = o_orderkey`, `l2.l_orderkey = l1.l_orderkey`, `l3.l_orderkey = l1.l_orderkey`

### l_suppkey (fk, int32_t, raw) — l1, l2, l3
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows)
- This query: `l1.l_suppkey = s_suppkey`, `l2.l_suppkey <> l1.l_suppkey`, `l3.l_suppkey <> l1.l_suppkey`

### l_commitdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_commitdate.bin` (59,986,052 rows)
- This query: `l1.l_receiptdate > l1.l_commitdate` and `l3.l_receiptdate > l3.l_commitdate`

### l_receiptdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_receiptdate.bin` (59,986,052 rows)
- This query: `l1.l_receiptdate > l1.l_commitdate` and `l3.l_receiptdate > l3.l_commitdate`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| supplier | 100,000 | Filtered dimension | s_suppkey | 65536 |
| nation | 25 | Dimension filter | n_nationkey | 65536 |
| orders | 15,000,000 | Filtered fact | o_orderkey | 65536 |
| lineitem | 59,986,052 | Self-joined fact | l_orderkey | 65536 |

## Query Analysis
- **Nation filter**: SAUDI ARABIA → ~4% → ~4,000 suppliers.
- **Order filter**: o_orderstatus = 'F' → ~49% → ~7.35M orders.
- **l1 condition**: l_receiptdate > l_commitdate (supplier was late) → ~62.5%.
- **EXISTS (l2)**: Another lineitem for same order with DIFFERENT suppkey → most multi-line orders qualify.
- **NOT EXISTS (l3)**: No OTHER supplier's lineitem for same order was late (receiptdate > commitdate).
- **Aggregation**: GROUP BY s_name → ~4,000 groups. COUNT(*).
- **Output**: ORDER BY numwait DESC, s_name ASC. LIMIT 100.

## Indexes

### lineitem_l_orderkey_grouped (sorted_grouped on l_orderkey)
- File: `storage/indexes/lineitem_l_orderkey_grouped.bin`
- Layout: Header: `uint64_t num_entries`. Body: `uint32_t[num_entries*2]` interleaved `[start, count, ...]`.
- Usage: For each order, lookup all its lineitem rows to check EXISTS/NOT EXISTS conditions.

### orders_o_orderkey_lookup (dense_pk_array on o_orderkey)
- File: `storage/indexes/orders_o_orderkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[orderkey] = row_id`, `-1` if missing.
- Usage: Lookup order row_id by l_orderkey → check o_orderstatus.

### supplier_s_suppkey_lookup (dense_pk_array on s_suppkey)
- File: `storage/indexes/supplier_s_suppkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[suppkey] = row_id`, `-1` if missing.
- Usage: Lookup supplier row_id for output fields (s_name).

## Recommended Approach
1. Load nation. Find SAUDI ARABIA nationkey. Scan supplier: build bitset/bool array of SA suppkeys.
2. Scan lineitem: for each row where `l_suppkey` is an SA supplier AND `l_receiptdate > l_commitdate`:
   - This is a candidate l1 row. Record (l_orderkey, l_suppkey).
3. For each candidate: check order status via orders_o_orderkey_lookup → `o_orderstatus == 'F'`.
4. For qualifying candidates: use lineitem_l_orderkey_grouped to get all lineitems for same order.
   - **EXISTS check (l2)**: Is there any lineitem with different suppkey? (usually yes for multi-line orders)
   - **NOT EXISTS check (l3)**: Is there NO other lineitem with different suppkey that is also late (receiptdate > commitdate)?
5. If both checks pass: increment `count[s_suppkey]++`.
6. For output: get s_name for each qualifying suppkey. Sort by (numwait DESC, s_name ASC). LIMIT 100.

### Optimization
Instead of scanning all 60M lineitems, consider:
- First identify orders with status 'F' (via orders scan or o_orderstatus array).
- Then for each F-order, use grouped index to get its lineitems. Check SA supplier, late delivery, EXISTS/NOT EXISTS.
