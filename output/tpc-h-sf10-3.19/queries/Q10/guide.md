# Q10 Guide — Returned Item Reporting

## Column Reference

### c_custkey (pk, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows)
- This query: `c_custkey = o_custkey` → join key, GROUP BY, output

### c_name (text, varlen string)
- Files: `storage/customer/c_name_offsets.bin` (uint32_t[1500001]), `storage/customer/c_name_data.bin`
- This query: GROUP BY, output

### c_acctbal (measure, double, raw)
- File: `storage/customer/c_acctbal.bin` (1,500,000 rows)
- This query: GROUP BY, output

### c_address (text, varlen string)
- Files: `storage/customer/c_address_offsets.bin`, `storage/customer/c_address_data.bin`
- This query: GROUP BY, output

### c_phone (text, varlen string)
- Files: `storage/customer/c_phone_offsets.bin`, `storage/customer/c_phone_data.bin`
- This query: GROUP BY, output

### c_comment (text, varlen string)
- Files: `storage/customer/c_comment_offsets.bin`, `storage/customer/c_comment_data.bin`
- This query: GROUP BY, output

### c_nationkey (fk, int32_t, raw)
- File: `storage/customer/c_nationkey.bin` (1,500,000 rows)
- This query: `c_nationkey = n_nationkey` → join key

### n_nationkey (pk, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)
- This query: `c_nationkey = n_nationkey` → join key

### n_name (text, varlen string)
- Files: `storage/nation/n_name_offsets.bin`, `storage/nation/n_name_data.bin`
- This query: GROUP BY, output

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows)
- This query: `l_orderkey = o_orderkey` → join key

### o_custkey (fk, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows)
- This query: `c_custkey = o_custkey` → join key

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows)
- This query: `o_orderdate >= '1993-10-01' AND o_orderdate < '1994-01-01'`
  → C++ `o_orderdate[i] >= 8674 && o_orderdate[i] < 8766`

### l_orderkey (fk, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: `l_orderkey = o_orderkey` → join key

### l_returnflag (flag, int8_t, char_as_byte)
- File: `storage/lineitem/l_returnflag.bin` (59,986,052 rows)
- This query: `l_returnflag = 'R'` → C++ `l_returnflag[i] == 'R'` (== 82)
- Selectivity: ~24%

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → revenue

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → revenue

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| customer | 1,500,000 | Dimension | c_custkey | 65536 |
| nation | 25 | Dimension | n_nationkey | 65536 |
| orders | 15,000,000 | Filtered fact | o_orderkey | 65536 |
| lineitem | 59,986,052 | Filtered fact | l_orderkey | 65536 |

## Query Analysis
- **Date filter**: o_orderdate in [1993-10-01, 1994-01-01) → ~3.8% → ~570K orders.
- **Returnflag filter**: l_returnflag = 'R' → ~24% of lineitems.
- **Aggregation**: GROUP BY c_custkey (effectively) → up to ~570K groups (but fewer customers have returns in period). SUM(l_extendedprice * (1 - l_discount)).
- **Output**: ORDER BY revenue DESC, LIMIT 20. Top-20 via partial sort or priority queue.

## Indexes

### orders_o_orderdate_zonemap (zone_map on o_orderdate)
- File: `storage/indexes/orders_o_orderdate_zonemap.bin`
- Layout: Header: `uint64_t num_blocks`, `uint32_t block_size` (=65536). Body: `int32_t[num_blocks*2]` (min, max pairs).
- Usage: Skip blocks where `max < 8674 || min >= 8766`.

### lineitem_l_orderkey_grouped (sorted_grouped on l_orderkey)
- File: `storage/indexes/lineitem_l_orderkey_grouped.bin`
- Layout: Header: `uint64_t num_entries`. Body: `uint32_t[num_entries*2]` interleaved `[start, count, ...]`.
- Usage: For each qualifying order, lookup its lineitem rows.

### customer_c_custkey_lookup (dense_pk_array on c_custkey)
- File: `storage/indexes/customer_c_custkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[custkey] = row_id`, `-1` if missing.
- Usage: Lookup customer row_id by o_custkey for nationkey and output fields.

## Recommended Approach
1. Load nation table (25 rows). Build nationkey→n_name mapping.
2. Scan orders with zonemap for date filter [8674, 8766). Collect qualifying (orderkey, custkey) pairs.
3. For each qualifying order, use lineitem_l_orderkey_grouped to get lineitems.
4. Filter lineitems: `l_returnflag == 'R'`. Accumulate `l_extendedprice * (1 - l_discount)` keyed by c_custkey.
5. After aggregation, find top-20 by revenue. For each, lookup customer data via c_custkey_lookup for output fields (c_name, c_acctbal, c_address, c_phone, c_comment) and n_name via c_nationkey.
6. Output sorted by revenue DESC, LIMIT 20.
