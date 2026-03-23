# Q5 Guide — Local Supplier Volume

## Column Reference

### r_regionkey (pk, int32_t, raw)
- File: `storage/region/r_regionkey.bin` (5 rows)
- This query: `n_regionkey = r_regionkey` → join key

### r_name (text, varlen string)
- Files: `storage/region/r_name_offsets.bin` (uint32_t[6]), `storage/region/r_name_data.bin`
- This query: `r_name = 'ASIA'` → find ASIA regionkey

### n_nationkey (pk, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)
- This query: `s_nationkey = n_nationkey`, `c_nationkey = s_nationkey` → join key

### n_name (text, varlen string)
- Files: `storage/nation/n_name_offsets.bin` (uint32_t[26]), `storage/nation/n_name_data.bin`
- This query: GROUP BY n_name (output column)

### n_regionkey (fk, int32_t, raw)
- File: `storage/nation/n_regionkey.bin` (25 rows)
- This query: `n_regionkey = r_regionkey` → filter to ASIA nations

### c_custkey (pk, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows)
- This query: `c_custkey = o_custkey` → join key

### c_nationkey (fk, int32_t, raw)
- File: `storage/customer/c_nationkey.bin` (1,500,000 rows)
- This query: `c_nationkey = s_nationkey` → locality constraint (customer and supplier same nation)

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows)
- This query: `l_orderkey = o_orderkey` → join key

### o_custkey (fk, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows)
- This query: `c_custkey = o_custkey` → join key

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows)
- This query: `o_orderdate >= '1994-01-01' AND o_orderdate < '1995-01-01'`
  → C++ `o_orderdate[i] >= 8766 && o_orderdate[i] < 9131`

### l_orderkey (fk, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: `l_orderkey = o_orderkey` → join key

### l_suppkey (fk, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows)
- This query: `l_suppkey = s_suppkey` → join key

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → revenue

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → revenue

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows)
- This query: `l_suppkey = s_suppkey` → join key

### s_nationkey (fk, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows)
- This query: `s_nationkey = n_nationkey` AND `c_nationkey = s_nationkey` → locality constraint

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| region | 5 | Dimension filter | r_regionkey | 65536 |
| nation | 25 | Dimension | n_nationkey | 65536 |
| customer | 1,500,000 | Dimension | c_custkey | 65536 |
| orders | 15,000,000 | Filtered fact | o_orderkey | 65536 |
| lineitem | 59,986,052 | Fact | l_orderkey | 65536 |
| supplier | 100,000 | Dimension | s_suppkey | 65536 |

## Query Analysis
- **Key constraint**: Customer and supplier must be in the SAME nation (c_nationkey = s_nationkey), and that nation must be in ASIA.
- **Date filter**: o_orderdate in [1994-01-01, 1995-01-01) → ~15% selectivity → ~2.25M orders.
- **Region filter**: r_name='ASIA' → 1/5 regions → ~5 ASIA nations → ~20% of customers/suppliers.
- **Aggregation**: GROUP BY n_name → 5 groups (ASIA nations), SUM(revenue).
- **Output**: ORDER BY revenue DESC.

## Indexes

### orders_o_orderdate_zonemap (zone_map on o_orderdate)
- File: `storage/indexes/orders_o_orderdate_zonemap.bin`
- Layout: Header: `uint64_t num_blocks`, `uint32_t block_size` (=65536). Body: `int32_t[num_blocks*2]` (min, max pairs).
- Usage: Skip blocks where `max < 8766 || min >= 9131`.

### customer_c_custkey_lookup (dense_pk_array on c_custkey)
- File: `storage/indexes/customer_c_custkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]` where `arr[custkey] = row_id`, `-1` if missing.
- Usage: For each order's o_custkey, lookup customer row_id → get c_nationkey.

### supplier_s_suppkey_lookup (dense_pk_array on s_suppkey)
- File: `storage/indexes/supplier_s_suppkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]` where `arr[suppkey] = row_id`, `-1` if missing.
- Usage: For each lineitem's l_suppkey, lookup supplier row_id → get s_nationkey.

### lineitem_l_orderkey_grouped (sorted_grouped on l_orderkey)
- File: `storage/indexes/lineitem_l_orderkey_grouped.bin`
- Layout: Header: `uint64_t num_entries`. Body: `uint32_t[num_entries*2]` interleaved `[start, count, ...]`.
- Usage: For each qualifying order, lookup its lineitem rows by o_orderkey.

## Recommended Approach
1. Load region/nation (tiny). Find ASIA regionkey, collect ASIA nationkeys. Build nationkey→n_name mapping.
2. Build supplier nationkey array: for each suppkey, s_nationkey (can use direct array since supplier sorted by s_suppkey, or load supplier_s_suppkey_lookup).
3. Scan orders with zonemap for date filter. For qualifying orders, lookup customer via c_custkey_lookup → get c_nationkey. Check if ASIA nation.
4. For qualifying ASIA-customer orders, use lineitem_l_orderkey_grouped to get lineitems.
5. For each lineitem, lookup supplier nationkey. Check c_nationkey == s_nationkey (same nation).
6. If match: accumulate `l_extendedprice * (1 - l_discount)` into that nation's revenue bucket.
7. Output 5 groups sorted by revenue DESC.
