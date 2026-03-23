# Q8 Guide — National Market Share

## Column Reference

### p_partkey (pk, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows)
- This query: `p_partkey = l_partkey` → join key

### p_type (category, int16_t, dictionary)
- File: `storage/part/p_type.bin` (2,000,000 rows), dict: `storage/part/p_type_dict.bin`
- This query: `p_type = 'ECONOMY ANODIZED STEEL'` → load dict, find matching code, then `p_type[i] == target_code`
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int16_t sequential from 0
- Selectivity: ~0.07% → ~1,400 matching parts

### r_regionkey (pk, int32_t, raw)
- File: `storage/region/r_regionkey.bin` (5 rows)
- This query: `n1.n_regionkey = r_regionkey` → filter to AMERICA region

### r_name (text, varlen string)
- Files: `storage/region/r_name_offsets.bin`, `storage/region/r_name_data.bin`
- This query: `r_name = 'AMERICA'` → find AMERICA regionkey

### n_nationkey (pk, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)
- This query: `c_nationkey = n1.n_nationkey`, `s_nationkey = n2.n_nationkey`

### n_name (text, varlen string)
- Files: `storage/nation/n_name_offsets.bin`, `storage/nation/n_name_data.bin`
- This query: `n2.n_name = 'BRAZIL'` for CASE expression (supplier nation)

### n_regionkey (fk, int32_t, raw)
- File: `storage/nation/n_regionkey.bin` (25 rows)
- This query: `n1.n_regionkey = r_regionkey` → filter customer to AMERICA region

### c_custkey (pk, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows)
- This query: `o_custkey = c_custkey` → join key

### c_nationkey (fk, int32_t, raw)
- File: `storage/customer/c_nationkey.bin` (1,500,000 rows)
- This query: `c_nationkey = n1.n_nationkey` → filter to AMERICA region

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows)
- This query: `l_orderkey = o_orderkey` → join key

### o_custkey (fk, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows)
- This query: `o_custkey = c_custkey` → join key

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows)
- This query: `o_orderdate BETWEEN '1995-01-01' AND '1996-12-31'`
  → C++ `o_orderdate[i] >= 9131 && o_orderdate[i] <= 9861`
- Also: `EXTRACT(YEAR FROM o_orderdate)` → derive year

### l_orderkey (fk, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: `l_orderkey = o_orderkey` → join key

### l_partkey (fk, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows)
- This query: `p_partkey = l_partkey` → join key

### l_suppkey (fk, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows)
- This query: `s_suppkey = l_suppkey` → join key

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows)
- This query: `l_extendedprice * (1 - l_discount)` → volume

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows)
- This query: `l_extendedprice * (1 - l_discount)` → volume

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows)
- This query: `s_suppkey = l_suppkey` → join key

### s_nationkey (fk, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows)
- This query: `s_nationkey = n2.n_nationkey` → determine if supplier is BRAZIL

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| part | 2,000,000 | Filtered dimension | p_partkey | 65536 |
| region | 5 | Dimension filter | r_regionkey | 65536 |
| nation | 25 | Dimension | n_nationkey | 65536 |
| customer | 1,500,000 | Dimension | c_custkey | 65536 |
| orders | 15,000,000 | Filtered fact | o_orderkey | 65536 |
| lineitem | 59,986,052 | Fact | l_orderkey | 65536 |
| supplier | 100,000 | Dimension | s_suppkey | 65536 |

## Query Analysis
- **Part filter**: p_type = 'ECONOMY ANODIZED STEEL' → ~0.07% → ~1,400 parts. Very selective — start here.
- **Date filter**: o_orderdate in [1995-01-01, 1996-12-31] → ~30% → ~4.5M orders.
- **Region filter**: AMERICA → ~20% of customers.
- **Aggregation**: GROUP BY o_year → 2 groups (1995, 1996). Compute SUM(CASE WHEN nation='BRAZIL' THEN volume ELSE 0) / SUM(volume).
- **Output**: ORDER BY o_year.

## Indexes

### part_p_partkey_lookup (dense_pk_array on p_partkey)
- File: `storage/indexes/part_p_partkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]` where `arr[partkey] = row_id`, `-1` if missing.
- Usage: Given l_partkey, check if the part has matching p_type.

### orders_o_orderkey_lookup (dense_pk_array on o_orderkey)
- File: `storage/indexes/orders_o_orderkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]` where `arr[orderkey] = row_id`, `-1` if missing.
- Usage: Given l_orderkey, lookup order → get o_custkey and o_orderdate.

### customer_c_custkey_lookup (dense_pk_array on c_custkey)
- File: `storage/indexes/customer_c_custkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]` where `arr[custkey] = row_id`, `-1` if missing.
- Usage: Given o_custkey, lookup customer → get c_nationkey for AMERICA check.

### supplier_s_suppkey_lookup (dense_pk_array on s_suppkey)
- File: `storage/indexes/supplier_s_suppkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]` where `arr[suppkey] = row_id`, `-1` if missing.
- Usage: Given l_suppkey, lookup supplier → get s_nationkey for BRAZIL check.

### orders_o_orderdate_zonemap (zone_map on o_orderdate)
- File: `storage/indexes/orders_o_orderdate_zonemap.bin`
- Layout: Header: `uint64_t num_blocks`, `uint32_t block_size` (=65536). Body: `int32_t[num_blocks*2]` (min, max pairs).

## Recommended Approach
1. Load part p_type dict, find code for 'ECONOMY ANODIZED STEEL'. Build bitset/set of qualifying partkeys.
2. Load region/nation. Find AMERICA regionkey, collect AMERICA nation nationkeys. Find BRAZIL nationkey.
3. Build customer nationkey array (or use c_custkey_lookup + c_nationkey).
4. Build supplier nationkey array (or use s_suppkey_lookup + s_nationkey).
5. Scan lineitem: for each row, check if l_partkey is qualifying part.
   - If yes: lookup order by l_orderkey → check date in [1995,1996]. Lookup customer → check AMERICA region.
   - If all pass: compute volume = l_extendedprice * (1 - l_discount). Check if supplier nation = BRAZIL.
   - Accumulate into year-bucket: brazil_volume[year] and total_volume[year].
6. Compute mkt_share = brazil_volume / total_volume for each year. Output sorted by o_year.
