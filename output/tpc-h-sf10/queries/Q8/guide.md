# Q8 Guide — National Market Share

## Column Reference

### p_partkey (PK, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows of int32_t)
- This query: join key `p_partkey = l_partkey`

### p_type (string, uint8_t, dictionary)
- File: `storage/part/p_type.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_type_dict.bin`
- This query: `p_type = 'ECONOMY ANODIZED STEEL'` → load dict, find matching code
- Selectivity: ~0.007

### l_partkey (FK, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows of int32_t)
- This query: join key `p_partkey = l_partkey`

### l_suppkey (FK, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows of int32_t)
- This query: join key `s_suppkey = l_suppkey`

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: join key `l_orderkey = o_orderkey`

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: volume = `l_extendedprice * (1 - l_discount)`

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: volume calculation

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: join key `l_orderkey = o_orderkey`

### o_custkey (FK, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows of int32_t)
- This query: join key `o_custkey = c_custkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows of int32_t)
- This query: `o_orderdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'`
  → C++: `>= days_from_civil(1995,1,1) && <= days_from_civil(1996,12,31)`
- Also: `EXTRACT(YEAR FROM o_orderdate)` for grouping

### c_custkey (PK, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows of int32_t)
- This query: join key `o_custkey = c_custkey`

### c_nationkey (FK, int32_t, raw)
- File: `storage/customer/c_nationkey.bin` (1,500,000 rows of int32_t)
- This query: `c_nationkey = n1.n_nationkey` → filter to AMERICA region customers

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: join key `s_suppkey = l_suppkey`

### s_nationkey (FK, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows of int32_t)
- This query: `s_nationkey = n2.n_nationkey` → identify if supplier nation is BRAZIL

### n_nationkey (PK, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)

### n_name (string, uint8_t, dictionary)
- File: `storage/nation/n_name.bin` (25 rows of uint8_t)
- Dict: `storage/nation/n_name_dict.bin`
- This query: identify BRAZIL nationkey for CASE WHEN

### n_regionkey (FK, int32_t, raw)
- File: `storage/nation/n_regionkey.bin` (25 rows of int32_t)
- This query: `n_regionkey = r_regionkey` → filter AMERICA region

### r_regionkey (PK, int32_t, raw)
- File: `storage/region/r_regionkey.bin` (5 rows)

### r_name (string, uint8_t, dictionary)
- File: `storage/region/r_name.bin` (5 rows of uint8_t)
- Dict: `storage/region/r_name_dict.bin`
- This query: `r_name = 'AMERICA'`

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| part     | 2,000,000  | dimension | p_partkey  | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey | 65536      |
| customer | 1,500,000  | dimension | c_custkey  | 65536      |
| supplier | 100,000    | dimension | s_suppkey  | 65536      |
| nation   | 25         | dimension | n_nationkey| 65536      |
| region   | 5          | dimension | r_regionkey| 65536      |

## Query Analysis
- **Pattern**: 8-way join, highly selective part filter, date range, region filter on customer, nation check on supplier
- **Strategy**:
  1. Find AMERICA region nationkeys and BRAZIL nationkey from dimension tables
  2. Build bitset of AMERICA-region customer custkeys (scan customer.c_nationkey)
  3. Build `supp_is_brazil[]` dense array: `suppkey → bool` (100K entries)
  4. Build bitset/set of qualifying partkeys where `p_type == 'ECONOMY ANODIZED STEEL'` (~0.007 * 2M = ~14K parts)
  5. Scan orders with date filter (1995-2 years), check customer is in AMERICA → collect qualifying orderkeys
  6. Scan lineitem: check orderkey qualifies, check partkey in qualifying set, get supp_is_brazil. Accumulate per year: total_volume and brazil_volume
  7. Output: `mkt_share = brazil_volume / total_volume` per year, ~2 rows

## Indexes

### part_pk_idx (dense_pk on p_partkey)
- File: `storage/indexes/part_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by partkey → row_id, sentinel -1

### orders_pk_idx (dense_pk on o_orderkey)
- File: `storage/indexes/orders_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by orderkey → row_id, sentinel -1

### supplier_pk_idx (dense_pk on s_suppkey)
- File: `storage/indexes/supplier_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by suppkey → row_id, sentinel -1

### orders_orderdate_zm (zone_map on o_orderdate)
- File: `storage/indexes/orders_o_orderdate_zonemap.bin`
- Usage: Skip blocks outside 1995-01-01 to 1996-12-31
