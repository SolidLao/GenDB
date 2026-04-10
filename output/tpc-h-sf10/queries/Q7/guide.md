# Q7 Guide — Volume Shipping

## Column Reference

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: join key `s_suppkey = l_suppkey`

### s_nationkey (FK, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows of int32_t)
- This query: `s_nationkey = n1.n_nationkey` — identifies supplier's nation

### l_suppkey (FK, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows of int32_t)
- This query: join key `s_suppkey = l_suppkey`

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: join key `o_orderkey = l_orderkey`

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows of int32_t)
- This query: `l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'`
  → C++: `l_shipdate[i] >= days_from_civil(1995, 1, 1) && l_shipdate[i] <= days_from_civil(1996, 12, 31)`
- Also: `EXTRACT(YEAR FROM l_shipdate)` → compute year from days_since_epoch

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `l_extendedprice * (1 - l_discount)` = volume

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: volume calculation

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: join key `o_orderkey = l_orderkey`

### o_custkey (FK, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`

### c_custkey (PK, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`

### c_nationkey (FK, int32_t, raw)
- File: `storage/customer/c_nationkey.bin` (1,500,000 rows of int32_t)
- This query: `c_nationkey = n2.n_nationkey` — identifies customer's nation

### n_nationkey (PK, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows of int32_t)
- This query: join keys for both n1 and n2

### n_name (string, uint8_t, dictionary)
- File: `storage/nation/n_name.bin` (25 rows of uint8_t)
- Dict: `storage/nation/n_name_dict.bin`
- This query: `n_name IN ('FRANCE', 'GERMANY')` — load dict, find codes for FRANCE and GERMANY. Filter: `(supp_nation == FRANCE_key && cust_nation == GERMANY_key) || (supp_nation == GERMANY_key && cust_nation == FRANCE_key)`

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| supplier | 100,000    | dimension | s_suppkey  | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey | 65536      |
| customer | 1,500,000  | dimension | c_custkey  | 65536      |
| nation   | 25         | dimension | n_nationkey| 65536      |

## Query Analysis
- **Pattern**: 5-way join (plus nation used twice), date filter, nation pair filter, group by (supp_nation, cust_nation, year)
- **Strategy**:
  1. Find FRANCE and GERMANY nationkeys from nation dict
  2. Build `supp_nationkey[]` dense array (100K entries): `suppkey → nationkey`. Mark suppliers whose nationkey is FRANCE or GERMANY.
  3. Build `cust_nationkey[]` dense array (1.5M entries): `custkey → nationkey`. Mark customers whose nationkey is FRANCE or GERMANY.
  4. Build `orderkey → custkey` lookup using orders (scan o_custkey, o_orderkey)
  5. Scan lineitem with date filter (`l_shipdate BETWEEN 1995-01-01 AND 1996-12-31`):
     - Get supp_nation from `supp_nationkey[l_suppkey]`
     - Get cust_nation from `cust_nationkey[orderkey_to_custkey[l_orderkey]]`
     - Check nation pair filter
     - Extract year from l_shipdate, accumulate revenue by (supp_nation, cust_nation, year)
  6. Only ~4 groups: (FRANCE,GERMANY,1995), (FRANCE,GERMANY,1996), (GERMANY,FRANCE,1995), (GERMANY,FRANCE,1996)
- **Year extraction**: From days_since_epoch, use civil_from_days() inverse or store a precomputed lookup

## Indexes

### orders_pk_idx (dense_pk on o_orderkey)
- File: `storage/indexes/orders_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by orderkey → row_id, sentinel -1
- Usage: Look up o_custkey for each l_orderkey

### supplier_pk_idx (dense_pk on s_suppkey)
- File: `storage/indexes/supplier_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by suppkey → row_id, sentinel -1

### lineitem_shipdate_zm (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout: `struct { int32_t min_val; int32_t max_val; }` per block, 65536 rows/block
- Usage: Skip blocks outside the 2-year date range (1995-01-01 to 1996-12-31)
