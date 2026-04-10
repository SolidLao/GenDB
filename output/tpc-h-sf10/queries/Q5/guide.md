# Q5 Guide — Local Supplier Volume

## Column Reference

### c_custkey (PK, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`

### c_nationkey (FK, int32_t, raw)
- File: `storage/customer/c_nationkey.bin` (1,500,000 rows of int32_t)
- This query: join key `c_nationkey = s_nationkey` (local supplier constraint)

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: join key `l_orderkey = o_orderkey`

### o_custkey (FK, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows of int32_t)
- This query: `o_orderdate >= DATE '1994-01-01' AND o_orderdate < DATE '1995-01-01'`
  → C++: `o_orderdate[i] >= days_from_civil(1994, 1, 1) && o_orderdate[i] < days_from_civil(1995, 1, 1)`
- Selectivity: ~0.157 (1 year)

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: join key `l_orderkey = o_orderkey`

### l_suppkey (FK, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows of int32_t)
- This query: join key `l_suppkey = s_suppkey`, also `c_nationkey = s_nationkey`

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `SUM(l_extendedprice * (1 - l_discount))`

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: revenue calculation

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: join key `l_suppkey = s_suppkey`

### s_nationkey (FK, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows of int32_t)
- This query: `c_nationkey = s_nationkey` (local constraint), `s_nationkey = n_nationkey`

### n_nationkey (PK, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows of int32_t)
- This query: join key

### n_name (string, uint8_t, dictionary)
- File: `storage/nation/n_name.bin` (25 rows of uint8_t)
- Dict: `storage/nation/n_name_dict.bin`
- This query: GROUP BY, output — decode dict codes to strings for final results

### n_regionkey (FK, int32_t, raw)
- File: `storage/nation/n_regionkey.bin` (25 rows of int32_t)
- This query: join key `n_regionkey = r_regionkey`

### r_regionkey (PK, int32_t, raw)
- File: `storage/region/r_regionkey.bin` (5 rows of int32_t)

### r_name (string, uint8_t, dictionary)
- File: `storage/region/r_name.bin` (5 rows of uint8_t)
- Dict: `storage/region/r_name_dict.bin`
- This query: `r_name = 'ASIA'` → load dict, find ASIA code

## Table Stats
| Table    | Rows       | Role      | Sort Order              | Block Size |
|----------|------------|-----------|-------------------------|------------|
| customer | 1,500,000  | dimension | c_custkey               | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey              | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey              | 65536      |
| supplier | 100,000    | dimension | s_suppkey               | 65536      |
| nation   | 25         | dimension | n_nationkey             | 65536      |
| region   | 5          | dimension | r_regionkey             | 65536      |

## Query Analysis
- **Pattern**: 6-way join with region filter and "local supplier" constraint (c_nationkey = s_nationkey)
- **Strategy**:
  1. Build Asian nations set: find ASIA region code → collect nationkeys where `n_regionkey == asia_regionkey`
  2. Build suppkey→nationkey lookup: dense array `supp_nationkey[suppkey]` for suppliers in Asian nations (max_suppkey=100,000)
  3. Build custkey→nationkey lookup: dense array `cust_nationkey[custkey]` for customers in Asian nations (max_custkey=1,500,000)
  4. Scan orders: filter by date range (~0.157), check `cust_nationkey[o_custkey]` is a valid Asian nation → collect qualifying orderkeys with their customer nationkeys
  5. Scan lineitem for qualifying orders: check `supp_nationkey[l_suppkey] == customer_nationkey` (local supplier constraint), accumulate revenue by nationkey
  6. Output: 5 groups (Asian nations), ORDER BY revenue DESC
- **Estimated output**: ~5 rows

## Indexes

### orders_pk_idx (dense_pk on o_orderkey)
- File: `storage/indexes/orders_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by orderkey → row_id, sentinel -1

### supplier_pk_idx (dense_pk on s_suppkey)
- File: `storage/indexes/supplier_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by suppkey → row_id, sentinel -1
- Usage: Build suppkey→nationkey dense array directly from s_nationkey column (since supplier is sorted by s_suppkey and PK = row position starting from 1)

### orders_orderdate_zm (zone_map on o_orderdate)
- File: `storage/indexes/orders_o_orderdate_zonemap.bin`
- Layout: `struct { int32_t min_val; int32_t max_val; }` per block, 65536 rows/block
- Usage: Skip blocks outside the 1-year date range

### lineitem_orderkey_idx (dense_range on l_orderkey)
- File: `storage/indexes/lineitem_orderkey_idx.bin`
- Layout: `struct { uint32_t start; uint32_t count; }` indexed by orderkey
- Usage: For each qualifying order, find lineitem rows directly
