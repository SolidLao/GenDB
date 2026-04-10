# Q10 Guide — Returned Item Reporting

## Column Reference

### c_custkey (PK, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`, GROUP BY, output

### c_name (string, varlen)
- File: `storage/customer/c_name.bin` (offsets, uint32_t[1500001]), `storage/customer/c_name_data.bin`
- This query: GROUP BY, output

### c_acctbal (decimal, double, raw)
- File: `storage/customer/c_acctbal.bin` (1,500,000 rows of double)
- This query: GROUP BY, output

### c_phone (string, varlen)
- File: `storage/customer/c_phone.bin` (offsets), `storage/customer/c_phone_data.bin`
- This query: GROUP BY, output

### c_address (string, varlen)
- File: `storage/customer/c_address.bin` (offsets), `storage/customer/c_address_data.bin`
- This query: GROUP BY, output

### c_comment (string, varlen)
- File: `storage/customer/c_comment.bin` (offsets), `storage/customer/c_comment_data.bin`
- This query: GROUP BY, output

### c_nationkey (FK, int32_t, raw)
- File: `storage/customer/c_nationkey.bin` (1,500,000 rows of int32_t)
- This query: join key `c_nationkey = n_nationkey`

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: join key `l_orderkey = o_orderkey`

### o_custkey (FK, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows of int32_t)
- This query: join key `c_custkey = o_custkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows of int32_t)
- This query: `o_orderdate >= DATE '1993-10-01' AND o_orderdate < DATE '1994-01-01'`
  → C++: `>= days_from_civil(1993, 10, 1) && < days_from_civil(1994, 1, 1)`
- Selectivity: ~0.039 (3 months)

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: join key `l_orderkey = o_orderkey`

### l_returnflag (char1, int8_t, raw_char)
- File: `storage/lineitem/l_returnflag.bin` (59,986,052 rows of int8_t)
- This query: `l_returnflag = 'R'` → C++: `l_returnflag[i] == 'R'`
- Selectivity: ~0.242

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `SUM(l_extendedprice * (1 - l_discount))`

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: revenue calculation

### n_nationkey (PK, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)

### n_name (string, uint8_t, dictionary)
- File: `storage/nation/n_name.bin` (25 rows of uint8_t)
- Dict: `storage/nation/n_name_dict.bin`
- This query: GROUP BY, output — decode nationkey → n_name via dict

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| customer | 1,500,000  | dimension | c_custkey  | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |
| nation   | 25         | dimension | n_nationkey| 65536      |

## Query Analysis
- **Pattern**: 4-way join, date filter on orders, returnflag filter on lineitem, group by customer, top-20
- **Strategy**:
  1. Scan orders with date filter (~0.039 → ~585K orders). Collect qualifying (orderkey, custkey) pairs.
  2. Build hash set of qualifying orderkeys, or build orderkey→custkey map.
  3. Scan lineitem: check `l_returnflag == 'R'` AND orderkey in qualifying set. Accumulate revenue per custkey.
  4. Top-20 by revenue DESC. For result rows, look up customer details via customer_pk_idx.
  5. Decode n_name from nation dict using customer's c_nationkey.
- **Group by optimization**: Since the effective group key is c_custkey (all other GROUP BY columns are functionally dependent on custkey), use custkey as the hash map key and look up other attributes only for top-20 results.

## Indexes

### orders_pk_idx (dense_pk on o_orderkey)
- File: `storage/indexes/orders_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by orderkey → row_id, sentinel -1

### customer_pk_idx (dense_pk on c_custkey)
- File: `storage/indexes/customer_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by custkey → row_id, sentinel -1
- Usage: Look up customer attributes for top-20 results

### orders_orderdate_zm (zone_map on o_orderdate)
- File: `storage/indexes/orders_o_orderdate_zonemap.bin`
- Layout: `struct { int32_t min_val; int32_t max_val; }` per block, 65536 rows/block
- Usage: Skip blocks outside the 3-month date window

### lineitem_orderkey_idx (dense_range on l_orderkey)
- File: `storage/indexes/lineitem_orderkey_idx.bin`
- Layout: `struct { uint32_t start; uint32_t count; }` indexed by orderkey
- Usage: For each qualifying order, find its lineitem rows

### nation_pk_idx (dense_pk on n_nationkey)
- File: `storage/indexes/nation_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by nationkey → row_id, sentinel -1
