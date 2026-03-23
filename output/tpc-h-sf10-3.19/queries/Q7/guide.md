# Q7 Guide — Volume Shipping

## Column Reference

### n_nationkey (pk, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)
- This query: `s_nationkey = n1.n_nationkey`, `c_nationkey = n2.n_nationkey` → join keys

### n_name (text, varlen string)
- Files: `storage/nation/n_name_offsets.bin` (uint32_t[26]), `storage/nation/n_name_data.bin`
- This query: Filter `(n1='FRANCE' AND n2='GERMANY') OR (n1='GERMANY' AND n2='FRANCE')`, output as supp_nation/cust_nation

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows)
- This query: `s_suppkey = l_suppkey` → join key

### s_nationkey (fk, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows)
- This query: `s_nationkey = n1.n_nationkey` → supplier nation

### l_orderkey (fk, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: `o_orderkey = l_orderkey` → join key

### l_suppkey (fk, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows)
- This query: `s_suppkey = l_suppkey` → join key

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows)
- This query: `l_shipdate BETWEEN '1995-01-01' AND '1996-12-31'`
  → C++ `l_shipdate[i] >= 9131 && l_shipdate[i] <= 9861`
- Also: `EXTRACT(YEAR FROM l_shipdate)` → derive year from days_since_epoch

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows)
- This query: `l_extendedprice * (1 - l_discount)` → volume

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows)
- This query: `l_extendedprice * (1 - l_discount)` → volume

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows)
- This query: `o_orderkey = l_orderkey` → join key

### o_custkey (fk, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15,000,000 rows)
- This query: `c_custkey = o_custkey` → join key

### c_custkey (pk, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1,500,000 rows)
- This query: `c_custkey = o_custkey` → join key

### c_nationkey (fk, int32_t, raw)
- File: `storage/customer/c_nationkey.bin` (1,500,000 rows)
- This query: `c_nationkey = n2.n_nationkey` → customer nation

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| nation | 25 | Dimension filter | n_nationkey | 65536 |
| supplier | 100,000 | Dimension | s_suppkey | 65536 |
| lineitem | 59,986,052 | Fact (scanned) | l_orderkey | 65536 |
| orders | 15,000,000 | Dimension | o_orderkey | 65536 |
| customer | 1,500,000 | Dimension | c_custkey | 65536 |

## Query Analysis
- **Nation filter**: Only FRANCE↔GERMANY pairs → 2 nationkeys. ~4% of suppliers per nation, ~4% of customers.
- **Date filter**: l_shipdate in [1995-01-01, 1996-12-31] → ~29% selectivity → ~17.4M lineitems.
- **Year extraction**: From days_since_epoch, compute year (1995 or 1996).
- **Aggregation**: GROUP BY (supp_nation, cust_nation, l_year) → 4 groups max (2 nation pairs × 2 years).
- **Output**: ORDER BY supp_nation, cust_nation, l_year.

## Indexes

### lineitem_l_shipdate_zonemap (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout: Header: `uint64_t num_blocks`, `uint32_t block_size` (=65536). Body: `int32_t[num_blocks*2]` (min, max pairs).
- Usage: Skip blocks where `max < 9131 || min > 9861`.

### supplier_s_suppkey_lookup (dense_pk_array on s_suppkey)
- File: `storage/indexes/supplier_s_suppkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]` where `arr[suppkey] = row_id`, `-1` if missing.
- Usage: Lookup supplier row_id by l_suppkey → get s_nationkey.

### orders_o_orderkey_lookup (dense_pk_array on o_orderkey)
- File: `storage/indexes/orders_o_orderkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]` where `arr[orderkey] = row_id`, `-1` if missing.
- Usage: Lookup order row_id by l_orderkey → get o_custkey.

### customer_c_custkey_lookup (dense_pk_array on c_custkey)
- File: `storage/indexes/customer_c_custkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]` where `arr[custkey] = row_id`, `-1` if missing.
- Usage: Lookup customer row_id by o_custkey → get c_nationkey.

## Recommended Approach
1. Load nation table. Find nationkeys for FRANCE and GERMANY. Let `nk_france`, `nk_germany`.
2. Build supplier nationkey array: `supp_nation[suppkey] = s_nationkey[supplier_lookup[suppkey]]` or directly since supplier is sorted by s_suppkey (suppkey = row_id + 1 typically, but use the lookup for safety).
3. Build customer nationkey array: `cust_nation[custkey] = c_nationkey[customer_lookup[custkey]]`.
4. Precompute `orders_custkey` array for O(1) access by orderkey via orders_o_orderkey_lookup.
5. Scan lineitem with shipdate zonemap. For qualifying lineitems (date in range):
   a. Get supp_nationkey from l_suppkey. Check if FRANCE or GERMANY.
   b. Get o_custkey from l_orderkey via orders lookup. Get cust_nationkey via customer lookup.
   c. Check if valid pair: (supp=FRANCE,cust=GERMANY) or (supp=GERMANY,cust=FRANCE).
   d. If match: compute year from l_shipdate, accumulate volume into (supp_nation, cust_nation, year) bucket.
6. Output 4 groups sorted by supp_nation, cust_nation, l_year.

### Year from days_since_epoch
To extract year from days_since_epoch, use civil_from_days or approximate:
```cpp
// Howard Hinnant's algorithm (inverse of parse_date)
int days_to_year(int32_t days) {
    days += 719468;
    int era = (days >= 0 ? days : days - 146096) / 146097;
    unsigned doe = (unsigned)(days - era * 146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    return (int)yoe + era * 400;
}
```
Or for just 1995/1996: check if `days >= 9131 && days <= 9495` → 1995, else 1996 (since range is [9131, 9861] and 1996-01-01 = 9496).
