# Q15 Guide — Top Supplier

## Column Reference

### l_suppkey (FK, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows of int32_t)
- This query: GROUP BY (as supplier_no in CTE)

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows of int32_t)
- This query: `l_shipdate >= DATE '1996-01-01' AND l_shipdate < DATE '1996-04-01'`
  → C++: `>= days_from_civil(1996, 1, 1) && < days_from_civil(1996, 4, 1)`
- Selectivity: ~3/76 ≈ 0.039 (3 months)

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `SUM(l_extendedprice * (1 - l_discount))` = total_revenue per supplier

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: revenue calculation

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: join key `s_suppkey = supplier_no`, output

### s_name (string, varlen)
- File: `storage/supplier/s_name.bin` (offsets), `storage/supplier/s_name_data.bin`
- This query: output

### s_address (string, varlen)
- File: `storage/supplier/s_address.bin` (offsets), `storage/supplier/s_address_data.bin`
- This query: output

### s_phone (string, varlen)
- File: `storage/supplier/s_phone.bin` (offsets), `storage/supplier/s_phone_data.bin`
- This query: output

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |
| supplier | 100,000    | dimension | s_suppkey  | 65536      |

## Query Analysis
- **Pattern**: CTE aggregation (revenue per suppkey), find MAX, join back to supplier
- **Strategy**:
  1. Scan lineitem with date filter (use zone map). For qualifying rows, accumulate `revenue[l_suppkey] += l_extendedprice * (1 - l_discount)`.
     Use dense array: `double revenue[max_suppkey+1]` initialized to 0.0 (100K * 8 = 800KB).
  2. Find MAX(revenue) across all suppkeys
  3. Find all suppkeys where `revenue[suppkey] == max_revenue`
  4. For matching suppkeys, look up supplier attributes via supplier_pk_idx
  5. Output: ORDER BY s_suppkey (typically 1 row)

## Indexes

### lineitem_shipdate_zm (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout: `struct { int32_t min_val; int32_t max_val; }` per block, 65536 rows/block
- Usage: Skip blocks outside the 3-month date range (~96% of blocks skipped)

### supplier_pk_idx (dense_pk on s_suppkey)
- File: `storage/indexes/supplier_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by suppkey → row_id, sentinel -1
- Usage: Look up supplier attributes for the winning supplier(s)
