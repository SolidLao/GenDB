# Q15 Guide — Top Supplier

## Column Reference

### l_suppkey (fk, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows)
- This query: GROUP BY l_suppkey (= supplier_no in CTE)

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows)
- This query: `l_shipdate >= '1996-01-01' AND l_shipdate < '1996-04-01'`
  → C++ `l_shipdate[i] >= 9497 && l_shipdate[i] < 9588`

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → total_revenue

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → total_revenue

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows)
- This query: `s_suppkey = supplier_no` → join key, output

### s_name (text, varlen string)
- Files: `storage/supplier/s_name_offsets.bin`, `storage/supplier/s_name_data.bin`
- This query: output

### s_address (text, varlen string)
- Files: `storage/supplier/s_address_offsets.bin`, `storage/supplier/s_address_data.bin`
- This query: output

### s_phone (text, varlen string)
- Files: `storage/supplier/s_phone_offsets.bin`, `storage/supplier/s_phone_data.bin`
- This query: output

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| lineitem | 59,986,052 | Filtered fact | l_orderkey | 65536 |
| supplier | 100,000 | Dimension | s_suppkey | 65536 |

## Query Analysis
- **CTE (revenue0)**: Filter lineitem by shipdate [1996-01-01, 1996-04-01) → ~3.7% → ~2.2M rows. GROUP BY l_suppkey → ~100K groups. SUM revenue per supplier.
- **Subquery**: Find MAX(total_revenue) from CTE.
- **Main**: Join supplier with CTE where total_revenue = max. Typically 1 supplier.
- **Output**: ORDER BY s_suppkey.

## Indexes

### lineitem_l_shipdate_zonemap (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout: Header: `uint64_t num_blocks`, `uint32_t block_size` (=65536). Body: `int32_t[num_blocks*2]` (min, max pairs).
- Usage: Skip blocks where `max < 9497 || min >= 9588`.

### supplier_s_suppkey_lookup (dense_pk_array on s_suppkey)
- File: `storage/indexes/supplier_s_suppkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[suppkey] = row_id`, `-1` if missing.
- Usage: For matching supplier(s), lookup row_id to get output fields.

## Recommended Approach
1. Allocate `double revenue[max_suppkey+1] = {0}` (100K entries, ~800KB).
2. Load shipdate zonemap. For qualifying blocks:
   - Scan l_shipdate for [9497, 9588). For qualifying rows:
   - `revenue[l_suppkey[i]] += l_extendedprice[i] * (1.0 - l_discount[i])`.
3. Find `max_revenue = max over all revenue[]` entries.
4. Scan revenue array: find all suppkeys where `revenue[suppkey] == max_revenue` (use epsilon for floating point: `fabs(revenue[sk] - max_revenue) < 1e-6` or exact comparison if using integer cents).
5. For each matching suppkey, lookup supplier via s_suppkey_lookup → get s_name, s_address, s_phone.
6. Output sorted by s_suppkey.
