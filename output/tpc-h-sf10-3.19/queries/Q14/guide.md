# Q14 Guide — Promotion Effect

## Column Reference

### l_partkey (fk, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows)
- This query: `l_partkey = p_partkey` → join key

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows)
- This query: `l_shipdate >= '1995-09-01' AND l_shipdate < '1995-10-01'`
  → C++ `l_shipdate[i] >= 9374 && l_shipdate[i] < 9404`

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows)
- This query: `l_extendedprice * (1 - l_discount)` → revenue

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows)
- This query: `l_extendedprice * (1 - l_discount)` → revenue

### p_partkey (pk, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows)
- This query: `l_partkey = p_partkey` → join key

### p_type (category, int16_t, dictionary)
- File: `storage/part/p_type.bin` (2,000,000 rows), dict: `storage/part/p_type_dict.bin`
- This query: `p_type LIKE 'PROMO%'` → load dict, find all codes where string starts with "PROMO"
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int16_t sequential from 0

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| lineitem | 59,986,052 | Filtered fact | l_orderkey | 65536 |
| part | 2,000,000 | Dimension | p_partkey | 65536 |

## Query Analysis
- **Date filter**: l_shipdate in [1995-09-01, 1995-10-01) → ~1.2% → ~720K lineitems.
- **No GROUP BY**: Single-row output: `100 * promo_revenue / total_revenue`.
- **Type check**: For each date-qualifying lineitem, lookup part type → check if PROMO%.

## Indexes

### lineitem_l_shipdate_zonemap (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout: Header: `uint64_t num_blocks`, `uint32_t block_size` (=65536). Body: `int32_t[num_blocks*2]` (min, max pairs).
- Usage: Skip blocks where `max < 9374 || min >= 9404`.

### part_p_partkey_lookup (dense_pk_array on p_partkey)
- File: `storage/indexes/part_p_partkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[partkey] = row_id`, `-1` if missing.
- Usage: Given l_partkey, lookup part row_id → get p_type.

## Recommended Approach
1. Load p_type dict. Build bitset/set of codes starting with "PROMO". Build array: for each partkey, whether it's a promo part (bool array indexed by partkey, using part_p_partkey_lookup or direct since part is sorted by p_partkey).
2. Precompute `is_promo[partkey]` bool array for O(1) lookup.
3. Load shipdate zonemap. For qualifying blocks:
   - Scan l_shipdate for date range [9374, 9404).
   - For qualifying rows: compute `revenue = l_extendedprice * (1 - l_discount)`.
   - Lookup `is_promo[l_partkey[i]]`. Accumulate promo_sum and total_sum.
4. Output: `100.0 * promo_sum / total_sum`.
