# Q14 Guide — Promotion Effect

## Column Reference

### l_partkey (FK, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows of int32_t)
- This query: join key `l_partkey = p_partkey`

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows of int32_t)
- This query: `l_shipdate >= DATE '1995-09-01' AND l_shipdate < DATE '1995-10-01'`
  → C++: `>= days_from_civil(1995, 9, 1) && < days_from_civil(1995, 10, 1)`
- Selectivity: ~1/76 ≈ 0.013 (1 month out of ~76 months)

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `l_extendedprice * (1 - l_discount)` — both in CASE and denominator

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: revenue calculation

### p_partkey (PK, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows of int32_t)
- This query: join key `l_partkey = p_partkey`

### p_type (string, uint8_t, dictionary)
- File: `storage/part/p_type.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_type_dict.bin`
- This query: `p_type LIKE 'PROMO%'` → load dict, build boolean array `is_promo[code]` for all codes where string starts with "PROMO"

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |
| part     | 2,000,000  | dimension | p_partkey  | 65536      |

## Query Analysis
- **Pattern**: 2-way join (lineitem → part), date filter, CASE aggregation, scalar output
- **Strategy**:
  1. Pre-build `is_promo[]` boolean array indexed by p_type dict code
  2. Pre-build `partkey_type[]` dense array: `partkey → p_type code` (2M entries of uint8_t, ~2MB). Since part is sorted by p_partkey (PK starting from 1), can read p_type.bin directly indexed by partkey-1.
  3. Scan lineitem with zone map to find blocks in the 1-month date range
  4. For qualifying rows: compute `revenue = l_extendedprice * (1 - l_discount)`
     - `total_revenue += revenue`
     - Look up part type: `type_code = partkey_type[l_partkey]`
     - If `is_promo[type_code]`: `promo_revenue += revenue`
  5. Output: `100.0 * promo_revenue / total_revenue`

## Indexes

### lineitem_shipdate_zm (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout: `struct { int32_t min_val; int32_t max_val; }` per block, 65536 rows/block
- Usage: Skip blocks entirely outside the 1-month window. Only ~1.3% of rows qualify, so zone map should skip ~98% of blocks.

### part_pk_idx (dense_pk on p_partkey)
- File: `storage/indexes/part_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by partkey → row_id, sentinel -1
- Usage: Alternative to dense array approach — look up p_type for each partkey
