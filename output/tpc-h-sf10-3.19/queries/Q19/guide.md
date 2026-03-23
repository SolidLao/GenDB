# Q19 Guide — Discounted Revenue

## Column Reference

### l_partkey (fk, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows)
- This query: `p_partkey = l_partkey` → join key

### l_quantity (measure, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows)
- This query: Range checks per OR branch: [1,11], [10,20], [20,30]

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows)
- This query: `SUM(l_extendedprice * (1 - l_discount))` → revenue

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows)
- This query: `l_extendedprice * (1 - l_discount)` → revenue

### l_shipmode (category, int8_t, dictionary)
- File: `storage/lineitem/l_shipmode.bin` (59,986,052 rows), dict: `storage/lineitem/l_shipmode_dict.bin`
- This query: `l_shipmode IN ('AIR', 'AIR REG')` → load dict, find codes for 'AIR' and 'AIR REG'
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0
- Selectivity: ~13% (~2/7 modes, but AIR REG is separate from AIR)

### l_shipinstruct (category, int8_t, dictionary)
- File: `storage/lineitem/l_shipinstruct.bin` (59,986,052 rows), dict: `storage/lineitem/l_shipinstruct_dict.bin`
- This query: `l_shipinstruct = 'DELIVER IN PERSON'` → load dict, find code
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0
- Selectivity: ~25%

### p_partkey (pk, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows)
- This query: `p_partkey = l_partkey` → join key

### p_brand (category, int8_t, dictionary)
- File: `storage/part/p_brand.bin` (2,000,000 rows), dict: `storage/part/p_brand_dict.bin`
- This query: `p_brand = 'Brand#12'` / `'Brand#23'` / `'Brand#34'` per OR branch
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0

### p_container (category, int8_t, dictionary)
- File: `storage/part/p_container.bin` (2,000,000 rows), dict: `storage/part/p_container_dict.bin`
- This query: Container sets per branch: {'SM CASE','SM BOX','SM PACK','SM PKG'}, {'MED BAG','MED BOX','MED PKG','MED PACK'}, {'LG CASE','LG BOX','LG PACK','LG PKG'}
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0

### p_size (attribute, int32_t, raw)
- File: `storage/part/p_size.bin` (2,000,000 rows)
- This query: `p_size BETWEEN 1 AND 5` / `1 AND 10` / `1 AND 15` per branch

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| lineitem | 59,986,052 | Fact | l_orderkey | 65536 |
| part | 2,000,000 | Dimension | p_partkey | 65536 |

## Query Analysis
- **Three OR branches**: Each has different brand, container set, quantity range, size range.
- **Common lineitem filters**: l_shipmode IN ('AIR','AIR REG') AND l_shipinstruct = 'DELIVER IN PERSON'. Combined: ~3.25%.
- **Per-branch part filters**: each ~4% brand × ~10% container × size range = very selective.
- **Aggregation**: Single SUM output — no GROUP BY.
- **Output**: Single value: revenue.

## Indexes

### part_p_partkey_lookup (dense_pk_array on p_partkey)
- File: `storage/indexes/part_p_partkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[partkey] = row_id`, `-1` if missing.
- Usage: Given l_partkey, lookup part row_id → check brand, container, size.

## Recommended Approach
1. Load dicts: l_shipmode, l_shipinstruct, p_brand, p_container. Find relevant codes.
2. Precompute per-partkey: which OR branch (if any) applies. Build arrays indexed by partkey:
   - `branch[partkey]` = 0 (none), 1, 2, or 3 depending on brand + container + size match.
   - Scan part table once to build this. Store quantity range per branch.
3. Scan lineitem:
   - Check `l_shipinstruct == deliver_in_person_code && (l_shipmode == air_code || l_shipmode == airreg_code)`.
   - If passes: lookup `branch[l_partkey]`. If non-zero:
     - Check quantity range for that branch.
     - If passes: accumulate `l_extendedprice * (1 - l_discount)`.
4. Output single SUM value.

### Branch quantity ranges:
- Branch 1 (Brand#12, SM containers, size 1-5): `l_quantity >= 1 && l_quantity <= 11`
- Branch 2 (Brand#23, MED containers, size 1-10): `l_quantity >= 10 && l_quantity <= 20`
- Branch 3 (Brand#34, LG containers, size 1-15): `l_quantity >= 20 && l_quantity <= 30`
