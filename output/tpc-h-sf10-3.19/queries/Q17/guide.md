# Q17 Guide — Small-Quantity-Order Revenue

## Column Reference

### p_partkey (pk, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows)
- This query: `p_partkey = l_partkey` → join key; also correlated subquery join

### p_brand (category, int8_t, dictionary)
- File: `storage/part/p_brand.bin` (2,000,000 rows), dict: `storage/part/p_brand_dict.bin`
- This query: `p_brand = 'Brand#23'` → load dict, find code, filter
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0
- Selectivity: ~4% → ~80K parts

### p_container (category, int8_t, dictionary)
- File: `storage/part/p_container.bin` (2,000,000 rows), dict: `storage/part/p_container_dict.bin`
- This query: `p_container = 'MED BOX'` → load dict, find code, filter
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0
- Selectivity: ~2.5% → ~50K parts. Combined with brand: ~0.1% → ~2K parts.

### l_partkey (fk, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows)
- This query: `p_partkey = l_partkey` → join key

### l_quantity (measure, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows)
- This query: `l_quantity < 0.2 * AVG(l_quantity)` per part (correlated subquery)

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows)
- This query: `SUM(l_extendedprice) / 7.0` → avg_yearly output

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| part | 2,000,000 | Filtered dimension | p_partkey | 65536 |
| lineitem | 59,986,052 | Fact | l_orderkey | 65536 |

## Query Analysis
- **Part filter**: p_brand='Brand#23' AND p_container='MED BOX' → ~0.1% → ~2K qualifying parts.
- **Correlated subquery**: For each qualifying part, compute AVG(l_quantity) across all its lineitems, then sum l_extendedprice for lineitems where l_quantity < 0.2 * avg.
- **Output**: Single value: SUM(l_extendedprice) / 7.0.

## Indexes

### part_p_partkey_lookup (dense_pk_array on p_partkey)
- File: `storage/indexes/part_p_partkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[partkey] = row_id`, `-1` if missing.
- Usage: Can be used to check part properties by partkey, but better to precompute qualifying partkey set.

## Recommended Approach
1. Load p_brand dict, p_container dict. Find codes for 'Brand#23' and 'MED BOX'.
2. Scan part: collect all partkeys where `p_brand == brand23_code && p_container == medbox_code`. Store as set/bitset. (~2K partkeys)
3. **Pass 1**: Scan lineitem. For each row where `l_partkey` is in qualifying set:
   - Accumulate per-partkey: `sum_qty[partkey] += l_quantity`, `count[partkey]++`.
4. Compute `avg_qty[partkey] = sum_qty[partkey] / count[partkey]`, then `threshold[partkey] = 0.2 * avg_qty[partkey]`.
5. **Pass 2**: Scan lineitem again. For each row where `l_partkey` is in qualifying set AND `l_quantity < threshold[l_partkey]`:
   - Accumulate `total_extendedprice += l_extendedprice`.
6. Output: `total_extendedprice / 7.0`.

### Optimization
Can combine into single pass by first computing avg per part, then scanning again. Or use a two-pass approach. With only ~2K qualifying parts, the per-partkey hash map is tiny and fits in L1 cache.
