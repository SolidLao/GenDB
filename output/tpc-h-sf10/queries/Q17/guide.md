# Q17 Guide — Small-Quantity-Order Revenue

## Column Reference

### p_partkey (PK, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows of int32_t)
- This query: join key `p_partkey = l_partkey`

### p_brand (string, uint8_t, dictionary)
- File: `storage/part/p_brand.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_brand_dict.bin`
- This query: `p_brand = 'Brand#23'` → load dict, find code for Brand#23

### p_container (string, uint8_t, dictionary)
- File: `storage/part/p_container.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_container_dict.bin`
- This query: `p_container = 'MED BOX'` → load dict, find code for MED BOX

### l_partkey (FK, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows of int32_t)
- This query: join key `p_partkey = l_partkey`, also correlated subquery

### l_quantity (decimal, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows of double)
- This query: `l_quantity < 0.2 * AVG(l_quantity)` per partkey; also used in correlated subquery `AVG(l_quantity)`

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `SUM(l_extendedprice) / 7.0`

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| part     | 2,000,000  | dimension | p_partkey  | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |

## Query Analysis
- **Pattern**: Join with correlated subquery — for qualifying parts, compare each lineitem's quantity against the average quantity for that part
- **Strategy**:
  1. Find qualifying partkeys: scan part, filter `p_brand == brand23_code && p_container == medbox_code`. With ~1/25 * ~1/40 selectivity ≈ ~0.001 → ~2,000 parts.
  2. Pre-compute AVG(l_quantity) per qualifying partkey:
     - First pass over lineitem: for each row where `l_partkey` is in qualifying set, accumulate (sum, count) per partkey
     - Compute `threshold[partkey] = 0.2 * sum / count`
  3. Second pass over lineitem (or merge with first): for qualifying partkeys, if `l_quantity < threshold[partkey]`, accumulate `l_extendedprice`
  4. Output: `total / 7.0`
- **Optimization**: Since qualifying partkeys are few (~2K), store them in a hash set or bitset for fast lookup during lineitem scan.

## Indexes

### part_pk_idx (dense_pk on p_partkey)
- File: `storage/indexes/part_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by partkey → row_id, sentinel -1
