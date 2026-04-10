# Q19 Guide — Discounted Revenue

## Column Reference

### l_partkey (FK, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows of int32_t)
- This query: join key `p_partkey = l_partkey`

### l_quantity (decimal, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows of double)
- This query: range filters — 3 bands: `[1,11]`, `[10,20]`, `[20,30]`

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `SUM(l_extendedprice * (1 - l_discount))`

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: revenue calculation

### l_shipmode (string, uint8_t, dictionary)
- File: `storage/lineitem/l_shipmode.bin` (59,986,052 rows of uint8_t)
- Dict: `storage/lineitem/l_shipmode_dict.bin`
- This query: `l_shipmode IN ('AIR', 'AIR REG')` → load dict, find codes for AIR and AIR REG

### l_shipinstruct (string, uint8_t, dictionary)
- File: `storage/lineitem/l_shipinstruct.bin` (59,986,052 rows of uint8_t)
- Dict: `storage/lineitem/l_shipinstruct_dict.bin`
- This query: `l_shipinstruct = 'DELIVER IN PERSON'` → load dict, find code

### p_partkey (PK, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows of int32_t)
- This query: join key `p_partkey = l_partkey`

### p_brand (string, uint8_t, dictionary)
- File: `storage/part/p_brand.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_brand_dict.bin`
- This query: 3 brand checks — 'Brand#12', 'Brand#23', 'Brand#34' → load dict, find 3 codes

### p_container (string, uint8_t, dictionary)
- File: `storage/part/p_container.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_container_dict.bin`
- This query: 3 sets of containers:
  - SM: ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG')
  - MED: ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK')
  - LG: ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG')
  → Load dict, find codes for each, build lookup arrays

### p_size (integer, int32_t, raw)
- File: `storage/part/p_size.bin` (2,000,000 rows of int32_t)
- This query: 3 size ranges: `[1,5]`, `[1,10]`, `[1,15]`

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| lineitem | 59,986,052 | fact      | l_orderkey | 65536      |
| part     | 2,000,000  | dimension | p_partkey  | 65536      |

## Query Analysis
- **Pattern**: 2-way join, 3 OR branches each combining part and lineitem filters, scalar aggregate
- **Strategy**:
  1. Pre-build part lookup arrays: For each partkey, store (brand_code, container_code, size) in a dense struct array indexed by partkey (2M entries).
  2. Pre-compute which (brand, container, size) combinations match each of the 3 OR branches. Encode as a `uint8_t classify[partkey]` where bit 0=branch1, bit 1=branch2, bit 2=branch3 (or 0=no match).
  3. Pre-compute shipmode and shipinstruct filters for lineitem: find dict codes for AIR, AIR REG, DELIVER IN PERSON.
  4. Scan lineitem: first check `l_shipmode IN (AIR, AIR_REG) && l_shipinstruct == DELIVER_IN_PERSON` (shared across all branches). If passes, look up `classify[l_partkey]`:
     - Branch 1: `classify & 1` AND `l_quantity >= 1 && l_quantity <= 11`
     - Branch 2: `classify & 2` AND `l_quantity >= 10 && l_quantity <= 20`
     - Branch 3: `classify & 4` AND `l_quantity >= 20 && l_quantity <= 30`
  5. If any branch matches, accumulate `l_extendedprice * (1 - l_discount)`
  6. Output: single scalar

## Indexes

### part_pk_idx (dense_pk on p_partkey)
- File: `storage/indexes/part_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by partkey → row_id, sentinel -1
- Usage: Build dense part attribute arrays
