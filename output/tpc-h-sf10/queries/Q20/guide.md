# Q20 Guide — Potential Part Promotion

## Column Reference

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: IN subquery from partsupp

### s_name (string, varlen)
- File: `storage/supplier/s_name.bin` (offsets), `storage/supplier/s_name_data.bin`
- This query: output, ORDER BY s_name ASC

### s_address (string, varlen)
- File: `storage/supplier/s_address.bin` (offsets), `storage/supplier/s_address_data.bin`
- This query: output

### s_nationkey (FK, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows of int32_t)
- This query: `s_nationkey = n_nationkey` → filter to CANADA

### n_nationkey (PK, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)

### n_name (string, uint8_t, dictionary)
- File: `storage/nation/n_name.bin` (25 rows of uint8_t)
- Dict: `storage/nation/n_name_dict.bin`
- This query: `n_name = 'CANADA'` → find CANADA nationkey

### p_partkey (PK, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows of int32_t)
- This query: inner subquery filter

### p_name (string, varlen)
- File: `storage/part/p_name.bin` (offsets), `storage/part/p_name_data.bin`
- This query: `p_name LIKE 'forest%'` → scan varlen for strings starting with "forest"
- Selectivity: ~0.007 → ~14,000 parts

### ps_partkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8,000,000 rows of int32_t)
- This query: `ps_partkey IN (qualifying partkeys)`

### ps_suppkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8,000,000 rows of int32_t)
- This query: result of subquery → drives outer IN filter

### ps_availqty (integer, int32_t, raw)
- File: `storage/partsupp/ps_availqty.bin` (8,000,000 rows of int32_t)
- This query: `ps_availqty > 0.5 * SUM(l_quantity)` correlated check

### l_partkey (FK, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows of int32_t)
- This query: correlated subquery `l_partkey = ps_partkey`

### l_suppkey (FK, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows of int32_t)
- This query: correlated subquery `l_suppkey = ps_suppkey`

### l_quantity (decimal, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows of double)
- This query: `0.5 * SUM(l_quantity)` in correlated subquery

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows of int32_t)
- This query: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'`
  → C++: `>= days_from_civil(1994, 1, 1) && < days_from_civil(1995, 1, 1)`

## Table Stats
| Table    | Rows       | Role      | Sort Order              | Block Size |
|----------|------------|-----------|-------------------------|------------|
| supplier | 100,000    | dimension | s_suppkey               | 65536      |
| nation   | 25         | dimension | n_nationkey             | 65536      |
| part     | 2,000,000  | dimension | p_partkey               | 65536      |
| partsupp | 8,000,000  | fact      | (ps_partkey, ps_suppkey) | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey              | 65536      |

## Query Analysis
- **Pattern**: Nested subqueries — find parts matching "forest%", then partsupp entries where availqty exceeds half the shipped quantity, then suppliers in CANADA
- **Strategy**:
  1. Find CANADA nationkey from nation dict
  2. Build set of Canadian supplier suppkeys (bitset, ~100K bits)
  3. Build set of "forest%" partkeys: scan p_name varlen → ~14K partkeys (bitset, 2M bits)
  4. Pre-compute SUM(l_quantity) per (l_partkey, l_suppkey) for qualifying partkeys and date-filtered lineitems:
     - Scan lineitem with date filter, check `l_partkey` in forest-partkeys bitset
     - Accumulate sum per (partkey, suppkey) pair using partsupp_composite_hash for key mapping, or a separate hash map
  5. Scan partsupp: for rows where `ps_partkey` is in forest set AND `ps_suppkey` is Canadian:
     - Check `ps_availqty > 0.5 * precomputed_sum[ps_partkey, ps_suppkey]`
     - If passes, add ps_suppkey to result set
  6. Output qualifying suppliers: look up s_name, s_address, ORDER BY s_name

## Indexes

### partsupp_partkey_idx (dense_range on ps_partkey)
- File: `storage/indexes/partsupp_partkey_idx.bin`
- Layout: `struct { uint32_t start; uint32_t count; }` indexed by partkey
- Usage: For each forest-partkey, find its partsupp rows

### partsupp_composite_hash (hash on (ps_partkey, ps_suppkey))
- File: `storage/indexes/partsupp_composite_hash.bin`
- Meta: `storage/indexes/partsupp_composite_hash_meta.txt`
- Layout: 16-byte entries: `{ int32_t partkey, int32_t suppkey, int32_t row_id, int32_t pad }`
- Capacity: 16,777,216, mask: 16,777,215
- Hash function (verbatim):
  ```cpp
  uint64_t h = (uint64_t)(uint32_t)pk * 2654435761ULL ^ (uint64_t)(uint32_t)sk * 40503ULL;
  ```
- Probe: `slot = h & mask`, linear probing, empty sentinel: `row_id == -1`
- Usage: Look up partsupp row_id for (partkey, suppkey) to get ps_availqty

### supplier_pk_idx (dense_pk on s_suppkey)
- File: `storage/indexes/supplier_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by suppkey → row_id, sentinel -1

### lineitem_shipdate_zm (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Usage: Skip blocks outside the 1994 year range
