# Q2 Guide — Minimum Cost Supplier

## Column Reference

### p_partkey (PK, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows of int32_t)
- This query: join key `p_partkey = ps_partkey`, output column

### p_size (integer, int32_t, raw)
- File: `storage/part/p_size.bin` (2,000,000 rows of int32_t)
- This query: `p_size = 15` → C++: `p_size[i] == 15`

### p_type (string, uint8_t, dictionary)
- File: `storage/part/p_type.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_type_dict.bin` — format: `uint32_t count`, then `count` entries of `{uint16_t len, char[len]}`
- This query: `p_type LIKE '%BRASS'` → Load dict, find all codes where string ends with "BRASS", then `type_matches_brass[p_type[i]]`

### p_mfgr (string, uint8_t, dictionary)
- File: `storage/part/p_mfgr.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_mfgr_dict.bin`
- This query: output column — decode via dict lookup for final results only

### ps_partkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8,000,000 rows of int32_t)
- This query: join key `ps_partkey = p_partkey`

### ps_suppkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8,000,000 rows of int32_t)
- This query: join key `ps_suppkey = s_suppkey`

### ps_supplycost (decimal, double, raw)
- File: `storage/partsupp/ps_supplycost.bin` (8,000,000 rows of double)
- This query: `MIN(ps_supplycost)` in subquery, equality check in outer

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: join key `s_suppkey = ps_suppkey`

### s_nationkey (FK, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows of int32_t)
- This query: join key `s_nationkey = n_nationkey`

### s_acctbal (decimal, double, raw)
- File: `storage/supplier/s_acctbal.bin` (100,000 rows of double)
- This query: output, ORDER BY `s_acctbal DESC`

### s_name (string, varlen)
- File: `storage/supplier/s_name.bin` (offsets, uint32_t[100001]), `storage/supplier/s_name_data.bin`
- This query: output, ORDER BY `s_name ASC`

### s_address (string, varlen)
- File: `storage/supplier/s_address.bin` (offsets), `storage/supplier/s_address_data.bin`
- This query: output only

### s_phone (string, varlen)
- File: `storage/supplier/s_phone.bin` (offsets), `storage/supplier/s_phone_data.bin`
- This query: output only

### s_comment (string, varlen)
- File: `storage/supplier/s_comment.bin` (offsets), `storage/supplier/s_comment_data.bin`
- This query: output only

### n_nationkey (PK, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows of int32_t)
- This query: join key `s_nationkey = n_nationkey`

### n_name (string, uint8_t, dictionary)
- File: `storage/nation/n_name.bin` (25 rows of uint8_t)
- Dict: `storage/nation/n_name_dict.bin`
- This query: output column, ORDER BY `n_name ASC`

### n_regionkey (FK, int32_t, raw)
- File: `storage/nation/n_regionkey.bin` (25 rows of int32_t)
- This query: join key `n_regionkey = r_regionkey`

### r_regionkey (PK, int32_t, raw)
- File: `storage/region/r_regionkey.bin` (5 rows of int32_t)
- This query: join key

### r_name (string, uint8_t, dictionary)
- File: `storage/region/r_name.bin` (5 rows of uint8_t)
- Dict: `storage/region/r_name_dict.bin`
- This query: `r_name = 'EUROPE'` → Load dict, find code for "EUROPE", then `r_name[i] == europe_code`

## Table Stats
| Table    | Rows      | Role      | Sort Order              | Block Size |
|----------|-----------|-----------|-------------------------|------------|
| part     | 2,000,000 | dimension | p_partkey               | 65536      |
| partsupp | 8,000,000 | fact      | (ps_partkey, ps_suppkey) | 65536      |
| supplier | 100,000   | dimension | s_suppkey               | 65536      |
| nation   | 25        | dimension | n_nationkey             | 65536      |
| region   | 5         | dimension | r_regionkey             | 65536      |

## Query Analysis
- **Pattern**: Multi-way join with correlated subquery for MIN(ps_supplycost)
- **Strategy**:
  1. Build set of European nation keys: load region dict, find EUROPE code → get r_regionkey → filter nation rows where `n_regionkey == europe_regionkey` → collect `n_nationkey` values into a set
  2. Build set of European supplier suppkeys: scan supplier, check `s_nationkey` in European nations set
  3. Filter parts: scan part, apply `p_size == 15` (selectivity ~0.02) AND `p_type LIKE '%BRASS'` (selectivity ~0.04) → combined ~0.0008 → ~1,600 qualifying parts
  4. For each qualifying part: use **partsupp_partkey_idx** to find all partsupp rows for that partkey (4 rows each). Filter to European suppliers. Find MIN(ps_supplycost) among those. Then find the supplier(s) with that min cost.
  5. Output with ORDER BY s_acctbal DESC, n_name, s_name, p_partkey LIMIT 100

## Indexes

### part_pk_idx (dense_pk on p_partkey)
- File: `storage/indexes/part_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by partkey → row_id, sentinel -1
- Usage: Direct lookup `part_pk_index[partkey]` → row_id

### supplier_pk_idx (dense_pk on s_suppkey)
- File: `storage/indexes/supplier_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by suppkey → row_id, sentinel -1
- Usage: Given ps_suppkey, look up supplier row: `supplier_pk_index[ps_suppkey]`

### partsupp_partkey_idx (dense_range on ps_partkey)
- File: `storage/indexes/partsupp_partkey_idx.bin`
- Meta: `storage/indexes/partsupp_partkey_idx_meta.txt`
- Layout: `struct { uint32_t start; uint32_t count; }` array, 8 bytes per entry, indexed by partkey
- Usage: For a qualifying partkey, `entry = index[partkey]` → rows `[entry.start, entry.start + entry.count)` in partsupp. Since partsupp is sorted by (ps_partkey, ps_suppkey), all rows for a given partkey are contiguous.
- Sentinel: `{0, 0}` means no rows for that key

### nation_pk_idx (dense_pk on n_nationkey)
- File: `storage/indexes/nation_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by nationkey → row_id, sentinel -1
