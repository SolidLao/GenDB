# Q16 Guide — Parts/Supplier Relationship

## Column Reference

### p_partkey (PK, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows of int32_t)
- This query: join key `p_partkey = ps_partkey`

### p_brand (string, uint8_t, dictionary)
- File: `storage/part/p_brand.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_brand_dict.bin`
- This query: `p_brand <> 'Brand#45'` → load dict, find Brand#45 code, exclude it. GROUP BY column.

### p_type (string, uint8_t, dictionary)
- File: `storage/part/p_type.bin` (2,000,000 rows of uint8_t)
- Dict: `storage/part/p_type_dict.bin`
- This query: `p_type NOT LIKE 'MEDIUM POLISHED%'` → load dict, find codes where string starts with "MEDIUM POLISHED", build exclusion set. GROUP BY column.

### p_size (integer, int32_t, raw)
- File: `storage/part/p_size.bin` (2,000,000 rows of int32_t)
- This query: `p_size IN (49, 14, 23, 45, 19, 3, 36, 9)` → C++: check against set of 8 values. GROUP BY column.

### ps_partkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8,000,000 rows of int32_t)
- This query: join key `p_partkey = ps_partkey`

### ps_suppkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8,000,000 rows of int32_t)
- This query: `COUNT(DISTINCT ps_suppkey)`, also NOT IN subquery check

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: NOT IN subquery

### s_comment (string, varlen)
- File: `storage/supplier/s_comment.bin` (offsets), `storage/supplier/s_comment_data.bin`
- This query: `s_comment LIKE '%Customer%Complaints%'` → scan supplier comments to build exclusion set of suppkeys

## Table Stats
| Table    | Rows      | Role      | Sort Order              | Block Size |
|----------|-----------|-----------|-------------------------|------------|
| part     | 2,000,000 | dimension | p_partkey               | 65536      |
| partsupp | 8,000,000 | fact      | (ps_partkey, ps_suppkey) | 65536      |
| supplier | 100,000   | dimension | s_suppkey               | 65536      |

## Query Analysis
- **Pattern**: Anti-join (exclude bad suppliers), join part→partsupp, 3 filters on part, COUNT DISTINCT
- **Strategy**:
  1. Build excluded supplier set: scan supplier s_comment varlen, find "Customer" then "Complaints" pattern. Collect bad suppkeys into bitset (~100K bits = 12.5KB).
  2. Scan part: apply 3 filters (brand != Brand#45, type NOT LIKE 'MEDIUM POLISHED%', size IN set). Collect qualifying (partkey, brand_code, type_code, p_size) tuples.
  3. For each qualifying part: use **partsupp_partkey_idx** to find partsupp rows. Each partkey has 4 partsupp entries. For each ps_suppkey, check not in excluded set.
  4. Group by (p_brand code, p_type code, p_size), count distinct ps_suppkeys per group. Use a hash map of `(brand_code, type_code, size) → unordered_set<int32_t>` or similar.
  5. Output: ORDER BY supplier_cnt DESC, p_brand, p_type, p_size. Decode dict codes to strings for output.

## Indexes

### partsupp_partkey_idx (dense_range on ps_partkey)
- File: `storage/indexes/partsupp_partkey_idx.bin`
- Meta: `storage/indexes/partsupp_partkey_idx_meta.txt`
- Layout: `struct { uint32_t start; uint32_t count; }` indexed by partkey, 8 bytes per entry
- Usage: For each qualifying partkey, `entry = index[partkey]` → partsupp rows at `[entry.start, entry.start + entry.count)`. Typically count=4 per partkey.
- Sentinel: `{0, 0}` = no rows
