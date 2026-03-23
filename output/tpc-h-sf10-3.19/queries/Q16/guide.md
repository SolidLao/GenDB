# Q16 Guide — Parts/Supplier Relationship

## Column Reference

### p_partkey (pk, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows)
- This query: `p_partkey = ps_partkey` → join key

### p_brand (category, int8_t, dictionary)
- File: `storage/part/p_brand.bin` (2,000,000 rows), dict: `storage/part/p_brand_dict.bin`
- This query: `p_brand <> 'Brand#45'` → load dict, find code for 'Brand#45', exclude it
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0
- GROUP BY p_brand

### p_type (category, int16_t, dictionary)
- File: `storage/part/p_type.bin` (2,000,000 rows), dict: `storage/part/p_type_dict.bin`
- This query: `p_type NOT LIKE 'MEDIUM POLISHED%'` → load dict, find codes NOT starting with "MEDIUM POLISHED"
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int16_t sequential from 0
- GROUP BY p_type

### p_size (attribute, int32_t, raw)
- File: `storage/part/p_size.bin` (2,000,000 rows)
- This query: `p_size IN (49, 14, 23, 45, 19, 3, 36, 9)` → C++ check against set of 8 values
- GROUP BY p_size

### ps_partkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8,000,000 rows)
- This query: `p_partkey = ps_partkey` → join key

### ps_suppkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8,000,000 rows)
- This query: `COUNT(DISTINCT ps_suppkey)` → supplier_cnt; also NOT IN subquery

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows)
- This query: NOT IN subquery: `ps_suppkey NOT IN (SELECT s_suppkey FROM supplier WHERE s_comment LIKE '%Customer%Complaints%')`

### s_comment (text, varlen string)
- Files: `storage/supplier/s_comment_offsets.bin`, `storage/supplier/s_comment_data.bin`
- This query: `s_comment LIKE '%Customer%Complaints%'` → find supplier suppkeys to exclude

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| part | 2,000,000 | Filtered dimension | p_partkey | 65536 |
| partsupp | 8,000,000 | Bridge | ps_partkey, ps_suppkey | 65536 |
| supplier | 100,000 | Exclusion filter | s_suppkey | 65536 |

## Query Analysis
- **Part filters**: p_brand <> 'Brand#45' (~96%), p_type NOT LIKE 'MEDIUM POLISHED%' (~98%), p_size IN (...) (~16%). Combined: ~15% → ~300K qualifying parts.
- **Supplier exclusion**: s_comment LIKE '%Customer%Complaints%' → very few suppliers excluded (~handful).
- **Aggregation**: GROUP BY (p_brand, p_type, p_size) → ~18,000 groups. COUNT(DISTINCT ps_suppkey) per group.
- **Output**: ORDER BY supplier_cnt DESC, p_brand, p_type, p_size.

## Indexes

### partsupp_ps_partkey_grouped (sorted_grouped on ps_partkey)
- File: `storage/indexes/partsupp_ps_partkey_grouped.bin`
- Layout: Header: `uint64_t num_entries`. Body: `uint32_t[num_entries*2]` interleaved `[start, count, ...]`.
- Usage: For each qualifying part, lookup its partsupp rows to get ps_suppkey values.

## Recommended Approach
1. Load supplier s_comment varlen. Find suppkeys where comment matches `%Customer%Complaints%`. Build excluded_suppkeys set/bitset.
2. Load p_brand dict, p_type dict. Identify excluded brand code ('Brand#45') and excluded type codes ('MEDIUM POLISHED%').
3. Build size set: {49, 14, 23, 45, 19, 3, 36, 9}.
4. Load partsupp_ps_partkey_grouped index.
5. Scan part: for each part, apply 3 filters (brand, type, size).
   - If qualifying: use grouped index to get partsupp rows for this partkey.
   - For each partsupp row: check `ps_suppkey NOT IN excluded_suppkeys`.
   - Collect distinct ps_suppkey values per (p_brand, p_type, p_size) group.
6. COUNT distinct suppkeys per group. Sort by (supplier_cnt DESC, p_brand ASC, p_type ASC, p_size ASC).
7. For output: decode p_brand and p_type codes via dict to strings.
