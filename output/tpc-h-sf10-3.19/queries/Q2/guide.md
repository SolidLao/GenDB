# Q2 Guide — Minimum Cost Supplier

## Column Reference

### p_partkey (pk, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows)
- This query: `p_partkey = ps_partkey` → join key; also in output and ORDER BY

### p_size (attribute, int32_t, raw)
- File: `storage/part/p_size.bin` (2,000,000 rows)
- This query: `p_size = 15` → C++ `p_size[i] == 15`

### p_type (category, int16_t, dictionary)
- File: `storage/part/p_type.bin` (2,000,000 rows), dict: `storage/part/p_type_dict.bin`
- This query: `p_type LIKE '%BRASS'` → load dict, find all codes where string ends with "BRASS", then `matching_codes.count(p_type[i])`
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int16_t sequential from 0

### p_mfgr (category, int8_t, dictionary)
- File: `storage/part/p_mfgr.bin` (2,000,000 rows), dict: `storage/part/p_mfgr_dict.bin`
- This query: output column only
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0

### ps_partkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8,000,000 rows)
- This query: `p_partkey = ps_partkey` → join key

### ps_suppkey (fk, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8,000,000 rows)
- This query: `s_suppkey = ps_suppkey` → join key

### ps_supplycost (measure, double, raw)
- File: `storage/partsupp/ps_supplycost.bin` (8,000,000 rows)
- This query: `MIN(ps_supplycost)` in subquery, `ps_supplycost = min_cost` in outer

### s_suppkey (pk, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows)
- This query: `s_suppkey = ps_suppkey` → join key

### s_nationkey (fk, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows)
- This query: `s_nationkey = n_nationkey` → join key

### s_acctbal (measure, double, raw)
- File: `storage/supplier/s_acctbal.bin` (100,000 rows)
- This query: output column, ORDER BY `s_acctbal DESC`

### s_name (text, varlen string)
- Files: `storage/supplier/s_name_offsets.bin` (uint32_t[100001]), `storage/supplier/s_name_data.bin`
- This query: output column, ORDER BY component

### s_address (text, varlen string)
- Files: `storage/supplier/s_address_offsets.bin`, `storage/supplier/s_address_data.bin`
- This query: output column only

### s_phone (text, varlen string)
- Files: `storage/supplier/s_phone_offsets.bin`, `storage/supplier/s_phone_data.bin`
- This query: output column only

### s_comment (text, varlen string)
- Files: `storage/supplier/s_comment_offsets.bin`, `storage/supplier/s_comment_data.bin`
- This query: output column only

### n_nationkey (pk, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows)
- This query: `s_nationkey = n_nationkey` → join key

### n_name (text, varlen string)
- Files: `storage/nation/n_name_offsets.bin` (uint32_t[26]), `storage/nation/n_name_data.bin`
- This query: output column, ORDER BY component

### n_regionkey (fk, int32_t, raw)
- File: `storage/nation/n_regionkey.bin` (25 rows)
- This query: `n_regionkey = r_regionkey` → join key

### r_regionkey (pk, int32_t, raw)
- File: `storage/region/r_regionkey.bin` (5 rows)
- This query: `n_regionkey = r_regionkey` → join key

### r_name (text, varlen string)
- Files: `storage/region/r_name_offsets.bin` (uint32_t[6]), `storage/region/r_name_data.bin`
- This query: `r_name = 'EUROPE'` → load string, compare

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| part | 2,000,000 | Filtered dimension | p_partkey | 65536 |
| partsupp | 8,000,000 | Bridge (1:N from part) | ps_partkey, ps_suppkey | 65536 |
| supplier | 100,000 | Dimension | s_suppkey | 65536 |
| nation | 25 | Dimension | n_nationkey | 65536 |
| region | 5 | Dimension | r_regionkey | 65536 |

## Query Analysis
- **Strategy**: Filter part (p_size=15 AND p_type LIKE '%BRASS'), then for each matching part, lookup partsupp rows via grouped index, filter to EUROPE suppliers, find MIN(ps_supplycost), then find the supplier with that min cost.
- **Selectivity**: p_size=15 → ~2% (40K parts), p_type LIKE '%BRASS' → ~20%. Combined: ~0.4% → ~8K matching parts.
- **Region filter**: r_name='EUROPE' → 1 of 5 regions → ~5 nations → ~20% of suppliers (~20K).
- **Subquery**: For each qualifying part, scan its partsupp rows (4 per part via grouped index), filter to EUROPE suppliers, find MIN(ps_supplycost).
- **Aggregation**: None (just MIN per part in subquery).
- **Output**: ORDER BY s_acctbal DESC, n_name, s_name, p_partkey; LIMIT 100.

## Indexes

### partsupp_ps_partkey_grouped (sorted_grouped on ps_partkey)
- File: `storage/indexes/partsupp_ps_partkey_grouped.bin`
- Layout:
  - Header: `uint64_t num_entries` (= max_partkey + 1)
  - Body: `uint32_t[num_entries * 2]` interleaved as `[start_0, count_0, start_1, count_1, ...]`
  - For a given partkey `pk`: `start = body[pk * 2]`, `count = body[pk * 2 + 1]`
  - Rows `start..start+count-1` in partsupp columns all have `ps_partkey == pk`
- Usage: For each qualifying part, lookup partsupp rows by partkey → O(1) lookup, 4 rows per part (TPC-H has exactly 4 suppliers per part).

### supplier_s_suppkey_lookup (dense_pk_array on s_suppkey)
- File: `storage/indexes/supplier_s_suppkey_lookup.bin`
- Layout:
  - Header: `uint64_t num_entries` (= max_suppkey + 1)
  - Body: `int32_t[num_entries]` where `arr[suppkey] = row_id`, `-1` if missing
- Usage: For each ps_suppkey, lookup supplier row_id in O(1) to get s_nationkey, then check if EUROPE.

### part_p_partkey_lookup (dense_pk_array on p_partkey)
- File: `storage/indexes/part_p_partkey_lookup.bin`
- Layout:
  - Header: `uint64_t num_entries` (= max_partkey + 1)
  - Body: `int32_t[num_entries]` where `arr[partkey] = row_id`, `-1` if missing
- Usage: Not strictly needed (can scan part table directly), but available for random access by partkey.

## Recommended Approach
1. Load region/nation (tiny). Find EUROPE regionkey, then collect nation keys in that region.
2. Build set of EUROPE supplier row_ids: scan supplier, check `s_nationkey` in EUROPE nation set.
3. Load p_type dict, find codes matching `%BRASS`. Scan part: filter `p_size==15` AND type code matches.
4. For each qualifying partkey, use `partsupp_ps_partkey_grouped` to get its partsupp rows.
5. For each partsupp row, check if ps_suppkey maps to a EUROPE supplier (via supplier_s_suppkey_lookup → check nationkey).
6. Track MIN(ps_supplycost) among EUROPE suppliers for each part.
7. Second pass: find the supplier matching the min cost, collect output fields.
8. Sort results by (s_acctbal DESC, n_name ASC, s_name ASC, p_partkey ASC), LIMIT 100.
