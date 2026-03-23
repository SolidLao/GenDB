# Q6 Guide

```sql
SELECT s.name, p.stmt, n.tag, p.plabel,
       SUM(n.value) AS total_value, COUNT(*) AS cnt
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = 'USD' AND p.stmt = 'IS' AND s.fy = 2023
      AND n.value IS NOT NULL
GROUP BY s.name, p.stmt, n.tag, p.plabel
ORDER BY total_value DESC
LIMIT 200;
```

## Column Reference

### uom_code (dict_code, int8_t) — num
- File: `num/uom_code.bin` (39,401,761 rows × 1 byte)
- This query: `WHERE uom = 'USD'` → `uom_code == usd_code`.
  Load: `usd_code = load_dict("indexes/uom_codes.bin")["USD"]`.
- Selectivity: 87% → ~34.3M rows pass after zone map + uom check.

### adsh_code (dict_code, int32_t) — num
- File: `num/adsh_code.bin` (39,401,761 rows × 4 bytes)
- This query: join key for `num→sub` and composite key `(adsh_code, tagver_code)`
  for `num→pre` probe via `is_set`.

### tagver_code (dict_code, int32_t) — num
- File: `num/tagver_code.bin` (39,401,761 rows × 4 bytes)
- This query: composite join key for `num→pre`; also enables O(1) tag string decode
  for `SELECT n.tag`. Skip rows with `tagver_code == -1`.

### value (decimal, double) — num
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- This query: `SUM(n.value)` and `COUNT(*)` per group.

### fy (integer, int16_t) — sub
- File: `sub/fy.bin` (86,135 rows × 2 bytes)
- This query: `WHERE s.fy = 2023` → `sub_fy[adsh_code] == 2023`.
  Load full array (172KB). Selectivity: ~20%.

### name (varlen string) — sub
- Files: `sub/name_offsets.bin` (86,136 × uint32_t), `sub/name_data.bin` (char[])
- This query: GROUP BY `s.name` and SELECT `s.name`.
- Access pattern:
  ```cpp
  string name(name_data + name_offsets[adsh_code],
              name_offsets[adsh_code+1] - name_offsets[adsh_code]);
  ```
- Grouping strategy: use `adsh_code` as integer proxy for `name` in the group key
  (since adsh → name is 1:1); decode name string only for final output rows.

### stmt_code (dict_code, int8_t) — pre
- File: `pre/stmt_code.bin` (9,600,799 rows × 1 byte)
- This query: `WHERE p.stmt = 'IS'` → `stmt_code == is_code`.
  Load: `is_code = load_dict("indexes/stmt_codes.bin")["IS"]`.
- Selectivity: ~18% of pre rows → ~1.73M entries in `is_set`.

### adsh_code (dict_code, int32_t) — pre
- File: `pre/adsh_code.bin` (9,600,799 rows × 4 bytes)
- This query: part of `is_set` composite key.

### tagver_code (dict_code, int32_t) — pre
- File: `pre/tagver_code.bin` (9,600,799 rows × 4 bytes)
- This query: part of `is_set` composite key; also indexes into `plabel`.

### plabel (varlen string) — pre
- Files: `pre/plabel_offsets.bin` (9,600,800 × uint32_t), `pre/plabel_data.bin` (char[])
- This query: GROUP BY `p.plabel`, SELECT `p.plabel`.
- Critical note: pre is sorted by (adsh_code, tagver_code), so the plabel at row `i`
  in `pre/plabel_offsets.bin` corresponds to `pre_adsh_code[i]` and `pre_tagver_code[i]`.
- Access requires knowing the pre row index for a given (adsh_code, tagver_code):
  scan pre to build `is_set` and simultaneously record the pre row index for plabel decode.

### tag_str (varlen string) — tag
- Files: `tag/tag_offsets.bin` (1,070,663 × uint32_t), `tag/tag_data.bin` (char[])
- This query: `SELECT n.tag` — decoded via `tagver_code`.
- Access: `tag = string(tag_data + tag_offsets[tagver_code], ...)`

## Table Stats

| Table | Rows       | Role                     | Sort Order               | Block Size |
|-------|------------|--------------------------|--------------------------|------------|
| num   | 39,401,761 | fact (scan)              | (uom_code, ddate)        | 100,000    |
| sub   | 86,135     | dimension (probe)        | adsh (= code order)      | 100,000    |
| pre   | 9,600,799  | dimension (set + decode) | (adsh_code, tagver_code) | 100,000    |
| tag   | 1,070,662  | dimension (decode only)  | tagver_code order        | 100,000    |

## Query Analysis
- **Pre-processing**: scan pre to build a map from `(adsh_code, tagver_code)` key
  to pre row index (for plabel lookup) for rows where `stmt_code == is_code`:
  ```cpp
  // is_map: uint64_t key → uint32_t pre_row_index
  unordered_map<uint64_t, uint32_t> is_map;
  for (size_t i = 0; i < pre_N; ++i) {
      if (pre_stmt_code[i] == is_code) {
          uint64_t key = ((uint64_t)(uint32_t)pre_adsh_code[i] << 32)
                       | (uint32_t)pre_tagver_code[i];
          is_map[key] = (uint32_t)i;  // store pre row index for plabel decode
      }
  }
  // ~18% × 9.6M = ~1.73M entries
  ```

- **Main scan of num** (with zone maps):
  For each num row where `uom_code == usd_code && tagver_code != -1`:
  1. Check `sub_fy[adsh_code] == 2023` (20% pass)
  2. Probe `is_map`:
     ```cpp
     uint64_t key = ((uint64_t)(uint32_t)adsh_code << 32) | (uint32_t)tagver_code;
     auto it = is_map.find(key);
     if (it == is_map.end()) continue;
     uint32_t pre_row = it->second;
     ```
  3. Group key: `(adsh_code, tagver_code, pre_row)`.
     Equivalent to `(name, stmt, tag, plabel)` where:
     - `name` ← `sub_name[adsh_code]` (decode at output)
     - `stmt` = fixed 'IS' for all rows (from is_code)
     - `tag` ← `tag_data[tag_offsets[tagver_code]..]` (decode at output)
     - `plabel` ← `plabel_data[plabel_offsets[pre_row]..]` (decode at output)
  4. Accumulate `sum_value` (double) and `cnt` (int64_t) per group.

- **Group key simplification**: use `(adsh_code, pre_row)` as integer group key
  (tagver_code is implicit from pre_row since pre is sorted by adsh_code, tagver_code).
  Estimated groups: ~100,000.
- **Output**: decode name, tag, plabel for result rows; sort by total_value DESC; limit 200.

## Indexes

### pre_adsh_tagver_set (sorted_array on pre.adsh_code, pre.tagver_code)
- File: `indexes/pre_adsh_tagver_set.bin`
- Layout:
  ```
  uint64_t n_unique      // = 8,631,890
  n_unique × uint64_t    // sorted ascending
  ```
- Key construction (verbatim from build_indexes.cpp):
  ```cpp
  keys[i] = ((uint64_t)(uint32_t)adsh_codes[i] << 32) | (uint32_t)tagver_codes[i];
  ```
- **For Q6**: do NOT use this file directly. Instead, build `is_map` from raw pre columns
  (see Query Analysis). The sorted set does not store plabel row indices needed for output.

### uom_codes (dict_file on uom)
- File: `indexes/uom_codes.bin`
- Layout: `uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }`
- Usage: `usd_code = load_dict("indexes/uom_codes.bin")["USD"]`.

### stmt_codes (dict_file on stmt)
- File: `indexes/stmt_codes.bin`
- Layout: `uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }`
- Usage: `is_code = load_dict("indexes/stmt_codes.bin")["IS"]`.

### num_zone_maps (zone_map on uom_code, ddate)
- File: `indexes/num_zone_maps.bin`
- Layout:
  ```cpp
  uint32_t n_blocks;   // = 395
  struct ZoneMap { int8_t min_uom, max_uom; int32_t min_ddate, max_ddate; };
  // sizeof(ZoneMap) = 12 (2-byte padding between max_uom and min_ddate)
  ```
- Usage: skip block `b` if `zm[b].min_uom > usd_code || zm[b].max_uom < usd_code`.
