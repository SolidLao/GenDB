# Q4 Guide

```sql
SELECT s.sic, t.tlabel, p.stmt,
       COUNT(DISTINCT s.cik) AS num_companies,
       SUM(n.value) AS total_value,
       AVG(n.value) AS avg_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN tag t ON n.tag = t.tag AND n.version = t.version
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = 'USD' AND p.stmt = 'EQ'
      AND s.sic BETWEEN 4000 AND 4999
      AND n.value IS NOT NULL AND t.abstract = 0
GROUP BY s.sic, t.tlabel, p.stmt
HAVING COUNT(DISTINCT s.cik) >= 2
ORDER BY total_value DESC
LIMIT 500;
```

## Column Reference

### uom_code (dict_code, int8_t) — num
- File: `num/uom_code.bin` (39,401,761 rows × 1 byte)
- This query: `WHERE uom = 'USD'` → `uom_code == usd_code`.
  Load: `usd_code = load_dict("indexes/uom_codes.bin")["USD"]`.

### adsh_code (dict_code, int32_t) — num
- File: `num/adsh_code.bin` (39,401,761 rows × 4 bytes)
- This query: join key for `num→sub` (O(1) lookup: `sub[adsh_code]`) and also part of
  composite key `(adsh_code, tagver_code)` for the `num→pre` join via `pre_adsh_tagver_set`.

### tagver_code (dict_code, int32_t) — num
- File: `num/tagver_code.bin` (39,401,761 rows × 4 bytes)
- This query: join key for `num→tag` (O(1): `tag[tagver_code]`) and for `num→pre`
  composite key. Skip rows where `tagver_code == -1`.

### value (decimal, double) — num
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- This query: `SUM(n.value)` and `AVG(n.value)` per group. Accumulate sum and count.

### sic (integer, int16_t) — sub
- File: `sub/sic.bin` (86,135 rows × 2 bytes)
- This query: `WHERE s.sic BETWEEN 4000 AND 4999` → `sub_sic[adsh_code] >= 4000 &&
  sub_sic[adsh_code] <= 4999`. Load full array (86135 × 2 = 172KB).
- Selectivity: ~4% → ~3,445 adsh values in scope.

### cik (integer, int32_t) — sub
- File: `sub/cik.bin` (86,135 rows × 4 bytes)
- This query: `COUNT(DISTINCT s.cik)` per group and HAVING filter.
  Load full array (86135 × 4 = 344KB).

### abstract (integer, int8_t) — tag
- File: `tag/abstract.bin` (1,070,662 rows × 1 byte)
- This query: `t.abstract = 0` → `tag_abstract[tagver_code] == 0`.
  Load full array (1,070,662 × 1 = ~1MB). Selectivity: ~95% pass.

### tlabel (varlen string) — tag
- Files: `tag/tlabel_offsets.bin` (1,070,663 × uint32_t), `tag/tlabel_data.bin` (char[])
- This query: GROUP BY `t.tlabel`, SELECT `t.tlabel`.
- Access pattern:
  ```cpp
  string tlabel(tlabel_data + tlabel_offsets[tagver_code],
                tlabel_offsets[tagver_code+1] - tlabel_offsets[tagver_code]);
  ```
- Grouping strategy: group by `(sic, tagver_code, stmt_code)` using integer keys;
  decode tlabel only for output rows after aggregation.

### stmt_code (dict_code, int8_t) — pre
- File: `pre/stmt_code.bin` (9,600,799 rows × 1 byte)
- This query: `WHERE p.stmt = 'EQ'` → `stmt_code == eq_code`.
  Load: `eq_code = load_dict("indexes/stmt_codes.bin")["EQ"]`.
- Selectivity: ~11% of pre rows have stmt='EQ'.

### adsh_code (dict_code, int32_t) — pre
- File: `pre/adsh_code.bin` (9,600,799 rows × 4 bytes)
- This query: used as part of `pre_adsh_tagver_set` key to verify num row has a
  matching pre row with stmt='EQ'. See index usage below.

### tagver_code (dict_code, int32_t) — pre
- File: `pre/tagver_code.bin` (9,600,799 rows × 4 bytes)
- This query: same as adsh_code — part of the composite join key.

## Table Stats

| Table | Rows       | Role                  | Sort Order                    | Block Size |
|-------|------------|-----------------------|-------------------------------|------------|
| num   | 39,401,761 | fact (scan)           | (uom_code, ddate)             | 100,000    |
| sub   | 86,135     | dimension (probe)     | adsh (= code order)           | 100,000    |
| tag   | 1,070,662  | dimension (probe)     | tagver_code order             | 100,000    |
| pre   | 9,600,799  | dimension (set probe) | (adsh_code, tagver_code)      | 100,000    |

## Query Analysis
- **Pre-processing**: scan `pre/stmt_code.bin` + `pre/adsh_code.bin` + `pre/tagver_code.bin`
  to build a hash set of `(adsh_code, tagver_code)` pairs where `stmt_code == eq_code`:
  ```cpp
  unordered_set<uint64_t> eq_set;
  for (size_t i = 0; i < pre_N; ++i) {
      if (pre_stmt_code[i] == eq_code) {
          uint64_t key = ((uint64_t)(uint32_t)pre_adsh_code[i] << 32)
                       | (uint32_t)pre_tagver_code[i];
          eq_set.insert(key);
      }
  }
  // ~11% × 9.6M = ~1.06M entries in eq_set
  ```

  **Alternative using pre_adsh_tagver_set.bin**: load the full sorted set and scan pre
  once to check stmt. However, building eq_set from raw pre columns is simpler.

- **Main scan of num** (with zone maps for USD skipping):
  For each num row where `uom_code == usd_code && tagver_code != -1`:
  1. Check `sub_sic[adsh_code] BETWEEN 4000 AND 4999` (4% pass)
  2. Check `tag_abstract[tagver_code] == 0` (95% pass)
  3. Build join key and probe `eq_set`:
     ```cpp
     uint64_t key = ((uint64_t)(uint32_t)adsh_code << 32) | (uint32_t)tagver_code;
     if (eq_set.count(key)) { /* accumulate */ }
     ```
  4. Group key: `(sub_sic[adsh_code], tagver_code, eq_code)` — stmt is fixed 'EQ' for all
     passing rows, so group by `(sic, tagver_code)`.
  5. Accumulate: `sum_value`, `count_value`, and `unordered_set<int32_t> ciks` per group.

- **HAVING**: `COUNT(DISTINCT cik) >= 2` → filter groups with ≥2 distinct ciks.
- **Output**: decode tlabel for surviving groups, sort by total_value DESC, limit 500.

## Indexes

### pre_adsh_tagver_set (sorted_array on pre.adsh_code, pre.tagver_code)
- File: `indexes/pre_adsh_tagver_set.bin`
- Layout:
  ```
  uint64_t n_unique      // = 8,631,890 unique pairs
  n_unique × uint64_t    // sorted ascending
  ```
- Key construction (from build_indexes.cpp, verbatim):
  ```cpp
  keys[i] = ((uint64_t)(uint32_t)adsh_codes[i] << 32) | (uint32_t)tagver_codes[i];
  ```
  This casts each int32_t to uint32_t before combining, preserving bit patterns
  (handles tagver_code=-1 → 0xFFFFFFFF correctly at the bit level).
- **For Q4**: do NOT load this file as a set directly. Instead, build `eq_set` by
  scanning pre columns for `stmt_code == eq_code` (see Query Analysis above).
  This avoids loading all 8.6M pairs when only 11% have stmt='EQ'.

### uom_codes (dict_file on uom)
- File: `indexes/uom_codes.bin`
- Layout: `uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }`
- Usage: `usd_code = load_dict("indexes/uom_codes.bin")["USD"]`.

### stmt_codes (dict_file on stmt)
- File: `indexes/stmt_codes.bin`
- Layout: `uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }`
- Usage: `eq_code = load_dict("indexes/stmt_codes.bin")["EQ"]`.

### num_zone_maps (zone_map on uom_code, ddate)
- File: `indexes/num_zone_maps.bin`
- Layout:
  ```cpp
  uint32_t n_blocks;   // = 395
  struct ZoneMap { int8_t min_uom, max_uom; int32_t min_ddate, max_ddate; };
  // sizeof(ZoneMap) = 12 (2-byte padding between max_uom and min_ddate)
  ```
- Usage: skip block `b` if `zm[b].min_uom > usd_code || zm[b].max_uom < usd_code`.
