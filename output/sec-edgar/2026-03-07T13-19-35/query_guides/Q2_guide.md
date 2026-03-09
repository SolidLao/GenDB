# Q2 Guide

```sql
-- Decorrelated from original correlated scalar subquery
SELECT s.name, n.tag, n.value
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN (
    SELECT adsh, tag, MAX(value) AS max_value
    FROM num
    WHERE uom = 'pure' AND value IS NOT NULL
    GROUP BY adsh, tag
) m ON n.adsh = m.adsh AND n.tag = m.tag AND n.value = m.max_value
WHERE n.uom = 'pure' AND s.fy = 2022 AND n.value IS NOT NULL
ORDER BY n.value DESC, s.name, n.tag
LIMIT 100;
```

## Column Reference

### uom_code (dict_code, int8_t) — num
- File: `num/uom_code.bin` (39,401,761 rows × 1 byte)
- This query: `WHERE uom = 'pure'` → load `pure_code` at startup from `indexes/uom_codes.bin`;
  filter `uom_code == pure_code`.
- Selectivity: 0.24% → ~94,564 rows pass; zone maps skip ~99.76% of 395 blocks.

### adsh_code (dict_code, int32_t) — num
- File: `num/adsh_code.bin` (39,401,761 rows × 4 bytes)
- This query: join key `n.adsh = s.adsh`. Since `sub` is sorted by adsh and
  `adsh_code = row_index_in_sub`, `sub` row lookup is O(1): `sub_row = adsh_code`.
- Also subquery group key: `GROUP BY adsh, tag` uses `(adsh_code, tagver_code)`.

### tagver_code (dict_code, int32_t) — num
- File: `num/tagver_code.bin` (39,401,761 rows × 4 bytes)
- This query: subquery group key + join `n.tag = m.tag` (resolved via tagver_code).
  `tag` string ← `tag/tag_data.bin[tag_offsets[tagver_code]..tag_offsets[tagver_code+1]]`.
- Note: `tagver_code = -1` for num rows whose (tag,version) was not in tag.csv; skip these.

### value (decimal, double) — num
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- This query: subquery `MAX(value)`, outer filter `n.value IS NOT NULL`, output `n.value`.
- Encoding: raw IEEE 754 double. All stored rows have non-null values (NULL rows were
  skipped during ingestion). No NaN check needed after zone-map + uom_code filter.

### fy (integer, int16_t) — sub
- File: `sub/fy.bin` (86,135 rows × 2 bytes)
- This query: `WHERE s.fy = 2022` → filter `sub[adsh_code].fy == 2022`.
- Selectivity: ~20% of sub rows have fy=2022 → ~17,227 adsh values in scope.

### name (varlen string) — sub
- Files: `sub/name_offsets.bin` (86,136 × uint32_t), `sub/name_data.bin` (char[])
- This query: `SELECT s.name` output only; accessed for final 100 result rows.
- Access pattern: `name = string(name_data + name_offsets[adsh_code],
                                  name_offsets[adsh_code+1] - name_offsets[adsh_code])`

### tag_str (varlen string) — tag
- Files: `tag/tag_offsets.bin` (1,070,663 × uint32_t), `tag/tag_data.bin` (char[])
- This query: `SELECT n.tag` output; decoded via `tagver_code`.
- Access pattern: `tag = string(tag_data + tag_offsets[tagver_code],
                                 tag_offsets[tagver_code+1] - tag_offsets[tagver_code])`

## Table Stats

| Table | Rows       | Role               | Sort Order          | Block Size |
|-------|------------|--------------------|---------------------|------------|
| num   | 39,401,761 | fact (scan ×2)     | (uom_code, ddate)   | 100,000    |
| sub   | 86,135     | dimension (probe)  | adsh (= code order) | 100,000    |
| tag   | 1,070,662  | dimension (decode) | tagver_code order   | 100,000    |

## Query Analysis
- **Two-pass strategy** over num (both filtered by `uom = 'pure'`, zone maps apply to both):
  1. **Pass 1 — subquery**: scan num, keep `uom_code == pure_code` rows, build hash map
     `unordered_map<uint64_t, double> max_val` keyed by
     `(uint64_t)(uint32_t)adsh_code << 32 | (uint32_t)tagver_code` → `MAX(value)`.
  2. **Pass 2 — main scan**: scan num again with same zone-map skipping, check
     `uom_code == pure_code && sub[adsh_code].fy == 2022` and
     `value == max_val[(adsh_code, tagver_code)]` (exact double equality is valid
     here since we stored the value verbatim and compare to the stored max).
- **Expected matching rows**: ~94,564 pure rows × 20% fy filter × fraction at max ≈ small.
- **Top-100 output**: partial sort or priority queue on (value DESC, name ASC, tag ASC).

## Indexes

### num_zone_maps (zone_map on uom_code, ddate)
- File: `indexes/num_zone_maps.bin`
- Layout:
  ```cpp
  uint32_t n_blocks;   // = 395 (ceil(39401761 / 100000))
  // Then n_blocks × struct ZoneMap (sizeof = 12 bytes):
  struct ZoneMap { int8_t min_uom, max_uom; int32_t min_ddate, max_ddate; };
  // Offsets: min_uom@0, max_uom@1, [2B padding], min_ddate@4, max_ddate@8
  ```
- Usage for this query (both passes): skip block `b` if
  `zm[b].min_uom > pure_code || zm[b].max_uom < pure_code`.
  The num table is sorted by (uom_code, ddate), so 'pure' rows are contiguous:
  only ~1 block range contains pure rows → ~394 of 395 blocks skipped per pass.
- Read: `fread(&n_blocks, 4, 1, f); fread(zmaps, sizeof(ZoneMap), n_blocks, f);`
  Block `b` covers rows `[b*100000, min((b+1)*100000, N))`.

### uom_codes (dict_file on uom)
- File: `indexes/uom_codes.bin`
- Layout: `uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }`
- Usage: `pure_code = load_dict("indexes/uom_codes.bin")["pure"]` at query startup.
  NEVER hardcode the numeric value — codes are assigned by insertion order during ingest.
