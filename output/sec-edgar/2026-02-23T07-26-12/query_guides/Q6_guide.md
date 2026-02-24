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
ORDER BY total_value DESC LIMIT 200;
```

## Column Reference

### num.uom (dict_string, int16_t, dict_int16)
- File: `num/uom.bin` (39,401,761 × 2 = 78.8 MB)
- Dict: `num/uom_dict.txt` (201 entries). `int16_t usd_code = find_code(uom_dict, "USD");` (C2)
- Filter: `uom = 'USD'` (selectivity 0.872). Zone map identifies USD segment.

### num.value (double)
- File: `num/value.bin` (39,401,761 × 8 = 315 MB). NULL = NaN.
- **C29 CRITICAL**: max value 1e14. Use int64_t cents accumulation for SUM:
  ```cpp
  int64_t iv = llround(v * 100.0); sum_cents += iv; cnt++;
  ```

### num.adsh (dict_string, int32_t, dict_int32)
- File: `num/adsh.bin` (39,401,761 × 4 = 157.6 MB). Join key to sub and pre.

### num.tag (dict_string, int32_t, dict_int32)
- File: `num/tag.bin` (39,401,761 × 4 = 157.6 MB)
- Dict: `num/tag_dict.txt` (198,311 entries). GROUP BY tag → use tag_code as key; decode at output.
- This query: output column. Decode via `tag_dict[code]`.

### num.version (dict_string, int32_t, dict_int32)
- File: `num/version.bin` (39,401,761 × 4 = 157.6 MB). Join key to pre.

### sub.fy (integer, int32_t)
- File: `sub/fy.bin` (86,135 × 4 = 344 KB)
- This query: `fy = 2023` (selectivity 0.10 → ~8.6K sub rows qualify)
- Zone map: `indexes/sub_fy_zone_map.bin` (9 blocks). Filter during sub hash build.

### sub.name (dict_string, int32_t, dict_int32)
- File: `sub/name.bin` (86,135 × 4 = 344 KB)
- Dict: `sub/name_dict.txt`. GROUP BY name → use name_code; decode at output.

### sub.adsh (dict_string, int32_t, dict_int32)
- File: `sub/adsh.bin` (86,135 × 4 = 344 KB). Join key.

### pre.stmt (dict_string, int16_t, dict_int16)
- File: `pre/stmt.bin` (9,600,799 × 2 = 19.2 MB)
- Dict: `pre/stmt_dict.txt` (8 entries). `int16_t is_code = find_code(stmt_dict, "IS");` (C2)
- Filter: `stmt = 'IS'` (selectivity 0.18 → ~1.73M pre rows)
- Zone map: `indexes/pre_stmt_zone_map.bin` — pre sorted by stmt; IS blocks are contiguous (IS=code 0, all first blocks).

### pre.plabel (dict_string, int32_t, dict_int32)
- File: `pre/plabel.bin` (9,600,799 × 4 = 38.4 MB)
- Dict: `pre/plabel_dict.txt` (697,126 entries). GROUP BY plabel → use plabel_code; decode at output.

### pre.adsh / pre.tag / pre.version — join keys
- Files: `pre/adsh.bin`, `pre/tag.bin`, `pre/version.bin` (9,600,799 × 4 each)
- Index: `pre_atv_hash` for lookup of pre row given (adsh,tag,version). BUT: for inner join,
  we need to retrieve `plabel_code` for the matching pre row. Two options:
  a) **Runtime hash**: build a filtered hash at query time: scan pre IS blocks → `(adsh,tag,version) → (plabel_code)`.
  b) Modify `pre_atv_hash` usage: look up row_id → access `plabel[row_id]`.
  **Recommended**: option (b) — probe `pre_atv_hash` for the row_id, then read `pre/plabel.bin[row_id]`.

## Table Stats

| Table | Rows       | Role      | Sort Order    | Block Size |
|-------|------------|-----------|---------------|------------|
| num   | 39,401,761 | fact      | (uom, ddate)  | 100,000    |
| sub   | 86,135     | dimension | (fy, sic)     | 10,000     |
| pre   | 9,600,799  | fact      | stmt (asc)    | 100,000    |

## Query Analysis
Three-way join: num ⋈ sub ⋈ pre (inner joins on composite keys).

**Recommended strategy**:
1. **Build sub hash** (fy=2023): `adsh_code → {name_code}`. ~8.6K entries.
   Cap = next_pow2(8600 × 2) = 16384.
2. **Scan pre (stmt='IS' blocks only)** using `pre_stmt_zone_map`:
   Build runtime hash: `(adsh_code, tag_code, version_code) → plabel_code`.
   ~1.73M entries. Cap = next_pow2(1.73M × 2) = 4194304 (67MB).
   Note: multiple pre rows can share (adsh,tag,version) — use first plabel encountered.
3. **Scan num** (USD segment via zone map):
   For each row where `!isnan(v)`:
   - Probe sub hash (adsh) → if miss, skip.
   - Probe pre IS hash (adsh,tag,version) → if miss, skip.
   - Accumulate into group key: `(name_code, is_code, tag_code, plabel_code)` → `sum_cents, cnt`.
4. **Top 200**: partial_sort or min-heap by total_value DESC.

**Group key** (all int32_t):
```cpp
struct GroupKey { int32_t name_code; int16_t stmt_code; int32_t tag_code; int32_t plabel_code; };
```
Estimated groups: ~50K (from workload analysis). Unordered_map with custom hash is fine.

## Indexes

### pre_stmt_zone_map (zone_map on pre.stmt, int16_t)
- File: `indexes/pre_stmt_zone_map.bin` — 97 blocks of 100K rows
- pre is sorted by stmt → IS rows (code 0) are first blocks, then other stmt values follow.
- Usage: find blocks where `min_val <= is_code <= max_val`. Row offset = cumsum of row_counts.
  Skip all blocks after the IS segment ends (once min_val > is_code).

### num_uom_zone_map (zone_map on num.uom, int16_t)
- File: `indexes/num_uom_zone_map.bin` — 395 blocks of 100K rows
- Same usage as Q2/Q3: find USD contiguous segment.

### pre_atv_hash (hash: (adsh,tag,version) → row_id in pre)
- File: `indexes/pre_atv_hash.bin`
- Layout: `[uint64_t cap=33554432] [PreATVSlot × 33554432]` (536 MB)
- `PreATVSlot`: `{ int32_t adsh; int32_t tag; int32_t version; int32_t row_id; }` empty: `adsh==INT32_MIN`
- **For Q6**: use only for stmt='IS' rows. The hash contains ALL pre rows; probe will succeed even for non-IS rows. So either:
  - Filter: if `pre/stmt.bin[row_id] != is_code`, skip (load stmt column).
  - Or **build runtime hash** of IS-only (adsh,tag,version)→plabel_code instead (avoids 536MB file load).
  - **Recommendation**: build runtime hash at query time from pre stmt=IS scan (only 1.73M rows), much smaller (67MB vs 536MB) and no post-filter needed.
- Hash function: `h = adsh*2654435761ULL ^ tag*40503ULL ^ version*48271ULL`

### sub_adsh_hash (hash: adsh_code → sub row_id)
- File: `indexes/sub_adsh_hash.bin` — 262,144 slots × 8 bytes = 2 MB
- Usage: probe for adsh → row_id → check sub/fy.bin[row_id] == 2023.

## Output Precision (C29)

```cpp
// Decode group key at output time:
string name   = name_dict[key.name_code];
string stmt_s = stmt_dict[key.stmt_code];   // = "IS"
string tag_s  = tag_dict[key.tag_code];
string plabel = plabel_dict[key.plabel_code];

// total_value: int64_t cents → decimal string
int64_t s = group.sum_cents;
printf("%s,%s,%s,%s,%lld.%02lld,%lld\n",
       name.c_str(), stmt_s.c_str(), tag_s.c_str(), plabel.c_str(),
       s/100, std::abs(s%100), group.cnt);
```
