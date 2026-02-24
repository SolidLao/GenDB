# Q2 Guide

```sql
-- Decorrelated form of: find max-value fact per (adsh,tag) where uom='pure',
-- then join to sub for fy=2022, ordered by value DESC, limit 100.
SELECT s.name, n.tag, n.value
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN (
    SELECT adsh, tag, MAX(value) AS max_value
    FROM num WHERE uom = 'pure' AND value IS NOT NULL
    GROUP BY adsh, tag
) m ON n.adsh = m.adsh AND n.tag = m.tag AND n.value = m.max_value
WHERE n.uom = 'pure' AND s.fy = 2022 AND n.value IS NOT NULL
ORDER BY n.value DESC, s.name, n.tag
LIMIT 100;
```

## Column Reference

### num.uom (dict_string, int16_t, dict_int16)
- File: `num/uom.bin` (39,401,761 rows × 2 bytes = 78.8 MB)
- Dict: `num/uom_dict.txt` — 201 entries. Load as `vector<string> uom_dict`.
- This query: `uom = 'pure'` → find code: `int16_t pure_code = find_code(uom_dict, "pure");` (selectivity ≈ 0.0024, ~94K rows)
- **C2**: NEVER hardcode the code value. Always load dict and search.
- Zone map: `indexes/num_uom_zone_map.bin` — num sorted by uom → skip all blocks where max < pure_code or min > pure_code.

### num.value (double)
- File: `num/value.bin` (39,401,761 rows × 8 bytes = 315 MB)
- NULL sentinel: `NaN` (std::isnan). This query: `value IS NOT NULL` → `!std::isnan(v)`
- **C29 WARNING**: values up to 1e14. For `MAX(value)` — use `double` comparison (no accumulation issue).
  For output only (no SUM/AVG on this column in Q2), double is fine.

### num.adsh (dict_string, int32_t, dict_int32)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes = 157.6 MB)
- Dict: `num/adsh_dict.txt` — 86,135 entries.
- This query: join key to sub. Probe `sub_adsh_hash` for each qualifying num row.

### num.tag (dict_string, int32_t, dict_int32)
- File: `num/tag.bin` (39,401,761 rows × 4 bytes = 157.6 MB)
- Dict: `num/tag_dict.txt` — 198,311 entries. Load for output decode.
- This query: GROUP BY adsh+tag for inner MAX aggregation; output decoded string.

### sub.fy (integer, int32_t)
- File: `sub/fy.bin` (86,135 rows × 4 bytes = 344 KB)
- Zone map: `indexes/sub_fy_zone_map.bin` (9 blocks of 10K rows).
- This query: `fy = 2022` — filter sub rows during hash table build.

### sub.name (dict_string, int32_t, dict_int32)
- File: `sub/name.bin` (86,135 rows × 4 bytes = 344 KB)
- Dict: `sub/name_dict.txt` — up to 86,135 entries.
- This query: output column. Decode at output time via `name_dict[code]`.

### sub.adsh (dict_string, int32_t, dict_int32)
- File: `sub/adsh.bin` (86,135 rows × 4 bytes = 344 KB)
- This query: join key from num. Use `sub_adsh_hash` for O(1) lookup.

## Table Stats

| Table | Rows       | Role      | Sort Order    | Block Size |
|-------|------------|-----------|---------------|------------|
| num   | 39,401,761 | fact      | (uom, ddate)  | 100,000    |
| sub   | 86,135     | dimension | (fy, sic)     | 10,000     |

## Query Analysis
- **Step 1**: Build hash map of qualifying sub rows: `fy == 2022` → `{adsh_code → (name_code)}`, ~27K entries.
- **Step 2**: Scan num with `uom = pure_code AND !isnan(value)` using zone maps on uom.
  Selectivity ≈ 0.0024 → ~94K qualifying num rows. Zone map skips ~99.7% of blocks.
- **Step 3**: For each qualifying num row, probe sub hash → keep if sub row found (fy=2022).
- **Step 4**: Compute MAX(value) per (adsh_code, tag_code) using `unordered_map<pair<int32_t,int32_t>, double>`.
  Estimated ~94K groups (most adsh+tag combos are unique for pure).
- **Step 5**: Second pass over qualifying num rows (cached in memory from step 2/3): filter where `value == max_value[adsh,tag]`.
- **Step 6**: Collect results, sort by (value DESC, name ASC, tag ASC), LIMIT 100.
- Alternative single-pass: collect all qualifying rows → build max map → filter in one pass.

## Indexes

### num_uom_zone_map (zone_map on num.uom, int16_t)
- File: `indexes/num_uom_zone_map.bin`
- Layout: `[uint32_t num_blocks=395] [ZoneBlock<int16_t> × 395]`
- `ZoneBlock<int16_t>`: `{ int16_t min_val; int16_t max_val; uint32_t row_count; }`
- num is sorted by uom → all rows with `uom=pure_code` are contiguous.
- **Usage**: find first block where `min_val <= pure_code <= max_val`, scan only those blocks.
- Row offset for block i = block_size × i (all blocks have size 100,000 except last).

### sub_adsh_hash (hash on sub.adsh, int32_t → row_id)
- File: `indexes/sub_adsh_hash.bin`
- Layout: `[uint64_t ht_cap=262144] [SubAdsSlot × 262144]`
- `SubAdsSlot`: `{ int32_t adsh_code; int32_t row_id; }` (empty: adsh_code == INT32_MIN)
- Hash function: `(uint64_t)(uint32_t)adsh_code * 2654435761ULL`
- **Usage**: for each qualifying num row with adsh_code, probe → get sub row_id → check sub.fy[row_id] == 2022.
- Bounded probing: `for (uint64_t p = 0; p < cap; ++p) { idx = (h+p) & (cap-1); if empty break; if match → row_id }`

## C++ Pattern (outline)

```cpp
// Load dicts
vector<string> uom_dict = load_dict(db + "/num/uom_dict.txt");
int16_t pure_code = find_code(uom_dict, "pure");  // C2: never hardcode
vector<string> tag_dict  = load_dict(db + "/num/tag_dict.txt");
vector<string> name_dict = load_dict(db + "/sub/name_dict.txt");

// Build sub fy=2022 hash: adsh_code → name_code
// (mmap sub/adsh.bin, sub/fy.bin, sub/name.bin; filter fy==2022)
unordered_map<int32_t,int32_t> sub_map;  // adsh_code → name_code

// Zone-map guided scan of num
// Load uom zone map; find block range for pure_code
// For each qualifying block: scan rows
// Collect {adsh, tag, value} tuples into vector where uom==pure_code && !isnan(v) && sub_map.count(adsh)

// Build max-value map: (adsh_code, tag_code) → max double
unordered_map<uint64_t, double> max_map;

// Final filter + top-100 sort
vector<tuple<double,string,string>> results;  // (value, name, tag)
// partial_sort or min-heap for top 100 (P6)
```
