# Q6 Guide

## SQL
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
Expected output: 200 rows.

## Column Reference

### num/uom.bin (dict_string, int16_t)
- File: `num/uom.bin` (39,401,761 rows, 75.2 MB)
- Dict: `num/uom_dict.txt` — load at runtime (C2).
  - `int16_t usd_code = find_code(uom_dict, "USD"); // code 0 at ingest time`
- Filter: `n.uom = 'USD'` → ~88.7% qualifying

### num/value.bin (monetary_decimal, double)
- File: `num/value.bin` (39,401,761 rows, 300.6 MB)
- **C29 WARNING:** `SUM(n.value)` MUST accumulate as int64_t cents.
  - `sum_cents += llround(value * 100.0);`
  - Max value observed: 5.94e13 — double SUM will give wrong results.

### num/adsh.bin (dict_string, int32_t, global dict)
- File: `num/adsh.bin` (39,401,761 rows, 150.3 MB)
- JOIN key for sub and pre.

### num/tag.bin (dict_string, int32_t, global dict)
- File: `num/tag.bin` (39,401,761 rows, 150.3 MB)
- GROUP BY key (as raw int32_t code). Output: decode from `tag_global_dict.txt`.

### num/version.bin (dict_string, int32_t, global dict)
- File: `num/version.bin` (39,401,761 rows, 150.3 MB)
- JOIN key for pre only (not in output).

### sub/fy.bin (integer, int32_t)
- File: `sub/fy.bin` (86,135 rows, 0.3 MB)
- Filter: `s.fy = 2023` → ~10% of sub rows (~8.6K rows)
- Direct comparison: `fy[i] == 2023`

### sub/name.bin (dict_string, int32_t)
- File: `sub/name.bin` (86,135 rows, 0.3 MB)
- Dict: `sub/name_dict.txt` — load at runtime (C2).
- GROUP BY key. Output: decode `name_dict[name_code]`.

### sub/adsh.bin (dict_string, int32_t, global dict)
- File: `sub/adsh.bin` (86,135 rows, 0.3 MB)

### pre/stmt.bin (dict_string, int16_t)
- File: `pre/stmt.bin` (9,600,799 rows, 18.3 MB)
- Dict: `pre/stmt_dict.txt` — load at runtime (C2).
  - `int16_t is_code = find_code(stmt_dict, "IS"); // code 1 at ingest time`
- Filter: `p.stmt = 'IS'` → ~19.4% of pre (~1.86M rows). Pre-filtered in pre_is_hash.
- p.stmt is always 'IS' in GROUP BY output — decode for printing.

### pre/plabel.bin (dict_string, int32_t)
- File: `pre/plabel.bin` (9,600,799 rows, 36.6 MB)
- Dict: `pre/plabel_dict.txt` — load at runtime (C2).
- GROUP BY key. Output: decode `plabel_dict[plabel_code]`.
- **WARNING (C30):** GROUP BY must include ALL 4 columns: (name_code, stmt_code, tag_code, plabel_code).
  Missing any one dimension causes N-fold duplication of aggregate values.

### pre/adsh.bin, pre/tag.bin, pre/version.bin (int32_t, global dicts)
- JOIN keys with num.

## Table Stats
| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | none       | 65,536     |
| sub   | 86,135     | dimension | adsh       | 65,536     |
| pre   | 9,600,799  | dimension | none       | 65,536     |

## Query Analysis
**Joins:** num→sub (adsh), num→pre (adsh+tag+version)
**Filters:**
1. n.uom='USD': 88.7%
2. s.fy=2023: ~10% of sub (~8.6K rows → very few qualifying adsh codes)
3. p.stmt='IS': ~19.4% of pre (pre-filtered via pre_is_hash)

**GROUP BY:** (s.name, p.stmt, n.tag, p.plabel) — 4 columns (C30: all must be in hash key!)
**Aggregates:** SUM(value) [int64_t cents], COUNT(*)
**Output:** 200 rows ordered by total_value DESC

## Indexes

### sub_adsh_hash (hash on sub.adsh)
- File: `indexes/sub_adsh_hash.bin`
- Layout: `[uint32_t cap=262144][{int32_t adsh_code, uint32_t row_idx} × 262144]`
- Size: 2.0 MB
- Hash: `h = ((uint32_t)adsh_code * 2654435761u) & (cap-1)`, bounded probe (C24)
- Usage: adsh_code → sub_row → sub_fy[sub_row] (filter), sub_name[sub_row] (GROUP BY key)

### pre_is_hash (hash: pre rows with stmt='IS', stores plabel_code)
- File: `indexes/pre_is_hash.bin`
- Layout: `[uint32_t cap=4194304][{int32_t adsh, int32_t tag, int32_t ver, int32_t plabel} × 4194304]`
- Size: 64.0 MB
- Hash: `h = (((uint32_t)a*2654435761u)^((uint32_t)b*1234567891u)^((uint32_t)c*2246822519u)) & (cap-1)`
- Empty slot: adsh_code == INT32_MIN
- Probe: returns plabel_code for matching (adsh, tag, version), or -1 if not found
- Usage: given (num.adsh, num.tag, num.version), check if a pre row with stmt='IS' exists
  AND retrieve plabel_code for GROUP BY
  ```cpp
  struct IsSlot { int32_t adsh_code; int32_t tag_code; int32_t ver_code; int32_t plabel_code; };
  int32_t found_plabel = -1;
  uint32_t h = hash3(adsh, tag, ver) & is_mask;
  for (uint32_t p = 0; p < is_cap; p++) {  // C24: bounded
      uint32_t slot = (h + p) & is_mask;
      if (is_ht[slot].adsh_code == INT32_MIN) break;
      if (is_ht[slot].adsh_code == adsh && is_ht[slot].tag_code == tag && is_ht[slot].ver_code == ver) {
          found_plabel = is_ht[slot].plabel_code; break;
      }
  }
  if (found_plabel < 0) continue;  // no pre row with stmt='IS'
  ```

## Implementation Strategy
**Step 1:** Pre-build fy=2023 qualifying adsh set from sub.
```cpp
// Scan sub: collect adsh_codes where fy==2023
std::unordered_set<int32_t> fy2023_adsh;
for (int i = 0; i < sub_rows; i++)
    if (sub_fy[i] == 2023) fy2023_adsh.insert(sub_adsh[i]);
// Also build: adsh_code → name_code map for fy=2023 rows
```

**Step 2:** Parallel scan of num with thread-local aggregation (P17, P20):
```cpp
#pragma omp parallel for schedule(dynamic, 65536)
for (int64_t i = 0; i < num_rows; i++) {
    if (num_uom[i] != usd_code) continue;
    if (!fy2023_adsh.count(num_adsh[i])) continue;
    // Look up pre_is_hash for (adsh, tag, version) → plabel_code
    int32_t plabel_code = probe_is_hash(num_adsh[i], num_tag[i], num_ver[i]);
    if (plabel_code < 0) continue;
    // Look up sub for name_code
    int32_t sub_row = probe_sub_hash(num_adsh[i]);
    int32_t name_code = sub_name[sub_row];
    // Aggregate into thread-local map keyed by (name_code, is_code, tag_code, plabel_code)
    // C30: ALL 4 GROUP BY dimensions must be in the key!
    auto& agg = local_map[{name_code, is_code, num_tag[i], plabel_code}];
    agg.sum_cents += llround(num_value[i] * 100.0);
    agg.count++;
}
```

**Step 3:** Merge thread-local maps. Sort top 200 by sum_cents DESC. Decode strings. Output.

## Output Format
```
name,stmt,tag,plabel,total_value,cnt
Apple Inc,IS,Revenues,Net sales,1234567890.12,42
...
```
- stmt: always 'IS' (decode from stmt_dict)
- tag: decode from `adsh_global_dict.txt`... wait, from `tag_global_dict.txt`
- total_value: from int64_t cents → `sum_cents/100` . `abs(sum_cents%100)` (C29)
- cnt: plain integer
