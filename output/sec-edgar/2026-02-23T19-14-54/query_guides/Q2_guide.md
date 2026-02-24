# Q2 Guide

## SQL
```sql
-- Decorrelated from correlated scalar subquery. Semantically equivalent.
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
Expected output: 100 rows.

## Column Reference

### num/uom.bin (dict_string, int16_t)
- File: `num/uom.bin` (39,401,761 rows, 75.2 MB)
- Dict: `num/uom_dict.txt` (201 entries) — load at runtime (C2, NEVER hardcode codes)
  - `int16_t pure_code = find_code(uom_dict, "pure"); // at ingest time: code 10`
  - Filter: `n.uom = 'pure'` → `uom[i] == pure_code`
- Selectivity: ~1/1000 (very selective, only ~39K rows match)

### num/value.bin (monetary_decimal, double)
- File: `num/value.bin` (39,401,761 rows, 300.6 MB)
- Stored as double. Individual values fit in double precision.
- Filter: `value IS NOT NULL` → no nulls in this dataset (null_fraction=0.0), skip check.
- C29 WARNING: For SUM/AVG aggregations on other queries — use int64_t cents.
  For Q2, we only use value for MAX comparison and final output — double arithmetic is fine here.
- This query: `n.value = m.max_value` (equality float comparison) → use exact double equality.

### num/adsh.bin (dict_string, int32_t, global dict)
- File: `num/adsh.bin` (39,401,761 rows, 150.3 MB)
- Dict: `adsh_global_dict.txt` (86,135 entries, codes 0..86134)
- JOIN key with sub.adsh and m.adsh.

### num/tag.bin (dict_string, int32_t, global dict)
- File: `num/tag.bin` (39,401,761 rows, 150.3 MB)
- Dict: `tag_global_dict.txt` (198,311 entries)
- GROUP BY key for subquery m, JOIN key for m join.
- Output: decode `tag_dict[tag_code]` for final output.

### sub/fy.bin (integer, int32_t)
- File: `sub/fy.bin` (86,135 rows, 0.3 MB)
- Filter: `s.fy = 2022` — compare directly: `fy[i] == 2022`
- Selectivity: ~17% of sub rows (~14.6K rows)

### sub/adsh.bin (dict_string, int32_t, global dict)
- File: `sub/adsh.bin` (86,135 rows, 0.3 MB)
- JOIN key. Shares same global adsh dict as num/adsh.bin.

### sub/name.bin (dict_string, int32_t)
- File: `sub/name.bin` (86,135 rows, 0.3 MB)
- Dict: `sub/name_dict.txt` — load at runtime.
- Output: decode `name_dict[name_code]` for final output.

## Table Stats
| Table | Rows       | Role       | Sort Order | Block Size |
|-------|------------|------------|------------|------------|
| num   | 39,401,761 | fact       | none       | 65,536     |
| sub   | 86,135     | dimension  | adsh       | 65,536     |

## Query Analysis
**Join pattern:** num → sub (FK-PK on adsh). Sub is tiny (86K rows).
**Filters:**
- `n.uom = 'pure'`: very selective (~1/1000 → ~39K rows from 39M)
- `s.fy = 2022`: ~17% of sub (14.6K rows)
**Subquery m:** MAX(value) per (adsh, tag) where uom='pure'
**Final join:** n must match m on (adsh, tag, value=max_value) — picks the max-value row per group
**Output:** 100 rows ordered by value DESC

## Indexes

### sub_adsh_hash (hash on sub.adsh)
- File: `indexes/sub_adsh_hash.bin`
- Layout: `[uint32_t cap=262144][{int32_t adsh_code, uint32_t row_idx} × 262144]`
- Size: 2.0 MB (fits entirely in L3 cache)
- Usage: mmap and probe for each qualifying num row to look up sub.fy and sub.name
- Probe pattern:
  ```cpp
  struct SubSlot { int32_t adsh_code; uint32_t row_idx; };
  uint32_t cap, mask;  // read from file header
  // probe:
  uint32_t h = ((uint32_t)adsh_code * 2654435761u) & mask;
  for (uint32_t p = 0; p < cap; p++) {
      uint32_t slot = (h + p) & mask;
      if (slots[slot].adsh_code == INT32_MIN) break;  // empty
      if (slots[slot].adsh_code == adsh_code) {
          uint32_t sub_row = slots[slot].row_idx;
          // use sub_fy[sub_row], sub_name[sub_row]
          break;
      }
  }
  ```

## Implementation Strategy
**Two-pass approach (efficient for this query):**

**Pass 1:** Build (adsh_code, tag_code) → max_value hash map
- Filter num for uom='pure', value!=NULL
- For each qualifying row: update max_value per (adsh, tag) key
- Expected rows: ~39K → small hash map, fits in L2 cache
- Cap = next_power_of_2(39K * 2) = 65,536

**Pass 2:** Filter for final result
- Scan num again for uom='pure', value!=NULL
- Look up (adsh, tag) in max_value map → check n.value == max_value
- Look up sub_adsh_hash to get fy → filter s.fy == 2022
- If both conditions met: candidate for output

**Collect up to 100 top results:** Use a min-heap of size 100 keyed by value DESC (P6).
Then decode name and tag strings from dicts for output.

## Output Format
```
name,tag,value
Apple Inc,SomeXBRLTag,1234567.89
...
```
Print value with enough precision (e.g., `%.6g` or `%.2f`).
