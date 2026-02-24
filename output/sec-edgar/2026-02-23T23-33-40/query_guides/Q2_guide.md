# Q2 Guide

```sql
-- Decorrelated form of: SELECT MAX(n.value) per (tag, adsh) then join back
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

## Table Stats
| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | (none)     | 65536      |
| sub   | 86,135     | dimension | (none)     | 65536      |

## Query Analysis
- **Two joins**: num→sub (FK-PK on adsh) and num→self-agg (adsh, tag) → MAX(value)
- Filters on num: `uom = 'pure'` (selectivity ~0.002 → only ~79K rows pass), `value IS NOT NULL`
- Filter on sub: `fy = 2022` (selectivity ~0.317 → ~27K sub rows)
- Self-join aggregation: GROUP BY (adsh, tag) to compute MAX(value); result is inner-joined back
- Output: 100 rows sorted by n.value DESC, s.name ASC, n.tag ASC (C33: include stable tiebreakers)
- Strategy: (1) scan num with uom='pure' AND value IS NOT NULL → collect filtered rows;
  (2) build MAX-value map keyed by (adsh_code, tag_code);
  (3) probe sub_adsh_index to get sub row; check sub.fy == 2022;
  (4) final filter: n.value == max_value for that (adsh, tag) group;
  (5) top-100 by (value DESC, name ASC, tag ASC)

## Column Reference

### num.adsh (dict_string, int32_t)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/adsh_dict.txt` — global dict, identical content to sub/adsh_dict.txt
- This query: join key for num→sub (probe sub_adsh_index) and num→self-agg grouping key

### num.tag (dict_string, int32_t)
- File: `num/tag.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/tag_dict.txt` — global dict shared with pre/tag_dict.txt and tag/tag_dict.txt
- This query: self-agg grouping key (with adsh); output column — decode via `tag_dict[code]`

### num.uom (dict_string, int16_t)
- File: `num/uom.bin` (39,401,761 rows × 2 bytes = ~75MB)
- Dict: `num/uom_dict.txt` — load at runtime to find code for 'pure'
  ```cpp
  std::vector<std::string> uom_dict; // load num/uom_dict.txt
  int16_t pure_code = -1;
  for (int16_t c = 0; c < (int16_t)uom_dict.size(); ++c)
      if (uom_dict[c] == "pure") { pure_code = c; break; }
  // Filter: skip row if uom_code != pure_code
  ```
- Selectivity: ~0.002 → only ~79K of 39.4M rows pass this filter

### num.value (decimal, double)
- File: `num/value.bin` (39,401,761 rows × 8 bytes = ~300MB)
- NULL encoding: `std::numeric_limits<double>::quiet_NaN()` — test with `std::isnan(v)`
- This query: `value IS NOT NULL` filter + `MAX(value)` aggregation + equality comparison
- **C29 WARNING**: max observed value ~3×10¹⁵. For Q2 this is a MAX not SUM, so double
  comparison and storage of MAX is safe. But final output of `n.value` must be printed with
  sufficient precision: use `printf("%.10g", value)` or similar to avoid precision loss in display.
- Self-join equality: `n.value == m.max_value` — compare as double (exact bit equality is safe
  since max_value was read from the same binary column)

### sub.adsh (dict_string, int32_t)
- File: `sub/adsh.bin` (86,135 rows × 4 bytes = ~336KB)
- Dict: `sub/adsh_dict.txt` — same global dict as num/adsh_dict.txt
- This query: join key (looked up via sub_adsh_index)

### sub.fy (integer, int32_t)
- File: `sub/fy.bin` (86,135 rows × 4 bytes = ~336KB)
- This query: filter `s.fy = 2022` → `sub_fy[sub_row] == 2022` (raw integer comparison)
- Selectivity: ~0.317 → ~27K sub rows qualify

### sub.name (dict_string, int32_t)
- File: `sub/name.bin` (86,135 rows × 4 bytes = ~336KB)
- Dict: `sub/name_dict.txt` — load at runtime
- This query: output column — decode via `name_dict[code].c_str()`; double-quote (C31)
- Also used as tiebreaker in ORDER BY: compare name codes (sort by string, not code)

## Indexes

### sub_adsh_index (hash on sub.adsh)
- File: `sf3.gendb/indexes/sub_adsh_index.bin`
- **Struct layout** (verbatim from build_indexes.cpp):
  ```cpp
  struct SubSlot { int32_t adsh_code; int32_t sub_row; };
  // sizeof(SubSlot) == 8 bytes
  ```
- **File layout**: `uint32_t cap | cap × SubSlot`
- **Sentinel**: `adsh_code == -1` (empty slot)
- **Hash function** (verbatim from build_indexes.cpp):
  ```cpp
  static inline uint32_t hash_i32(int32_t k) {
      uint32_t x = (uint32_t)k;
      x = ((x >> 16) ^ x) * 0x45d9f3bU;
      x = ((x >> 16) ^ x) * 0x45d9f3bU;
      x = (x >> 16) ^ x;
      return x;
  }
  ```
- **Capacity**: `next_pow2(86135 × 2)` = 262,144 (load ≤50%)
- **Header parse** (C27/C32 — must be at function scope):
  ```cpp
  const char* sub_idx_raw = /* mmap sub_adsh_index.bin */;
  uint32_t sub_cap  = *(const uint32_t*)sub_idx_raw;
  uint32_t sub_mask = sub_cap - 1;
  const SubSlot* sub_ht = (const SubSlot*)(sub_idx_raw + 4);
  ```
- **Probe pattern** (C24: bounded loop):
  ```cpp
  uint32_t slot = hash_i32(adsh_code) & sub_mask;
  for (uint32_t probe = 0; probe < sub_cap; ++probe) {
      uint32_t s = (slot + probe) & sub_mask;
      if (sub_ht[s].adsh_code == -1) break;          // empty — not found
      if (sub_ht[s].adsh_code == adsh_code) {
          int32_t sub_row = sub_ht[s].sub_row;
          // use sub_row to access sub columns
          break;
      }
  }
  ```
- **Usage in Q2**: for each num row passing uom='pure' AND value IS NOT NULL filter,
  probe this index to get sub_row; then check `sub_fy[sub_row] == 2022`

## Output
- Columns: `s.name` (string), `n.tag` (string), `n.value` (double)
- Double-quote name and tag strings (C31)
- Sort tiebreaker order: `n.value DESC`, then `s.name ASC` (string compare), then `n.tag ASC` (string compare) — C33 ensures deterministic output for LIMIT 100
- date_encoding: no date columns in this query — no epoch conversion needed
