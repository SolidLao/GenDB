# Q2 Guide

## Query
```sql
-- Decorrelated form of: find the row(s) per (tag,adsh) with the maximum value (uom='pure')
-- joined to sub for company name, filtered to fy=2022. LIMIT 100 by value DESC.
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
| Table | Rows       | Role      | Sort Order                                | Block Size |
|-------|------------|-----------|-------------------------------------------|------------|
| num   | 39,401,761 | fact      | unsorted                                  | 100,000    |
| sub   | 86,135     | dimension | unsorted                                  | 100,000    |

## Column Reference

### num.uom (string_dict, int16_t — dict-encoded)
- File: `sf3.gendb/num/uom.bin` — 39,401,761 × 2 bytes = 78,803,522 bytes
- Dict: `sf3.gendb/num/uom_dict.txt` — ~15 distinct values; 'pure' ≈ 0.24% of rows
- This query: `WHERE uom = 'pure'` → load dict at runtime, find pure_code, filter `uom_code == pure_code`
- Selectivity: 0.24% → ~94,564 rows pass
- Dict loading pattern:
  ```cpp
  std::vector<std::string> uom_dict;
  // read sf3.gendb/num/uom_dict.txt line by line
  int16_t pure_code = -1;
  for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
      if (uom_dict[i] == "pure") { pure_code = i; break; }
  ```

### num.value (monetary, double — raw IEEE 754)
- File: `sf3.gendb/num/value.bin` — 39,401,761 × 8 bytes = 315,214,088 bytes
- Null sentinel: `NaN` — check with `std::isnan(v)`
- This query: `WHERE value IS NOT NULL` → `!std::isnan(v)`, `n.value = m.max_value`
- **No C29 concern for Q2**: Q2 outputs individual double values (no SUM aggregation)
- This query also uses value in `MAX(value)` subquery aggregation — standard double comparison is fine for MAX

### num.adsh (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/adsh.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- Dict: `sf3.gendb/shared/adsh_dict.txt` — 86,135 values
- This query: join key to sub_adsh_hash; also GROUP BY key in subquery `m`

### num.tag (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/tag.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- Dict: `sf3.gendb/shared/tag_dict.txt` — ~200,000 distinct values
- This query: `GROUP BY tag` in subquery `m`; `ORDER BY n.tag` (lexicographic on string, so decode for sort); output column

### sub.fy (integer, int32_t — raw)
- File: `sf3.gendb/sub/fy.bin` — 86,135 × 4 bytes = 344,540 bytes
- Null sentinel: `INT32_MIN`
- This query: `WHERE s.fy = 2022` → `fy == 2022`
- Selectivity on sub: ~19.6% → ~16,882 sub rows qualify

### sub.name (string_dict, int32_t — dict-encoded)
- File: `sf3.gendb/sub/name.bin` — 86,135 × 4 bytes = 344,540 bytes
- Dict: `sf3.gendb/sub/name_dict.txt`
- This query: output column `s.name`; also tiebreaker in `ORDER BY s.name`
- C31: always double-quote name output — company names may contain commas
- Dict loading pattern:
  ```cpp
  std::vector<std::string> name_dict;
  // read sf3.gendb/sub/name_dict.txt line by line
  // output: printf("\"%s\"", name_dict[name_code].c_str())
  ```

### sub.adsh (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/sub/adsh.bin` — 86,135 × 4 bytes = 344,540 bytes
- This query: join target (probed via sub_adsh_hash index)

## Indexes

### sub_adsh_hash (hash on sub.adsh)
- File: `sf3.gendb/sub/indexes/sub_adsh_hash.bin`
- File layout: `[uint32_t cap=262144][SubADSHSlot × 262144]`
- File size: 4 + 262144 × 16 = 4,194,308 bytes
- Header parse (MUST be at function scope per C32):
  ```cpp
  const char*  sub_raw  = ...; // mmap result
  uint32_t     sub_cap  = *(const uint32_t*)sub_raw;        // = 262144
  uint32_t     sub_mask = sub_cap - 1;                      // = 262143
  const SubADSHSlot* sub_ht = (const SubADSHSlot*)(sub_raw + 4);
  ```
- Slot struct (exact from build_indexes.cpp):
  ```cpp
  struct SubADSHSlot {
      int32_t adsh_code;  // INT32_MIN = empty slot
      int32_t row_id;
      int32_t _pad0;
      int32_t _pad1;
  };  // 16 bytes
  ```
- Empty sentinel: `adsh_code == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  static inline uint64_t hash_int32(int32_t key) {
      return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
  }
  ```
- Probe pattern:
  ```cpp
  uint32_t pos = (uint32_t)(hash_int32(adsh_code) & sub_mask);
  for (uint32_t probe = 0; probe < sub_cap; probe++) {  // C24: bounded
      uint32_t slot = (pos + probe) & sub_mask;
      if (sub_ht[slot].adsh_code == INT32_MIN) break;   // not found
      if (sub_ht[slot].adsh_code == adsh_code) {
          int32_t sub_row = sub_ht[slot].row_id;
          // check sub_fy[sub_row] == 2022
          break;
      }
  }
  ```
- Usage in Q2: probe `n.adsh_code` → get `sub_row_id` → check `sub_fy[sub_row] == 2022`

## Query Analysis

### Execution Strategy
1. **Pass 1 — Build subquery `m`**: scan num, filter `uom_code == pure_code && !std::isnan(value)`.
   For each passing row, update `max_map[(adsh_code, tag_code)] = max(current, value)`.
   - Estimated qualifying rows: 0.24% × 39.4M ≈ 94,500 rows
   - Estimated groups (adsh, tag): ~80,000
   - Hash table capacity: `next_pow2(80000 × 2) = 262144`

2. **Pass 2 — Main join**: scan num again, filter `uom_code == pure_code && !std::isnan(value)`.
   For each passing row:
   - Lookup `max_map[(adsh_code, tag_code)]` → check `value == max_value` (exact double compare)
   - Probe `sub_adsh_hash` with `adsh_code` → get `sub_row`
   - Check `sub_fy[sub_row] == 2022`
   - If all pass → candidate row

3. **Top-100**: collect all candidates, partial_sort or min-heap by (value DESC, name ASC, tag ASC)
   - C33: tiebreakers name + tag ensure deterministic output

### Subquery hash map key
```cpp
struct MaxKey {
    int32_t adsh_code;
    int32_t tag_code;
};
// or encode as int64: ((int64_t)adsh_code << 32) | (uint32_t)tag_code
```

### Output encoding
```cpp
// value: output as double, e.g. printf("%.10g", value) or enough precision
// name: C31 — printf("\"%s\"", name_dict[name_code].c_str())
// tag:  printf("\"%s\"", tag_dict[tag_code].c_str()) — tags can contain commas
```

### Sort order for LIMIT 100
```
ORDER BY n.value DESC, s.name ASC, n.tag ASC
```
Decode name and tag strings for string comparison. Tiebreaker prevents non-determinism (C33).
