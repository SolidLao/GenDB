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

## Table Stats
| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | (none)     | 65536      |
| sub   | 86,135     | dimension | (none)     | 65536      |
| pre   | 9,600,799  | fact      | (none)     | 65536      |

## Query Analysis
- **Three-table join**: num→sub (adsh, FK-PK), num→pre (adsh,tag,version, FK-FK multi-value)
- Filters: `n.uom = 'USD'` (~0.872), `s.fy = 2023` (~0.309), `p.stmt = 'IS'` (~0.180),
  `n.value IS NOT NULL` (~0.99)
- Combined selectivity: 0.872 × 0.309 × 0.180 × 0.99 ≈ 0.048 → ~1.9M num row events
- GROUP BY (s.name_code, p.stmt_code, n.tag_code, p.plabel_code): high cardinality
- Aggregates: SUM(n.value), COUNT(*)
- Strategy: (1) scan num → filter uom='USD' AND value IS NOT NULL;
  (2) probe sub_adsh_index → check fy==2023;
  (3) probe pre_join_index → for each matching pre_row, check stmt=='IS';
  (4) accumulate into (name_code, stmt_code, tag_code, plabel_code) group
- **C29**: SUM(n.value) MUST use int64_t cents
- **C15**: GROUP BY key must include ALL FOUR dimensions: name, stmt, tag, plabel (C30!)
- **C31**: double-quote name, stmt, tag, plabel strings in output
- **C33**: stable tiebreaker for LIMIT 200 (add name or plabel as secondary sort key)

## Column Reference

### num.adsh (dict_string, int32_t)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/adsh_dict.txt` — global dict
- This query: probe key for sub_adsh_index AND pre_join_index

### num.tag (dict_string, int32_t)
- File: `num/tag.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/tag_dict.txt` — global dict (same as pre/tag_dict.txt)
- This query: GROUP BY key (code) + pre_join_index probe key + output column
- Decode for output: `tag_dict[tag_code].c_str()` — double-quote (C31)

### num.version (dict_string, int32_t)
- File: `num/version.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/version_dict.txt` — global dict (same as pre/version_dict.txt)
- This query: pre_join_index probe key (NOT in GROUP BY or output)

### num.uom (dict_string, int16_t)
- File: `num/uom.bin` (39,401,761 rows × 2 bytes = ~75MB)
- Dict: `num/uom_dict.txt` — load at runtime
  ```cpp
  std::vector<std::string> uom_dict;
  int16_t usd_code = -1;
  for (int16_t c = 0; c < (int16_t)uom_dict.size(); ++c)
      if (uom_dict[c] == "USD") { usd_code = c; break; }
  ```
- Selectivity: ~0.872

### num.value (decimal, double)
- File: `num/value.bin` (39,401,761 rows × 8 bytes = ~300MB)
- NULL encoding: `std::numeric_limits<double>::quiet_NaN()` — test with `std::isnan(v)`
- **C29 HARD CONSTRAINT**: SUM MUST use int64_t cents:
  ```cpp
  int64_t iv = llround(v * 100.0);
  sum_cents += iv;
  ```
  Output: `printf("%lld.%02lld", sum_cents / 100, std::abs(sum_cents % 100))`

### sub.adsh (dict_string, int32_t)
- File: `sub/adsh.bin` (86,135 rows × 4 bytes = ~336KB)
- This query: looked up via sub_adsh_index

### sub.fy (integer, int32_t)
- File: `sub/fy.bin` (86,135 rows × 4 bytes = ~336KB)
- This query: filter `s.fy = 2023` → `sub_fy[sub_row] == 2023`
- Selectivity: ~0.309 → ~26.6K sub rows qualify

### sub.name (dict_string, int32_t)
- File: `sub/name.bin` (86,135 rows × 4 bytes = ~336KB)
- Dict: `sub/name_dict.txt` — load at runtime
- This query: GROUP BY key (code) + output column
- Decode: `name_dict[name_code].c_str()` — double-quote (C31)
- **C30 NOTE**: name_code alone is NOT sufficient as GROUP BY key — must combine with
  stmt_code, tag_code, plabel_code. Missing any dimension merges distinct groups.

### pre.adsh (dict_string, int32_t)
- File: `pre/adsh.bin` (9,600,799 rows × 4 bytes = ~37MB)
- This query: pre_join_index lookup key (not directly scanned; accessed via index payload)

### pre.tag (dict_string, int32_t)
- File: `pre/tag.bin` (9,600,799 rows × 4 bytes = ~37MB)
- This query: pre_join_index lookup key

### pre.version (dict_string, int32_t)
- File: `pre/version.bin` (9,600,799 rows × 4 bytes = ~37MB)
- This query: pre_join_index lookup key

### pre.stmt (dict_string, int16_t)
- File: `pre/stmt.bin` (9,600,799 rows × 2 bytes = ~18MB)
- Dict: `pre/stmt_dict.txt` — load at runtime to find 'IS' code
  ```cpp
  std::vector<std::string> stmt_dict;
  int16_t is_code = -1;
  for (int16_t c = 0; c < (int16_t)stmt_dict.size(); ++c)
      if (stmt_dict[c] == "IS") { is_code = c; break; }
  ```
- This query: filter `p.stmt = 'IS'` on matched pre rows; also GROUP BY key + output
- Decode: `stmt_dict[is_code].c_str()` for output — double-quote (C31)
- Note: since we filter p.stmt == is_code, stmt_code in output is always is_code;
  still decode from dict (C2, C18)

### pre.plabel (dict_string, int32_t)
- File: `pre/plabel.bin` (9,600,799 rows × 4 bytes = ~37MB)
- Dict: `pre/plabel_dict.txt` — load at runtime
- This query: GROUP BY key (code) + output column
- Decode: `plabel_dict[plabel_code].c_str()` — double-quote (C31)

## Indexes

### sub_adsh_index (hash on sub.adsh)
- File: `sf3.gendb/indexes/sub_adsh_index.bin`
- **Struct layout**:
  ```cpp
  struct SubSlot { int32_t adsh_code; int32_t sub_row; };
  // sizeof(SubSlot) == 8 bytes
  ```
- **File layout**: `uint32_t cap | cap × SubSlot`
- **Sentinel**: `adsh_code == -1`
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
- **Capacity**: `next_pow2(86135 × 2)` = 262,144 slots
- **Header parse** (C27/C32 — function scope):
  ```cpp
  const char* sub_idx_raw = /* mmap sf3.gendb/indexes/sub_adsh_index.bin */;
  uint32_t sub_cap  = *(const uint32_t*)sub_idx_raw;
  uint32_t sub_mask = sub_cap - 1;
  const SubSlot* sub_ht = (const SubSlot*)(sub_idx_raw + 4);
  ```
- **Probe** (C24): `hash_i32(adsh_code) & sub_mask`, bounded loop sub_cap iterations

### pre_join_index (hash_multivalue on pre.(adsh,tag,version))
- File: `sf3.gendb/indexes/pre_join_index.bin`
- **Struct layout** (verbatim from build_indexes.cpp):
  ```cpp
  struct PreSlot {
      int32_t  adsh_code;
      int32_t  tag_code;
      int32_t  version_code;
      uint32_t payload_offset;
      uint32_t payload_count;
  };
  // sizeof(PreSlot) == 20 bytes
  ```
- **File layout**: `uint32_t cap | cap × PreSlot | uint32_t payload_pool[]`
- **pool_start**: `pre_idx_raw + 4 + (size_t)pre_cap * 20`
- **Sentinel**: `adsh_code == -1`
- **Hash function** (verbatim from build_indexes.cpp):
  ```cpp
  static inline uint32_t hash_i32x3(int32_t a, int32_t b, int32_t c) {
      uint64_t h = (uint64_t)(uint32_t)a * 2654435761ULL;
      h ^= (uint64_t)(uint32_t)b * 2246822519ULL;
      h ^= (uint64_t)(uint32_t)c * 3266489917ULL;
      return (uint32_t)(h ^ (h >> 32));
  }
  ```
- **Capacity**: `next_pow2(n_unique × 2)` where n_unique = distinct (adsh,tag,version) in pre
- **Header parse** (C27/C32 — function scope):
  ```cpp
  const char* pre_idx_raw = /* mmap */;
  uint32_t pre_cap  = *(const uint32_t*)pre_idx_raw;
  uint32_t pre_mask = pre_cap - 1;
  const PreSlot* pre_ht = (const PreSlot*)(pre_idx_raw + 4);
  const uint32_t* pre_pool = (const uint32_t*)(pre_idx_raw + 4 + (size_t)pre_cap * 20);
  ```
- **Probe** (C24):
  ```cpp
  uint32_t slot = hash_i32x3(adsh_code, tag_code, version_code) & pre_mask;
  for (uint32_t probe = 0; probe < pre_cap; ++probe) {
      uint32_t s = (slot + probe) & pre_mask;
      if (pre_ht[s].adsh_code == -1) break;
      if (pre_ht[s].adsh_code == adsh_code &&
          pre_ht[s].tag_code == tag_code &&
          pre_ht[s].version_code == version_code) {
          for (uint32_t pi = 0; pi < pre_ht[s].payload_count; ++pi) {
              uint32_t pre_row = pre_pool[pre_ht[s].payload_offset + pi];
              if (pre_stmt[pre_row] == is_code) {
                  int32_t plabel_code = pre_plabel[pre_row];
                  // accumulate into group (name_code, is_code, tag_code, plabel_code)
              }
          }
          break;
      }
  }
  ```
- **Multi-value**: up to 20 pre_rows per (adsh,tag,version) key (from workload_analysis)
  — must iterate ALL payload entries and check stmt filter on each

## Aggregation
- Key struct:
  ```cpp
  struct Q6Key {
      int32_t name_code;
      int16_t stmt_code;
      int32_t tag_code;
      int32_t plabel_code;
  };
  ```
  (C15: all four GROUP BY dimensions must be in key; C30: missing any one → exact factor mismatch)
- Values: `int64_t sum_cents` (C29), `int64_t cnt`
- Thread-local maps merged after parallel scan (P17/P20 — mandatory; shared map → catastrophic)
- P23: size aggregation map for filtered cardinality, not raw table size

## Output
- Columns: s.name (string), p.stmt (string), n.tag (string), p.plabel (string),
  total_value (int64_t cents → formatted), cnt (int64_t)
- **C31**: double-quote ALL four string columns — plabel especially may contain commas
- Sort: total_value DESC; stable tiebreaker (e.g., name_code ASC) — C33
- LIMIT 200
