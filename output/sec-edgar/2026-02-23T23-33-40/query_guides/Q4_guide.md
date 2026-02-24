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

## Table Stats
| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | (none)     | 65536      |
| sub   | 86,135     | dimension | (none)     | 65536      |
| tag   | 1,070,662  | dimension | (none)     | 65536      |
| pre   | 9,600,799  | fact       | (none)     | 65536      |

## Query Analysis
- **Four-way join**: num→sub (adsh), num→tag (tag,version), num→pre (adsh,tag,version)
- Filters: `n.uom = 'USD'` (~0.872), `s.sic BETWEEN 4000 AND 4999` (~0.061),
  `t.abstract = 0` (~0.95), `p.stmt = 'EQ'` (~0.112), `n.value IS NOT NULL` (~0.99)
- Combined selectivity ~0.006 → ~230K num rows expected to survive all filters
- GROUP BY (s.sic, t.tlabel_code, p.stmt_code): medium cardinality
- Aggregates: COUNT(DISTINCT s.cik), SUM(n.value), AVG(n.value)
- HAVING: COUNT(DISTINCT s.cik) >= 2
- Strategy: (1) probe sub_adsh_index per num row → get sub_row → check sic BETWEEN 4000-4999;
  (2) probe tag_index per (tag_code, version_code) → get tag_row → check abstract==0;
  (3) probe pre_join_index per (adsh_code, tag_code, version_code) → get pre_rows[];
      for each pre_row check stmt=='EQ';
  (4) accumulate into aggregation map keyed by (sic, tlabel_code, stmt_code)
- **C29**: SUM/AVG on num.value MUST use int64_t cents accumulator
- **C15**: GROUP BY key must include ALL THREE: sic, tlabel_code, stmt_code
- **C31**: double-quote tlabel strings in output (may contain commas)

## Column Reference

### num.adsh (dict_string, int32_t)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/adsh_dict.txt` — global dict
- This query: join key for sub_adsh_index probe AND pre_join_index probe

### num.tag (dict_string, int32_t)
- File: `num/tag.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/tag_dict.txt` — global dict (same content as tag/tag_dict.txt and pre/tag_dict.txt)
- This query: join key for tag_index probe AND pre_join_index probe

### num.version (dict_string, int32_t)
- File: `num/version.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/version_dict.txt` — global dict (same content as tag/version_dict.txt and pre/version_dict.txt)
- This query: join key for tag_index probe AND pre_join_index probe

### num.uom (dict_string, int16_t)
- File: `num/uom.bin` (39,401,761 rows × 2 bytes = ~75MB)
- Dict: `num/uom_dict.txt` — load at runtime to find 'USD' code
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
- **C29 HARD CONSTRAINT**: SUM and AVG MUST use int64_t cents:
  ```cpp
  int64_t iv = llround(v * 100.0);
  sum_cents += iv;
  cnt++;
  // AVG output: (double)sum_cents / cnt / 100.0
  // SUM output: printf("%lld.%02lld", sum_cents/100, std::abs(sum_cents%100))
  ```

### sub.adsh (dict_string, int32_t)
- File: `sub/adsh.bin` (86,135 rows × 4 bytes = ~336KB)
- This query: looked up via sub_adsh_index

### sub.sic (integer, int32_t)
- File: `sub/sic.bin` (86,135 rows × 4 bytes = ~336KB)
- This query: filter `s.sic BETWEEN 4000 AND 4999` → `sub_sic[sub_row] >= 4000 && sub_sic[sub_row] <= 4999`
- Selectivity: ~0.061 → ~5.25K qualifying sub rows
- Also: GROUP BY key (raw int32_t, output directly)

### sub.cik (integer, int32_t)
- File: `sub/cik.bin` (86,135 rows × 4 bytes = ~336KB)
- This query: `COUNT(DISTINCT s.cik)` — per group, collect distinct cik values
  (small group cardinality → use `std::unordered_set<int32_t>` or sorted vector per group)

### tag.tag (dict_string, int32_t)
- File: `tag/tag.bin` (1,070,662 rows × 4 bytes = ~4MB)
- This query: join key (looked up via tag_index alongside version)

### tag.version (dict_string, int32_t)
- File: `tag/version.bin` (1,070,662 rows × 4 bytes = ~4MB)
- This query: join key (looked up via tag_index alongside tag)

### tag.abstract (integer, int32_t)
- File: `tag/abstract.bin` (1,070,662 rows × 4 bytes = ~4MB)
- This query: filter `t.abstract = 0` → `tag_abstract[tag_row] == 0`
- Selectivity: ~0.95 (nearly all rows qualify)

### tag.tlabel (dict_string, int32_t)
- File: `tag/tlabel.bin` (1,070,662 rows × 4 bytes = ~4MB)
- Dict: `tag/tlabel_dict.txt` — load at runtime
- This query: GROUP BY key (code) + output column — decode via `tlabel_dict[code]`
- **C31**: tlabel strings may contain commas → always double-quote: `printf("\"%s\"", ...)`

### pre.adsh (dict_string, int32_t)
- File: `pre/adsh.bin` (9,600,799 rows × 4 bytes = ~37MB)
- This query: join key (pre_join_index lookup)

### pre.tag (dict_string, int32_t)
- File: `pre/tag.bin` (9,600,799 rows × 4 bytes = ~37MB)
- This query: join key (pre_join_index lookup)

### pre.version (dict_string, int32_t)
- File: `pre/version.bin` (9,600,799 rows × 4 bytes = ~37MB)
- This query: join key (pre_join_index lookup)

### pre.stmt (dict_string, int16_t)
- File: `pre/stmt.bin` (9,600,799 rows × 2 bytes = ~18MB)
- Dict: `pre/stmt_dict.txt` — load at runtime to find code for 'EQ'
  ```cpp
  std::vector<std::string> stmt_dict;
  int16_t eq_code = -1;
  for (int16_t c = 0; c < (int16_t)stmt_dict.size(); ++c)
      if (stmt_dict[c] == "EQ") { eq_code = c; break; }
  ```
- Selectivity: ~0.112 → ~1.08M pre rows qualify
- Also: GROUP BY key (code) + output — decode via `stmt_dict[code]`

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
  const char* sub_idx_raw = /* mmap */;
  uint32_t sub_cap  = *(const uint32_t*)sub_idx_raw;
  uint32_t sub_mask = sub_cap - 1;
  const SubSlot* sub_ht = (const SubSlot*)(sub_idx_raw + 4);
  ```
- **Probe** (C24): `hash_i32(adsh_code) & sub_mask`, bounded loop sub_cap iterations

### tag_index (hash on tag.(tag,version))
- File: `sf3.gendb/indexes/tag_index.bin`
- **Struct layout** (verbatim from build_indexes.cpp):
  ```cpp
  struct TagSlot { int32_t tag_code; int32_t version_code; int32_t tag_row; };
  // sizeof(TagSlot) == 12 bytes
  ```
- **File layout**: `uint32_t cap | cap × TagSlot`
- **Sentinel**: `tag_code == -1`
- **Hash function** (verbatim from build_indexes.cpp):
  ```cpp
  static inline uint32_t hash_i32x2(int32_t a, int32_t b) {
      uint64_t h = (uint64_t)(uint32_t)a * 2654435761ULL
                 ^ (uint64_t)(uint32_t)b * 2246822519ULL;
      return (uint32_t)(h ^ (h >> 32));
  }
  ```
- **Capacity**: `next_pow2(1070662 × 2)` = 4,194,304 slots (2^22); ~50MB
- **Header parse** (C27/C32 — function scope):
  ```cpp
  const char* tag_idx_raw = /* mmap */;
  uint32_t tag_cap  = *(const uint32_t*)tag_idx_raw;
  uint32_t tag_mask = tag_cap - 1;
  const TagSlot* tag_ht = (const TagSlot*)(tag_idx_raw + 4);
  ```
- **Probe** (C24):
  ```cpp
  uint32_t slot = hash_i32x2(tag_code, version_code) & tag_mask;
  for (uint32_t probe = 0; probe < tag_cap; ++probe) {
      uint32_t s = (slot + probe) & tag_mask;
      if (tag_ht[s].tag_code == -1) break;
      if (tag_ht[s].tag_code == tag_code && tag_ht[s].version_code == version_code) {
          int32_t tag_row = tag_ht[s].tag_row;
          // check tag_abstract[tag_row] == 0
          // then use tag_tlabel[tag_row] as GROUP BY key
          break;
      }
  }
  ```

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
- **pool_start**: `file_ptr + 4 + cap * 20` bytes from file start
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
- **Capacity**: `next_pow2(n_unique × 2)` where n_unique = distinct (adsh,tag,version) tuples in pre
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
          // iterate matched pre rows
          for (uint32_t pi = 0; pi < pre_ht[s].payload_count; ++pi) {
              uint32_t pre_row = pre_pool[pre_ht[s].payload_offset + pi];
              // check pre_stmt[pre_row] == eq_code
              // if yes: contribute to aggregation
          }
          break;
      }
  }
  ```
- **Multi-value format**: for a given (adsh,tag,version) key, there can be multiple pre rows
  (right_max_duplicates=20 per workload_analysis). Each match yields a pre_row index.

## Aggregation
- Key struct: `struct Q4Key { int32_t sic; int32_t tlabel_code; int16_t stmt_code; }`
  (C15: all three GROUP BY dimensions must be in key)
- Values: `int64_t sum_cents`, `int64_t cnt`, `std::unordered_set<int32_t> cik_set` (per group)
- Thread-local reduction then merge (P17/P20)
- HAVING: `cik_set.size() >= 2`

## Output
- Columns: sic (int32_t), tlabel (string), stmt (string), num_companies (int64_t), total_value, avg_value
- **C31**: double-quote tlabel AND stmt strings
- Sort: total_value DESC; LIMIT 500
- AVG output: `(double)sum_cents / cnt / 100.0` — use `printf("%.2f", avg_val)`
