# Q24 Guide

```sql
-- Anti-join rewrite of NOT EXISTS correlated subquery
SELECT n.tag, n.version, COUNT(*) AS cnt, SUM(n.value) AS total
FROM num n
LEFT JOIN pre p ON n.tag = p.tag AND n.version = p.version AND n.adsh = p.adsh
WHERE n.uom = 'USD' AND n.ddate BETWEEN 20230101 AND 20231231
      AND n.value IS NOT NULL
      AND p.adsh IS NULL
GROUP BY n.tag, n.version
HAVING COUNT(*) > 10
ORDER BY cnt DESC
LIMIT 100;
```

## Table Stats
| Table | Rows       | Role | Sort Order | Block Size |
|-------|------------|------|------------|------------|
| num   | 39,401,761 | fact | (none)     | 65536      |
| pre   | 9,600,799  | fact | (none)     | 65536      |

## Query Analysis
- **Anti-join**: LEFT JOIN pre + `p.adsh IS NULL` check ≡ NOT EXISTS(SELECT 1 FROM pre WHERE ...)
- Filters on num: `uom = 'USD'` (~0.872), `ddate BETWEEN 20230101 AND 20231231` (~0.20),
  `value IS NOT NULL` (~0.99)
- Combined pre-join selectivity: ~0.172 → ~6.8M num rows pass all three num-side filters
- Anti-join: keep only num rows with NO matching (adsh,tag,version) in pre (p.adsh IS NULL)
- GROUP BY (n.tag, n.version); HAVING COUNT(*) > 10; LIMIT 100 by cnt DESC
- Strategy: (1) load zone map → skip ddate blocks outside [20230101,20231231];
  (2) scan qualifying num blocks with all three filters;
  (3) for each passing row, probe pre_join_index with (adsh,tag,version);
      if NO match (sentinel hit or empty), this row qualifies for anti-join;
  (4) accumulate into (tag_code, version_code) aggregation map;
  (5) HAVING filter; top-100 by cnt DESC
- **C29**: SUM(n.value) MUST use int64_t cents
- **C33**: stable tiebreaker for LIMIT 100

## Column Reference

### num.adsh (dict_string, int32_t)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/adsh_dict.txt` — global dict (same content as pre/adsh_dict.txt)
- This query: probe key for pre_join_index anti-join (NOT in GROUP BY or output)

### num.tag (dict_string, int32_t)
- File: `num/tag.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/tag_dict.txt` — global dict (same content as pre/tag_dict.txt)
- This query: GROUP BY key + pre_join_index probe key + output column
- Decode for output: `tag_dict[tag_code].c_str()` — double-quote (C31)

### num.version (dict_string, int32_t)
- File: `num/version.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/version_dict.txt` — global dict (same content as pre/version_dict.txt)
- This query: GROUP BY key + pre_join_index probe key + output column
- Decode for output: `version_dict[version_code].c_str()` — double-quote (C31)

### num.ddate (yyyymmdd_integer, int32_t)
- File: `num/ddate.bin` (39,401,761 rows × 4 bytes = ~150MB)
- **Encoding**: raw YYYYMMDD integer — NOT epoch days. Do NOT use date_utils.h.
- This query: `n.ddate BETWEEN 20230101 AND 20231231` → integer comparison:
  `ddate_val >= 20230101 && ddate_val <= 20231231`
- Selectivity: ~0.20 → ~7.9M rows pass
- **Zone-map-guided scan**: use num_ddate_zone_map to skip entire 65536-row blocks
  where `block.max_ddate < 20230101 || block.min_ddate > 20231231`

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
- **C29 HARD CONSTRAINT**: SUM MUST use int64_t cents:
  ```cpp
  int64_t iv = llround(v * 100.0);
  sum_cents += iv;
  ```
  Output: `printf("%lld.%02lld", sum_cents / 100, std::abs(sum_cents % 100))`

## Indexes

### num_ddate_zone_map (zone_map on num.ddate)
- File: `sf3.gendb/indexes/num_ddate_zone_map.bin`
- **Layout** (verbatim from build_indexes.cpp / ingest.cpp):
  `uint32_t num_blocks | per block: int32_t min_ddate, int32_t max_ddate, uint32_t row_count`
- Block size: 65536 rows (each block = one entry in zone map)
- Total blocks: `ceil(39401761 / 65536)` = 602 blocks (last block may be partial)
- **Each block entry** (12 bytes):
  ```cpp
  struct ZMBlock { int32_t min_ddate; int32_t max_ddate; uint32_t row_count; };
  ```
- **Usage pattern** (C19: only skip on zone-map indexed columns):
  ```cpp
  const char* zm_raw = /* mmap num_ddate_zone_map.bin */;
  uint32_t num_zm_blocks = *(const uint32_t*)zm_raw;
  const ZMBlock* zm = (const ZMBlock*)(zm_raw + 4);

  // Load num column arrays
  const int32_t* ddate_col   = /* mmap num/ddate.bin */;
  const int16_t* uom_col     = /* mmap num/uom.bin */;
  const int32_t* adsh_col    = /* mmap num/adsh.bin */;
  const int32_t* tag_col     = /* mmap num/tag.bin */;
  const int32_t* version_col = /* mmap num/version.bin */;
  const double*  value_col   = /* mmap num/value.bin */;

  uint32_t row_offset = 0;
  for (uint32_t b = 0; b < num_zm_blocks; ++b) {
      uint32_t block_rows = zm[b].row_count;
      // Skip block entirely if no overlap with [20230101, 20231231]
      if (zm[b].max_ddate < 20230101 || zm[b].min_ddate > 20231231) {
          row_offset += block_rows;
          continue;
      }
      // Process rows in this block
      for (uint32_t r = row_offset; r < row_offset + block_rows; ++r) {
          int32_t ddate = ddate_col[r];
          if (ddate < 20230101 || ddate > 20231231) continue;
          if (uom_col[r] != usd_code) continue;
          double v = value_col[r];
          if (std::isnan(v)) continue;
          // All num-side filters pass — anti-join probe
          // ...
      }
      row_offset += block_rows;
  }
  ```

### pre_join_index (hash_multivalue on pre.(adsh,tag,version)) — used as anti-join probe
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
- **Capacity**: `next_pow2(n_unique × 2)` where n_unique = distinct (adsh,tag,version) keys in pre
- **Header parse** (C27/C32 — function scope):
  ```cpp
  const char* pre_idx_raw = /* mmap */;
  uint32_t pre_cap  = *(const uint32_t*)pre_idx_raw;
  uint32_t pre_mask = pre_cap - 1;
  const PreSlot* pre_ht = (const PreSlot*)(pre_idx_raw + 4);
  // Note: payload_pool not needed for anti-join (only existence check)
  ```
- **Anti-join probe** (C24: bounded loop; p.adsh IS NULL means NO match found):
  ```cpp
  bool found_in_pre = false;
  uint32_t slot = hash_i32x3(adsh_code, tag_code, version_code) & pre_mask;
  for (uint32_t probe = 0; probe < pre_cap; ++probe) {
      uint32_t s = (slot + probe) & pre_mask;
      if (pre_ht[s].adsh_code == -1) break;   // empty slot → not in pre
      if (pre_ht[s].adsh_code == adsh_code &&
          pre_ht[s].tag_code == tag_code &&
          pre_ht[s].version_code == version_code) {
          found_in_pre = true;
          break;
      }
  }
  if (!found_in_pre) {
      // p.adsh IS NULL condition satisfied → accumulate into aggregation
      int64_t iv = llround(v * 100.0);
      // add to group (tag_code, version_code): sum_cents += iv; cnt++;
  }
  ```
- **Anti-join semantics**: probe for existence only — do NOT iterate payload_pool;
  ANY match (regardless of payload_count) disqualifies the row

## Aggregation
- Key struct: `struct Q24Key { int32_t tag_code; int32_t version_code; }`
- Values: `int64_t sum_cents` (C29), `int64_t cnt`
- HAVING: `cnt > 10`
- Estimated groups: medium cardinality (distinct non-pre-covered (tag,version) pairs)
- Thread-local reduction then merge (P17/P20)

## Output
- Columns: n.tag (string), n.version (string), cnt (int64_t), total (formatted)
- Double-quote tag and version strings (C31)
- Sort: cnt DESC; stable tiebreaker (e.g., tag_code ASC or string ASC) — C33
- LIMIT 100
- date_encoding: ddate is raw YYYYMMDD int32_t — no epoch conversion
