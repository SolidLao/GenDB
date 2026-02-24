# Q4 Guide

## Query
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
| Table | Rows       | Role      | Sort Order                                | Block Size |
|-------|------------|-----------|-------------------------------------------|------------|
| num   | 39,401,761 | fact      | unsorted                                  | 100,000    |
| sub   | 86,135     | dimension | unsorted                                  | 100,000    |
| tag   | 1,070,662  | dimension | unsorted                                  | 100,000    |
| pre   | 9,600,799  | fact/dim  | adsh_code ASC, tag_code ASC, ver_code ASC | 100,000    |

## Column Reference

### num.uom (string_dict, int16_t — dict-encoded)
- File: `sf3.gendb/num/uom.bin` — 39,401,761 × 2 bytes = 78,803,522 bytes
- Dict: `sf3.gendb/num/uom_dict.txt` — ~15 distinct values
- This query: `WHERE n.uom = 'USD'` → find `usd_code` at runtime
- Selectivity: ~87.2% → ~34,338,335 rows pass
- Dict loading pattern:
  ```cpp
  std::vector<std::string> uom_dict;
  int16_t usd_code = -1;
  for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
      if (uom_dict[i] == "USD") { usd_code = i; break; }
  ```

### num.value (monetary, double — raw IEEE 754)
- File: `sf3.gendb/num/value.bin` — 39,401,761 × 8 bytes = 315,214,088 bytes
- Null sentinel: `NaN` — check with `std::isnan(v)`
- This query: `WHERE value IS NOT NULL`, `SUM(n.value)`, `AVG(n.value)`
- ⚠️ **C29 HARD CONSTRAINT**: SUM must accumulate as `int64_t` cents:
  ```cpp
  int64_t iv = llround(v * 100.0);
  sum_cents += iv;
  // AVG: avg_value = (double)sum_cents / (100.0 * count)
  ```

### num.adsh (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/adsh.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- This query: join key to sub (via sub_adsh_hash) and to pre (via pre_triple_hash)

### num.tag (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/tag.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- This query: join key to tag (via tag_pair_hash) and to pre (via pre_triple_hash)

### num.version (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/version.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- This query: join key to tag (via tag_pair_hash) and to pre (via pre_triple_hash)

### sub.sic (integer, int32_t — raw)
- File: `sf3.gendb/sub/sic.bin` — 86,135 × 4 bytes = 344,540 bytes
- Null sentinel: `INT32_MIN`
- This query: `WHERE s.sic BETWEEN 4000 AND 4999` → `sic >= 4000 && sic <= 4999`
- Selectivity: ~4.1% of sub rows → ~3,532 qualifying rows
- This query: `GROUP BY s.sic` — use raw int32_t as group key component

### sub.cik (integer, int32_t — raw)
- File: `sf3.gendb/sub/cik.bin` — 86,135 × 4 bytes = 344,540 bytes
- This query: `COUNT(DISTINCT s.cik)` — collect distinct cik values per group

### tag.abstract (integer, int32_t — raw)
- File: `sf3.gendb/tag/abstract.bin` — 1,070,662 × 4 bytes = 4,282,648 bytes
- This query: `WHERE t.abstract = 0` → `abstract[tag_row] == 0`
- Selectivity: ~98% of tag rows have abstract=0

### tag.tlabel (string_dict, int32_t — dict-encoded)
- File: `sf3.gendb/tag/tlabel.bin` — 1,070,662 × 4 bytes = 4,282,648 bytes
- Dict: `sf3.gendb/tag/tlabel_dict.txt`
- This query: `GROUP BY t.tlabel` — use tlabel_code as group key component; output decoded string
- C31: **always double-quote tlabel output** — tlabel strings frequently contain commas
  e.g., "Equity, Including Portion Attributable to Noncontrolling Interest"
- Dict loading pattern:
  ```cpp
  std::vector<std::string> tlabel_dict;
  // read sf3.gendb/tag/tlabel_dict.txt line by line
  // output: printf("\"%s\"", tlabel_dict[tlabel_code].c_str())
  ```

### pre.stmt (string_dict, int16_t — dict-encoded)
- File: `sf3.gendb/pre/stmt.bin` — 9,600,799 × 2 bytes = 19,201,598 bytes
- Dict: `sf3.gendb/pre/stmt_dict.txt` — 6 distinct values: BS, IS, CF, EQ, CI, UN
- Null sentinel: code = -1
- This query: `WHERE p.stmt = 'EQ'` → find `eq_code` at runtime; filter `stmt_code == eq_code`
- Selectivity: ~11% of pre rows → ~1,056,088 rows
- This query: `GROUP BY p.stmt` — only one value ('EQ') will appear in output
- Dict loading pattern:
  ```cpp
  std::vector<std::string> stmt_dict;
  int16_t eq_code = -1;
  for (int16_t i = 0; i < (int16_t)stmt_dict.size(); i++)
      if (stmt_dict[i] == "EQ") { eq_code = i; break; }
  ```

## Indexes

### sub_adsh_hash (hash on sub.adsh)
- File: `sf3.gendb/sub/indexes/sub_adsh_hash.bin`
- File layout: `[uint32_t cap=262144][SubADSHSlot × 262144]`
- File size: 4 + 262144 × 16 = 4,194,308 bytes
- Header parse (at function scope — C32):
  ```cpp
  uint32_t     sub_cap  = *(const uint32_t*)sub_raw;
  uint32_t     sub_mask = sub_cap - 1;
  const SubADSHSlot* sub_ht = (const SubADSHSlot*)(sub_raw + 4);
  ```
- Slot struct (exact from build_indexes.cpp):
  ```cpp
  struct SubADSHSlot {
      int32_t adsh_code;  // INT32_MIN = empty
      int32_t row_id;
      int32_t _pad0;
      int32_t _pad1;
  };  // 16 bytes
  ```
- Empty sentinel: `adsh_code == INT32_MIN`
- Hash function (verbatim):
  ```cpp
  static inline uint64_t hash_int32(int32_t key) {
      return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
  }
  ```
- Probe → get `sub_row_id` → check `sub_sic[sub_row] BETWEEN 4000 AND 4999`

### tag_pair_hash (hash on tag.(tag, version))
- File: `sf3.gendb/tag/indexes/tag_pair_hash.bin`
- File layout: `[uint32_t cap=4194304][TagPairSlot × 4194304]`
- File size: 4 + 4194304 × 16 = 67,108,868 bytes
- Header parse (at function scope — C32):
  ```cpp
  uint32_t     tph_cap  = *(const uint32_t*)tag_raw;
  uint32_t     tph_mask = tph_cap - 1;
  const TagPairSlot* tph_ht = (const TagPairSlot*)(tag_raw + 4);
  ```
- Slot struct (exact from build_indexes.cpp):
  ```cpp
  struct TagPairSlot {
      int32_t tag_code;   // INT32_MIN = empty
      int32_t ver_code;
      int32_t row_id;
      int32_t _pad;
  };  // 16 bytes
  ```
- Empty sentinel: `tag_code == INT32_MIN`
- Hash function (verbatim):
  ```cpp
  static inline uint64_t hash_combine(uint64_t h1, uint64_t h2) {
      return h1 ^ (h2 * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL + (h1 << 6) + (h1 >> 2));
  }
  // For tag_pair_hash:
  uint64_t h = hash_combine(hash_int32(tag_code), hash_int32(ver_code));
  uint32_t pos = (uint32_t)(h & tph_mask);
  ```
- Probe pattern:
  ```cpp
  uint32_t pos = (uint32_t)(hash_combine(hash_int32(tc), hash_int32(vc)) & tph_mask);
  for (uint32_t probe = 0; probe < tph_cap; probe++) {  // C24: bounded
      uint32_t slot = (pos + probe) & tph_mask;
      if (tph_ht[slot].tag_code == INT32_MIN) break;
      if (tph_ht[slot].tag_code == tc && tph_ht[slot].ver_code == vc) {
          int32_t tag_row = tph_ht[slot].row_id;
          // check tag_abstract[tag_row] == 0
          // read tag_tlabel[tag_row] for GROUP BY
          break;
      }
  }
  ```

### pre_triple_hash (hash on pre.(adsh, tag, version))
- File: `sf3.gendb/pre/indexes/pre_triple_hash.bin`
- File layout: `[uint32_t cap=33554432][PreTripleSlot × 33554432]`
- File size: 4 + 33554432 × 16 = 536,870,916 bytes (~512 MB)
- Header parse (at function scope — C32):
  ```cpp
  uint32_t     pth_cap  = *(const uint32_t*)pre_raw;
  uint32_t     pth_mask = pth_cap - 1;
  const PreTripleSlot* pth_ht = (const PreTripleSlot*)(pre_raw + 4);
  ```
- Slot struct (exact from build_indexes.cpp):
  ```cpp
  struct PreTripleSlot {
      int32_t adsh_code;  // INT32_MIN = empty
      int32_t tag_code;
      int32_t ver_code;
      int32_t row_id;     // FIRST row in sorted pre for this (adsh,tag,version)
  };  // 16 bytes
  ```
- Empty sentinel: `adsh_code == INT32_MIN`
- Hash function (verbatim — nested hash_combine):
  ```cpp
  // Three-key combination:
  uint64_t h = hash_combine(
                   hash_combine(hash_int32(adsh_code), hash_int32(tag_code)),
                   hash_int32(ver_code));
  uint32_t pos = (uint32_t)(h & pth_mask);
  ```
- Probe → get `first_row_id` in **sorted** pre → scan forward while (adsh,tag,version) match
- **Multi-value semantics**: pre is sorted by (adsh_code, tag_code, ver_code). The index stores
  only the FIRST row_id for each unique key. To enumerate ALL pre rows for a given (adsh,tag,version):
  ```cpp
  int32_t r = first_row_id;
  while (r < pre_N &&
         pre_adsh[r] == adsh_code &&
         pre_tag[r]  == tag_code  &&
         pre_ver[r]  == ver_code) {
      // check pre_stmt[r] == eq_code
      r++;
  }
  ```
- Usage in Q4: find first_row_id, scan forward checking `stmt_code == eq_code` to get stmt for join

## Query Analysis

### Join order (recommended)
1. **Scan num** (39.4M rows): apply `uom == usd_code && !std::isnan(value)`
2. **Probe sub_adsh_hash**: get sub_row → apply `sic BETWEEN 4000 AND 4999`
3. **Probe tag_pair_hash**: get tag_row → apply `abstract == 0`, read `tlabel_code`
4. **Probe pre_triple_hash**: get first_row_id → scan forward for `stmt == eq_code`
5. If all joins succeed + stmt filter passes → accumulate into aggregation hash map

### Filter selectivities (compounded)
- uom='USD': 87.2% of 39.4M ≈ 34.3M pass
- value NOT NULL: ~98% ≈ 33.6M pass
- sic 4000-4999: 4.1% of those → ~1.38M pass
- abstract=0: ~98% ≈ 1.35M pass
- stmt='EQ': ~11% ≈ 148K pass
- Estimated surviving rows: ~148,000 → ~2,000 groups

### Aggregation struct
```cpp
struct Q4Group {
    int64_t sum_cents = 0;
    int64_t count     = 0;       // for AVG = sum_cents / (100.0 * count)
    std::unordered_set<int32_t> cik_set;  // COUNT(DISTINCT cik)
};
// Key: (sic, tlabel_code, stmt_code) — all three GROUP BY dimensions (C15)
// Encode: struct { int32_t sic; int32_t tlabel_code; int16_t stmt_code; }
```
**C15 warning**: key MUST include all three GROUP BY columns: sic, tlabel_code, stmt_code.

### HAVING filter
```
HAVING COUNT(DISTINCT s.cik) >= 2
→ cik_set.size() >= 2
```

### Output encoding
```cpp
// C31: double-quote tlabel (contains commas), also quote stmt for safety
printf("%d,\"%s\",\"%s\",%zu,",
       sic, tlabel_dict[tlabel_code].c_str(), stmt_dict[stmt_code].c_str(),
       cik_set.size());
// C29: total_value and avg_value
int64_t sc = sum_cents;
printf("%lld.%02lld,", (long long)(sc/100), (long long)std::abs(sc%100));
double avg = (double)sum_cents / (100.0 * count);
printf("%.2f\n", avg);
```

### Sort / LIMIT
- `ORDER BY total_value DESC LIMIT 500`
- C33: add stable tiebreaker (e.g., sic ASC, tlabel_code ASC) if needed

### Prefetch hint
```cpp
madvise(pre_raw, pre_sz, MADV_WILLNEED);   // 512 MB — start early
madvise(tag_raw, tag_sz, MADV_WILLNEED);   // 64 MB
madvise(sub_raw, sub_sz, MADV_WILLNEED);   // 4 MB
```
