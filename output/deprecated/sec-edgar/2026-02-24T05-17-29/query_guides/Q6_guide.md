# Q6 Guide

## Query
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
| Table | Rows       | Role      | Sort Order                                | Block Size |
|-------|------------|-----------|-------------------------------------------|------------|
| num   | 39,401,761 | fact      | unsorted                                  | 100,000    |
| sub   | 86,135     | dimension | unsorted                                  | 100,000    |
| pre   | 9,600,799  | fact/dim  | adsh_code ASC, tag_code ASC, ver_code ASC | 100,000    |

## Column Reference

### num.uom (string_dict, int16_t — dict-encoded)
- File: `sf3.gendb/num/uom.bin` — 39,401,761 × 2 bytes = 78,803,522 bytes
- Dict: `sf3.gendb/num/uom_dict.txt` — ~15 distinct values
- This query: `WHERE n.uom = 'USD'` → find `usd_code` at runtime; filter `uom_code == usd_code`
- Selectivity: ~87.2% → ~34.3M rows pass
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
- This query: `WHERE value IS NOT NULL`, `SUM(n.value)`
- ⚠️ **C29 HARD CONSTRAINT**: SUM must accumulate as `int64_t` cents:
  ```cpp
  int64_t iv = llround(v * 100.0);
  sum_cents += iv;
  // Output: printf("%lld.%02lld", sum_cents/100, std::abs(sum_cents%100))
  ```

### num.adsh (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/adsh.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- This query: join key to sub (sub_adsh_hash) and pre (pre_triple_hash)

### num.tag (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/tag.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- Dict: `sf3.gendb/shared/tag_dict.txt` — ~200,000 distinct values
- This query: `GROUP BY n.tag` — use tag_code as group key component; output decoded string
- Also join key to pre_triple_hash

### num.version (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/version.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- This query: join key to pre_triple_hash (not output)

### sub.fy (integer, int32_t — raw)
- File: `sf3.gendb/sub/fy.bin` — 86,135 × 4 bytes = 344,540 bytes
- Null sentinel: `INT32_MIN`
- This query: `WHERE s.fy = 2023` → `fy[sub_row] == 2023`
- Selectivity: ~18.8% of sub rows → ~16,193 qualifying filings

### sub.name (string_dict, int32_t — dict-encoded)
- File: `sf3.gendb/sub/name.bin` — 86,135 × 4 bytes = 344,540 bytes
- Dict: `sf3.gendb/sub/name_dict.txt`
- This query: `GROUP BY s.name` — use name_code as group key component; output decoded string
- C31: **always double-quote name** — company names contain commas
- Dict loading pattern:
  ```cpp
  std::vector<std::string> name_dict;
  // read sf3.gendb/sub/name_dict.txt line by line
  // output: printf("\"%s\"", name_dict[name_code].c_str())
  ```

### pre.stmt (string_dict, int16_t — dict-encoded)
- File: `sf3.gendb/pre/stmt.bin` — 9,600,799 × 2 bytes = 19,201,598 bytes
- Dict: `sf3.gendb/pre/stmt_dict.txt` — 6 distinct values: BS, IS, CF, EQ, CI, UN
- Null sentinel: code = -1
- This query: `WHERE p.stmt = 'IS'` → find `is_code` at runtime; filter `stmt_code == is_code`
- Selectivity: ~17.8% of pre rows → ~1,708,942 rows
- This query: `GROUP BY p.stmt` — only 'IS' in output; use stmt_code as group key
- Dict loading pattern:
  ```cpp
  std::vector<std::string> stmt_dict;
  int16_t is_code = -1;
  for (int16_t i = 0; i < (int16_t)stmt_dict.size(); i++)
      if (stmt_dict[i] == "IS") { is_code = i; break; }
  ```

### pre.plabel (string_dict, int32_t — dict-encoded)
- File: `sf3.gendb/pre/plabel.bin` — 9,600,799 × 4 bytes = 38,403,196 bytes
- Dict: `sf3.gendb/pre/plabel_dict.txt`
- This query: `GROUP BY p.plabel` — use plabel_code as group key component; output decoded string
- C31: **always double-quote plabel** — presentation labels frequently contain commas
- Dict loading pattern:
  ```cpp
  std::vector<std::string> plabel_dict;
  // read sf3.gendb/pre/plabel_dict.txt line by line
  // output: printf("\"%s\"", plabel_dict[plabel_code].c_str())
  ```

## Indexes

### sub_adsh_hash (hash on sub.adsh)
- File: `sf3.gendb/sub/indexes/sub_adsh_hash.bin`
- File layout: `[uint32_t cap=262144][SubADSHSlot × 262144]`
- File size: 4 + 262144 × 16 = 4,194,308 bytes
- Header parse (MUST be at function scope — C32):
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
  uint32_t pos = (uint32_t)(hash_int32(adsh_code) & sub_mask);
  ```
- Probe → get `sub_row_id` → check `sub_fy[sub_row] == 2023`; read `sub_name[sub_row]`

### pre_triple_hash (hash on pre.(adsh, tag, version))
- File: `sf3.gendb/pre/indexes/pre_triple_hash.bin`
- File layout: `[uint32_t cap=33554432][PreTripleSlot × 33554432]`
- File size: 4 + 33554432 × 16 = 536,870,916 bytes (~512 MB)
- Header parse (MUST be at function scope — C32, C27):
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
      int32_t row_id;     // FIRST row in sorted pre for this key
  };  // 16 bytes
  ```
- Empty sentinel: `adsh_code == INT32_MIN`
- Hash function (verbatim — nested hash_combine):
  ```cpp
  static inline uint64_t hash_combine(uint64_t h1, uint64_t h2) {
      return h1 ^ (h2 * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL + (h1 << 6) + (h1 >> 2));
  }
  // Three-key probe:
  uint64_t h = hash_combine(
                   hash_combine(hash_int32(adsh_code), hash_int32(tag_code)),
                   hash_int32(ver_code));
  uint32_t pos = (uint32_t)(h & pth_mask);
  ```
- Probe pattern:
  ```cpp
  uint32_t pos = (uint32_t)(hash_combine(
      hash_combine(hash_int32(ak), hash_int32(tc)), hash_int32(vc)) & pth_mask);
  for (uint32_t probe = 0; probe < pth_cap; probe++) {  // C24: bounded
      uint32_t slot = (pos + probe) & pth_mask;
      if (pth_ht[slot].adsh_code == INT32_MIN) break;   // not found → skip row
      if (pth_ht[slot].adsh_code == ak &&
          pth_ht[slot].tag_code  == tc &&
          pth_ht[slot].ver_code  == vc) {
          int32_t first_row = pth_ht[slot].row_id;
          // scan forward while (adsh,tag,version) match
          break;
      }
  }
  ```
- **Multi-value semantics**: pre is sorted by (adsh_code, tag_code, ver_code). Index stores FIRST
  row_id. Scan forward to enumerate all pre rows with the same key:
  ```cpp
  int32_t r = first_row;
  while (r < pre_N &&
         pre_adsh[r] == ak && pre_tag[r] == tc && pre_ver[r] == vc) {
      if (pre_stmt[r] == is_code) {
          // use pre_stmt[r], pre_plabel[r] for group key + output
      }
      r++;
  }
  ```

## Query Analysis

### Join order (recommended)
1. **Scan num** (39.4M rows): apply `uom_code == usd_code && !std::isnan(value)`
2. **Probe sub_adsh_hash**: get sub_row → apply `sub_fy[sub_row] == 2023`; read `name_code`
3. **Probe pre_triple_hash**: get first_row_id → scan forward for `stmt_code == is_code`; read `plabel_code`
4. If all succeed → accumulate into aggregation map

### Combined filter selectivity
- uom='USD' (87.2%) × value NOT NULL (98%) × fy=2023 (18.8%) × stmt='IS' (17.8%)
- ≈ 87.2% × 98% × 18.8% × 17.8% ≈ 2.87% of 39.4M ≈ ~1.13M surviving rows
- Estimated groups (name, stmt, tag, plabel): ~50,000

### Aggregation struct
```cpp
struct Q6Group {
    int64_t sum_cents = 0;
    int64_t count     = 0;
};
// Key MUST include all 4 GROUP BY dimensions (C15, C30):
// name_code (int32_t) + stmt_code (int16_t) + tag_code (int32_t) + plabel_code (int32_t)
struct Q6Key {
    int32_t name_code;
    int32_t tag_code;
    int32_t plabel_code;
    int16_t stmt_code;
    int16_t _pad;
};
```
**C30 warning**: Q6 iter_2 bug — missing GROUP BY dimension caused 2× factor mismatch. Key MUST include
all four of: name_code, stmt_code, tag_code, plabel_code.

### Output encoding
```cpp
// C31: double-quote all string columns
printf("\"%s\",\"%s\",\"%s\",\"%s\",",
       name_dict[name_code].c_str(),
       stmt_dict[stmt_code].c_str(),
       tag_dict[tag_code].c_str(),
       plabel_dict[plabel_code].c_str());
// C29: total_value
int64_t sc = sum_cents;
printf("%lld.%02lld,%lld\n",
       (long long)(sc/100), (long long)std::abs(sc%100),
       (long long)count);
```

### Sort / LIMIT
- `ORDER BY total_value DESC LIMIT 200`
- C33: add stable tiebreaker (name_code ASC or tag_code ASC) to prevent non-determinism

### Prefetch hint
- pre_triple_hash is 512 MB — start madvise early:
  ```cpp
  madvise(pre_raw, pre_sz, MADV_WILLNEED);  // 512 MB
  madvise(sub_raw, sub_sz, MADV_WILLNEED);  // 4 MB
  ```
- P27: if both index files loaded, use parallel madvise sections
