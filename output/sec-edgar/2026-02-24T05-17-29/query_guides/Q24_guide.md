# Q24 Guide

## Query
```sql
-- Anti-join: num rows (uom='USD', ddate in 2023, value NOT NULL)
-- that have NO matching row in pre for (tag, version, adsh).
-- Rewritten from NOT EXISTS correlated subquery to LEFT JOIN + IS NULL.
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
| Table | Rows       | Role      | Sort Order                                | Block Size |
|-------|------------|-----------|-------------------------------------------|------------|
| num   | 39,401,761 | fact      | unsorted                                  | 100,000    |
| pre   | 9,600,799  | anti-join | adsh_code ASC, tag_code ASC, ver_code ASC | 100,000    |

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

### num.ddate (integer_yyyymmdd, int32_t — raw)
- File: `sf3.gendb/num/ddate.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- Encoding: **raw YYYYMMDD integer** — NOT epoch days. No date conversion needed.
- This query: `WHERE n.ddate BETWEEN 20230101 AND 20231231`
  → C++ comparison: `ddate >= 20230101 && ddate <= 20231231` (raw integer compare)
- Selectivity: ~27% of rows → ~10.6M pass the date filter
- **Do NOT use date_utils.h** — this column is raw int32_t YYYYMMDD, not epoch days

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
- Dict: `sf3.gendb/shared/adsh_dict.txt` — 86,135 values
- This query: anti-join probe key into pre_triple_hash

### num.tag (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/tag.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- Dict: `sf3.gendb/shared/tag_dict.txt` — ~200,000 distinct values
- This query: `GROUP BY n.tag` — use tag_code as group key; anti-join probe key; output decoded string
- Dict loading pattern:
  ```cpp
  std::vector<std::string> tag_dict;
  // read sf3.gendb/shared/tag_dict.txt line by line
  // output: printf("\"%s\"", tag_dict[tag_code].c_str()) — C31 defensive quoting
  ```

### num.version (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/version.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- Dict: `sf3.gendb/shared/version_dict.txt` — ~90,000 distinct values
- This query: `GROUP BY n.version` — use ver_code as group key; anti-join probe key; output decoded string
- Dict loading pattern:
  ```cpp
  std::vector<std::string> version_dict;
  // read sf3.gendb/shared/version_dict.txt line by line
  // output: printf("\"%s\"", version_dict[ver_code].c_str())
  ```

## Indexes

### pre_triple_hash (hash on pre.(adsh, tag, version)) — used for ANTI-JOIN
- File: `sf3.gendb/pre/indexes/pre_triple_hash.bin`
- File layout: `[uint32_t cap=33554432][PreTripleSlot × 33554432]`
- File size: 4 + 33554432 × 16 = 536,870,916 bytes (~512 MB)
- Header parse (MUST be at function scope — C32, C27):
  ```cpp
  uint32_t     pth_cap  = *(const uint32_t*)pre_raw;     // = 33554432
  uint32_t     pth_mask = pth_cap - 1;                   // = 33554431
  const PreTripleSlot* pth_ht = (const PreTripleSlot*)(pre_raw + 4);
  ```
- Slot struct (exact from build_indexes.cpp):
  ```cpp
  struct PreTripleSlot {
      int32_t adsh_code;  // INT32_MIN = empty
      int32_t tag_code;
      int32_t ver_code;
      int32_t row_id;
  };  // 16 bytes
  ```
- Empty sentinel: `adsh_code == INT32_MIN`
- Hash function (verbatim):
  ```cpp
  static inline uint64_t hash_int32(int32_t key) {
      return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
  }
  static inline uint64_t hash_combine(uint64_t h1, uint64_t h2) {
      return h1 ^ (h2 * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL + (h1 << 6) + (h1 >> 2));
  }
  // Three-key: (adsh, tag, version)
  uint64_t h = hash_combine(
                   hash_combine(hash_int32(adsh_code), hash_int32(tag_code)),
                   hash_int32(ver_code));
  uint32_t pos = (uint32_t)(h & pth_mask);
  ```
- **Anti-join probe pattern**: for each num row that passes filters, probe the index.
  If NOT FOUND → this num row has no matching pre row → include in output.
  ```cpp
  bool found_in_pre = false;
  uint32_t pos = (uint32_t)(hash_combine(
      hash_combine(hash_int32(ak), hash_int32(tc)), hash_int32(vc)) & pth_mask);
  for (uint32_t probe = 0; probe < pth_cap; probe++) {  // C24: bounded
      uint32_t slot = (pos + probe) & pth_mask;
      if (pth_ht[slot].adsh_code == INT32_MIN) break;   // empty → not found
      if (pth_ht[slot].adsh_code == ak &&
          pth_ht[slot].tag_code  == tc &&
          pth_ht[slot].ver_code  == vc) {
          found_in_pre = true;
          break;
      }
  }
  if (!found_in_pre) {
      // p.adsh IS NULL → accumulate into aggregation
  }
  ```

## Query Analysis

### Execution Strategy
1. **Prefetch**: madvise pre_triple_hash (512 MB, MADV_WILLNEED) before scan
2. **Scan num** (39.4M rows): apply `uom_code == usd_code && ddate >= 20230101 && ddate <= 20231231 && !std::isnan(value)`
   - Combined selectivity: ~87.2% × 27% × 98% ≈ ~23% → ~9.0M rows survive all three filters
3. **Anti-join**: for each surviving row, probe pre_triple_hash with `(adsh_code, tag_code, ver_code)`
   - If NOT found → include row
   - Note: the join condition is `n.tag=p.tag AND n.version=p.version AND n.adsh=p.adsh` — all three keys required
4. **Aggregate**: for surviving anti-join rows, GROUP BY `(tag_code, ver_code)`:
   ```cpp
   // Key: (int64_t)tag_code << 32 | (uint32_t)ver_code  OR  struct{int32_t,int32_t}
   // Per group: count++; sum_cents += llround(value * 100.0)
   ```
5. **HAVING**: filter groups with `count > 10`
6. **Sort/Limit**: sort by cnt DESC, take top 100
   - C33: add tiebreaker (tag_code ASC, ver_code ASC) for determinism

### Estimated cardinalities
- Rows passing all 3 num filters: ~9.0M
- Rows with no pre match (anti-join survivors): expect small fraction — most USD 2023 rows have pre entries
  Estimated: ~5,000–50,000 rows → ~5,000 groups → HAVING(>10) leaves ~few hundred
- Aggregation hash cap: `next_pow2(5000 × 2) = 16384`

### Aggregation struct
```cpp
struct Q24Group {
    int64_t count     = 0;
    int64_t sum_cents = 0;
};
// Key: struct { int32_t tag_code; int32_t ver_code; }
// Hash: hash_combine(hash_int32(tag_code), hash_int32(ver_code))
```

### Output encoding
```cpp
// C31: quote tag and version strings defensively
printf("\"%s\",\"%s\",%lld,",
       tag_dict[tag_code].c_str(),
       version_dict[ver_code].c_str(),
       (long long)count);
// C29: SUM as int64_t cents
int64_t sc = sum_cents;
printf("%lld.%02lld\n",
       (long long)(sc/100), (long long)std::abs(sc%100));
```

### Prefetch hint
```cpp
// pre_triple_hash is 512 MB — critical to madvise early
madvise(pre_raw, pre_sz, MADV_WILLNEED);
// Then immediately start scanning num so I/O and compute overlap
```

### Performance notes
- P19: pre_triple_hash (512 MB) fits in DRAM (376 GB total) but exceeds L3 cache (44 MB).
  Anti-join probes are random accesses into 512 MB → expect cache misses. Prefetch helps.
- P21: Bloom filter pre-screening not needed here since we're doing anti-join (absence check).
  The pre_triple_hash probe IS the anti-join — empty-slot detection = not-found = anti-join pass.
- Thread-local aggregation is safe: group by (tag_code, ver_code) → ~5,000 groups → minimal contention.
