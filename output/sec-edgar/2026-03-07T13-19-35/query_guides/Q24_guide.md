# Q24 Guide

```sql
-- Rewritten from NOT EXISTS anti-join (portable form)
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

The `p.adsh IS NULL` condition (anti-join) means: keep only num rows that have **no**
matching row in pre for the same `(adsh, tag, version)` triple.

## Column Reference

### uom_code (dict_code, int8_t) — num
- File: `num/uom_code.bin` (39,401,761 rows × 1 byte)
- This query: `WHERE uom = 'USD'` → `uom_code == usd_code`.
  Load: `usd_code = load_dict("indexes/uom_codes.bin")["USD"]`.
- Combined with ddate filter, zone maps can skip blocks where both conditions exclude.

### ddate (integer, int32_t) — num
- File: `num/ddate.bin` (39,401,761 rows × 4 bytes)
- This query: `BETWEEN 20230101 AND 20231231` → `ddate >= 20230101 && ddate <= 20231231`.
- Encoding: raw YYYYMMDD integer — range predicates work directly on integer comparison.
- Selectivity: ~0.2% of all num rows (very selective combined with uom='USD').
- Zone map benefit: combined filter. The zone map stores min/max ddate per block;
  skip block `b` if `zm[b].max_ddate < 20230101 || zm[b].min_ddate > 20231231`
  **in addition to** the uom_code block-skip check.

### adsh_code (dict_code, int32_t) — num
- File: `num/adsh_code.bin` (39,401,761 rows × 4 bytes)
- This query: composite anti-join key with `tagver_code`:
  ```cpp
  uint64_t key = ((uint64_t)(uint32_t)adsh_code << 32) | (uint32_t)tagver_code;
  bool in_pre = anti_set.count(key) > 0;
  if (!in_pre) { /* row passes anti-join */ }
  ```

### tagver_code (dict_code, int32_t) — num
- File: `num/tagver_code.bin` (39,401,761 rows × 4 bytes)
- This query: composite anti-join key; also used as index into tag table for output decode.
- Note: `tagver_code == -1` for num rows with (tag,version) not found in tag.csv.
  Such rows cannot match pre (pre also uses tagver_code), so they always pass the anti-join.
  Handle: cast to uint32_t before key construction — `-1` becomes `0xFFFFFFFF`,
  which is a valid (non-matching) key in the anti-set.

### value (decimal, double) — num
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- This query: `SUM(n.value)` per (tag, version) group.

### tag_str (varlen string) — tag
- Files: `tag/tag_offsets.bin` (1,070,663 × uint32_t), `tag/tag_data.bin` (char[])
- This query: GROUP BY `n.tag`, SELECT `n.tag` — decoded via `tagver_code`.
- Access pattern:
  ```cpp
  string tag(tag_data + tag_offsets[tagver_code],
             tag_offsets[tagver_code+1] - tag_offsets[tagver_code]);
  ```
- Grouping strategy: group by `tagver_code` (int32_t) as integer proxy; decode tag string
  only for output rows (after HAVING filter and top-100 selection).

### version_str (varlen string) — tag
- Files: `tag/version_offsets.bin` (1,070,663 × uint32_t), `tag/version_data.bin` (char[])
- This query: GROUP BY `n.version`, SELECT `n.version` — decoded via `tagver_code`.
- Access pattern:
  ```cpp
  string ver(version_data + version_offsets[tagver_code],
             version_offsets[tagver_code+1] - version_offsets[tagver_code]);
  ```
- Grouping strategy: same as tag_str — group by `tagver_code`; decode at output.

## Table Stats

| Table | Rows       | Role                   | Sort Order               | Block Size |
|-------|------------|------------------------|--------------------------|------------|
| num   | 39,401,761 | fact (scan)            | (uom_code, ddate)        | 100,000    |
| pre   | 9,600,799  | anti-join (set probe)  | (adsh_code, tagver_code) | 100,000    |
| tag   | 1,070,662  | dimension (decode)     | tagver_code order        | 100,000    |

## Query Analysis
- **Anti-join setup**: load `indexes/pre_adsh_tagver_set.bin` into an `unordered_set<uint64_t>`
  at query startup. This set contains all unique `(adsh_code, tagver_code)` pairs in pre.
  A num row passes the anti-join if its key is **absent** from this set:
  ```cpp
  // Load pre_adsh_tagver_set.bin
  FILE* f = fopen("indexes/pre_adsh_tagver_set.bin", "rb");
  uint64_t n_unique; fread(&n_unique, sizeof(uint64_t), 1, f);
  unordered_set<uint64_t> anti_set;
  anti_set.reserve(n_unique * 2);
  vector<uint64_t> keys(n_unique);
  fread(keys.data(), sizeof(uint64_t), n_unique, f);
  fclose(f);
  for (uint64_t k : keys) anti_set.insert(k);
  // n_unique = 8,631,890 pairs; ~66MB for the raw array, ~130MB for hash set
  ```

- **Zone-map-guided scan of num**:
  ```cpp
  for (size_t b = 0; b < n_blocks; ++b) {
      // Skip block if uom_code range excludes usd_code
      if (zm[b].min_uom > usd_code || zm[b].max_uom < usd_code) continue;
      // Skip block if ddate range excludes [20230101, 20231231]
      if (zm[b].max_ddate < 20230101 || zm[b].min_ddate > 20231231) continue;
      // Process rows in block [b*100000, min((b+1)*100000, N))
      for (size_t i = lo; i < hi; ++i) {
          if (uom_code[i] != usd_code) continue;
          if (ddate[i] < 20230101 || ddate[i] > 20231231) continue;
          // Anti-join check
          uint64_t key = ((uint64_t)(uint32_t)adsh_code[i] << 32)
                       | (uint32_t)tagver_code[i];
          if (anti_set.count(key)) continue;  // skip: has pre match
          // Accumulate for GROUP BY tagver_code
          group_cnt[tagver_code[i]]++;
          group_sum[tagver_code[i]] += value[i];
      }
  }
  ```
  The dual zone map filter (uom + ddate) is highly effective: USD 2023 rows are a
  tiny subset (~0.17% combined selectivity ≈ ~67K rows surviving both filters).

- **HAVING COUNT(*) > 10**: filter groups; expected ~2315 tag/version pairs in dataset,
  most with few 2023 USD orphan rows; HAVING will reduce to a small number.
- **Output**: sort by cnt DESC, limit 100; decode tag and version strings for result rows.

## Indexes

### pre_adsh_tagver_set (sorted_array on pre.adsh_code, pre.tagver_code)
- File: `indexes/pre_adsh_tagver_set.bin`
- Layout:
  ```
  uint64_t n_unique      // = 8,631,890 unique (adsh_code, tagver_code) pairs
  n_unique × uint64_t    // sorted ascending
  ```
- Key construction (verbatim from build_indexes.cpp line 92):
  ```cpp
  keys[i] = ((uint64_t)(uint32_t)adsh_codes[i] << 32) | (uint32_t)tagver_codes[i];
  ```
  The cast to `(uint32_t)` before shifting is critical: it ensures that
  `tagver_code = -1` (int32_t) becomes `0xFFFFFFFF` (uint32_t) rather than
  sign-extending across the upper 32 bits.
- **For Q24**: load all `n_unique` keys into `unordered_set<uint64_t>` for O(1) anti-join
  probes. The file header is a `uint64_t` (8 bytes), not `uint32_t`.
- File size: 8 + 8,631,890 × 8 = 69,055,128 bytes (~65.9 MB).

### num_zone_maps (zone_map on uom_code, ddate)
- File: `indexes/num_zone_maps.bin`
- Layout:
  ```cpp
  uint32_t n_blocks;   // = 395 (ceil(39401761 / 100000))
  // Then n_blocks × ZoneMap:
  struct ZoneMap { int8_t min_uom, max_uom; int32_t min_ddate, max_ddate; };
  // sizeof(ZoneMap) = 12 bytes (2-byte implicit padding between max_uom and min_ddate)
  // Field offsets: min_uom@0, max_uom@1, [pad@2-3], min_ddate@4, max_ddate@8
  ```
- File size: 4 + 395 × 12 = 4,744 bytes.
- **For Q24** — dual-condition block skipping:
  ```cpp
  // Skip block b if EITHER:
  //   (a) uom range excludes usd_code:
  if (zm[b].min_uom > usd_code || zm[b].max_uom < usd_code) continue;
  //   (b) ddate range has no overlap with [20230101, 20231231]:
  if (zm[b].max_ddate < 20230101 || zm[b].min_ddate > 20231231) continue;
  ```
  Since num is sorted by (uom_code ASC, ddate ASC), USD blocks are contiguous and
  2023-ddate USD blocks are further concentrated — the combined filter is very aggressive.

### uom_codes (dict_file on uom)
- File: `indexes/uom_codes.bin`
- Layout: `uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }`
- Usage: `usd_code = load_dict("indexes/uom_codes.bin")["USD"]`.
  NEVER hardcode the numeric value of usd_code.
