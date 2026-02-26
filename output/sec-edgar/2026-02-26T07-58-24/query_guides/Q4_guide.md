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

## Column Reference

### num.uom (dict_int8, int8_t — pre-seeded)
- File: `num/uom.bin` (39,401,761 rows × 1 byte)
- Dict: `shared/uom.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- Pre-seeded order: USD=0, shares=1, pure=2
- This query: `uom = 'USD'` → load dict at runtime; `uom_code == usd_code`

### num.value (double, raw — NaN = NULL)
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- Null sentinel: `std::isnan(v)`
- This query: `value IS NOT NULL`; aggregated as `SUM(n.value)` and `AVG(n.value)`
  (maintain `sum` and `count` per group for AVG)

### num.adsh (dict_int32, int32_t — FK into sub)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes)
- Dict: `sub/adsh.bin` — N×char[20] sorted; code = sub row_id
- This query: join to sub (direct array index); also part of pre join key (adsh, tag, ver)

### num.tag (dict_int32, int32_t — shared dict)
- File: `num/tag.bin` (39,401,761 rows × 4 bytes)
- Dict: `shared/tag_numpre.dict` — format `[n:uint32][len:uint16, bytes...]*n`
  (198,311 entries; codes shared between num and pre)
- This query: join key for tag→tag table; join key for num→pre lookup

### num.version (dict_int32, int32_t — shared dict)
- File: `num/version.bin` (39,401,761 rows × 4 bytes)
- Dict: `shared/version_numpre.dict` — format same; 83,815 entries
- This query: join key for tag→tag table; join key for num→pre lookup

### sub.sic (int16_t, raw)
- File: `sub/sic.bin` (86,135 rows × 2 bytes)
- This query: `s.sic BETWEEN 4000 AND 4999` → `(int16_t)4000 <= sic[adsh_code] <= (int16_t)4999`
- Selectivity ≈ 4.7% of sub rows → ~4,048 filings pass sic filter

### sub.cik (int32_t, raw)
- File: `sub/cik.bin` (86,135 rows × 4 bytes)
- This query: `COUNT(DISTINCT s.cik)` per group; HAVING `>= 2`

### tag.abstract (int8_t, raw)
- File: `tag/abstract.bin` (1,070,662 rows × 1 byte)
- This query: `t.abstract = 0` → `abstract[tag_row_id] == 0`
- Selectivity: 96.4% of tag rows have abstract=0 (per workload_analysis)

### tag.tlabel (varlen)
- Offsets: `tag/tlabel.offsets` — (1,070,662 + 1) × int64_t
- Data: `tag/tlabel.data` — raw bytes
- This query: GROUP BY t.tlabel; output column
- Null: `offsets[i] == offsets[i+1]`
- Read: `data[offsets[i] .. offsets[i+1]]`

### pre.stmt (dict_int8, int8_t — pre-seeded)
- File: `pre/stmt.bin` (9,600,799 rows × 1 byte)
- Dict: `shared/stmt.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- Pre-seeded order: BS=0, IS=1, CF=2, EQ=3, CI=4
- This query: `p.stmt = 'EQ'` → load dict at runtime; find code for "EQ" →
  `stmt_code == eq_code` (expected: 3, but MUST load from dict)
- Selectivity: EQ rows ≈ 13% of pre (1,242,132 / 9,600,799)

### pre.adsh (dict_int32, int32_t)
- File: `pre/adsh.bin` (9,600,799 rows × 4 bytes)
- Same encoding as num.adsh (shared sub/adsh.bin dict; code = sub row_id)

### pre.tag (dict_int32, int32_t)
- File: `pre/tag.bin` (9,600,799 rows × 4 bytes)
- Same shared dict as num.tag (`shared/tag_numpre.dict`)

### pre.version (dict_int32, int32_t)
- File: `pre/version.bin` (9,600,799 rows × 4 bytes)
- Same shared dict as num.version (`shared/version_numpre.dict`)

## Table Stats

| Table | Rows       | Role      | Sort Order    | Block Size |
|-------|------------|-----------|---------------|------------|
| num   | 39,401,761 | fact       | (uom, ddate)  | 100,000    |
| sub   | 86,135     | dimension  | adsh          | 10,000     |
| tag   | 1,070,662  | dimension  | (none)        | 100,000    |
| pre   | 9,600,799  | fact       | (stmt, adsh)  | 100,000    |

## Query Analysis

**Multi-table join strategy:**

1. **Build tag lookup array** (O(1) per probe):
   - Load `tag/abstract.bin`; for each tag row_id: store abstract flag
   - Load `tag/tlabel` varlen strings for all non-abstract rows
   - No hash join needed — tag_pk_hash gives tag_str → row_id, but here num.tag is already
     a code pointing into the same shared dict. Need: `num.tag_code → tag.row_id` mapping.
   - This requires resolving the shared dict code to a tag table row_id via tag_pk_hash.

2. **Build tag code → tag row_id map** using tag_pk_hash:
   - For each tag table row, its row_id is known; its dict codes in shared/tag_numpre.dict
     may differ from tag table row ordering. Need to build:
     `tag_code (in shared dict) → tag.row_id (in tag table)`
   - Method: iterate shared/tag_numpre.dict entries, reconstruct tag string, look up in
     tag_pk_hash to get tag row_id; store in array `tag_code_to_row[tag_code] = row_id`

3. **Scan num (USD filter) + sub filter + tag filter + pre join:**
   - For each num row: `uom==USD && !isnan(value) && sub/sic[adsh_code] BETWEEN 4000-4999`
   - Look up tag row_id: `tag_row = tag_code_to_row[num.tag_code]`; skip if `abstract[tag_row] != 0`
   - Look up pre: binary search `pre_key_sorted` for (adsh_code, tag_code, ver_code)
   - For each matching pre row_id: check `pre/stmt.bin[pre_row_id] == eq_code`
   - Accumulate into group map keyed by `(sic, tlabel_ptr, stmt_code)` → `(sum, count, cik_set)`

4. **HAVING + output:**
   - Keep groups with `COUNT(DISTINCT cik) >= 2`
   - Decode tlabel, sort by total_value DESC, emit top 500

## Indexes

### num_zonemaps (zone_map on uom, ddate)
- File: `indexes/num_zonemaps.bin`
- Layout (from build_indexes.cpp lines 99–134):
  ```
  [n_blocks : int32]
  per block (100,000 rows):
    uom_min  : int8_t
    uom_max  : int8_t
    ddate_min: int32_t
    ddate_max: int32_t
  ```
  n_blocks = 395 blocks
- Usage: skip non-USD blocks; num sorted by uom_code so USD (code=0) is contiguous.

### tag_pk_hash (Robin Hood hash on tag+version → tag.row_id)
- File: `indexes/tag_pk_hash.bin`
- Struct (build_indexes.cpp lines 210–216, `#pragma pack(push,1)`):
  ```cpp
  struct TagHashSlot {
      uint64_t key_hash;  // combined hash of tag and version strings
      int32_t  row_id;    // tag table row index; INT32_MIN = empty slot
      int32_t  _pad;
  };
  ```
- File layout: `[capacity:uint32][num_entries:uint32][slots: TagHashSlot × capacity]`
- capacity = next_pow2(1,070,662 × 2) = 2,097,152
- Empty slot: `row_id == INT32_MIN`
- Hash function (build_indexes.cpp lines 231–238):
  ```cpp
  uint64_t h1 = fnv64(t.data(), t.size());   // FNV-64a of tag string
  uint64_t h2 = fnv64(v.data(), v.size());   // FNV-64a of version string
  uint64_t kh = h1 ^ (h2 * 0x9e3779b97f4a7c15ULL);
  if (!kh) kh = 1;
  ```
  where `fnv64` is:
  ```cpp
  uint64_t h = 14695981039346656037ULL;
  for each byte b: h ^= b; h *= 1099511628211ULL;
  return h ? h : 1;
  ```
- Probing: `pos = kh & (cap-1)`; linear probe; match when `slot.key_hash == kh`
  (verify by re-reading tag+version strings to exclude hash collisions)
- Usage: resolve shared-dict tag_code → tag table row_id for abstract check and tlabel

### tag_zonemaps (zone_map on abstract)
- File: `indexes/tag_zonemaps.bin`
- Layout (from build_indexes.cpp lines 172–200):
  ```
  [n_blocks : int32]
  per block (100,000 rows):
    abstract_min: int8_t
    abstract_max: int8_t
  ```
  n_blocks = ceil(1,070,662 / 100,000) = 11 blocks
- Usage: skip tag blocks where `abstract_min == 1 && abstract_max == 1` (all-abstract).
  96.4% of tag rows have abstract=0 so most blocks are mixed; zone maps are a minor aid here.

### pre_key_sorted (sorted array on adsh+tag+version → row_id)
- File: `indexes/pre_key_sorted.bin`
- Struct (build_indexes.cpp lines 325–329, `#pragma pack(push,1)`):
  ```cpp
  struct PreKeyEntry {
      int32_t adsh, tag, ver, row_id;
  };
  ```
- File layout: `[n:uint32][entries: PreKeyEntry × n]`
  n = 9,600,799; sorted by (adsh, tag, ver)
- Usage: for each qualifying num row (adsh_code, tag_code, ver_code), binary search
  for lower_bound and upper_bound of matching (adsh, tag, ver) — yields all pre row_ids
  sharing that key. Then check `pre/stmt.bin[row_id] == eq_code` for each.
- Binary search comparison order: adsh → tag → ver

### pre_zonemaps (zone_map on stmt, adsh)
- File: `indexes/pre_zonemaps.bin`
- Layout (from build_indexes.cpp lines 136–170):
  ```
  [n_blocks : int32]
  per block (100,000 rows):
    stmt_min : int8_t
    stmt_max : int8_t
    adsh_min : int32_t
    adsh_max : int32_t
  ```
  n_blocks = 97 blocks
- Usage: pre sorted by (stmt, adsh). EQ blocks (code=3) are contiguous. Zone maps let
  the pre scan skip non-EQ stmt blocks when building a pre-side hash for the join.
  For Q4's pre_key_sorted approach, zone maps are used for initial block pruning when
  doing a full pre scan rather than point lookups.
