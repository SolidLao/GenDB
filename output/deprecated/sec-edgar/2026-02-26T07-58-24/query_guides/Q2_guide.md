# Q2 Guide

```sql
-- Decorrelated form of correlated MAX subquery
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

## Column Reference

### num.uom (dict_int8, int8_t — pre-seeded)
- File: `num/uom.bin` (39,401,761 rows × 1 byte)
- Dict: `shared/uom.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- Pre-seeded insertion order (ingest.cpp lines 393–394): USD=0, shares=1, pure=2
- This query: `uom = 'pure'` → load `shared/uom.dict` at runtime; find code for "pure" →
  compare `uom_code == pure_code` (expected: 2, but MUST load from dict, never hardcode)
- Note: 201 distinct UOM values; codes 128–200 stored as negative int8_t (-128..-56) due to
  signed overflow. USD≈87% of rows, pure≈0.3% of rows.

### num.value (double, raw — NaN = NULL)
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- Null sentinel: IEEE 754 NaN (`std::isnan(v)`)
- This query: `value IS NOT NULL` → `!std::isnan(value)`; used in MAX aggregation and
  equality join `n.value = m.max_value`

### num.adsh (dict_int32, int32_t — FK into sub)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes)
- Dict: `sub/adsh.bin` — N×char[20] sorted array; code = row index (0-based)
- This query: join key for `n.adsh = s.adsh` (both encoded as same int32_t code);
  also GROUP BY adsh in subquery m

### num.tag (dict_int32, int32_t)
- File: `num/tag.bin` (39,401,761 rows × 4 bytes)
- Dict: `shared/tag_numpre.dict` — format `[n:uint32][len:uint16, bytes...]*n`
  (198,311 entries; built by num parse, extended by pre parse in ingest.cpp)
- This query: GROUP BY tag in subquery m; join key `n.tag = m.tag`;
  output column (decode via dict)

### num.version (dict_int32, int32_t)
- File: `num/version.bin` (39,401,761 rows × 4 bytes)
- Dict: `shared/version_numpre.dict` — format same as tag dict
  (83,815 entries)
- Not in SELECT output but needed for deduplication context (not used in Q2 filtering)

### sub.adsh (char[20], raw_fixed20 — PK)
- File: `sub/adsh.bin` (86,135 rows × 20 bytes; sorted array = master adsh dict)
- Code = row index; lookup: code c → `sub/adsh.bin[c*20 .. c*20+20]`

### sub.fy (int16_t, raw)
- File: `sub/fy.bin` (86,135 rows × 2 bytes)
- This query: `s.fy = 2022` → `fy[row] == (int16_t)2022`
- Selectivity ≈ 21% (16,890 / 86,135 rows pass)

### sub.name (dict_int32, int32_t)
- File: `sub/name.bin` (86,135 rows × 4 bytes)
- Dict: `sub/name.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- This query: SELECT output only; decode: load `sub/name.dict`; `strs[name_code]`

## Table Stats

| Table | Rows       | Role      | Sort Order    | Block Size |
|-------|------------|-----------|---------------|------------|
| num   | 39,401,761 | fact      | (uom, ddate)  | 100,000    |
| sub   | 86,135     | dimension | adsh          | 10,000     |

## Query Analysis

**Phase 1 — Build subquery m (pure-uom MAX per adsh+tag):**
- Scan `num/uom.bin` + `num/value.bin` + `num/adsh.bin` + `num/tag.bin`
- Keep rows where `uom_code == pure_code && !isnan(value)`
- Selectivity of pure ≈ 0.3% → ~118,000 rows pass
- GROUP BY (adsh_code, tag_code) → build hash map: `{adsh_code, tag_code} → max_value`
- Estimated groups: ~118,000 (most adsh+tag combos unique for pure)

**Phase 2 — Probe outer num (same filter) against m:**
- Same scan as Phase 1; for each passing row, lookup `{adsh_code, tag_code}` in m map
- Keep rows where `value == max_value` (double equality)

**Phase 3 — Join to sub for fy=2022 filter:**
- For each row passing Phase 2: `adsh_code` is valid sub row_id
- Check `sub/fy.bin[adsh_code] == 2022`
- ~21% of sub rows have fy=2022; after join with pure rows, further reduces result

**Phase 4 — Decode + top-100:**
- Decode name (sub/name.dict), tag (shared/tag_numpre.dict)
- Sort by (value DESC, name, tag), emit top 100

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
  n_blocks = ceil(39,401,761 / 100,000) = 395 blocks
- Usage: num is sorted by (uom_code, ddate). USD rows (code=0) occupy ~87% contiguously.
  pure rows (code=2) cluster after shares (code=1). Use zone maps to skip blocks where
  `uom_max < pure_code || uom_min > pure_code`. This reduces blocks scanned from 395 to
  those containing pure rows only (≈2 blocks of ~118k pure rows).

### sub_adsh_hash (hash on adsh — Robin Hood, TagHashSlot)
- File: `indexes/sub_adsh_hash.bin`
- Struct (build_indexes.cpp lines 210–216, `#pragma pack(push,1)`):
  ```cpp
  struct TagHashSlot {
      uint64_t key_hash;  // FNV-64a of 20-byte adsh string
      int32_t  row_id;    // sub row index (= adsh_code); INT32_MIN = empty
      int32_t  _pad;      // padding
  };
  ```
- File layout: `[capacity:uint32][num_entries:uint32][slots: TagHashSlot × capacity]`
- capacity = next_pow2(86,135 × 2) = 262,144
- Empty slot: `row_id == INT32_MIN`
- Hash function (fnv64, build_indexes.cpp lines 66–72):
  ```cpp
  uint64_t fnv64(const void* data, size_t len) {
      uint64_t h = 14695981039346656037ULL;
      const uint8_t* p = (const uint8_t*)data;
      for (size_t i = 0; i < len; i++) { h ^= p[i]; h *= 1099511628211ULL; }
      return h ? h : 1;
  }
  ```
  Key: `fnv64(adsh_str, 20)` (20-byte zero-padded fixed string from sub/adsh.bin)
- Probing: linear probe from `pos = kh & (cap-1)`; stop at empty slot or `key_hash == kh`
  (verify by reading the 20-byte adsh from sub/adsh.bin to confirm no collision)
- Usage for Q2: NOT directly needed — since `num.adsh` is already encoded as `adsh_code`
  (= sub row_id), the join `n.adsh = s.adsh` is a direct array index into `sub/fy.bin[adsh_code]`

### num_adsh_index (inverted index — adsh_code → list of num row_ids)
- File: `indexes/num_adsh_index.bin`
- Struct (build_indexes.cpp lines 424–426):
  ```cpp
  struct AdshEntry { int32_t adsh_code, offset, count; };
  ```
- File layout:
  ```
  [n_entries : int32]
  [header    : AdshEntry × n_entries]   (sorted by adsh_code)
  [total_rowids : uint32]
  [rowids    : int32 × total_rowids]
  ```
- Usage for Q2: Can be used to probe all num rows for a specific adsh_code when
  building the m subquery join — binary search header by adsh_code, then iterate
  `rowids[offset .. offset+count]`. Most useful when joining from sub→num direction.
