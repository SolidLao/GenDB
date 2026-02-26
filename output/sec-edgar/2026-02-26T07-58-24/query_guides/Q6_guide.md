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

## Column Reference

### num.uom (dict_int8, int8_t — pre-seeded)
- File: `num/uom.bin` (39,401,761 rows × 1 byte)
- Dict: `shared/uom.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- Pre-seeded order: USD=0, shares=1, pure=2
- This query: `uom = 'USD'` → load dict at runtime; `uom_code == usd_code`
- USD selectivity ≈ 87% of num rows

### num.value (double, raw — NaN = NULL)
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- Null sentinel: `std::isnan(v)`
- This query: `value IS NOT NULL`; aggregated as `SUM(n.value)` and `COUNT(*)`

### num.adsh (dict_int32, int32_t — FK into sub)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes)
- Dict: `sub/adsh.bin` — N×char[20] sorted; code = sub row_id
- This query: join to sub (direct array index); also part of pre join key

### num.tag (dict_int32, int32_t — shared dict)
- File: `num/tag.bin` (39,401,761 rows × 4 bytes)
- Dict: `shared/tag_numpre.dict` — format `[n:uint32][len:uint16, bytes...]*n`
  (198,311 entries; shared between num and pre)
- This query: join key for num→pre; GROUP BY n.tag; output column (decode via dict)

### num.version (dict_int32, int32_t — shared dict)
- File: `num/version.bin` (39,401,761 rows × 4 bytes)
- Dict: `shared/version_numpre.dict` — format same; 83,815 entries
- This query: join key for num→pre only (not in GROUP BY or SELECT output)

### sub.fy (int16_t, raw)
- File: `sub/fy.bin` (86,135 rows × 2 bytes)
- This query: `s.fy = 2023` → `fy[adsh_code] == (int16_t)2023`
- Selectivity ≈ 19% (16,219 / 86,135 rows per workload_analysis)

### sub.name (dict_int32, int32_t)
- File: `sub/name.bin` (86,135 rows × 4 bytes)
- Dict: `sub/name.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- This query: GROUP BY s.name; output column (decode via dict)
- Composite group key: (name_code:int32, stmt_code:int8, tag_code:int32) + plabel identity

### pre.stmt (dict_int8, int8_t — pre-seeded)
- File: `pre/stmt.bin` (9,600,799 rows × 1 byte)
- Dict: `shared/stmt.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- Pre-seeded order: BS=0, IS=1, CF=2, EQ=3, CI=4
- This query: `p.stmt = 'IS'` → load dict at runtime; find code for "IS" →
  `stmt_code == is_code` (expected: 1, but MUST load from dict)
- Selectivity: IS rows ≈ 18% of pre (1,767,049 / 9,600,799)

### pre.adsh (dict_int32, int32_t)
- File: `pre/adsh.bin` (9,600,799 rows × 4 bytes)
- Same encoding as num.adsh (shared sub/adsh.bin dict)

### pre.tag (dict_int32, int32_t)
- File: `pre/tag.bin` (9,600,799 rows × 4 bytes)
- Same shared dict as num.tag (`shared/tag_numpre.dict`)

### pre.version (dict_int32, int32_t)
- File: `pre/version.bin` (9,600,799 rows × 4 bytes)
- Same shared dict as num.version (`shared/version_numpre.dict`)

### pre.plabel (varlen)
- Offsets: `pre/plabel.offsets` — (9,600,799 + 1) × int64_t
- Data: `pre/plabel.data` — raw bytes
- This query: GROUP BY p.plabel; output column
- Null: `offsets[i] == offsets[i+1]` (zero-length span = null/empty)
- Read: `data.data() + offsets[i]` for length `offsets[i+1] - offsets[i]`
- Note: plabel is varlen so grouping by plabel requires string comparison or interning;
  use pre_row_id as group proxy during aggregation, then decode at output time

## Table Stats

| Table | Rows       | Role      | Sort Order    | Block Size |
|-------|------------|-----------|---------------|------------|
| num   | 39,401,761 | fact       | (uom, ddate)  | 100,000    |
| sub   | 86,135     | dimension  | adsh          | 10,000     |
| pre   | 9,600,799  | fact       | (stmt, adsh)  | 100,000    |

## Query Analysis

**Recommended join strategy — forward scan num, probe pre per row:**

1. **Build fy=2023 bitset for sub** (86,135 bits = 10KB):
   - Load `sub/fy.bin`; build boolean array `fy2023[adsh_code]`

2. **Scan num (USD + value IS NOT NULL):**
   - Use num_zonemaps to skip non-USD blocks
   - For each passing row: check `fy2023[adsh_code]`
   - Combined filter selectivity: ≈ 87% (USD) × 19% (fy=2023) ≈ 16.5% of num → ~6.5M rows

3. **Probe pre_key_sorted for (adsh_code, tag_code, ver_code):**
   - Binary search for lower/upper bound in pre_key_sorted
   - For each matching pre row_id: check `pre/stmt.bin[row_id] == is_code`
   - pre IS rows ≈ 18% of 9.6M → ~1.73M IS rows total; intersection with fy=2023 further reduces

4. **Aggregate into group map:**
   - Group key: `(name_code:int32, stmt_code:int8, tag_code:int32, pre_row_id:int32)`
     (use pre_row_id as proxy for plabel identity — unique per (adsh,tag,ver) key in pre)
   - Accumulate `sum_value` and `count`
   - Estimated groups ≈ 100,000 per workload_analysis

5. **Decode + output:**
   - For each group: decode name via sub/name.dict, tag via shared/tag_numpre.dict,
     read plabel via pre/plabel.offsets + pre/plabel.data using pre_row_id
   - Sort by total_value DESC; emit top 200

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
- Usage: skip non-USD blocks; USD rows are contiguous after null/negative-code prefix.
  Block range to scan: find first block with `uom_min <= usd_code <= uom_max`, scan
  until all USD blocks are consumed.

### pre_key_sorted (sorted array on adsh+tag+version → row_id)
- File: `indexes/pre_key_sorted.bin`
- Struct (build_indexes.cpp lines 325–329, `#pragma pack(push,1)`):
  ```cpp
  struct PreKeyEntry {
      int32_t adsh, tag, ver, row_id;
  };
  ```
- File layout: `[n:uint32][entries: PreKeyEntry × n]`
  n = 9,600,799; sorted by (adsh, tag, ver) lexicographically
- Usage: for each qualifying num row with (adsh_code, tag_code, ver_code):
  - `std::lower_bound` on entries comparing `(adsh, tag, ver)` tuple
  - Walk forward while `entry.adsh == adsh_code && entry.tag == tag_code && entry.ver == ver_code`
  - Collect all matching row_ids; check `pre/stmt.bin[row_id] == is_code`
- Slot size: 16 bytes; 9.6M entries = 153 MB; fits comfortably in 376 GB RAM

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
- Usage for Q6: pre sorted by (stmt, adsh). IS rows (code=1) form a contiguous segment.
  If building a pre-side hash of IS rows, use zone maps to scan only IS-stmt blocks.
  Alternatively, if using pre_key_sorted for point lookups from num side, pre_zonemaps
  serve as a consistency check / alternative scan path.
