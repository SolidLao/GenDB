# Q24 Guide

```sql
-- Anti-join: num rows with no matching pre entry (LEFT JOIN ... WHERE pre.adsh IS NULL)
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

## Column Reference

### num.uom (dict_int8, int8_t — pre-seeded)
- File: `num/uom.bin` (39,401,761 rows × 1 byte)
- Dict: `shared/uom.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- Pre-seeded order: USD=0, shares=1, pure=2
- This query: `uom = 'USD'` → load dict at runtime; `uom_code == usd_code`
- USD selectivity ≈ 87%

### num.ddate (int32_t, raw — yyyymmdd integer)
- File: `num/ddate.bin` (39,401,761 rows × 4 bytes)
- Encoding: stored as yyyymmdd integer (e.g., 20230101)
- This query: `ddate BETWEEN 20230101 AND 20231231` →
  `ddate[row] >= 20230101 && ddate[row] <= 20231231`
- Selectivity ≈ 46% of num rows per workload_analysis (among rows matching other filters)
- Combined with USD: ≈ 87% × 46% ≈ 40% of num → ~15.7M rows before anti-join

### num.value (double, raw — NaN = NULL)
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- Null sentinel: `std::isnan(v)`
- This query: `value IS NOT NULL`; aggregated as `SUM(n.value)`

### num.adsh (dict_int32, int32_t — FK into sub)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes)
- Dict: `sub/adsh.bin` — N×char[20] sorted; code = sub row_id
- This query: part of anti-join probe key: `(adsh_code, tag_code, ver_code)`

### num.tag (dict_int32, int32_t — shared dict)
- File: `num/tag.bin` (39,401,761 rows × 4 bytes)
- Dict: `shared/tag_numpre.dict` — format `[n:uint32][len:uint16, bytes...]*n`
  (198,311 entries)
- This query: anti-join probe key; GROUP BY n.tag; output column (decode via dict)

### num.version (dict_int32, int32_t — shared dict)
- File: `num/version.bin` (39,401,761 rows × 4 bytes)
- Dict: `shared/version_numpre.dict` — format same; 83,815 entries
- This query: anti-join probe key; GROUP BY n.version; output column (decode via dict)

### pre.adsh / pre.tag / pre.version
- These are the build side of the anti-join — all represented in `pre_existence_hash`
- No individual pre column files are read during query execution; the existence hash
  subsumes them

## Table Stats

| Table | Rows       | Role      | Sort Order    | Block Size |
|-------|------------|-----------|---------------|------------|
| num   | 39,401,761 | fact       | (uom, ddate)  | 100,000    |
| pre   | 9,600,799  | build side | (stmt, adsh)  | 100,000    |

## Query Analysis

**Phase 1 — Pre-existence hash is pre-built at index build time**
- `pre_existence_hash` already contains all (adsh_code, tag_code, ver_code) tuples from pre
- No pre column scan needed at query time

**Phase 2 — Scan num with zone map acceleration:**
- Load `indexes/num_zonemaps.bin`; identify blocks where:
  `uom_min <= usd_code <= uom_max AND ddate_max >= 20230101 AND ddate_min <= 20231231`
- Because num is sorted by (uom_code, ddate), USD rows are contiguous, and within USD,
  ddate is sorted. Zone maps give tight block ranges:
  - Skip all non-USD blocks (uom_max < usd_code or uom_min > usd_code)
  - Within USD blocks, skip blocks with `ddate_max < 20230101` or `ddate_min > 20231231`

**Phase 3 — Anti-join via pre_existence_hash:**
- For each qualifying num row (uom=USD, ddate in range, value not null):
  - Compute `hash3i(adsh_code, tag_code, ver_code)` (see exact function below)
  - Probe `pre_existence_hash`; if NO matching slot found → row passes anti-join
  - If slot found (key matches) → row is excluded (has a pre entry)

**Phase 4 — Aggregate passing rows:**
- GROUP BY `(tag_code:int32, ver_code:int32)` → hash map → `(count, sum_value)`
- HAVING `count > 10`; decode tag + version strings; sort by cnt DESC; emit top 100
- Estimated groups: ~20,000 per workload_analysis

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
- Usage: dual filter on (uom=USD, ddate BETWEEN 20230101 AND 20231231)
  - First dimension (uom): skip blocks entirely outside USD code range
  - Second dimension (ddate): within USD blocks, skip blocks outside [20230101, 20231231]
  - Since num is sorted (uom, ddate), 2023 date range forms a contiguous subrange within
    the USD segment → zone maps provide near-perfect block pruning

### pre_existence_hash (Robin Hood hash for anti-join)
- File: `indexes/pre_existence_hash.bin`
- Struct (build_indexes.cpp lines 269–273, `#pragma pack(push,1)`):
  ```cpp
  struct PreExistSlot {
      int32_t adsh;  // adsh_code; INT32_MIN = empty slot
      int32_t tag;   // tag_code
      int32_t ver;   // ver_code
  };
  ```
- File layout: `[capacity:uint32][n_entries:uint32][slots: PreExistSlot × capacity]`
- capacity = next_pow2(9,600,799 × 2) = 33,554,432
- Empty slot: `adsh == INT32_MIN`
- Hash function `hash3i` (build_indexes.cpp lines 75–91) — exact verbatim:
  ```cpp
  uint64_t hash3i(int32_t a, int32_t b, int32_t c) {
      uint64_t h = 14695981039346656037ULL;
      uint32_t ua = (uint32_t)a, ub = (uint32_t)b, uc = (uint32_t)c;
      h ^= ua;       h *= 1099511628211ULL;
      h ^= (ua>>8);  h *= 1099511628211ULL;
      h ^= (ua>>16); h *= 1099511628211ULL;
      h ^= (ua>>24); h *= 1099511628211ULL;
      h ^= ub;       h *= 1099511628211ULL;
      h ^= (ub>>8);  h *= 1099511628211ULL;
      h ^= (ub>>16); h *= 1099511628211ULL;
      h ^= (ub>>24); h *= 1099511628211ULL;
      h ^= uc;       h *= 1099511628211ULL;
      h ^= (uc>>8);  h *= 1099511628211ULL;
      h ^= (uc>>16); h *= 1099511628211ULL;
      h ^= (uc>>24); h *= 1099511628211ULL;
      return h ? h : 1;
  }
  ```
- Probing: `pos = kh & (cap-1)`; linear probe forward; stop when:
  - `slot.adsh == INT32_MIN` → key absent (anti-join match: row has NO pre entry)
  - `slot.adsh == adsh_code && slot.tag == tag_code && slot.ver == ver_code` → key present
    (anti-join miss: row has a pre entry, exclude from output)
- Memory: 33,554,432 slots × 12 bytes/slot = 402 MB (fits in 376 GB RAM)
- Load factor at build time: 9,600,799 / 33,554,432 ≈ 0.286
