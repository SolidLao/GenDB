# Q3 Guide

```sql
SELECT s.name, s.cik, SUM(n.value) AS total_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = 'USD' AND s.fy = 2022 AND n.value IS NOT NULL
GROUP BY s.name, s.cik
HAVING SUM(n.value) > (
    SELECT AVG(sub_total) FROM (
        SELECT SUM(n2.value) AS sub_total
        FROM num n2
        JOIN sub s2 ON n2.adsh = s2.adsh
        WHERE n2.uom = 'USD' AND s2.fy = 2022 AND n2.value IS NOT NULL
        GROUP BY s2.cik
    ) avg_sub
)
ORDER BY total_value DESC
LIMIT 100;
```

## Column Reference

### num.uom (dict_int8, int8_t — pre-seeded)
- File: `num/uom.bin` (39,401,761 rows × 1 byte)
- Dict: `shared/uom.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- Pre-seeded insertion order (ingest.cpp lines 393–394): USD=0, shares=1, pure=2
- This query: `uom = 'USD'` → load `shared/uom.dict` at runtime; find code for "USD" →
  compare `uom_code == usd_code` (expected: 0, but MUST load from dict, never hardcode)
- USD rows ≈ 87% of num (≈34.3M rows pass)

### num.value (double, raw — NaN = NULL)
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- Null sentinel: IEEE 754 NaN (`std::isnan(v)`)
- This query: `value IS NOT NULL` → `!std::isnan(value)`; aggregated as `SUM(n.value)`

### num.adsh (dict_int32, int32_t — FK into sub)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes)
- Dict: `sub/adsh.bin` — N×char[20] sorted array; code = row index (= sub row_id)
- This query: join key — `adsh_code` directly indexes `sub/fy.bin[adsh_code]` and
  `sub/name.bin[adsh_code]`, `sub/cik.bin[adsh_code]`

### sub.fy (int16_t, raw)
- File: `sub/fy.bin` (86,135 rows × 2 bytes)
- This query: `s.fy = 2022` → `fy[adsh_code] == (int16_t)2022`
- Selectivity ≈ 21% (16,890 filings have fy=2022); combined with USD filter ≈ 18% of num rows

### sub.cik (int32_t, raw)
- File: `sub/cik.bin` (86,135 rows × 4 bytes)
- This query: GROUP BY s.cik; also used in HAVING subquery (GROUP BY s2.cik)
- ~8,886 distinct CIK values in sub

### sub.name (dict_int32, int32_t)
- File: `sub/name.bin` (86,135 rows × 4 bytes)
- Dict: `sub/name.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- This query: GROUP BY s.name; output column (decode for final result)
- Note: GROUP BY (s.name, s.cik) is finer-grained than GROUP BY s.cik alone;
  each company can file under multiple name variants

## Table Stats

| Table | Rows       | Role      | Sort Order   | Block Size |
|-------|------------|-----------|--------------|------------|
| num   | 39,401,761 | fact       | (uom, ddate) | 100,000    |
| sub   | 86,135     | dimension  | adsh         | 10,000     |

## Query Analysis

**HAVING subquery — compute threshold (one-time scalar):**
1. Scan num/uom.bin + num/adsh.bin + num/value.bin
2. Keep rows: `uom_code == usd_code && !isnan(value)`
3. For each passing row: check `sub/fy.bin[adsh_code] == 2022`
4. GROUP BY `sub/cik.bin[adsh_code]` → hash map `cik → sum`
   (≈8,886 groups max; fits in L2 cache)
5. Compute `AVG(sub_total)` over all groups → threshold scalar T

**Main query — compute per-(name,cik) sums:**
1. Same scan as above
2. GROUP BY `(sub/name.bin[adsh_code], sub/cik.bin[adsh_code])` →
   hash map `(name_code:int32, cik:int32) → sum`
   (≈8,886 groups; name_code+cik fit in a 64-bit composite key)
3. HAVING: keep groups where `sum > T`
4. Decode name_code via sub/name.dict; sort by total_value DESC; emit top 100

**Key optimization:** Both scans are identical — execute in a single pass:
- Maintain two hash maps simultaneously (keyed by cik, and by (name_code, cik))
- Compute T from the cik-level map, apply HAVING to the (name,cik)-level map

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
- Usage: USD rows (code=0) cluster at the start of num (after null/negative-code rows).
  Zone maps identify which blocks have `uom_min <= 0 <= uom_max` (USD-containing blocks).
  Since num is sorted by uom_code first, all USD rows are contiguous — zone maps let us
  skip non-USD blocks entirely, reducing scan to ~87% of num (the USD segment).

### sub_adsh_hash (hash on adsh — Robin Hood, TagHashSlot)
- File: `indexes/sub_adsh_hash.bin`
- Struct (build_indexes.cpp lines 210–216, `#pragma pack(push,1)`):
  ```cpp
  struct TagHashSlot {
      uint64_t key_hash;  // FNV-64a of 20-byte adsh string
      int32_t  row_id;    // sub row index (= adsh_code); INT32_MIN = empty
      int32_t  _pad;
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
  Key: `fnv64(adsh_str, 20)`
- Usage for Q3: NOT needed directly — `num.adsh` is already encoded as `adsh_code`
  (= sub row_id), so `sub/fy.bin[adsh_code]`, `sub/cik.bin[adsh_code]`, and
  `sub/name.bin[adsh_code]` are direct array reads. No hash lookup required.

### num_adsh_index (inverted index — adsh_code → list of num row_ids)
- File: `indexes/num_adsh_index.bin`
- Struct:
  ```cpp
  struct AdshEntry { int32_t adsh_code, offset, count; };
  ```
- File layout:
  ```
  [n_entries   : int32]
  [header      : AdshEntry × n_entries]   (sorted by adsh_code)
  [total_rowids: uint32]
  [rowids      : int32 × total_rowids]
  ```
- Usage for Q3: Alternative join direction — iterate over fy=2022 sub rows, look up
  their num rows via this index, then filter by uom. Less efficient than the forward
  scan because USD has 87% selectivity (forward scan is nearly full scan anyway).
