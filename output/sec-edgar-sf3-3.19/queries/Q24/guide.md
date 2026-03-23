# Q24 Guide

```sql
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

### num.adsh (fk_code, uint32_t, dict_shared_adsh)
- File: `num/adsh.bin` (39,401,761 rows of uint32_t)
- This query: part of LEFT JOIN key to pre (anti-join)

### num.tag (fk_code, uint32_t, dict_shared_tag)
- File: `num/tag.bin` (39,401,761 rows of uint32_t)
- Dictionary: `dictionaries/tag.dict`
- This query: part of LEFT JOIN key, GROUP BY component, output column

### num.version (fk_code, uint32_t, dict_shared_version)
- File: `num/version.bin` (39,401,761 rows of uint32_t)
- Dictionary: `dictionaries/version.dict`
- This query: part of LEFT JOIN key, GROUP BY component, output column

### num.uom (category, uint16_t, dict_uom)
- File: `num/uom.bin` (39,401,761 rows of uint16_t)
- Dictionary: `dictionaries/uom.dict` (~30 entries)
- This query: `WHERE n.uom = 'USD'` — selectivity ~84.4%

### num.ddate (date_int, int32_t, raw)
- File: `num/ddate.bin` (39,401,761 rows of int32_t)
- Encoding: raw YYYYMMDD integer (e.g., 20230101, 20231231)
- This query: `WHERE n.ddate BETWEEN 20230101 AND 20231231` — selectivity ~27.2% → ~10.7M rows

### num.value (measure, double, raw)
- File: `num/value.bin` (39,401,761 rows of double)
- This query: `SUM(n.value)` — aggregation target

### num.value_null (null_bitmap, uint8_t, raw)
- File: `num/value_null.bin` (39,401,761 rows of uint8_t)
- This query: `WHERE n.value IS NOT NULL` — filter value_null == 0

## Table Stats

| Table | Rows       | Role | Sort Order | Block Size |
|-------|------------|------|------------|------------|
| num   | 39,401,761 | fact | none       | 100,000    |
| pre   | 9,600,799  | fact | none       | 100,000    |

## Query Analysis

- **Pattern**: Anti-join (LEFT JOIN + IS NULL), filters on num, grouped aggregation with HAVING, ordered LIMIT
- **Anti-join semantics**: Keep num rows that have NO matching pre row on (adsh, tag, version)
- **Filter selectivities on num**:
  - uom='USD' → ~84.4%
  - ddate BETWEEN 20230101 AND 20231231 → ~27.2%
  - value IS NOT NULL → ~100%
  - Combined: ~84.4% * 27.2% ≈ 23.0% → ~9.1M rows pre-anti-join
  - Anti-join (no match in pre) → estimated ~50% → ~4.5M rows
- **Aggregation**: GROUP BY (tag_code, version_code) — estimated ~20,000 groups
- **HAVING**: COUNT(*) > 10
- **Output**: up to 100 rows ordered by cnt DESC

## Indexes

### pre_join (hash_multimap on pre.adsh + pre.tag + pre.version)
- File: `indexes/pre_join.idx` (hash table) + `indexes/pre_join_rowids.idx` (row ID array)
- Hash table layout: uint32_t capacity, uint32_t num_entries, then HashEntry[capacity]
- HashEntry struct (24 bytes):
  ```cpp
  struct HashEntry {
      uint64_t key_hash;
      uint32_t value;     // offset into rowids array
      uint32_t extra;     // count of matching row IDs
      uint8_t  occupied;
      uint8_t  pad[7];
  };
  ```
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  inline uint64_t hash_u32x3(uint32_t a, uint32_t b, uint32_t c) {
      uint64_t h = hash_u32x2(a, b);
      h ^= hash_u32(c) + 0x9e3779b97f4a7c15ULL + (h << 12) + (h >> 4);
      return h;
  }
  inline uint64_t hash_u32x2(uint32_t a, uint32_t b) {
      uint64_t x = (static_cast<uint64_t>(a) << 32) | b;
      x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
      x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
      x = x ^ (x >> 31);
      return x;
  }
  inline uint64_t hash_u32(uint32_t a) {
      uint64_t x = a;
      x = (x ^ (x >> 16)) * 0x45d9f3b;
      x = (x ^ (x >> 16)) * 0x45d9f3b;
      x = x ^ (x >> 16);
      return x;
  }
  ```
- **Anti-join usage**: compute `h = hash_u32x3(adsh_code, tag_code, version_code)`, slot = `(uint32_t)h & (capacity - 1)`, linear probe. If NO slot has `occupied==1 && key_hash==h` before hitting `occupied==0`, then the key has no match in pre → row qualifies for anti-join.
- Empty slot sentinel: occupied == 0

## Execution Strategy

1. Load dictionaries: uom, tag, version
2. Find uom code for "USD"
3. Load pre_join index (hash table only — rowids not needed for anti-join existence check)
4. Mmap num columns: uom, ddate, value, value_null, adsh, tag, version
5. **Scan num**: For each row where uom==usd_code AND ddate >= 20230101 AND ddate <= 20231231 AND value_null==0:
   - Probe pre_join hash table with (adsh_code, tag_code, version_code)
   - If NOT found (anti-join): accumulate into group map keyed by (tag_code, version_code)
     - COUNT(*), SUM(value)
6. Apply HAVING: keep groups where cnt > 10
7. Partial sort top-100 by cnt DESC
8. Decode tag and version codes via dictionaries for output
