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

### num.adsh (fk_code, uint32_t, dict_shared_adsh)
- File: `num/adsh.bin` (39,401,761 rows of uint32_t)
- This query: JOIN key to sub, part of JOIN key to pre

### num.tag (fk_code, uint32_t, dict_shared_tag)
- File: `num/tag.bin` (39,401,761 rows of uint32_t)
- This query: JOIN key to tag, part of JOIN key to pre

### num.version (fk_code, uint32_t, dict_shared_version)
- File: `num/version.bin` (39,401,761 rows of uint32_t)
- This query: JOIN key to tag, part of JOIN key to pre

### num.uom (category, uint16_t, dict_uom)
- File: `num/uom.bin` (39,401,761 rows of uint16_t)
- Dictionary: `dictionaries/uom.dict` (~30 entries)
- This query: `WHERE n.uom = 'USD'` — selectivity ~84.4%

### num.value (measure, double, raw)
- File: `num/value.bin` (39,401,761 rows of double)
- This query: `SUM(n.value)`, `AVG(n.value)` — aggregation target

### num.value_null (null_bitmap, uint8_t, raw)
- File: `num/value_null.bin` (39,401,761 rows of uint8_t)
- This query: `WHERE n.value IS NOT NULL` — filter value_null == 0

### sub.adsh (pk_code, uint32_t, dict_shared_adsh)
- File: `sub/adsh.bin` (86,135 rows of uint32_t)

### sub.sic (category, int16_t, raw)
- File: `sub/sic.bin` (86,135 rows of int16_t)
- This query: `WHERE s.sic BETWEEN 4000 AND 4999` — selectivity ~4.1% (~3,530 sub rows)
- Also: `GROUP BY s.sic` — group key component

### sub.cik (identifier, int32_t, raw)
- File: `sub/cik.bin` (86,135 rows of int32_t)
- This query: `COUNT(DISTINCT s.cik)` — distinct count per group, HAVING >= 2

### tag.tag (pk_code, uint32_t, dict_shared_tag)
- File: `tag/tag.bin` (1,070,662 rows of uint32_t)
- This query: JOIN key from num

### tag.version (pk_code, uint32_t, dict_shared_version)
- File: `tag/version.bin` (1,070,662 rows of uint32_t)
- This query: JOIN key from num

### tag.abstract (flag, int8_t, raw)
- File: `tag/abstract.bin` (1,070,662 rows of int8_t)
- This query: `WHERE t.abstract = 0` — selectivity ~100% (workload analysis shows all values = 0)

### tag.tlabel (string_code, uint32_t, dict_tlabel)
- File: `tag/tlabel.bin` (1,070,662 rows of uint32_t)
- Dictionary: `dictionaries/tlabel.dict` (~500,000 entries)
- This query: `GROUP BY t.tlabel` — group key component, output column

### pre.adsh (fk_code, uint32_t, dict_shared_adsh)
- File: `pre/adsh.bin` (9,600,799 rows of uint32_t)
### pre.tag (fk_code, uint32_t, dict_shared_tag)
- File: `pre/tag.bin` (9,600,799 rows of uint32_t)
### pre.version (fk_code, uint32_t, dict_shared_version)
- File: `pre/version.bin` (9,600,799 rows of uint32_t)

### pre.stmt (category, uint8_t, dict_stmt)
- File: `pre/stmt.bin` (9,600,799 rows of uint8_t)
- Dictionary: `dictionaries/stmt.dict` (~8 entries)
- This query: `WHERE p.stmt = 'EQ'` — selectivity ~12.9% (~1.24M pre rows)
- Also: `GROUP BY p.stmt` — group key component (but constant 'EQ' after filter, so effectively not a grouping dimension)

## Table Stats

| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | none       | 100,000    |
| sub   | 86,135     | dimension | none       | 100,000    |
| tag   | 1,070,662  | dimension | none       | 100,000    |
| pre   | 9,600,799  | fact      | none       | 100,000    |

## Query Analysis

- **Pattern**: 4-table join (num as driver, sub/tag/pre as joined), filters on all, grouped aggregation with HAVING, ordered LIMIT
- **Join chain**: num → sub (on adsh), num → tag (on tag+version), num → pre (on adsh+tag+version, M:N)
- **Filter selectivities**:
  - num: uom='USD' AND value IS NOT NULL → ~84.4% → ~33.3M
  - sub: sic BETWEEN 4000-4999 → ~4.1% → ~3,530 sub rows
  - tag: abstract=0 → ~100% (no filtering effect)
  - pre: stmt='EQ' → ~12.9%
  - Combined after joins: ~33.3M * 0.041 * 1.0 ≈ 1.36M, then pre match further reduces
- **Aggregation**: GROUP BY (sic, tlabel_code, stmt_code) — estimated ~5,000 groups
- **HAVING**: COUNT(DISTINCT cik) >= 2
- **Output**: 500 rows ordered by total_value DESC

## Indexes

### sub_adsh_lookup (dense_array on sub.adsh)
- File: `indexes/sub_adsh_lookup.idx`
- Layout: uint32_t size, then uint32_t[size] array
- Lookup: `array[adsh_code]` → sub row_id (UINT32_MAX if not found)
- Usage: For each num row, look up sub row to check sic filter and get cik

### tag_pk (hash on tag.tag + tag.version)
- File: `indexes/tag_pk.idx`
- Layout: uint32_t capacity, uint32_t num_entries, then HashEntry[capacity]
- HashEntry struct (24 bytes):
  ```cpp
  struct HashEntry {
      uint64_t key_hash;  // hash of (tag_code, version_code)
      uint32_t value;     // tag row_id
      uint32_t extra;     // 0 (unused for single-value)
      uint8_t  occupied;  // 1 if slot used, 0 if empty
      uint8_t  pad[7];    // alignment padding
  };
  ```
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  inline uint64_t hash_u32x2(uint32_t a, uint32_t b) {
      uint64_t x = (static_cast<uint64_t>(a) << 32) | b;
      x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
      x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
      x = x ^ (x >> 31);
      return x;
  }
  ```
- Lookup: compute `h = hash_u32x2(tag_code, version_code)`, slot = `(uint32_t)h & (capacity - 1)`, linear probe while `occupied && key_hash != h`, result row_id = `value`
- Capacity is power-of-2 (≥ 2 * num_entries), so mask = capacity - 1
- Empty slot sentinel: occupied == 0

### pre_join (hash_multimap on pre.adsh + pre.tag + pre.version)
- File: `indexes/pre_join.idx` (hash table) + `indexes/pre_join_rowids.idx` (row ID array)
- Hash table layout: uint32_t capacity, uint32_t num_entries, then HashEntry[capacity]
- HashEntry struct: same 24-byte struct as above
  - `value` = offset into row_ids array
  - `extra` = count of matching row IDs
- Row IDs file: uint32_t num_rows, then uint32_t[num_rows] sorted_row_ids
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  inline uint64_t hash_u32x3(uint32_t a, uint32_t b, uint32_t c) {
      uint64_t h = hash_u32x2(a, b);
      h ^= hash_u32(c) + 0x9e3779b97f4a7c15ULL + (h << 12) + (h >> 4);
      return h;
  }
  // where hash_u32 is:
  inline uint64_t hash_u32(uint32_t a) {
      uint64_t x = a;
      x = (x ^ (x >> 16)) * 0x45d9f3b;
      x = (x ^ (x >> 16)) * 0x45d9f3b;
      x = x ^ (x >> 16);
      return x;
  }
  ```
- Lookup: compute `h = hash_u32x3(adsh_code, tag_code, version_code)`, slot = `(uint32_t)h & (capacity - 1)`, linear probe while occupied. When `key_hash == h`, read offset = `value`, count = `extra`. Row IDs are at `rowids[offset .. offset+count-1]`.
- Empty slot sentinel: occupied == 0

## Execution Strategy

1. Load dictionaries: uom, stmt, tlabel
2. Find uom code for "USD", stmt code for "EQ"
3. Load indexes: sub_adsh_lookup, tag_pk, pre_join + pre_join_rowids
4. Mmap columns: sub.sic, sub.cik; tag.abstract, tag.tlabel; pre.stmt
5. **Pre-filter sub**: Build set of valid adsh_codes where sub.sic BETWEEN 4000 AND 4999
6. **Scan num**: For each row where uom==usd_code AND value_null==0:
   - Check adsh_code in valid sub set → get sub_row, read sic, cik
   - Lookup tag_pk with (tag_code, version_code) → get tag_row, check abstract==0, read tlabel_code
   - Lookup pre_join with (adsh_code, tag_code, version_code) → get list of pre_row_ids
   - For each pre_row: check pre.stmt[pre_row]==eq_code
   - For each qualifying combination: accumulate into group map keyed by (sic, tlabel_code, stmt_code)
     - Track SUM(value), COUNT for AVG, and a set of distinct cik values per group
7. Apply HAVING: COUNT(DISTINCT cik) >= 2
8. Partial sort top-500 by total_value DESC
9. Decode tlabel codes and stmt codes via dictionaries for output
