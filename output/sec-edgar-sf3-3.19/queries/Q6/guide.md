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

### num.adsh (fk_code, uint32_t, dict_shared_adsh)
- File: `num/adsh.bin` (39,401,761 rows of uint32_t)
- This query: JOIN key to sub, part of JOIN key to pre

### num.tag (fk_code, uint32_t, dict_shared_tag)
- File: `num/tag.bin` (39,401,761 rows of uint32_t)
- Dictionary: `dictionaries/tag.dict`
- This query: part of JOIN key to pre, GROUP BY component, output column

### num.version (fk_code, uint32_t, dict_shared_version)
- File: `num/version.bin` (39,401,761 rows of uint32_t)
- This query: part of JOIN key to pre

### num.uom (category, uint16_t, dict_uom)
- File: `num/uom.bin` (39,401,761 rows of uint16_t)
- Dictionary: `dictionaries/uom.dict` (~30 entries)
- This query: `WHERE n.uom = 'USD'` — selectivity ~84.4%

### num.value (measure, double, raw)
- File: `num/value.bin` (39,401,761 rows of double)
- This query: `SUM(n.value)` — aggregation target

### num.value_null (null_bitmap, uint8_t, raw)
- File: `num/value_null.bin` (39,401,761 rows of uint8_t)
- This query: `WHERE n.value IS NOT NULL` — filter value_null == 0

### sub.adsh (pk_code, uint32_t, dict_shared_adsh)
- File: `sub/adsh.bin` (86,135 rows of uint32_t)

### sub.fy (year, int16_t, raw)
- File: `sub/fy.bin` (86,135 rows of int16_t)
- This query: `WHERE s.fy = 2023` — selectivity ~18.8%

### sub.name (string_code, uint32_t, dict_name)
- File: `sub/name.bin` (86,135 rows of uint32_t)
- Dictionary: `dictionaries/name.dict` (~86,000 entries)
- This query: `GROUP BY s.name` — group key component, output column

### pre.adsh (fk_code, uint32_t, dict_shared_adsh)
- File: `pre/adsh.bin` (9,600,799 rows of uint32_t)
### pre.tag (fk_code, uint32_t, dict_shared_tag)
- File: `pre/tag.bin` (9,600,799 rows of uint32_t)
### pre.version (fk_code, uint32_t, dict_shared_version)
- File: `pre/version.bin` (9,600,799 rows of uint32_t)

### pre.stmt (category, uint8_t, dict_stmt)
- File: `pre/stmt.bin` (9,600,799 rows of uint8_t)
- Dictionary: `dictionaries/stmt.dict` (~8 entries)
- This query: `WHERE p.stmt = 'IS'` — selectivity ~18.4%
- Also: `GROUP BY p.stmt` — group key component (constant 'IS' after filter)

### pre.plabel (string_code, uint32_t, dict_plabel)
- File: `pre/plabel.bin` (9,600,799 rows of uint32_t)
- Dictionary: `dictionaries/plabel.dict` (~2,000,000 entries)
- This query: `GROUP BY p.plabel` — group key component, output column

## Table Stats

| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | none       | 100,000    |
| sub   | 86,135     | dimension | none       | 100,000    |
| pre   | 9,600,799  | fact      | none       | 100,000    |

## Query Analysis

- **Pattern**: 3-table join (num driver, sub dimension, pre M:N), filters, grouped aggregation, ordered LIMIT
- **Join chain**: num → sub (on adsh), num → pre (on adsh+tag+version, M:N)
- **Filter selectivities**:
  - num: uom='USD' AND value IS NOT NULL → ~84.4% → ~33.3M
  - sub: fy=2023 → ~18.8% → ~16,200 sub rows
  - pre: stmt='IS' → ~18.4%
  - Combined: ~33.3M * 0.188 * ~0.184 (pre match rate) ≈ 1.15M qualifying tuples
- **Aggregation**: GROUP BY (name_code, stmt_code, tag_code, plabel_code) — estimated ~500,000 groups
- **Output**: 200 rows ordered by total_value DESC

## Indexes

### sub_adsh_lookup (dense_array on sub.adsh)
- File: `indexes/sub_adsh_lookup.idx`
- Layout: uint32_t size, then uint32_t[size] array
- Lookup: `array[adsh_code]` → sub row_id (UINT32_MAX if not found)

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
- Row IDs file: uint32_t num_rows, then uint32_t[num_rows] sorted_row_ids
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
- Lookup: compute `h = hash_u32x3(adsh_code, tag_code, version_code)`, slot = `(uint32_t)h & (capacity - 1)`, linear probe while occupied. On match (`key_hash == h`): offset = `value`, count = `extra`. Row IDs at `rowids[offset .. offset+count-1]`.
- Empty slot sentinel: occupied == 0

## Execution Strategy

1. Load dictionaries: uom, stmt, tag, name, plabel
2. Find uom code for "USD", stmt code for "IS"
3. Load indexes: sub_adsh_lookup, pre_join + pre_join_rowids
4. Mmap columns: sub.fy, sub.name; pre.stmt, pre.plabel; num.*
5. **Pre-filter sub**: Build set of valid adsh_codes where sub.fy == 2023
6. **Scan num**: For each row where uom==usd_code AND value_null==0:
   - Check adsh_code in valid sub set → get sub_row, read name_code
   - Lookup pre_join with (adsh_code, tag_code, version_code) → list of pre_row_ids
   - For each pre_row: check pre.stmt[pre_row]==is_code → read plabel_code
   - For each qualifying combo: accumulate into group map keyed by (name_code, stmt_code, tag_code, plabel_code)
     - SUM(value), COUNT(*)
7. Partial sort top-200 by total_value DESC
8. Decode name, stmt, tag, plabel codes via dictionaries for output
