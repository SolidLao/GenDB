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

### num.uom_code (dict_code, uint8_t, dictionary-encoded via uom_dict)
- File: `num/uom_code.bin` (39,401,761 rows, 1 byte each)
- Dictionary: `num/uom_dict_offsets.bin` + `num/uom_dict_data.bin` (201 entries)
- This query: `WHERE uom = 'USD'` → find usdCode, use uom_offsets for row range

### num.sub_fk (foreign_key_to_sub, uint32_t, dense FK array)
- File: `num/sub_fk.bin` (39,401,761 rows, 4 bytes each)
- This query: `JOIN sub s ON n.adsh = s.adsh` → direct index into sub table

### num.tag_code (dict_code, uint32_t, dictionary-encoded via tag_dict)
- File: `num/tag_code.bin` (39,401,761 rows, 4 bytes each)
- Dictionary: `dicts/tag_dict_offsets.bin` + `dicts/tag_dict_data.bin` (198,311 entries)
- This query: join key for tag table lookup and pre index lookup

### num.version_code (dict_code, uint32_t, dictionary-encoded via version_dict)
- File: `num/version_code.bin` (39,401,761 rows, 4 bytes each)
- Dictionary: `dicts/version_dict_offsets.bin` + `dicts/version_dict_data.bin` (83,815 entries)
- This query: join key for tag table lookup and pre index lookup

### num.value (numeric_value, double, fixed encoding, NaN = NULL)
- File: `num/value.bin` (39,401,761 rows, 8 bytes each)
- This query: `WHERE value IS NOT NULL` → `!std::isnan(value)`; `SUM(n.value)`, `AVG(n.value)`

### sub.sic (industry_code, int16_t, fixed encoding)
- File: `sub/sic.bin` (86,135 rows, 2 bytes each)
- This query: `WHERE s.sic BETWEEN 4000 AND 4999` → `sic >= 4000 && sic <= 4999`; selectivity ~0.041
- Also: `GROUP BY s.sic`

### sub.cik (identifier, int32_t, fixed encoding)
- File: `sub/cik.bin` (86,135 rows, 4 bytes each)
- This query: `COUNT(DISTINCT s.cik)` — count distinct companies per group

### tag.tag_code (dict_code, uint32_t)
- File: `tag/tag_code.bin` (1,070,662 rows, 4 bytes each)
- This query: join key — looked up via tag_pk_index

### tag.version_code (dict_code, uint32_t)
- File: `tag/version_code.bin` (1,070,662 rows, 4 bytes each)
- This query: join key — looked up via tag_pk_index

### tag.abstract (flag, int8_t, fixed encoding)
- File: `tag/abstract.bin` (1,070,662 rows, 1 byte each)
- This query: `WHERE t.abstract = 0` → selectivity ~0.9999

### tag.tlabel (text, varlen_string)
- Files: `tag/tlabel_offsets.bin` + `tag/tlabel_data.bin`
- This query: `GROUP BY t.tlabel`; output column

### pre.stmt_code (dict_code, uint8_t, dictionary-encoded via stmt_dict)
- File: `pre/stmt_code.bin` (9,600,799 rows, 1 byte each)
- Dictionary: `pre/stmt_dict_offsets.bin` + `pre/stmt_dict_data.bin` (9 entries)
- This query: `WHERE p.stmt = 'EQ'` → find eqCode; use stmt_offsets
- Also: `GROUP BY p.stmt` — but since stmt='EQ' is filtered, p.stmt is constant in output

### pre.sub_fk (foreign_key_to_sub, uint32_t)
- File: `pre/sub_fk.bin` (9,600,799 rows, 4 bytes each)
- This query: join key in pre index (matches num.sub_fk)

### pre.tag_code (dict_code, uint32_t)
- File: `pre/tag_code.bin` (9,600,799 rows, 4 bytes each)
- This query: join key in pre index

### pre.version_code (dict_code, uint32_t)
- File: `pre/version_code.bin` (9,600,799 rows, 4 bytes each)
- This query: join key in pre index

## Table Stats

| Table | Rows       | Role      | Sort Order            | Block Size |
|-------|------------|-----------|------------------------|------------|
| num   | 39,401,761 | fact      | (uom_code, sub_fk)    | 100,000    |
| sub   | 86,135     | dimension | (none)                | 100,000    |
| tag   | 1,070,662  | dimension | (none)                | 100,000    |
| pre   | 9,600,799  | fact      | (stmt_code, sub_fk)   | 100,000    |

## Query Analysis

### Strategy: num-driven with index lookups

This is a 4-table join with heavy filtering. The most selective combination is:
- `uom = 'USD'`: 84.4% of num → ~33.2M rows
- `s.sic BETWEEN 4000-4999`: 4.1% of sub → ~3,500 sub rows
- `p.stmt = 'EQ'`: 12.9% of pre → ~1.24M pre rows
- `t.abstract = 0`: ~99.99% of tag → nearly all

**Recommended execution plan:**

1. **Pre-filter sub:** Load `sub/sic.bin` and `sub/cik.bin`. Build `sic_ok[sub_row] = (sic >= 4000 && sic <= 4999)`. Only ~3,500 sub rows qualify.
2. **Load tag.abstract** into memory for filtering.
3. **Find stmt='EQ' code:** Load stmt_dict, find eqCode.
4. **Use uom_offsets** to get USD row range in num.
5. **Scan USD rows in num:** For each row where `!isnan(value) && sic_ok[sub_fk[i]]`:
   - Look up tag table via **tag_pk_index**: `(tag_code[i], version_code[i])` → tag row. Check `abstract[tag_row] == 0`.
   - Probe **pre_by_adsh_tag_ver** index: `(sub_fk[i], tag_code[i], version_code[i])` → list of pre rows. Check if any pre row has `stmt_code == eqCode`.
   - If all checks pass: aggregate into group key `(sic, tag_row, eqCode)` → accumulate sum, count, and cik set.
6. **HAVING filter:** Keep groups where `cik_set.size() >= 2`.
7. **Decode and output:** Look up `tlabel` from tag table, decode `stmt` from stmt_dict. Sort by total_value DESC, limit 500.

### Key Optimizations
- The sic filter (4.1%) applied via sub is very selective — precompute a sub_fk filter
- tag_pk_index gives O(1) lookup for tag dimension attributes
- pre_by_adsh_tag_ver index avoids scanning 9.6M pre rows — probe per num row
- Within USD range, num is sorted by sub_fk → sub_fk values cluster, enabling sub attribute caching
- Group key can use `(int16_t sic, uint32_t tag_row)` since p.stmt is constant ('EQ')

## Indexes

### uom_offsets (offset_table on uom_code)
- File: `num/uom_offsets.bin`
- Layout: `uint32_t num_entries`, then `num_entries` pairs of `(uint64_t start, uint64_t end)`
- Usage: Get USD row range to avoid scanning non-USD rows

### tag_pk_index (hash on tag_code, version_code)
- File: `indexes/tag_pk_index.idx`
- Layout: `uint64_t table_size`, then `table_size` slots, each slot is:
```cpp
struct Slot {
    uint32_t tag_code;      // 4 bytes
    uint32_t version_code;  // 4 bytes
    uint32_t row_idx;       // 4 bytes (UINT32_MAX = empty)
};
// Total: 12 bytes per slot
```
- Empty sentinel: `row_idx == UINT32_MAX (0xFFFFFFFF)`
- Hash function (verbatim from build_indexes.cpp):
```cpp
static inline uint64_t hashKey2(uint32_t a, uint32_t b) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}
```
- Lookup: `slot = h & (table_size - 1)`, linear probe until `row_idx == UINT32_MAX` or key match
- **Lookup pattern:**
```cpp
uint64_t h = hashKey2(tc, vc) & (tableSize - 1);
while (true) {
    if (slots[h].row_idx == UINT32_MAX) { /* not found */ break; }
    if (slots[h].tag_code == tc && slots[h].version_code == vc) {
        uint32_t tagRow = slots[h].row_idx;
        // Use tagRow to access tag table columns
        break;
    }
    h = (h + 1) & (tableSize - 1);
}
```

### pre_by_adsh_tag_ver (bucket hash on sub_fk, tag_code, version_code)
- File: `indexes/pre_by_adsh_tag_ver.idx`
- **This is a multi-value index (1:N)** — each key can map to multiple pre rows
- Layout:
```
uint64_t num_buckets
uint64_t total_entries
uint64_t bucket_offsets[num_buckets + 1]   // cumulative offsets
Entry entries[total_entries]               // all entries, grouped by bucket
```
- Entry struct:
```cpp
struct Entry {
    uint32_t sub_fk;        // 4 bytes
    uint32_t tag_code;      // 4 bytes
    uint32_t version_code;  // 4 bytes
    uint32_t row_idx;       // 4 bytes
};
// Total: 16 bytes per entry
```
- Hash function (verbatim from build_indexes.cpp):
```cpp
static inline uint64_t hashKey3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= (uint64_t)c * 3266489917ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}
```
- Bucket lookup: `bucket = h & (num_buckets - 1)`
- **Multi-value lookup pattern:**
```cpp
uint64_t bucket = hashKey3(sfk, tc, vc) & (numBuckets - 1);
uint64_t bStart = bucketOffsets[bucket];
uint64_t bEnd = bucketOffsets[bucket + 1];
for (uint64_t j = bStart; j < bEnd; j++) {
    if (entries[j].sub_fk == sfk && entries[j].tag_code == tc && entries[j].version_code == vc) {
        uint32_t preRow = entries[j].row_idx;
        // Check stmt_code[preRow] == eqCode
        // May find multiple matching entries (1:N relationship)
    }
}
```

### stmt_offsets (offset_table on stmt_code)
- File: `pre/stmt_offsets.bin`
- Layout: `uint32_t num_entries`, then `num_entries` pairs of `(uint64_t start, uint64_t end)`
- Usage: Alternative approach — instead of probing pre index per num row, could scan only EQ-stmt pre rows and build a hash set of (sub_fk, tag_code, version_code) keys, then check membership during num scan.
