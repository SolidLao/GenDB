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
- This query: join key for pre index; `GROUP BY n.tag`; output column (decode for output)

### num.version_code (dict_code, uint32_t, dictionary-encoded via version_dict)
- File: `num/version_code.bin` (39,401,761 rows, 4 bytes each)
- Dictionary: `dicts/version_dict_offsets.bin` + `dicts/version_dict_data.bin` (83,815 entries)
- This query: join key for pre index

### num.value (numeric_value, double, fixed encoding, NaN = NULL)
- File: `num/value.bin` (39,401,761 rows, 8 bytes each)
- This query: `WHERE value IS NOT NULL` → `!std::isnan(value)`; `SUM(n.value)`, `COUNT(*)`

### sub.fy (year, int16_t, fixed encoding)
- File: `sub/fy.bin` (86,135 rows, 2 bytes each)
- This query: `WHERE s.fy = 2023` → `fy == 2023`; selectivity ~0.188

### sub.name (text, varlen_string)
- Files: `sub/name_offsets.bin` + `sub/name_data.bin`
- This query: `GROUP BY s.name`; output column

### pre.stmt_code (dict_code, uint8_t, dictionary-encoded via stmt_dict)
- File: `pre/stmt_code.bin` (9,600,799 rows, 1 byte each)
- Dictionary: `pre/stmt_dict_offsets.bin` + `pre/stmt_dict_data.bin` (9 entries)
- This query: `WHERE p.stmt = 'IS'` → find isCode; `GROUP BY p.stmt` — constant in output

### pre.sub_fk, pre.tag_code, pre.version_code
- Files: `pre/sub_fk.bin`, `pre/tag_code.bin`, `pre/version_code.bin`
- These are used as keys in the pre_by_adsh_tag_ver index

### pre.plabel (text, varlen_string)
- Files: `pre/plabel_offsets.bin` + `pre/plabel_data.bin` (9,600,799 rows)
- This query: `GROUP BY p.plabel`; output column

## Table Stats

| Table | Rows       | Role      | Sort Order            | Block Size |
|-------|------------|-----------|------------------------|------------|
| num   | 39,401,761 | fact      | (uom_code, sub_fk)    | 100,000    |
| sub   | 86,135     | dimension | (none)                | 100,000    |
| pre   | 9,600,799  | fact      | (stmt_code, sub_fk)   | 100,000    |

## Query Analysis

### Strategy: num-driven with pre index probe

**Recommended execution plan:**

1. **Pre-filter sub:** Load `sub/fy.bin`. Build `fy2023[sub_row] = (fy[sub_row] == 2023)`. Selectivity ~18.8% → ~16,200 sub rows qualify.
2. **Find dictionary codes:** Load uom_dict → find usdCode. Load stmt_dict → find isCode.
3. **Load pre index and pre columns:** Load pre_by_adsh_tag_ver index, load `pre/stmt_code.bin` and `pre/plabel` (offsets + data) into memory.
4. **Use uom_offsets** to get USD row range in num (~33.2M rows).
5. **Scan USD num rows:** For each row where `!isnan(value) && fy2023[sub_fk[i]]`:
   - Probe **pre_by_adsh_tag_ver** index with `(sub_fk[i], tag_code[i], version_code[i])`
   - For each matching pre entry: check `stmt_code[preRow] == isCode`
   - If match: aggregate into group key. Since `p.stmt` is constant ('IS'), the effective group key is `(sub_fk, tag_code, preRow_for_plabel)`.
6. **Group key design:** The group key is `(s.name, p.stmt, n.tag, p.plabel)`. Since p.stmt is constant:
   - Option A: Key on `(sub_fk, tag_code, plabel_hash)` — need plabel identity
   - Option B: Key on `(sub_fk, tag_code, pre_row_idx)` — but different pre rows could have same plabel
   - **Recommended:** Use `(sub_fk, tag_code, plabel_string)` or a proxy. Since sub_fk → name (many sub_fk can share a name), actually group by `(name_offset, tag_code, plabel_offset)` using the varlen offsets as identity.
   - Simplest correct approach: use a hash map keyed by `(sub_fk, tag_code, pre_row_idx)` where pre_row_idx determines plabel. At output, merge groups with identical `(name, tag, plabel)` if needed. However, since `(sub_fk, tag_code, version_code, pre_row)` is already unique per pre row, and each pre row has exactly one plabel, this is safe as long as different sub_fk with the same name are merged.
   - **Best approach:** Build a `sub_fk → name_id` map (group sub_fks by name string), then key on `(name_id, tag_code, plabel_content_hash)`.
7. **Decode and output:** Decode tag_dict → tag string, decode plabel from pre table. Sort by total_value DESC, limit 200.

### Estimated result sizes
- ~200,000 estimated groups
- Filters: USD (84.4%) × fy=2023 (18.8%) × IS stmt match × value not null
- Effective num rows after uom+fy filter: ~33.2M × 0.188 ≈ 6.2M rows to probe pre index

## Indexes

### uom_offsets (offset_table on uom_code)
- File: `num/uom_offsets.bin`
- Layout: `uint32_t num_entries`, then `num_entries` pairs of `(uint64_t start, uint64_t end)`
- Usage: Get USD row range

### pre_by_adsh_tag_ver (bucket hash on sub_fk, tag_code, version_code)
- File: `indexes/pre_by_adsh_tag_ver.idx`
- **Multi-value index (1:N)** — each (sub_fk, tag_code, version_code) can map to multiple pre rows
- Layout:
```
uint64_t num_buckets
uint64_t total_entries
uint64_t bucket_offsets[num_buckets + 1]   // cumulative
Entry entries[total_entries]
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
- Bucket lookup: `bucket = hashKey3(sfk, tc, vc) & (numBuckets - 1)`
- **Multi-value lookup pattern:**
```cpp
uint64_t bucket = hashKey3(sfk, tc, vc) & (numBuckets - 1);
uint64_t bStart = bucketOffsets[bucket];
uint64_t bEnd = bucketOffsets[bucket + 1];
for (uint64_t j = bStart; j < bEnd; j++) {
    if (entries[j].sub_fk == sfk && entries[j].tag_code == tc && entries[j].version_code == vc) {
        uint32_t preRow = entries[j].row_idx;
        // Check stmt_code[preRow] == isCode
        // Read plabel for this preRow
    }
}
```

### stmt_offsets (offset_table on stmt_code)
- File: `pre/stmt_offsets.bin`
- Layout: `uint32_t num_entries`, then `num_entries` pairs of `(uint64_t start, uint64_t end)`
- Usage: Alternative approach — scan only 'IS' stmt range in pre, build a hash set of `(sub_fk, tag_code, version_code)` keys with associated plabel, then probe during num scan.
