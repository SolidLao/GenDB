# Q2 Guide

```sql
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

### num.uom_code (dict_code, uint8_t, dictionary-encoded via uom_dict)
- File: `num/uom_code.bin` (39,401,761 rows, 1 byte each)
- Dictionary: `num/uom_dict_offsets.bin` + `num/uom_dict_data.bin` (201 entries)
- This query: `WHERE uom = 'pure'` → load uom_dict, find code for "pure", use uom_offsets to get row range
- **Loading pattern:**
```cpp
auto offsets = readColumn<uint64_t>("num/uom_dict_offsets.bin");
auto data = readFile("num/uom_dict_data.bin");
uint8_t pureCode = 255; // sentinel
for (size_t i = 0; i + 1 < offsets.size(); i++) {
    std::string_view sv(data.data() + offsets[i], offsets[i+1] - offsets[i]);
    if (sv == "pure") { pureCode = (uint8_t)i; break; }
}
```

### num.sub_fk (foreign_key_to_sub, uint32_t, dense FK array)
- File: `num/sub_fk.bin` (39,401,761 rows, 4 bytes each)
- This query: `JOIN sub s ON n.adsh = s.adsh` → sub_fk is a direct row index into the sub table
- Also used for the subquery self-join: grouping by (sub_fk, tag_code)

### num.tag_code (dict_code, uint32_t, dictionary-encoded via tag_dict)
- File: `num/tag_code.bin` (39,401,761 rows, 4 bytes each)
- Dictionary: `dicts/tag_dict_offsets.bin` + `dicts/tag_dict_data.bin` (198,311 entries, shared cross-table)
- This query: `GROUP BY adsh, tag` → group by (sub_fk, tag_code); output `n.tag` → decode tag_code

### num.value (numeric_value, double, fixed encoding, NaN = NULL)
- File: `num/value.bin` (39,401,761 rows, 8 bytes each)
- This query: `WHERE value IS NOT NULL` → `!std::isnan(value)`; `MAX(value)` aggregation; equality check `n.value = m.max_value`

### sub.fy (year, int16_t, fixed encoding)
- File: `sub/fy.bin` (86,135 rows, 2 bytes each)
- This query: `WHERE s.fy = 2022` → `fy == 2022`; selectivity ~0.196

### sub.name (text, varlen_string)
- Files: `sub/name_offsets.bin` (86,136 uint64_t values) + `sub/name_data.bin`
- This query: output column `s.name`, also ORDER BY `s.name`

## Table Stats

| Table | Rows       | Role      | Sort Order          | Block Size |
|-------|------------|-----------|---------------------|------------|
| num   | 39,401,761 | fact      | (uom_code, sub_fk)  | 100,000    |
| sub   | 86,135     | dimension | (none)              | 100,000    |

## Query Analysis

### Strategy
This query finds, for each (adsh, tag) group where uom='pure', the rows where value equals the MAX(value) of that group, then filters to fy=2022 filings.

**Recommended execution plan:**

1. **Use uom_offsets** to find the row range `[start, end)` for `uom_code == pureCode`. Selectivity ~3.5%, so ~1.38M rows.
2. **Load sub.fy** (86K rows) into memory. Build a bitset or boolean array: `fy2022[sub_row] = (fy[sub_row] == 2022)`.
3. **Phase 1 — Build max-value map:** Scan the pure-uom range. For each row where `!isnan(value)`:
   - Key: `(sub_fk, tag_code)` → accumulate `MAX(value)`
   - ~500K estimated groups
4. **Phase 2 — Collect matching rows:** Re-scan the pure-uom range. For each row where `!isnan(value)` AND `fy2022[sub_fk]` AND `value == max_map[(sub_fk, tag_code)]`:
   - Collect `(sub_fk, tag_code, value)` into result candidates
5. **Decode and sort:** Decode sub.name (varlen), tag_dict → tag string. Sort by `(value DESC, name ASC, tag ASC)`. Take top 100.

### Key Optimizations
- **uom_offsets eliminates 96.5% of rows** — only scan ~1.38M rows instead of 39.4M
- Within the pure-uom range, rows are sorted by sub_fk (secondary sort key), enabling streaming group boundaries
- sub table fits in cache (86K rows × 2 bytes for fy = 168 KB)
- Two-pass over same range is cache-friendly since the range is contiguous

## Indexes

### uom_offsets (offset_table on uom_code)
- File: `num/uom_offsets.bin`
- Layout: `uint32_t num_entries`, then `num_entries` pairs of `(uint64_t start, uint64_t end)`
  - Entry at index `c` gives row range `[start, end)` where `uom_code == c`
  - If a code has no rows: start=0, end=0
- **Usage pattern:**
```cpp
std::ifstream f("num/uom_offsets.bin", std::ios::binary);
uint32_t numEntries; f.read((char*)&numEntries, 4);
struct Range { uint64_t start, end; };
std::vector<Range> ranges(numEntries);
f.read((char*)ranges.data(), numEntries * sizeof(Range));
// Scan only rows in [ranges[pureCode].start, ranges[pureCode].end)
```
