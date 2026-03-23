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

### num.adsh (fk_code, uint32_t, dict_shared_adsh)
- File: `num/adsh.bin` (39,401,761 rows of uint32_t)
- This query: JOIN key to sub, and part of subquery GROUP BY (adsh, tag)

### num.tag (fk_code, uint32_t, dict_shared_tag)
- File: `num/tag.bin` (39,401,761 rows of uint32_t)
- Dictionary: `dictionaries/tag.dict`
- This query: part of subquery GROUP BY (adsh, tag), output column, ORDER BY component
- Decode via tag dictionary for output

### num.version (fk_code, uint32_t, dict_shared_version)
- File: `num/version.bin` (39,401,761 rows of uint32_t)
- Not directly used in Q2 filters/joins/output — not needed

### num.uom (category, uint16_t, dict_uom)
- File: `num/uom.bin` (39,401,761 rows of uint16_t)
- Dictionary: `dictionaries/uom.dict` (~30 entries)
- This query: `WHERE n.uom = 'pure'` — load dict, find code for "pure", compare uint16_t
- Selectivity: ~3.5% (~1.38M rows pass)

### num.value (measure, double, raw)
- File: `num/value.bin` (39,401,761 rows of double)
- This query: `MAX(value)` in subquery, `n.value = m.max_value` match, output, ORDER BY DESC

### num.value_null (null_bitmap, uint8_t, raw)
- File: `num/value_null.bin` (39,401,761 rows of uint8_t)
- This query: `WHERE value IS NOT NULL` — filter where value_null == 0

### sub.adsh (pk_code, uint32_t, dict_shared_adsh)
- File: `sub/adsh.bin` (86,135 rows of uint32_t)
- This query: JOIN key from num

### sub.fy (year, int16_t, raw)
- File: `sub/fy.bin` (86,135 rows of int16_t)
- This query: `WHERE s.fy = 2022` — selectivity ~19.6% (~16,900 sub rows pass)

### sub.name (string_code, uint32_t, dict_name)
- File: `sub/name.bin` (86,135 rows of uint32_t)
- Dictionary: `dictionaries/name.dict` (~86,000 entries)
- This query: output column, ORDER BY component
- Decode via name dictionary for output

## Table Stats

| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | none       | 100,000    |
| sub   | 86,135     | dimension | none       | 100,000    |

## Query Analysis

- **Pattern**: Self-join on num (via subquery) + join to sub, with filters, aggregation, ordered output with LIMIT
- **Subquery**: `SELECT adsh, tag, MAX(value) FROM num WHERE uom='pure' AND value IS NOT NULL GROUP BY adsh, tag`
  - Scan num once for uom='pure' AND value IS NOT NULL rows
  - Build hash map: (adsh_code, tag_code) -> max_value (double)
  - Estimated groups: ~1M distinct (adsh, tag) pairs among 'pure' rows
- **Main query join logic**:
  - For each num row where uom='pure' AND value IS NOT NULL:
    - Look up (adsh, tag) in max_value map
    - Check if n.value == max_value (exact double equality)
    - Look up sub row via sub_adsh_lookup index
    - Check s.fy == 2022
    - If all pass, emit (s.name, n.tag, n.value)
- **Optimization**: Single pass — compute max_value map first, then second pass to find matching rows
- **Order**: value DESC, name ASC (string), tag ASC (string) — LIMIT 100
- **Output**: 100 rows

## Indexes

### sub_adsh_lookup (dense_array on sub.adsh)
- File: `indexes/sub_adsh_lookup.idx`
- Layout: uint32_t size, then uint32_t[size] array
- Lookup: `array[adsh_code]` → sub row_id (UINT32_MAX if not found)
- Usage: For each matching num row, look up sub row by num.adsh code in O(1)

## Dictionary Loading Pattern

```cpp
// Load dictionary from .dict file
// Format: uint32_t count, uint32_t[count+1] offsets, char[] string_data
uint32_t count;
read(&count, 4);
std::vector<uint32_t> offsets(count + 1);
read(offsets.data(), (count + 1) * 4);
std::vector<char> str_data(offsets[count]);
read(str_data.data(), offsets[count]);
// Decode code -> string: std::string_view(str_data.data() + offsets[code], offsets[code+1] - offsets[code])
```

To find the code for a known string (e.g., "pure"):
```cpp
uint16_t pure_code = UINT16_MAX;
for (uint32_t i = 0; i < count; i++) {
    size_t len = offsets[i+1] - offsets[i];
    if (len == 4 && memcmp(str_data.data() + offsets[i], "pure", 4) == 0) {
        pure_code = i; break;
    }
}
```

## Execution Strategy

1. Load dictionaries: uom, tag, name
2. Find uom code for "pure"
3. Load sub_adsh_lookup index
4. Mmap sub.fy, sub.name
5. **Pass 1**: Scan num (uom, value_null, value, adsh, tag). For rows where uom==pure_code AND value_null==0, insert/update max_value in hash map keyed by (adsh_code, tag_code)
6. **Pass 2**: Scan num again. For rows where uom==pure_code AND value_null==0 AND value==max_map[(adsh,tag)]:
   - Lookup sub_row via sub_adsh_lookup[adsh_code]
   - Check sub.fy[sub_row] == 2022
   - If pass, collect result tuple
7. Partial sort (top-100) by value DESC, then name string ASC, then tag string ASC
8. Decode name and tag codes via dictionaries for output
