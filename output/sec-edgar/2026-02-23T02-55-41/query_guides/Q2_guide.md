# Q2 Guide

## SQL
```sql
-- Decorrelated from correlated scalar subquery (PostgreSQL can't decorrelate; DuckDB does it automatically)
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

### num.adsh (dict_shared_adsh, int32_t)
- File: `num/adsh.bin` (39,401,761 rows)
- Dict: `num/adsh_dict.txt` (shared; same codes as sub/adsh.bin)
- **Key optimization**: adsh_code == sub_row_index. No hash lookup needed for num→sub join.
- This query: join key to sub

### num.uom (dict_local, int32_t)
- File: `num/uom.bin` (39,401,761 rows)
- Dict: `num/uom_dict.txt` (~25 distinct)
- This query: `uom = 'pure'` → load dict, find code for "pure" at runtime; filter rows where uom_code == pure_code
- Selectivity: ~0.1% (pure is tiny fraction). Very selective filter.

### num.value (double)
- File: `num/value.bin` (39,401,761 rows, 8 bytes each)
- This query: `value IS NOT NULL` → `!std::isnan(value)` ; aggregation: `MAX(value)` per (adsh,tag); also output column
- NULL sentinel: `std::numeric_limits<double>::quiet_NaN()`

### num.tag (dict_shared_tag, int32_t)
- File: `num/tag.bin` (39,401,761 rows)
- Dict: `num/tag_dict.txt` (shared tag dict; same codes as tag/tag.bin)
- This query: group-by key for subquery; join key in outer query; output column (decode via dict)

### sub.fy (int32_t)
- File: `sub/fy.bin` (86,135 rows)
- This query: `s.fy = 2022` → filter sub rows; selectivity ~31.7% of sub

### sub.name (dict_local, int32_t)
- File: `sub/name.bin` (86,135 rows)
- Dict: `sub/name_dict.txt`
- This query: output column → decode via name_dict[name_code]

## Table Stats
| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | (none)     | 100,000    |
| sub   | 86,135     | dimension | adsh       | 100,000    |

## Query Analysis
**Phase 1 — Filter num for uom='pure' and value IS NOT NULL:**
- Scan num.uom.bin + num.value.bin; collect row indices where uom==pure_code && !isnan(value)
- Expected rows: ~39M × 0.001 = ~39,000 rows (very small)

**Phase 2 — Build subquery hash map: (adsh_code, tag_code) → MAX(value):**
- For each qualifying num row: key = (adsh_code, tag_code), update MAX(value)
- Estimated groups: ~5,000,000 (but with uom='pure' filter, much smaller: ~39k rows → ~39k groups max)
- Use `std::unordered_map<uint64_t, double>` with key = (adsh_code << 20) | tag_code (if fits) or pack differently

**Phase 3 — Apply outer filters and join:**
- For each qualifying num row (pure, not null), check if value == max_value for (adsh, tag)
- Filter: sub.fy == 2022 → adsh_code → sub_row = adsh_code → sub_fy[adsh_code] == 2022

**Phase 4 — Output:**
- Collect matching rows, decode names/tags via dicts
- sort by value DESC, name, tag; take top 100

## Indexes

### sub/indexes/adsh_row_map.bin (direct_array)
- File: `sub/indexes/adsh_row_map.bin`
- Layout: `int32_t arr[max_adsh_code+1]`, where `arr[adsh_code] = sub_row_index`
- **Note**: Since adsh_code == sub_row_index (sub processed first in ingest), `arr[k] == k`. Sub lookup is just: `int sub_row = adsh_code;`
- Usage: For each qualifying num row, `int sub_row = num_adsh[i]; bool fy_ok = sub_fy[sub_row] == 2022;`

## Implementation Notes
- Load uom_dict.txt → find pure_code: `int pure_code = -1; for(int i=0;i<uom_dict.size();i++) if(uom_dict[i]=="pure") pure_code=i;`
- Sub join via direct index: `sub_fy[adsh_code]` (adsh_code IS the sub row index)
- Pack (adsh,tag) key as `uint64_t key = ((uint64_t)(uint32_t)adsh_code << 32) | (uint32_t)tag_code`
- C9: hash map capacity = next_power_of_2(estimated_groups * 2)
