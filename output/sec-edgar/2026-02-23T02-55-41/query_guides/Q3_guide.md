# Q3 Guide

## SQL
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

### num.adsh (dict_shared_adsh, int32_t)
- File: `num/adsh.bin` (39,401,761 rows)
- Dict: `num/adsh_dict.txt`
- **Key optimization**: adsh_code == sub_row_index. `int sub_row = num_adsh[i];` (direct, no hash lookup)
- This query: join key to sub

### num.uom (dict_local, int32_t)
- File: `num/uom.bin` (39,401,761 rows)
- Dict: `num/uom_dict.txt`
- This query: `uom = 'USD'` → find USD code at runtime; filter rows; selectivity ~87%

### num.value (double)
- File: `num/value.bin` (39,401,761 rows)
- This query: `value IS NOT NULL` → `!std::isnan(value)`; `SUM(n.value)` per (name, cik) group
- NULL sentinel: `std::numeric_limits<double>::quiet_NaN()`

### sub.fy (int32_t)
- File: `sub/fy.bin` (86,135 rows)
- This query: `s.fy = 2022`; selectivity ~31.7% → ~27,300 matching sub rows

### sub.name (dict_local, int32_t)
- File: `sub/name.bin` (86,135 rows)
- Dict: `sub/name_dict.txt`
- This query: GROUP BY key (part 1 of 2); output column (decode via dict)

### sub.cik (int32_t)
- File: `sub/cik.bin` (86,135 rows)
- This query: GROUP BY key (part 2 of 2); output column

## Table Stats
| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact (probe) | (none)  | 100,000    |
| sub   | 86,135     | dimension (build) | adsh | 100,000 |

## Query Analysis

**Step 1 — Build sub filter array (fy=2022):**
- Scan sub.fy.bin (86,135 rows); create `bool fy2022[86135]` marking which sub rows satisfy fy=2022
- Sub is tiny (344KB for int32_t); fits entirely in L2 cache

**Step 2 — Scan num, filter uom='USD' + value IS NOT NULL + fy=2022 via direct sub lookup:**
- For each num row i: `int sub_row = num_adsh[i]; if (!fy2022[sub_row]) continue;`
- Accumulate: group key = (sub_name[sub_row], sub_cik[sub_row]) → use (name_code, cik) packed as uint64_t
- Pack group key: `uint64_t key = ((uint64_t)(uint32_t)sub_name[sub_row] << 32) | (uint32_t)sub_cik[sub_row]`
- Hash map: key → double sum_value

**Step 3 — Compute HAVING threshold (scalar subquery):**
- The inner subquery groups by s2.cik (not name), same filters
- Run a second scan or reuse step 2 data aggregated by cik
- `AVG(sub_total)` = sum of all per-cik totals / number of distinct ciks

**Step 4 — Apply HAVING, sort, limit:**
- Filter groups where total_value > threshold
- Sort DESC by total_value, take top 100

**Note on double pass:** Both the outer query and the scalar subquery scan the same filtered num data. Consider computing both in a single pass: accumulate per (name,cik) and per-cik simultaneously.

## Indexes

### sub/indexes/adsh_row_map.bin
- Layout: `int32_t arr[max_adsh_code+1]` (identity: `arr[k]=k`)
- Usage: `int sub_row = num_adsh[i];` — no index file needed in practice since adsh_code == sub_row_index directly
- The sub columns are accessed as: `sub_fy[adsh_code]`, `sub_name[adsh_code]`, `sub_cik[adsh_code]`

## Implementation Notes
- Load uom_dict.txt → find USD code: `int usd_code = -1; for(int i=0;i<uom_dict.size();i++) if(uom_dict[i]=="USD") usd_code=i;`
- Single-pass over num (39M rows): read adsh, uom, value; do direct sub lookup
- MADV_SEQUENTIAL on num columns; MADV_RANDOM on sub columns (small, random access pattern)
- Group key hash map size: ~27k groups for outer query → use `next_power_of_2(27000*2) = 65536` slots
- Inner scalar subquery: ~20k cik groups → `next_power_of_2(20000*2) = 65536` slots
- Output decode: `name_dict[sub_name[sub_row]]` for name string
