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

### num.adsh (fk_code, uint32_t, dict_shared_adsh)
- File: `num/adsh.bin` (39,401,761 rows of uint32_t)
- This query: JOIN key to sub

### num.uom (category, uint16_t, dict_uom)
- File: `num/uom.bin` (39,401,761 rows of uint16_t)
- Dictionary: `dictionaries/uom.dict` (~30 entries)
- This query: `WHERE n.uom = 'USD'` — find code for "USD", compare uint16_t
- Selectivity: ~84.4% (~33.3M rows pass)

### num.value (measure, double, raw)
- File: `num/value.bin` (39,401,761 rows of double)
- This query: `SUM(n.value)` — aggregation target

### num.value_null (null_bitmap, uint8_t, raw)
- File: `num/value_null.bin` (39,401,761 rows of uint8_t)
- This query: `WHERE n.value IS NOT NULL` — filter where value_null == 0

### sub.adsh (pk_code, uint32_t, dict_shared_adsh)
- File: `sub/adsh.bin` (86,135 rows of uint32_t)
- This query: JOIN key from num

### sub.fy (year, int16_t, raw)
- File: `sub/fy.bin` (86,135 rows of int16_t)
- This query: `WHERE s.fy = 2022` — selectivity ~19.6%

### sub.name (string_code, uint32_t, dict_name)
- File: `sub/name.bin` (86,135 rows of uint32_t)
- Dictionary: `dictionaries/name.dict` (~86,000 entries)
- This query: `GROUP BY s.name` — group key, output column

### sub.cik (identifier, int32_t, raw)
- File: `sub/cik.bin` (86,135 rows of int32_t)
- This query: `GROUP BY s.cik` — group key, output column; also GROUP BY in HAVING subquery

## Table Stats

| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | none       | 100,000    |
| sub   | 86,135     | dimension | none       | 100,000    |

## Query Analysis

- **Pattern**: Fact-dimension join with filters, grouped aggregation, HAVING with scalar subquery, ordered LIMIT
- **Join**: num.adsh -> sub.adsh (FK-PK, every num row matches exactly one sub row)
- **Filters combined selectivity**:
  - num: uom='USD' (~84.4%) AND value IS NOT NULL → ~84.4% of 39.4M ≈ 33.3M rows
  - sub: fy=2022 (~19.6%) → ~16,900 sub rows
  - Combined (after join): ~33.3M * 0.196 ≈ 6.5M qualifying rows
- **Aggregation**: GROUP BY (name_code, cik) — estimated ~15,000 groups
- **HAVING subquery**: Same scan as main but GROUP BY cik only → ~15,000 groups → compute AVG of those SUM values. This is a scalar threshold.
- **Optimization**: Both main query and HAVING subquery share the same base scan (num WHERE uom='USD' AND value IS NOT NULL JOIN sub WHERE fy=2022). Compute both in a single pass:
  - Map 1: (name_code, cik) -> SUM(value) for main query
  - Map 2: cik -> SUM(value) for HAVING subquery
  - Since name is functionally determined by (adsh -> sub row -> name), and cik is also from sub, the group key (name_code, cik) could have multiple name_codes per cik if different filings from the same company have different names. Safe approach: use both.
- **Order**: total_value DESC, LIMIT 100

## Indexes

### sub_adsh_lookup (dense_array on sub.adsh)
- File: `indexes/sub_adsh_lookup.idx`
- Layout: uint32_t size, then uint32_t[size] array
- Lookup: `array[adsh_code]` → sub row_id (UINT32_MAX if not found)
- Usage: For each num row passing filters, look up sub row to check fy and get name/cik

## Execution Strategy

1. Load dictionaries: uom, name
2. Find uom code for "USD"
3. Load sub_adsh_lookup index
4. Mmap sub columns: fy, name, cik
5. **Pre-filter sub**: Build a bitset or set of adsh_codes where sub.fy == 2022. Scan sub.fy (86K rows), for each row where fy==2022, record sub.adsh[row] as a valid code. This enables O(1) check during num scan.
6. **Single pass over num**: For each row where uom==usd_code AND value_null==0:
   - Check if adsh_code passes sub filter (lookup sub_adsh_lookup, check fy==2022 or use precomputed set)
   - Get sub_row, read name_code and cik
   - Accumulate into both hash maps
7. Compute HAVING threshold: iterate cik map, compute sum of all sub_totals / count → avg_sub_total
8. Filter main map groups: keep where total_value > avg_sub_total
9. Partial sort top-100 by total_value DESC
10. Decode name codes via dictionary for output
