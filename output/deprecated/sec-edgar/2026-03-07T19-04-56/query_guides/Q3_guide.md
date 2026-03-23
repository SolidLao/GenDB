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

### num.uom_code (dict_code, uint8_t, dictionary-encoded via uom_dict)
- File: `num/uom_code.bin` (39,401,761 rows, 1 byte each)
- Dictionary: `num/uom_dict_offsets.bin` + `num/uom_dict_data.bin` (201 entries)
- This query: `WHERE uom = 'USD'` → find code for "USD", use uom_offsets for row range
- **Loading pattern:**
```cpp
auto offsets = readColumn<uint64_t>("num/uom_dict_offsets.bin");
auto data = readFile("num/uom_dict_data.bin");
uint8_t usdCode = 255;
for (size_t i = 0; i + 1 < offsets.size(); i++) {
    std::string_view sv(data.data() + offsets[i], offsets[i+1] - offsets[i]);
    if (sv == "USD") { usdCode = (uint8_t)i; break; }
}
```

### num.sub_fk (foreign_key_to_sub, uint32_t, dense FK array)
- File: `num/sub_fk.bin` (39,401,761 rows, 4 bytes each)
- This query: `JOIN sub s ON n.adsh = s.adsh` → direct row index into sub table

### num.value (numeric_value, double, fixed encoding, NaN = NULL)
- File: `num/value.bin` (39,401,761 rows, 8 bytes each)
- This query: `WHERE value IS NOT NULL` → `!std::isnan(value)`; `SUM(n.value)` aggregation

### sub.fy (year, int16_t, fixed encoding)
- File: `sub/fy.bin` (86,135 rows, 2 bytes each)
- This query: `WHERE s.fy = 2022` → `fy == 2022`; selectivity ~0.196

### sub.cik (identifier, int32_t, fixed encoding)
- File: `sub/cik.bin` (86,135 rows, 4 bytes each)
- This query: `GROUP BY s.cik` (both main query and subquery); output column
- ~8,886 distinct values

### sub.name (text, varlen_string)
- Files: `sub/name_offsets.bin` + `sub/name_data.bin`
- This query: `GROUP BY s.name`; output column
- ~9,641 distinct values
- Note: GROUP BY (s.name, s.cik) — since cik determines the company, the group key is effectively cik. name is functionally dependent on cik but the SQL groups by both.

## Table Stats

| Table | Rows       | Role      | Sort Order          | Block Size |
|-------|------------|-----------|---------------------|------------|
| num   | 39,401,761 | fact      | (uom_code, sub_fk)  | 100,000    |
| sub   | 86,135     | dimension | (none)              | 100,000    |

## Query Analysis

### Strategy

Both the main query and the HAVING subquery scan the same data with the same filters (`uom='USD'`, `fy=2022`, `value IS NOT NULL`). Compute both in a single pass.

**Recommended execution plan:**

1. **Pre-filter sub table:** Load `sub/fy.bin` (86K × 2 bytes). Build a boolean array `fy2022[sub_row] = (fy[sub_row] == 2022)`. Also load `sub/cik.bin` for grouping.
2. **Use uom_offsets** to find row range for `uom_code == usdCode`. Selectivity ~84.4%, so ~33.2M rows.
3. **Single-pass aggregation:** Scan the USD range. For each row where `!isnan(value) && fy2022[sub_fk[i]]`:
   - Look up `cik = sub_cik[sub_fk[i]]`
   - Accumulate into `std::unordered_map<int32_t, double>` keyed by cik: `sum_by_cik[cik] += value`
   - This serves BOTH the main query grouping and the HAVING subquery
4. **Compute HAVING threshold:** `AVG(sub_total)` = sum of all group totals / number of groups = total_sum / num_cik_groups
5. **Filter and output:** For each cik group where `sum > threshold`:
   - Look up name from sub table (find any sub_fk with matching cik, or build a cik→name map during step 1)
   - Collect `(name, cik, total_value)`
6. **Sort** by total_value DESC, take top 100.

### Key Optimizations
- **Single pass** computes both the main aggregation and the HAVING subquery
- **uom_offsets** narrows scan to contiguous USD rows
- Within USD range, rows sorted by sub_fk — streaming access to sub dimension
- Group key is just `int32_t cik` — very compact hash map
- ~8,000 estimated groups — hash map fits in L1 cache
- Build a `cik_to_name` map (8,886 entries) from sub table once, avoiding repeated varlen lookups

## Indexes

### uom_offsets (offset_table on uom_code)
- File: `num/uom_offsets.bin`
- Layout: `uint32_t num_entries`, then `num_entries` pairs of `(uint64_t start, uint64_t end)`
  - Entry at index `c` gives row range `[start, end)` where `uom_code == c`
- Usage: Read `usdCode` range → scan only ~33.2M rows instead of 39.4M
- **Loading pattern:**
```cpp
std::ifstream f("num/uom_offsets.bin", std::ios::binary);
uint32_t numEntries; f.read((char*)&numEntries, 4);
struct Range { uint64_t start, end; };
std::vector<Range> ranges(numEntries);
f.read((char*)ranges.data(), numEntries * sizeof(Range));
uint64_t usdStart = ranges[usdCode].start;
uint64_t usdEnd = ranges[usdCode].end;
```
