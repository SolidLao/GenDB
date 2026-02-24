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
Expected output: 100 rows.

## Column Reference

### num/uom.bin (dict_string, int16_t)
- File: `num/uom.bin` (39,401,761 rows, 75.2 MB)
- Dict: `num/uom_dict.txt` — load at runtime (C2).
  - `int16_t usd_code = find_code(uom_dict, "USD"); // known code 0 at ingest time`
- Filter: `n.uom = 'USD'` → ~88.7% of rows (~34.9M rows)

### num/value.bin (monetary_decimal, double)
- File: `num/value.bin` (39,401,761 rows, 300.6 MB)
- **C29 WARNING — CRITICAL:** `SUM(n.value)` MUST use `int64_t cents` accumulation.
  - Max individual value observed in dataset: 5.94e13 (≫ 10^13 threshold)
  - Pattern:
    ```cpp
    int64_t sum_cents = 0;
    sum_cents += llround(value * 100.0);
    // Output: sum_cents/100 and abs(sum_cents%100)
    // printf("%lld.%02lld", sum_cents/100, llabs(sum_cents%100))
    ```
  - NEVER use double/float/long double accumulator for SUM. Kahan summation also FAILS.

### num/adsh.bin (dict_string, int32_t, global dict)
- File: `num/adsh.bin` (39,401,761 rows, 150.3 MB)
- Dict: `adsh_global_dict.txt` (86,135 entries)
- JOIN key with sub.adsh.

### sub/fy.bin (integer, int32_t)
- File: `sub/fy.bin` (86,135 rows, 0.3 MB)
- Filter: `s.fy = 2022` — compare directly: `fy[i] == 2022`
- Selectivity: ~17% (~14.6K sub rows)

### sub/cik.bin (integer, int32_t)
- File: `sub/cik.bin` (86,135 rows, 0.3 MB)
- GROUP BY key (main query groups by cik+name; HAVING subquery groups by cik only)

### sub/name.bin (dict_string, int32_t)
- File: `sub/name.bin` (86,135 rows, 0.3 MB)
- Dict: `sub/name_dict.txt` — load at runtime.
- GROUP BY key (outer query). Output decoded string.

### sub/adsh.bin (dict_string, int32_t, global dict)
- File: `sub/adsh.bin` (86,135 rows, 0.3 MB)
- JOIN key. Shares global adsh dict with num/adsh.bin.

## Table Stats
| Table | Rows       | Role       | Sort Order | Block Size |
|-------|------------|------------|------------|------------|
| num   | 39,401,761 | fact       | none       | 65,536     |
| sub   | 86,135     | dimension  | adsh       | 65,536     |

## Query Analysis
**Join:** num → sub on adsh (FK-PK). Sub is tiny.
**Outer filter:** n.uom='USD' (~34.9M rows) AND s.fy=2022 (~14.6K sub rows)
**GROUP BY:** (s.name, s.cik) for outer; (s.cik) for HAVING subquery
**HAVING:** total_value > AVG(sub_total_by_cik)
**Key insight:** HAVING threshold = AVG of per-cik SUM(value). Must compute per-cik sums first.

**Execution plan:**
1. Precompute sub lookup: build hash map from adsh_code → (cik, name_code, fy) for fy=2022 rows
2. Pass 1 (aggregation): for each num row with uom='USD':
   - Look up sub by adsh → get cik, name_code
   - Accumulate two hash maps simultaneously:
     a. (name_code, cik) → int64_t sum_cents  [outer GROUP BY]
     b. cik → int64_t sum_cents               [HAVING subquery GROUP BY]
3. Compute HAVING threshold: `threshold = sum_of_all_cik_totals / num_distinct_ciks`
4. Filter outer groups where total_value_cents > threshold_cents
5. Sort top 100 by total_value DESC, decode name strings, output

**HAVING threshold computation:**
```cpp
// After pass 1:
int64_t grand_sum_cents = 0;
int64_t num_ciks = 0;
for (auto& [cik, total] : cik_totals) {
    grand_sum_cents += total;  // sum of all per-cik totals
    num_ciks++;
}
// threshold = AVG(sub_total) = grand_sum_cents / num_ciks
// Compare: group_sum_cents * num_ciks > grand_sum_cents (avoids division)
```

## Indexes

### sub_adsh_hash (hash on sub.adsh)
- File: `indexes/sub_adsh_hash.bin`
- Layout: `[uint32_t cap=262144][{int32_t adsh_code, uint32_t row_idx} × 262144]`
- Size: 2.0 MB (fits in L3 cache)
- Usage: For each num row (after uom filter), look up sub row by adsh_code → get cik, name_code, fy
- Probe: `h = (adsh_code * 2654435761u) & (cap-1)`, linear probe with bound check (C24)

## Output Format
Print int64_t cents as decimal:
```cpp
int64_t cents = total_sum_cents;
bool neg = cents < 0;
if (neg) cents = -cents;
printf("%s%lld.%02lld", neg?"-":"", (long long)(cents/100), (long long)(cents%100));
```
Output 100 rows: `name,cik,total_value`
