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

### uom_code (dict_code, int8_t) — num
- File: `num/uom_code.bin` (39,401,761 rows × 1 byte)
- This query: `WHERE uom = 'USD'` → `uom_code == usd_code`.
  Load at startup: `usd_code = load_dict("indexes/uom_codes.bin")["USD"]`.
- Selectivity: 87% → ~34.3M rows pass; zone maps minimize skipped blocks
  (USD rows are first in sorted order, so most blocks have min_uom ≤ usd_code ≤ max_uom).

### adsh_code (dict_code, int32_t) — num
- File: `num/adsh_code.bin` (39,401,761 rows × 4 bytes)
- This query: join key. `sub` row is `sub[adsh_code]` (O(1) since adsh_code = sub row index).
- Also used to check `sub[adsh_code].fy == 2022` before accumulating value.

### value (decimal, double) — num
- File: `num/value.bin` (39,401,761 rows × 8 bytes)
- This query: `SUM(n.value)` into per-group accumulator (double or int64_t depending on range).
- All stored rows have non-null values (NULL rows skipped at ingestion time).

### fy (integer, int16_t) — sub
- File: `sub/fy.bin` (86,135 rows × 2 bytes)
- This query: `s.fy = 2022` → `sub_fy[adsh_code] == 2022`. Load full array into memory
  (86135 × 2 = 172KB — fits in L1 cache).
- Selectivity: ~20% → filters out ~80% of adsh values.

### cik (integer, int32_t) — sub
- File: `sub/cik.bin` (86,135 rows × 4 bytes)
- This query: GROUP BY `s.cik` and SELECT `s.cik`. Load full array (86135 × 4 = 344KB).
- HAVING subquery groups by `s2.cik` (not name); `cik` is the subquery group key.

### name (varlen string) — sub
- Files: `sub/name_offsets.bin` (86,136 × uint32_t), `sub/name_data.bin` (char[])
- This query: GROUP BY `s.name` and SELECT `s.name`.
- Access pattern:
  ```cpp
  // name for sub row i:
  string name(name_data + name_offsets[i], name_offsets[i+1] - name_offsets[i]);
  ```
- Grouping strategy: group by `(adsh_code, cik)` first (integer keys), then decode name
  for output. Since adsh_code uniquely determines name (sub is PK on adsh), grouping by
  adsh_code and cik is equivalent — decode name only for result rows.

## Table Stats

| Table | Rows       | Role                  | Sort Order          | Block Size |
|-------|------------|-----------------------|---------------------|------------|
| num   | 39,401,761 | fact (scan ×2)        | (uom_code, ddate)   | 100,000    |
| sub   | 86,135     | dimension (probe ×2)  | adsh (= code order) | 100,000    |

## Query Analysis
- **Two-pass strategy** (same filter `uom='USD' AND fy=2022` in both passes; zone maps apply):

  **Pass 1 — HAVING subquery**: compute `AVG(sub_total)` where `sub_total = SUM(value)` per
  cik for fy=2022 USD rows:
  ```
  unordered_map<int32_t, double> cik_sum;   // cik → sum
  // scan num: for each row where uom_code==usd_code && sub_fy[adsh_code]==2022:
  //   cik_sum[sub_cik[adsh_code]] += value;
  // then: avg_threshold = sum(cik_sum values) / cik_sum.size()
  ```
  Expected groups: ~430 distinct sic codes but ~72,000 distinct cik values; fy=2022 subset
  has ~17,000 ciks. `avg_threshold` is a scalar double.

  **Pass 2 — main query**: `SUM(value)` per `(adsh_code, cik)` group (equivalent to `name, cik`
  since adsh→name is 1:1):
  ```
  unordered_map<int64_t, double> group_sum;
  // key = (int64_t)adsh_code << 32 | (uint32_t)cik
  // or group by adsh_code (uniquely determines cik and name) → simpler
  unordered_map<int32_t, double> adsh_sum;  // adsh_code → sum
  // After scan: apply HAVING, then decode name for survivors, sort, limit 100
  ```
- **HAVING**: filter `adsh_sum[a] > avg_threshold`.
- **Output**: decode name for surviving adsh_codes, sort by total_value DESC, limit 100.

## Indexes

### num_zone_maps (zone_map on uom_code, ddate)
- File: `indexes/num_zone_maps.bin`
- Layout:
  ```cpp
  uint32_t n_blocks;   // = 395
  struct ZoneMap { int8_t min_uom, max_uom; int32_t min_ddate, max_ddate; };
  // sizeof(ZoneMap) = 12 (2 bytes padding between max_uom and min_ddate)
  ```
- Usage for this query (both passes): skip block `b` if
  `zm[b].min_uom > usd_code || zm[b].max_uom < usd_code`.
  USD rows form ~87% of data; zone map benefit is moderate (cannot skip USD-containing blocks).
  However, blocks at the tail (pure, shares, etc.) can be skipped.

### uom_codes (dict_file on uom)
- File: `indexes/uom_codes.bin`
- Layout: `uint8_t N; N × { int8_t code, uint8_t slen, char[slen] }`
- Usage: `usd_code = load_dict("indexes/uom_codes.bin")["USD"]` at query startup.
