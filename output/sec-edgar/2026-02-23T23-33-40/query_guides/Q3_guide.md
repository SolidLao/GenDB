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

## Table Stats
| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | (none)     | 65536      |
| sub   | 86,135     | dimension | (none)     | 65536      |

## Query Analysis
- **Two-pass** over the same data (main query + HAVING subquery share identical filters)
- Join: num→sub on adsh (FK-PK, use sub_adsh_index)
- Filters: `n.uom = 'USD'` (selectivity ~0.872), `s.fy = 2022` (~0.317), `n.value IS NOT NULL` (~0.99)
- Main aggregation: GROUP BY (s.name, s.cik) → SUM(n.value); ~27K distinct groups
- HAVING subquery: GROUP BY s2.cik → SUM(n2.value) per cik; compute AVG of those sums → threshold
- Strategy: (1) scan num once, collect (adsh_code, value) for uom='USD' AND value IS NOT NULL;
  (2) for each row, probe sub_adsh_index → get sub_row; filter fy==2022;
  (3) accumulate per-(name_code, cik) and per-cik sums simultaneously in one pass;
  (4) compute AVG of per-cik sums → threshold; filter main groups; top-100 output
- **C29**: SUM(n.value) MUST use int64_t cents accumulator (max value ~3×10¹⁵)
- **C15**: GROUP BY key MUST include BOTH s.name AND s.cik (two separate companies can share a name)
- **C33**: stable tiebreaker needed — add (s.name, s.cik) as secondary sort key

## Column Reference

### num.adsh (dict_string, int32_t)
- File: `num/adsh.bin` (39,401,761 rows × 4 bytes = ~150MB)
- Dict: `num/adsh_dict.txt` — global dict, same content as sub/adsh_dict.txt
- This query: join probe key → `sub_adsh_index` lookup

### num.uom (dict_string, int16_t)
- File: `num/uom.bin` (39,401,761 rows × 2 bytes = ~75MB)
- Dict: `num/uom_dict.txt` — load at runtime to find code for 'USD'
  ```cpp
  std::vector<std::string> uom_dict; // load num/uom_dict.txt line-by-line
  int16_t usd_code = -1;
  for (int16_t c = 0; c < (int16_t)uom_dict.size(); ++c)
      if (uom_dict[c] == "USD") { usd_code = c; break; }
  // Filter: skip row if uom_code != usd_code
  ```
- Selectivity: ~0.872 → ~34.3M rows pass

### num.value (decimal, double)
- File: `num/value.bin` (39,401,761 rows × 8 bytes = ~300MB)
- NULL encoding: `std::numeric_limits<double>::quiet_NaN()` — test with `std::isnan(v)`
- **C29 HARD CONSTRAINT**: max observed ~3×10¹⁵ >> 10¹³ threshold.
  SUM MUST accumulate as `int64_t` cents:
  ```cpp
  int64_t iv = llround(v * 100.0);
  sum_cents += iv;
  ```
  Output: `printf("%lld.%02lld", sum_cents / 100, std::abs(sum_cents % 100))`
  NEVER use double, long double, or Kahan sum for this column's SUM/AVG.

### sub.adsh (dict_string, int32_t)
- File: `sub/adsh.bin` (86,135 rows × 4 bytes = ~336KB)
- Dict: `sub/adsh_dict.txt` — same global dict as num/adsh_dict.txt
- This query: looked up indirectly via sub_adsh_index

### sub.fy (integer, int32_t)
- File: `sub/fy.bin` (86,135 rows × 4 bytes = ~336KB)
- This query: filter `s.fy = 2022` → `sub_fy[sub_row] == 2022`
- Selectivity: ~0.317 → ~27K sub rows qualify (combined with num uom filter: effective selectivity ~0.277)

### sub.name (dict_string, int32_t)
- File: `sub/name.bin` (86,135 rows × 4 bytes = ~336KB)
- Dict: `sub/name_dict.txt` — load at runtime
- This query: GROUP BY key (code) + output column (decode to string for output)
- Decode for output: `name_dict[name_code].c_str()` — double-quote (C31)

### sub.cik (integer, int32_t)
- File: `sub/cik.bin` (86,135 rows × 4 bytes = ~336KB)
- This query: GROUP BY key (raw int32_t) + output column
- C15: must be part of the hash map key alongside name_code

## Indexes

### sub_adsh_index (hash on sub.adsh)
- File: `sf3.gendb/indexes/sub_adsh_index.bin`
- **Struct layout** (verbatim from build_indexes.cpp):
  ```cpp
  struct SubSlot { int32_t adsh_code; int32_t sub_row; };
  // sizeof(SubSlot) == 8 bytes
  ```
- **File layout**: `uint32_t cap | cap × SubSlot`
- **Sentinel**: `adsh_code == -1` (empty slot)
- **Hash function** (verbatim from build_indexes.cpp):
  ```cpp
  static inline uint32_t hash_i32(int32_t k) {
      uint32_t x = (uint32_t)k;
      x = ((x >> 16) ^ x) * 0x45d9f3bU;
      x = ((x >> 16) ^ x) * 0x45d9f3bU;
      x = (x >> 16) ^ x;
      return x;
  }
  ```
- **Capacity**: `next_pow2(86135 × 2)` = 262,144 slots
- **Header parse** (C27/C32 — must be at function scope):
  ```cpp
  const char* sub_idx_raw = /* mmap sf3.gendb/indexes/sub_adsh_index.bin */;
  uint32_t sub_cap  = *(const uint32_t*)sub_idx_raw;
  uint32_t sub_mask = sub_cap - 1;
  const SubSlot* sub_ht = (const SubSlot*)(sub_idx_raw + 4);
  ```
- **Probe pattern** (C24: bounded loop):
  ```cpp
  uint32_t slot = hash_i32(adsh_code) & sub_mask;
  for (uint32_t probe = 0; probe < sub_cap; ++probe) {
      uint32_t s = (slot + probe) & sub_mask;
      if (sub_ht[s].adsh_code == -1) break;
      if (sub_ht[s].adsh_code == adsh_code) {
          int32_t sub_row = sub_ht[s].sub_row;
          // check sub_fy[sub_row] == 2022
          // then accumulate into (name_code, cik) and (cik) aggregators
          break;
      }
  }
  ```

## Aggregation

### Main aggregation: GROUP BY (s.name, s.cik)
- Key struct: `struct Q3Key { int32_t name_code; int32_t cik; }`
- Value: `int64_t sum_cents` (C29)
- Estimated ~27K distinct groups → cap = `next_pow2(27000 × 2)` = 65,536 (C9)
- Thread-local maps merged after parallel scan (P17/P20)

### HAVING subquery: GROUP BY s2.cik → AVG(SUM per cik)
- Same scan pass, accumulate per-cik sum simultaneously
- Key: `int32_t cik`; Value: `int64_t sum_cents`
- After scan: `double threshold_cents = (double)total_cik_sum_cents / n_ciks;`
- Filter main groups: `(double)group_sum_cents > threshold_cents`

## Output
- Columns: `s.name` (string), `s.cik` (int32_t), `total_value` (int64_t cents → formatted)
- Double-quote name string (C31): `printf("\"%s\"", name_dict[name_code].c_str())`
- Sort: `total_value DESC`; add (name_code, cik) as stable tiebreaker (C33)
- LIMIT 100
- date_encoding: no date columns in this query — no epoch conversion needed
