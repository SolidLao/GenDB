# Q3 Guide

## Query
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
| num   | 39,401,761 | fact      | unsorted   | 100,000    |
| sub   | 86,135     | dimension | unsorted   | 100,000    |

## Column Reference

### num.uom (string_dict, int16_t — dict-encoded)
- File: `sf3.gendb/num/uom.bin` — 39,401,761 × 2 bytes = 78,803,522 bytes
- Dict: `sf3.gendb/num/uom_dict.txt` — ~15 distinct values; 'USD' ≈ 87.2% of rows
- This query: `WHERE n.uom = 'USD'` → find `usd_code` at runtime; filter `uom_code == usd_code`
- Dict loading pattern:
  ```cpp
  std::vector<std::string> uom_dict;
  // read sf3.gendb/num/uom_dict.txt line by line
  int16_t usd_code = -1;
  for (int16_t i = 0; i < (int16_t)uom_dict.size(); i++)
      if (uom_dict[i] == "USD") { usd_code = i; break; }
  ```

### num.value (monetary, double — raw IEEE 754)
- File: `sf3.gendb/num/value.bin` — 39,401,761 × 8 bytes = 315,214,088 bytes
- Null sentinel: `NaN` — check with `std::isnan(v)`
- This query: `WHERE value IS NOT NULL`, `SUM(n.value)`
- ⚠️ **C29 HARD CONSTRAINT**: SUM must accumulate as `int64_t` cents
  ```cpp
  // CORRECT:
  int64_t iv = llround(v * 100.0);
  sum_cents += iv;
  // Output: printf("%lld.%02lld", sum_cents/100, std::abs(sum_cents%100))
  // WRONG: double/long double/Kahan accumulator — causes ±0.01 mismatches at values ≥10^13
  ```
- Max observed value in num: ~1.88×10^17; USD-filtered max typically <1×10^14
- int64_t cents max = 9.2×10^18, safe for per-company USD sums

### num.adsh (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/num/adsh.bin` — 39,401,761 × 4 bytes = 157,607,044 bytes
- Dict: `sf3.gendb/shared/adsh_dict.txt` — 86,135 values
- This query: join key → probe sub_adsh_hash → get sub_row_id

### sub.fy (integer, int32_t — raw)
- File: `sf3.gendb/sub/fy.bin` — 86,135 × 4 bytes = 344,540 bytes
- Null sentinel: `INT32_MIN`
- This query: `WHERE s.fy = 2022` → `fy[sub_row] == 2022`
- Selectivity: ~19.6% of sub rows → ~16,882 qualifying filings

### sub.cik (integer, int32_t — raw)
- File: `sf3.gendb/sub/cik.bin` — 86,135 × 4 bytes = 344,540 bytes
- This query: `GROUP BY s.cik` — group key component; also `GROUP BY s2.cik` in subquery

### sub.name (string_dict, int32_t — dict-encoded)
- File: `sf3.gendb/sub/name.bin` — 86,135 × 4 bytes = 344,540 bytes
- Dict: `sf3.gendb/sub/name_dict.txt`
- This query: `GROUP BY s.name` — group key component (use name_code); output decoded string
- C31: double-quote name output — company names contain commas
- Dict loading pattern:
  ```cpp
  std::vector<std::string> name_dict;
  // read sf3.gendb/sub/name_dict.txt line by line
  // output: printf("\"%s\"", name_dict[name_code].c_str())
  ```

### sub.adsh (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/sub/adsh.bin` — 86,135 × 4 bytes = 344,540 bytes
- This query: join target via sub_adsh_hash

## Indexes

### sub_adsh_hash (hash on sub.adsh)
- File: `sf3.gendb/sub/indexes/sub_adsh_hash.bin`
- File layout: `[uint32_t cap=262144][SubADSHSlot × 262144]`
- File size: 4 + 262144 × 16 = 4,194,308 bytes
- Header parse (declare at function scope — C32):
  ```cpp
  const char*  sub_raw  = ...; // mmap result
  uint32_t     sub_cap  = *(const uint32_t*)sub_raw;
  uint32_t     sub_mask = sub_cap - 1;
  const SubADSHSlot* sub_ht = (const SubADSHSlot*)(sub_raw + 4);
  ```
- Slot struct (exact from build_indexes.cpp):
  ```cpp
  struct SubADSHSlot {
      int32_t adsh_code;  // INT32_MIN = empty
      int32_t row_id;
      int32_t _pad0;
      int32_t _pad1;
  };  // 16 bytes
  ```
- Empty sentinel: `adsh_code == INT32_MIN`
- Hash function (verbatim):
  ```cpp
  static inline uint64_t hash_int32(int32_t key) {
      return (uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL;
  }
  ```
- Probe pattern:
  ```cpp
  uint32_t pos = (uint32_t)(hash_int32(adsh_code) & sub_mask);
  for (uint32_t probe = 0; probe < sub_cap; probe++) {  // C24: bounded
      uint32_t slot = (pos + probe) & sub_mask;
      if (sub_ht[slot].adsh_code == INT32_MIN) break;   // not found
      if (sub_ht[slot].adsh_code == adsh_code) {
          int32_t sub_row = sub_ht[slot].row_id;
          // check sub_fy[sub_row] == 2022
          break;
      }
  }
  ```

## Query Analysis

### Execution Strategy (two-pass, single num scan preferred)

**Step 1 — Subquery: compute threshold**
- Scan num + join sub: same filters (`uom='USD'`, `fy=2022`, `value IS NOT NULL`)
- Aggregate `SUM(value)` per `s2.cik` (not per adsh — use cik as group key)
- Estimated groups: ~8,000 distinct CIKs with fy=2022
- Compute `AVG(sub_total)` over all per-CIK sums → threshold `T`
- ⚠️ **C29**: accumulate per-CIK sums as `int64_t cents`; compute `T = total_cents_sum / (count * 100.0)` as double for HAVING comparison

**Step 2 — Main query: aggregate per (name, cik)**
- Same scan + join: aggregate `SUM(value)` per `(name_code, cik)` pair
- Estimated groups: ~16,000 (name, cik) pairs
- ⚠️ **C29**: accumulate as `int64_t cents`
- Filter: `sum_cents / 100.0 > T` (HAVING clause)
- Top-100 by total_value DESC

**Key GROUP BY consistency (C15)**:
- Main: key = `(name_code, cik)` — both dimensions must be in key
- Subquery: key = `cik` only

**Recommended**: single scan of num that accumulates BOTH aggregations simultaneously to avoid reading 39M rows twice.

### Aggregation structs
```cpp
// Subquery: per CIK
struct CikAgg { int64_t sum_cents = 0; };
// Key: cik (int32_t directly)

// Main: per (name, cik)
struct NameCikAgg { int64_t sum_cents = 0; };
// Key: (name_code << 32) | (uint32_t)cik  OR  struct{int32_t name_code, cik;}
```

### Output
```cpp
// C31: quote name
printf("\"%s\",%d,", name_dict[name_code].c_str(), cik);
// C29: print as decimal
int64_t sc = sum_cents;
int64_t whole = sc / 100;
int64_t frac  = std::abs(sc % 100);
printf("%lld.%02lld\n", (long long)whole, (long long)frac);
```

### Sort / LIMIT
- Sort top-100 by total_value DESC
- C33: add stable tiebreaker (e.g., name_code ASC or cik ASC) to prevent non-determinism
