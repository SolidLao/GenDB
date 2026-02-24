# Q3 Guide

```sql
SELECT s.name, s.cik, SUM(n.value) AS total_value
FROM num n JOIN sub s ON n.adsh = s.adsh
WHERE n.uom = 'USD' AND s.fy = 2022 AND n.value IS NOT NULL
GROUP BY s.name, s.cik
HAVING SUM(n.value) > (
    SELECT AVG(sub_total) FROM (
        SELECT SUM(n2.value) AS sub_total
        FROM num n2 JOIN sub s2 ON n2.adsh = s2.adsh
        WHERE n2.uom = 'USD' AND s2.fy = 2022 AND n2.value IS NOT NULL
        GROUP BY s2.cik
    ) avg_sub
)
ORDER BY total_value DESC LIMIT 100;
```

## Column Reference

### num.uom (dict_string, int16_t, dict_int16)
- File: `num/uom.bin` (39,401,761 × 2 = 78.8 MB)
- Dict: `num/uom_dict.txt` (201 entries). `int16_t usd_code = find_code(uom_dict, "USD");` (C2)
- This query: `uom = 'USD'` → selectivity 0.872 → ~34.3M rows qualify
- Zone map: `indexes/num_uom_zone_map.bin` — all USD rows are contiguous (num sorted by uom).
  Skip blocks where max_val < usd_code or min_val > usd_code.

### num.value (double)
- File: `num/value.bin` (39,401,761 × 8 = 315 MB)
- NULL: NaN. Filter: `!std::isnan(v)` (selectivity ~0.98)
- **C29 CRITICAL**: max observed value = 1e14. SUM must use int64_t cents accumulation:
  ```cpp
  // For each qualifying value v:
  int64_t iv = llround(v * 100.0);
  sum_cents += iv;
  // Output: printf("%lld.%02lld", sum_cents/100, abs(sum_cents%100))
  ```
  Do NOT use `double sum += v` — last-digit errors guaranteed at 1e14 scale.

### num.adsh (dict_string, int32_t, dict_int32)
- File: `num/adsh.bin` (39,401,761 × 4 = 157.6 MB)
- This query: join key to sub. Use `sub_adsh_hash` for O(1) lookup.

### sub.fy (integer, int32_t)
- File: `sub/fy.bin` (86,135 × 4 = 344 KB)
- Zone map: `indexes/sub_fy_zone_map.bin` (9 blocks of 10K)
- This query: `fy = 2022` — filter sub during hash build.

### sub.name (dict_string, int32_t, dict_int32)
- File: `sub/name.bin` (86,135 × 4 = 344 KB)
- Dict: `sub/name_dict.txt`. GROUP BY name → use name_code as group key; decode at output.

### sub.cik (integer, int32_t)
- File: `sub/cik.bin` (86,135 × 4 = 344 KB)
- This query: GROUP BY (name, cik); HAVING subquery groups by cik alone.

### sub.adsh (dict_string, int32_t, dict_int32)
- File: `sub/adsh.bin` (86,135 × 4 = 344 KB)
- Join key. The `sub_adsh_hash` maps adsh_code → row_id.

## Table Stats

| Table | Rows       | Role      | Sort Order   | Block Size |
|-------|------------|-----------|--------------|------------|
| num   | 39,401,761 | fact      | (uom, ddate) | 100,000    |
| sub   | 86,135     | dimension | (fy, sic)    | 10,000     |

## Query Analysis
This query has two logically identical scans of `num + sub` (main query + HAVING subquery).
**Optimization**: perform a single pass, building both aggregations simultaneously.

- **Step 1**: Build sub hash (fy=2022): `adsh_code → {name_code, cik}`. ~27K entries.
- **Step 2**: Zone-map-guided scan of num for `uom = 'USD'`.
  Parallel over chunks of the USD segment (which is contiguous).
  For each row: probe sub hash → if found:
    - Accumulate `sum_cents` into group map keyed by `(name_code, cik)` (for main query)
    - Accumulate `sum_cents` into group map keyed by `cik` alone (for HAVING subquery)
  Thread-local group maps → merge after barrier.
- **Step 3**: Compute AVG of per-cik sums → HAVING threshold.
- **Step 4**: Filter (name,cik) groups where total > threshold → sort DESC → LIMIT 100.
- Group counts: ~27K (name,cik) groups, ~12K cik groups — small enough for unordered_map.

## Indexes

### num_uom_zone_map (zone_map on num.uom, int16_t)
- File: `indexes/num_uom_zone_map.bin`
- Layout: `[uint32_t num_blocks=395] [ZoneBlock<int16_t> × 395]`
- `ZoneBlock<int16_t>`: `{ int16_t min_val; int16_t max_val; uint32_t row_count; }`
- num sorted by uom → USD segment is a contiguous range of blocks.
- **Usage pattern**:
  ```cpp
  uint32_t row_offset = 0;
  for (uint32_t b = 0; b < num_blocks; ++b) {
      if (blocks[b].max_val < usd_code || blocks[b].min_val > usd_code) {
          row_offset += blocks[b].row_count; continue;
      }
      // process rows [row_offset, row_offset + blocks[b].row_count)
      row_offset += blocks[b].row_count;
  }
  ```

### sub_adsh_hash (hash: adsh_code → sub row_id)
- File: `indexes/sub_adsh_hash.bin`
- Layout: `[uint64_t cap=262144] [SubAdsSlot × 262144]`
- `SubAdsSlot`: `{ int32_t adsh_code; int32_t row_id; }` empty: `adsh_code == INT32_MIN`
- Hash: `h = (uint64_t)(uint32_t)adsh_code * 2654435761ULL`
- Probe: `for (p=0; p<cap; ++p) { idx=(h+p)&(cap-1); if empty→miss; if match→found }`

## C++ Precision Pattern (C29)

```cpp
// Thread-local accumulation maps
unordered_map<uint64_t, int64_t> group_sum;  // key=(name_code<<32|cik) → cents
unordered_map<int32_t,  int64_t> cik_sum;    // cik → cents (for HAVING avg)

// For each qualifying num row:
int64_t iv = llround(value * 100.0);
uint64_t key = ((uint64_t)name_code << 32) | (uint32_t)cik;
group_sum[key] += iv;
cik_sum[cik] += iv;

// Compute HAVING threshold
int64_t total_cik_sum = 0; int64_t ncik = cik_sum.size();
for (auto& [k,v] : cik_sum) total_cik_sum += v;
double avg_cents = (double)total_cik_sum / ncik;

// Output format: sum_cents/100 as decimal
// printf("%.2f", (double)sum_cents / 100.0) — safe since sum_cents is exact int64
```
