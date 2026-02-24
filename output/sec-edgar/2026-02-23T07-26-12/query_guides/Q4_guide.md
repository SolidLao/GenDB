# Q4 Guide

```sql
SELECT s.sic, t.tlabel, p.stmt,
       COUNT(DISTINCT s.cik) AS num_companies,
       SUM(n.value) AS total_value, AVG(n.value) AS avg_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN tag t ON n.tag = t.tag AND n.version = t.version
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = 'USD' AND p.stmt = 'EQ'
      AND s.sic BETWEEN 4000 AND 4999
      AND n.value IS NOT NULL AND t.abstract = 0
GROUP BY s.sic, t.tlabel, p.stmt
HAVING COUNT(DISTINCT s.cik) >= 2
ORDER BY total_value DESC LIMIT 500;
```

## Column Reference

### num.uom (dict_string, int16_t, dict_int16)
- File: `num/uom.bin` (39,401,761 × 2 = 78.8 MB)
- Dict: `num/uom_dict.txt` (201 entries). `int16_t usd_code = find_code(uom_dict, "USD");` (C2)
- Filter: `uom = 'USD'` (selectivity 0.872). Zone map guides scan to USD segment.

### num.value (double)
- File: `num/value.bin` (39,401,761 × 8 = 315 MB). NULL = NaN.
- **C29 CRITICAL**: max value 1e14. SUM and AVG must accumulate via int64_t cents:
  ```cpp
  int64_t iv = llround(v * 100.0);
  sum_cents += iv; count++;
  // total_value = sum_cents/100 (output as "sum_cents/100.sum_cents%100")
  // avg_value   = (double)sum_cents / count / 100.0
  ```

### num.adsh (dict_string, int32_t, dict_int32)
- File: `num/adsh.bin` (39,401,761 × 4 = 157.6 MB). Join key to sub and pre.

### num.tag (dict_string, int32_t, dict_int32)
- File: `num/tag.bin` (39,401,761 × 4 = 157.6 MB). Join key to tag and pre.

### num.version (dict_string, int32_t, dict_int32)
- File: `num/version.bin` (39,401,761 × 4 = 157.6 MB). Join key to tag and pre.

### sub.sic (integer, int32_t)
- File: `sub/sic.bin` (86,135 × 4 = 344 KB)
- Zone map: `indexes/sub_sic_zone_map.bin` (9 blocks of 10K)
- This query: `sic BETWEEN 4000 AND 4999` (selectivity 0.061 → ~5.3K sub rows qualify)
- Filter during sub hash build: `sic >= 4000 && sic <= 4999`

### sub.cik (integer, int32_t)
- File: `sub/cik.bin` (86,135 × 4 = 344 KB)
- This query: COUNT(DISTINCT cik) per (sic, tlabel, stmt) group.

### sub.adsh (dict_string, int32_t, dict_int32)
- File: `sub/adsh.bin` (86,135 × 4 = 344 KB)
- Used for `sub_adsh_hash` lookup.

### tag.abstract (integer, int32_t)
- File: `tag/abstract.bin` (1,070,662 × 4 = 4.3 MB). Values: 0 or 1.
- This query: `abstract = 0`. Filter during tag hash build.
- Note: workload analysis says `frac_zero=1.0` meaning most are 0, but still must check.

### tag.tlabel (dict_string, int32_t, dict_int32)
- File: `tag/tlabel.bin` (1,070,662 × 4 = 4.3 MB)
- Dict: `tag/tlabel_dict.txt`. GROUP BY tlabel → use tlabel_code as key; decode at output.

### tag.tag / tag.version — join keys
- Files: `tag/tag.bin`, `tag/version.bin` (1,070,662 × 4 each = 4.3 MB each)
- Index: `tag_tv_hash` maps (tag_code, version_code) → tag row_id.

### pre.stmt (dict_string, int16_t, dict_int16)
- File: `pre/stmt.bin` (9,600,799 × 2 = 19.2 MB)
- Dict: `pre/stmt_dict.txt` (8 entries). `int16_t eq_code = find_code(stmt_dict, "EQ");` (C2)
- This query: `stmt = 'EQ'` (selectivity 0.11 → ~1.06M pre rows)
- Zone map: `indexes/pre_stmt_zone_map.bin` — pre sorted by stmt; EQ blocks are contiguous.

### pre.adsh / pre.tag / pre.version — join keys
- Files: `pre/adsh.bin`, `pre/tag.bin`, `pre/version.bin` (9,600,799 × 4 each)
- Index: `pre_atv_hash` maps (adsh_code, tag_code, version_code) → presence.
  But for Q4 we need an inner join on 3 keys, not an anti-join.

## Table Stats

| Table | Rows       | Role      | Sort Order    | Block Size |
|-------|------------|-----------|---------------|------------|
| num   | 39,401,761 | fact      | (uom, ddate)  | 100,000    |
| sub   | 86,135     | dimension | (fy, sic)     | 10,000     |
| tag   | 1,070,662  | dimension | natural       | 100,000    |
| pre   | 9,600,799  | fact      | stmt (asc)    | 100,000    |

## Query Analysis
Four-way join: num is the center; three dimension lookups per num row.

**Recommended strategy**:
1. **Build sub hash** (small, filtered by sic BETWEEN 4000-4999): `adsh_code → {sic, cik}`. ~5.3K entries.
2. **Build tag hash** (filtered by abstract=0): use `tag_tv_hash` from disk OR build a runtime hash from tag/tag.bin+version.bin+abstract.bin+tlabel.bin. Since tag_tv_hash stores all 1.07M entries, at runtime filter: only insert into lookup if abstract[row_id]==0. Result: `(tag_code, version_code) → tlabel_code`. ~1.07M entries (most abstract=0).
3. **Build pre hash for stmt='EQ'**: scan pre using `pre_stmt_zone_map` to find EQ blocks. Build `(adsh_code, tag_code, version_code) → present` hash. ~1.06M entries.
4. **Scan num** using `num_uom_zone_map` (USD segment). For each qualifying row:
   - Probe sub hash (adsh) → get sic, cik
   - Probe tag hash (tag,version) → get tlabel_code
   - Probe pre hash (adsh,tag,version) → must exist for inner join
   - If all probes succeed: add to group `(sic, tlabel_code, eq_code)` → sum cents, count, cik_set

**C9 Note for per-step hashes**:
- sub_filtered: ~5.3K entries → cap = next_pow2(5300*2) = 16384
- pre_eq_hash: ~1.06M entries → cap = next_pow2(1.06M*2) = 2097152 (32MB)
- group aggregation: ~5K groups → cap = next_pow2(5000*2) = 8192

## Indexes

### num_uom_zone_map (zone_map on num.uom, int16_t)
- File: `indexes/num_uom_zone_map.bin` — 395 blocks of 100K rows
- Usage: skip non-USD blocks; ~12% of blocks may span USD boundary; rest are pure USD.

### pre_stmt_zone_map (zone_map on pre.stmt, int16_t)
- File: `indexes/pre_stmt_zone_map.bin` — 97 blocks of 100K rows
- Usage: find EQ blocks. `int16_t eq_code = find_code(stmt_dict,"EQ");`
  Skip blocks where `max_val < eq_code || min_val > eq_code`.

### tag_tv_hash (hash: (tag_code, version_code) → tag row_id)
- File: `indexes/tag_tv_hash.bin`
- Layout: `[uint64_t cap=4194304] [TagTVSlot × 4194304]`
- `TagTVSlot`: `{ int32_t tag_code; int32_t version_code; int32_t row_id; int32_t _pad; }`
- Empty: `tag_code == INT32_MIN`
- Hash: `h = (uint64_t)tag_code * 2654435761ULL ^ (uint64_t)version_code * 40503ULL`
- Usage: `row_id = probe(tag_code, version_code);` then `if (abstract[row_id] == 0)` use it.

### sub_adsh_hash (hash: adsh_code → sub row_id)
- File: `indexes/sub_adsh_hash.bin` — 262,144 slots × 8 bytes = 2 MB
- Same layout as Q2 guide.

## C29 SUM/AVG Output

```cpp
// total_value output: exact int64 / 100
int64_t sum_cents = group.sum_cents;
long long whole = sum_cents / 100;
long long frac  = std::abs(sum_cents % 100);
printf("%lld.%02lld", whole, frac);

// avg_value output:
double avg = (double)sum_cents / group.count / 100.0;
printf("%.2f", avg);  // last digit may differ by 0.01 at extreme scales
```
