# Q4 Guide

## SQL
```sql
SELECT s.sic, t.tlabel, p.stmt,
       COUNT(DISTINCT s.cik) AS num_companies,
       SUM(n.value) AS total_value,
       AVG(n.value) AS avg_value
FROM num n
JOIN sub s ON n.adsh = s.adsh
JOIN tag t ON n.tag = t.tag AND n.version = t.version
JOIN pre p ON n.adsh = p.adsh AND n.tag = p.tag AND n.version = p.version
WHERE n.uom = 'USD' AND p.stmt = 'EQ'
      AND s.sic BETWEEN 4000 AND 4999
      AND n.value IS NOT NULL AND t.abstract = 0
GROUP BY s.sic, t.tlabel, p.stmt
HAVING COUNT(DISTINCT s.cik) >= 2
ORDER BY total_value DESC
LIMIT 500;
```
Expected output: 500 rows.

## Column Reference

### num/uom.bin (dict_string, int16_t)
- File: `num/uom.bin` (39,401,761 rows, 75.2 MB)
- Dict: `num/uom_dict.txt` — load at runtime (C2).
  - `int16_t usd_code = find_code(uom_dict, "USD"); // code 0 at ingest time`
- Filter: `n.uom = 'USD'` → ~88.7% of num rows (~34.9M qualifying rows)

### num/value.bin (monetary_decimal, double)
- File: `num/value.bin` (39,401,761 rows, 300.6 MB)
- **C29 WARNING:** `SUM(n.value)` must accumulate as int64_t cents (C29).
  - Pattern: `sum_cents += llround(value * 100.0);`
  - `AVG(n.value)`: compute as `(double)sum_cents / 100.0 / count`

### num/adsh.bin (dict_string, int32_t, global dict)
- File: `num/adsh.bin` (39,401,761 rows, 150.3 MB)
- JOIN key for sub and pre.

### num/tag.bin (dict_string, int32_t, global dict)
- File: `num/tag.bin` (39,401,761 rows, 150.3 MB)
- JOIN key for tag and pre.

### num/version.bin (dict_string, int32_t, global dict)
- File: `num/version.bin` (39,401,761 rows, 150.3 MB)
- JOIN key for tag and pre.

### sub/sic.bin (integer, int32_t)
- File: `sub/sic.bin` (86,135 rows, 0.3 MB)
- Filter: `s.sic BETWEEN 4000 AND 4999` → ~5% of sub rows (~4.3K rows)
- Direct int32_t comparison.

### sub/cik.bin (integer, int32_t)
- File: `sub/cik.bin` (86,135 rows, 0.3 MB)
- GROUP BY key, and `COUNT(DISTINCT s.cik)` within each (sic, tlabel) group.

### sub/adsh.bin (dict_string, int32_t, global dict)
- File: `sub/adsh.bin` (86,135 rows, 0.3 MB)

### tag/abstract.bin (boolean_integer, int32_t)
- File: `tag/abstract.bin` (1,070,662 rows, 4.1 MB)
- Filter: `t.abstract = 0` → ~100% of rows (workload analysis: abstract≈0 for all rows)
- Direct int32_t == 0 comparison.

### tag/tlabel.bin (dict_string, int32_t)
- File: `tag/tlabel.bin` (1,070,662 rows, 4.1 MB)
- Dict: `tag/tlabel_dict.txt` — load at runtime (C2).
- GROUP BY key. Output: decode `tlabel_dict[tlabel_code]`.

### tag/tag.bin (dict_string, int32_t, global dict)
- File: `tag/tag.bin` (1,070,662 rows, 4.1 MB)
- JOIN key with num.tag.

### tag/version.bin (dict_string, int32_t, global dict)
- File: `tag/version.bin` (1,070,662 rows, 4.1 MB)
- JOIN key with num.version.

### pre/stmt.bin (dict_string, int16_t)
- File: `pre/stmt.bin` (9,600,799 rows, 18.3 MB)
- Dict: `pre/stmt_dict.txt` — load at runtime (C2).
  - `int16_t eq_code = find_code(stmt_dict, "EQ"); // code 3 at ingest time`
- Filter: `p.stmt = 'EQ'` — this is pre-filtered in the pre_eq_hash index.
- In the GROUP BY: p.stmt is always 'EQ' due to the filter, but still include in output.

### pre/adsh.bin, pre/tag.bin, pre/version.bin (int32_t, global dicts)
- JOIN keys with num.

## Table Stats
| Table | Rows       | Role      | Sort Order | Block Size |
|-------|------------|-----------|------------|------------|
| num   | 39,401,761 | fact      | none       | 65,536     |
| sub   | 86,135     | dimension | adsh       | 65,536     |
| tag   | 1,070,662  | dimension | none       | 65,536     |
| pre   | 9,600,799  | dimension | none       | 65,536     |

## Query Analysis
**Joins:** num→sub (adsh), num→tag (tag+version), num→pre (adsh+tag+version)
**Filters applied in combination:**
1. n.uom='USD': 88.7% pass
2. s.sic BETWEEN 4000-4999: ~5% of sub → very few matching adsh codes
3. p.stmt='EQ': only ~10.2% of pre rows
4. t.abstract=0: ~100% pass

**Effective plan:** build sub filter first (sic 4000-4999), then scan num with multi-index lookups.

**Step 1:** Scan sub. For each sub row with sic IN [4000,4999], record adsh_code → (cik, sic).
  - Build hash set: `qualifying_adsh_codes` (small, ~4.3K entries)

**Step 2:** Scan num for uom='USD', value!=NULL:
  - Check adsh in qualifying_adsh_codes → if not, skip
  - Look up sub_adsh_hash → get cik, sic
  - Look up tag_tv_hash → get tag row → check abstract=0, get tlabel_code
  - Look up pre_eq_hash → check (adsh, tag, version) exists with stmt='EQ'
  - If all pass: aggregate into (sic, tlabel_code) → {int64_t sum_cents, int64_t count, set<cik>}

**Step 3:** HAVING COUNT(DISTINCT cik) >= 2, ORDER BY total_value DESC, LIMIT 500.

## Indexes

### sub_adsh_hash (hash on sub.adsh)
- File: `indexes/sub_adsh_hash.bin`
- Layout: `[uint32_t cap=262144][{int32_t adsh_code, uint32_t row_idx} × 262144]`
- Size: 2.0 MB
- Hash: `h = ((uint32_t)adsh_code * 2654435761u) & (cap-1)`, linear probe (C24)
- Usage: adsh_code → sub_row → sub_sic[sub_row], sub_cik[sub_row]

### tag_tv_hash (hash on tag.(tag,version))
- File: `indexes/tag_tv_hash.bin`
- Layout: `[uint32_t cap=4194304][{int32_t tag_code, int32_t ver_code, uint32_t row_idx} × 4194304]`
- Size: 48.0 MB
- Hash: `h = (((uint32_t)tag_code*2654435761u)^((uint32_t)ver_code*1234567891u)) & (cap-1)`
- Empty slot: tag_code == INT32_MIN
- Probe (C24): bounded for-loop
- Usage: (tag_code, ver_code) → tag_row → tag_abstract[tag_row], tag_tlabel[tag_row]
- Load pattern:
  ```cpp
  struct TagSlot { int32_t tag_code; int32_t ver_code; uint32_t row_idx; };
  // mmap indexes/tag_tv_hash.bin
  uint32_t cap = header[0];
  uint32_t mask = cap - 1;
  const TagSlot* tag_ht = reinterpret_cast<const TagSlot*>(raw + 4);
  ```

### pre_eq_hash (existence hash: pre rows with stmt='EQ')
- File: `indexes/pre_eq_hash.bin`
- Layout: `[uint32_t cap=4194304][{int32_t adsh, int32_t tag, int32_t ver} × 4194304]`
- Size: 48.0 MB
- Hash: `h = (((uint32_t)a*2654435761u)^((uint32_t)b*1234567891u)^((uint32_t)c*2246822519u)) & (cap-1)`
- Empty slot: adsh_code == INT32_MIN
- Probe: returns true if key found (existence check only)
- Usage: check if (num.adsh, num.tag, num.version) has a pre row with stmt='EQ'
  ```cpp
  struct AtvSlot { int32_t adsh_code; int32_t tag_code; int32_t ver_code; };
  bool exists_eq = false;
  uint32_t h = hash3(adsh, tag, ver) & eq_mask;
  for (uint32_t p = 0; p < eq_cap; p++) {  // C24: bounded
      uint32_t slot = (h + p) & eq_mask;
      if (eq_ht[slot].adsh_code == INT32_MIN) break;
      if (eq_ht[slot].adsh_code == adsh && eq_ht[slot].tag_code == tag && eq_ht[slot].ver_code == ver) {
          exists_eq = true; break;
      }
  }
  ```

## Output Format
```
sic,tlabel,stmt,num_companies,total_value,avg_value
4911,Revenues,EQ,5,1234567890.12,246913578.02
...
```
- total_value: print as decimal from int64_t cents
- avg_value: `(double)sum_cents / 100.0 / count` → printf("%.6g", avg_value)
- stmt is always 'EQ' in output (decode from stmt_dict)
