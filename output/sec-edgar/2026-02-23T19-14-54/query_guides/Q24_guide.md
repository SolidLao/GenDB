# Q24 Guide

## SQL
```sql
-- Anti-join: num facts with uom='USD' and ddate in 2023 that have NO matching pre row.
-- LEFT JOIN + p.adsh IS NULL is semantically equivalent to NOT EXISTS.
SELECT n.tag, n.version, COUNT(*) AS cnt, SUM(n.value) AS total
FROM num n
LEFT JOIN pre p ON n.tag = p.tag AND n.version = p.version AND n.adsh = p.adsh
WHERE n.uom = 'USD' AND n.ddate BETWEEN 20230101 AND 20231231
      AND n.value IS NOT NULL
      AND p.adsh IS NULL
GROUP BY n.tag, n.version
HAVING COUNT(*) > 10
ORDER BY cnt DESC
LIMIT 100;
```
Expected output: ≤100 rows (tag+version combos where no pre presentation exists, 2023 USD facts).

## Column Reference

### num/uom.bin (dict_string, int16_t)
- File: `num/uom.bin` (39,401,761 rows, 75.2 MB)
- Dict: `num/uom_dict.txt` — load at runtime (C2).
  - `int16_t usd_code = find_code(uom_dict, "USD"); // code 0 at ingest time`
- Filter: `n.uom = 'USD'` → ~88.7% qualifying

### num/ddate.bin (yyyymmdd_integer, int32_t)
- File: `num/ddate.bin` (39,401,761 rows, 150.3 MB)
- **Encoding:** Stored as raw YYYYMMDD integer (schema type INTEGER, NOT DATE).
  Values look like: 20201231, 20211231, etc. Values are in range [20150000, 20250000].
- Filter: `n.ddate BETWEEN 20230101 AND 20231231` → direct int32_t comparison:
  `ddate[i] >= 20230101 && ddate[i] <= 20231231`
  - NO epoch-day conversion needed. Compare to YYYYMMDD integer literals directly.
- Selectivity: ~10% of qualifying rows (2023 represents one year out of ~10 year span)

### num/value.bin (monetary_decimal, double)
- File: `num/value.bin` (39,401,761 rows, 300.6 MB)
- Filter: `value IS NOT NULL` → no nulls in dataset, skip check.
- **C29 WARNING:** `SUM(n.value)` must accumulate as int64_t cents:
  `sum_cents += llround(value * 100.0);`

### num/tag.bin (dict_string, int32_t, global dict)
- File: `num/tag.bin` (39,401,761 rows, 150.3 MB)
- Dict: `tag_global_dict.txt` (198,311 entries)
- GROUP BY key + anti-join key. Output: decode `tag_dict[tag_code]`.

### num/version.bin (dict_string, int32_t, global dict)
- File: `num/version.bin` (39,401,761 rows, 150.3 MB)
- Dict: `version_global_dict.txt` (83,815 entries)
- GROUP BY key + anti-join key. Output: decode `ver_dict[ver_code]`.

### num/adsh.bin (dict_string, int32_t, global dict)
- File: `num/adsh.bin` (39,401,761 rows, 150.3 MB)
- Anti-join key with pre (checked via pre_atv_hash).

### pre/adsh.bin, pre/tag.bin, pre/version.bin
- Not directly scanned. Anti-join handled via pre_atv_hash index.
- `p.adsh IS NULL` means: no pre row matches (num.adsh, num.tag, num.version).

## Table Stats
| Table | Rows       | Role           | Sort Order | Block Size |
|-------|------------|----------------|------------|------------|
| num   | 39,401,761 | probe (fact)   | none       | 65,536     |
| pre   | 9,600,799  | build (anti-join)| none    | 65,536     |

## Query Analysis
**Pattern:** Anti-join — keep num rows where NO matching pre row exists.
**Filters:**
1. n.uom='USD': ~88.7% → ~34.9M rows
2. n.ddate BETWEEN 20230101 AND 20231231: ~10% → ~3.49M rows
**Effective probe count:** ~3.49M num rows to anti-join against 9.6M pre rows.

**Critical warning:** pre has 9.6M rows. Building a hash table at query time for pre would
take 19-65s (P19: LLC cache thrashing with 384MB hash table). Use the pre-built pre_atv_hash
index instead (P11: zero build cost, mmap'd).

**Execution plan:**
1. Load pre_atv_hash index via mmap (384 MB — will be in OS page cache from build_indexes)
2. Scan num with zone-map guidance for ddate filter:
   - Use num_ddate_zone_map to skip blocks where max_ddate < 20230101 or min_ddate > 20231231
3. For each qualifying num row (uom='USD', ddate IN 2023, value!=NULL):
   - Probe pre_atv_hash with (num.adsh, num.tag, num.version)
   - If NOT FOUND in pre_atv_hash: include in aggregation
4. Aggregate: (tag_code, ver_code) → {int64_t sum_cents, int64_t count}
5. HAVING count > 10, ORDER BY cnt DESC, LIMIT 100
6. Decode tag and version strings for output

## Indexes

### pre_atv_hash (existence hash set: all pre rows, keyed on adsh+tag+version)
- File: `indexes/pre_atv_hash.bin`
- Layout: `[uint32_t cap=33554432][{int32_t adsh, int32_t tag, int32_t ver} × 33554432]`
- Size: 384.0 MB (2^25 slots × 12 bytes)
- Hash: `h = (((uint32_t)a*2654435761u)^((uint32_t)b*1234567891u)^((uint32_t)c*2246822519u)) & (cap-1)`
- Empty slot sentinel: adsh_code == INT32_MIN (set via std::fill during build — C20)
- Probe: for a num row, check if (adsh, tag, version) EXISTS in pre
  ```cpp
  struct AtvSlot { int32_t adsh_code; int32_t tag_code; int32_t ver_code; };
  // mmap indexes/pre_atv_hash.bin
  uint32_t atv_cap = *reinterpret_cast<const uint32_t*>(raw);
  const AtvSlot* atv_ht = reinterpret_cast<const AtvSlot*>(raw + 4);
  uint32_t atv_mask = atv_cap - 1;

  // Anti-join probe:
  bool found_in_pre = false;
  uint32_t h = hash3(num_adsh[i], num_tag[i], num_ver[i]) & atv_mask;
  for (uint32_t p = 0; p < atv_cap; p++) {  // C24: bounded probe
      uint32_t slot = (h + p) & atv_mask;
      const AtvSlot& s = atv_ht[slot];
      if (s.adsh_code == INT32_MIN) break;  // empty slot → not found
      if (s.adsh_code == num_adsh[i] && s.tag_code == num_tag[i] && s.ver_code == num_ver[i]) {
          found_in_pre = true; break;
      }
  }
  if (found_in_pre) continue;  // has a pre match → exclude (anti-join)
  // else: include in aggregation
  ```
- **Memory note:** 384 MB fits in system RAM (376 GB available). After build_indexes runs,
  OS page cache retains the file. First mmap access is fast.

### num_ddate_zone_map (zone map on num.ddate)
- File: `indexes/num_ddate_zone_map.bin`
- Layout: `[uint32_t num_blocks=602][{int32_t min_ddate, int32_t max_ddate, uint32_t block_size} × 602]`
- Size: ~7 KB (trivially small)
- Block size: 65,536 rows per block
- Skip condition for BETWEEN 20230101 AND 20231231:
  ```cpp
  struct ZoneBlock { int32_t min_val; int32_t max_val; uint32_t block_size; };
  // Skip block b if:
  if (zones[b].max_val < 20230101) { row_offset += zones[b].block_size; continue; }
  if (zones[b].min_val > 20231231) { row_offset += zones[b].block_size; continue; }
  // Process rows in block [row_offset, row_offset + block_size)
  ```
- **Effectiveness:** ddate values span ~2015-2025. If data has any temporal clustering
  (filings batched by period), zone maps can skip 80-90% of blocks. Otherwise all 602 blocks
  qualify (still correct, just no speedup). Always apply — cost is negligible.
- Usage pattern:
  ```cpp
  uint32_t row_offset = 0;
  for (uint32_t b = 0; b < num_blocks; b++) {
      if (zones[b].max_val < 20230101 || zones[b].min_val > 20231231) {
          row_offset += zones[b].block_size; continue;
      }
      for (uint32_t r = row_offset; r < row_offset + zones[b].block_size; r++) {
          if (num_uom[r] != usd_code) continue;
          if (num_ddate[r] < 20230101 || num_ddate[r] > 20231231) continue;
          // anti-join probe + aggregate
      }
      row_offset += zones[b].block_size;
  }
  ```

## Implementation Strategy
**Step 1:** mmap all needed columns: num/uom.bin, num/ddate.bin, num/adsh.bin, num/tag.bin,
  num/version.bin, num/value.bin. Also mmap indexes/pre_atv_hash.bin.

**Step 2:** Load zone map. Scan num with zone-map-guided block skip (C19: only use zone map
  on columns that have zone map indexes — num/ddate has one).

**Step 3:** Parallel scan with thread-local aggregation maps (P17, P20):
  ```cpp
  #pragma omp parallel for schedule(dynamic, 65536) num_threads(64)
  // thread-local: unordered_map<pair<int32_t,int32_t>, {int64_t sum_cents, int64_t count}>
  ```
  Merge thread-local maps after barrier.

**Step 4:** HAVING count > 10. Sort by count DESC. Limit 100. Decode strings. Output.

## Output Format
```
tag,version,cnt,total
SomeXBRLTag,us-gaap/2023,42,1234567890.12
...
```
- tag: `tag_dict[tag_code]` from `tag_global_dict.txt`
- version: `ver_dict[ver_code]` from `version_global_dict.txt`
- total: int64_t cents → print as decimal (C29)
- cnt: plain integer
