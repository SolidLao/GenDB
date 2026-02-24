# Q24 Guide

## SQL
```sql
-- Decorrelated LEFT JOIN anti-join (from NOT EXISTS correlated subquery)
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

## Column Reference

### num.ddate (int32_t, YYYYMMDD integer)
- File: `num/ddate.bin` (39,401,761 rows)
- **IMPORTANT**: ddate is stored as YYYYMMDD integer (e.g., 20230101), NOT epoch days.
- This query: `ddate BETWEEN 20230101 AND 20231231`; selectivity ~25% → ~9.85M rows pass
- Zone map enables block-level skipping: blocks with max_ddate < 20230101 or min_ddate > 20231231 are skipped entirely

### num.uom (dict_local, int32_t)
- File: `num/uom.bin` (39,401,761 rows)
- Dict: `num/uom_dict.txt`
- This query: `uom = 'USD'`; find USD code at runtime; selectivity 87%

### num.value (double)
- File: `num/value.bin` (39,401,761 rows)
- This query: `value IS NOT NULL` → `!std::isnan(value)`; SUM per (tag, version) group

### num.tag (dict_shared_tag, int32_t)
- File: `num/tag.bin` (39,401,761 rows)
- Dict: `num/tag_dict.txt` (shared; same codes as pre/tag.bin)
- This query: GROUP BY key (part 1 of 2); anti-join key; output (decode via tag_dict)

### num.version (dict_shared_version, int32_t)
- File: `num/version.bin` (39,401,761 rows)
- Dict: `num/version_dict.txt` (shared; same codes as pre/version.bin)
- This query: GROUP BY key (part 2 of 2); anti-join key; output (decode via version_dict)

### num.adsh (dict_shared_adsh, int32_t)
- File: `num/adsh.bin` (39,401,761 rows)
- This query: anti-join key (must match pre.adsh); check `p.adsh IS NULL` (i.e., no pre match)

### pre.adsh (dict_shared_adsh, int32_t)
- File: `pre/adsh.bin` (9,600,799 rows)
- Same shared adsh dict → same codes as num.adsh
- This query: anti-join key; `p.adsh IS NULL` means no matching pre row

### pre.tag (dict_shared_tag, int32_t)
- File: `pre/tag.bin` (9,600,799 rows)

### pre.version (dict_shared_version, int32_t)
- File: `pre/version.bin` (9,600,799 rows)

## Table Stats
| Table | Rows       | Role             | Sort Order | Block Size |
|-------|------------|------------------|------------|------------|
| num   | 39,401,761 | fact (probe)     | (none)     | 100,000    |
| pre   | 9,600,799  | anti-join (build)| adsh       | 100,000    |

## Query Analysis

**Step 1 — Build pre hash set (adsh_code, tag_code, ver_code):**
- Scan all of pre (9.6M rows): load adsh.bin, tag.bin, version.bin
- Build hash set: key = 96-bit triple (adsh_code, tag_code, ver_code)
- Efficient packing: `uint64_t k1 = ((uint64_t)(uint32_t)adsh << 32) | (uint32_t)tag; uint32_t k2 = ver;`
  Combined hash: `uint64_t h = k1 * 6364136223846793005ULL + k2 * 1442695040888963407ULL;`
- Hash set capacity: `next_power_of_2(9600799 * 2) = 33554432` (2^25)
- Each slot: `{int32_t adsh, int32_t tag, int32_t ver, int32_t valid}` = 16 bytes → 512MB total
  - Alternative: Use a bloom filter for fast rejection (false negatives impossible for anti-join pass-through)
  - Or: open-addressing hash set with 16-byte slots at 33M → ~512MB (feasible with 376GB RAM)

**Step 2 — Scan num with zone-map-guided ddate filter:**
- Load ddate zone map: `num/indexes/ddate_zone_map.bin`
- For each block of 100,000 num rows:
  - Read zone entry: if max_ddate < 20230101 OR min_ddate > 20231231 → skip entire block
  - Otherwise: scan block row by row, check `ddate BETWEEN 20230101 AND 20231231`
- Combined with uom='USD' and !isnan(value) filters

**Step 3 — Anti-join check:**
- For each qualifying num row (ddate in range, uom=USD, value not null):
  - Probe pre hash set with (adsh_code, tag_code, ver_code)
  - If NOT found in hash set → this num row has no pre match → include in result

**Step 4 — Aggregate: GROUP BY (tag_code, ver_code):**
- Estimated groups: ~50,000
- Hash map: `uint64_t key = ((uint64_t)(uint32_t)tag_code << 32) | (uint32_t)ver_code`
- Accumulate: count and sum_value per group

**Step 5 — Apply HAVING count > 10, sort by cnt DESC, limit 100.**

## Indexes

### num/indexes/ddate_zone_map.bin (zone_map on num.ddate)
- File: `num/indexes/ddate_zone_map.bin`
- Layout: `[uint32_t num_blocks][{int32_t min_val, int32_t max_val, uint32_t block_size}...]`
- Values are YYYYMMDD integers (e.g., min=19970930, max=20251231)
- Block size: 100,000 rows → ~394 blocks total
- Usage:
  ```cpp
  // Load zone map
  int fd = open(".../num/indexes/ddate_zone_map.bin", O_RDONLY);
  // read num_blocks, then array of {min,max,block_size}
  // For each block b:
  //   row_start = b * 100000;
  //   if (zone[b].max_val < 20230101 || zone[b].min_val > 20231231) skip;
  //   else scan rows [row_start, row_start + zone[b].block_size)
  ```
- C19: Only apply zone map skip for blocks where the filter can definitively exclude them.
- Expected benefit: ddate range 2023 is ~25% of data; depending on data order,
  potentially 50-75% of blocks can be skipped.

## Implementation Notes
- Ddate comparison: `ddate >= 20230101 && ddate <= 20231231` (direct integer comparison, no date conversion)
- Pre hash set: 9.6M entries, 33.5M slots × 16 bytes = ~512MB. Allocate upfront.
  ```cpp
  uint64_t cap = 33554432;  // 2^25 = next_power_of_2(9600799 * 2)
  uint64_t mask = cap - 1;
  struct PreSlot { int32_t adsh, tag, ver, valid; };
  std::vector<PreSlot> pre_ht(cap);
  std::fill(pre_ht.begin(), pre_ht.end(), PreSlot{0,0,0,0});  // C20: std::fill not memset
  ```
- C24: Bounded probe in pre hash set: `for(uint64_t p=0; p<cap; p++) { slot=(h+p)&mask; ... }`
- Anti-join probe: `found = false; for(p=0; p<cap; p++) { s=(h+p)&mask; if(!pre_ht[s].valid) break; if(pre_ht[s].adsh==ac && pre_ht[s].tag==tc && pre_ht[s].ver==vc) { found=true; break; } }; if(!found) include_row;`
- Output decode: `tag_dict[tag_code]`, `version_dict[ver_code]`
- Load order: pre (build hash set) → num (zone-map guided scan + probe)
