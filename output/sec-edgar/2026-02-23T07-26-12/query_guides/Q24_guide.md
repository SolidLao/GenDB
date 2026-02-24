# Q24 Guide

```sql
-- Anti-join: num rows (USD, 2023, not-null value) with NO matching pre row
SELECT n.tag, n.version, COUNT(*) AS cnt, SUM(n.value) AS total
FROM num n
LEFT JOIN pre p ON n.tag = p.tag AND n.version = p.version AND n.adsh = p.adsh
WHERE n.uom = 'USD' AND n.ddate BETWEEN 20230101 AND 20231231
      AND n.value IS NOT NULL
      AND p.adsh IS NULL          -- anti-join: no pre match
GROUP BY n.tag, n.version
HAVING COUNT(*) > 10
ORDER BY cnt DESC LIMIT 100;
```

## Column Reference

### num.uom (dict_string, int16_t, dict_int16)
- File: `num/uom.bin` (39,401,761 × 2 = 78.8 MB)
- Dict: `num/uom_dict.txt` (201 entries). `int16_t usd_code = find_code(uom_dict, "USD");` (C2)
- Filter: `uom = 'USD'` (selectivity 0.872). Zone map identifies USD segment.
- Combined with ddate filter: actual selectivity ~0.872 × 0.272 ≈ 0.237 → ~9.3M rows.

### num.ddate (integer, int32_t, YYYYMMDD)
- File: `num/ddate.bin` (39,401,761 × 4 = 157.6 MB)
- **NOT epoch days** — stored as YYYYMMDD integer. Compare directly:
  `ddate >= 20230101 && ddate <= 20231231`
- Zone map: `indexes/num_ddate_zone_map.bin` — within USD segment, ddate is sorted.
  Skip ddate blocks where `max_val < 20230101 || min_val > 20231231`.

### num.value (double)
- File: `num/value.bin` (39,401,761 × 8 = 315 MB). NULL = NaN.
- Filter: `!std::isnan(v)` (selectivity ~0.98)
- **C29 CRITICAL**: max value 1e14. SUM requires int64_t cents:
  ```cpp
  int64_t iv = llround(v * 100.0); sum_cents += iv; cnt++;
  ```

### num.adsh (dict_string, int32_t, dict_int32)
- File: `num/adsh.bin` (39,401,761 × 4 = 157.6 MB). Anti-join key.

### num.tag (dict_string, int32_t, dict_int32)
- File: `num/tag.bin` (39,401,761 × 4 = 157.6 MB)
- Dict: `num/tag_dict.txt` (198,311 entries). GROUP BY tag → use tag_code; decode at output.

### num.version (dict_string, int32_t, dict_int32)
- File: `num/version.bin` (39,401,761 × 4 = 157.6 MB)
- Dict: `num/version_dict.txt` (83,815 entries). GROUP BY version → use version_code; decode at output.

### pre.adsh (IS NULL check — anti-join indicator)
- Not stored as a column for this query. Absence in `pre_atv_hash` = IS NULL.
- Anti-join logic: `probe_pre_atv_hash(num_adsh, num_tag, num_version) == NOT_FOUND`

## Table Stats

| Table | Rows       | Role       | Sort Order    | Block Size |
|-------|------------|------------|---------------|------------|
| num   | 39,401,761 | fact       | (uom, ddate)  | 100,000    |
| pre   | 9,600,799  | anti-join  | stmt (asc)    | 100,000    |

## Query Analysis
**Key insight**: This is an anti-join — keep num rows that have NO matching pre row.
The pre table is used only for existence check, not for column projection.

**Recommended strategy (bloom filter optimization, P21)**:
1. **Pre-pass**: Load `pre/adsh.bin`, `pre/tag.bin`, `pre/version.bin` → build a **bloom filter**
   over (adsh_code, tag_code, version_code). Size: 9.6M × 1.5 bytes ≈ 14MB, false positive rate ~1%.
   This eliminates ~99% of probes into `pre_atv_hash`.
2. **Scan num** with zone maps:
   - Zone map on uom: find USD segment.
   - Zone map on ddate: within USD segment, skip blocks where ddate range excludes [20230101,20231231].
3. For each qualifying num row (uom=USD, ddate in range, !NaN):
   - Check bloom filter first → if NOT in bloom → definitely not in pre → count as anti-join pass.
   - If bloom positive (possible match) → probe `pre_atv_hash` for definitive check.
4. For anti-join rows: accumulate into group (tag_code, version_code) → cnt, sum_cents.
5. Filter HAVING cnt > 10; sort by cnt DESC; LIMIT 100.

**Alternative (simpler, without bloom filter)**:
Directly use `pre_atv_hash` for all ~9.3M qualifying num rows.
Warning: `pre_atv_hash` is 536MB >> L3 (44MB) → LLC misses on every probe → slow.
The bloom filter approach is strongly recommended (P21).

**Estimated qualifying groups**: ~200K (tag,version) pairs where no pre match exists.

## Indexes

### num_uom_zone_map (zone_map on num.uom, int16_t)
- File: `indexes/num_uom_zone_map.bin` — 395 blocks × 100K rows
- Skip non-USD blocks. USD segment is contiguous.

### num_ddate_zone_map (zone_map on num.ddate, int32_t)
- File: `indexes/num_ddate_zone_map.bin` — 395 blocks × 100K rows
- Layout: `[uint32_t num_blocks=395] [ZoneBlock<int32_t> × 395]`
- `ZoneBlock<int32_t>`: `{ int32_t min_val; int32_t max_val; uint32_t row_count; }`
- **min/max are YYYYMMDD integers** (not epoch days). Compare directly:
  `blocks[b].max_val < 20230101 || blocks[b].min_val > 20231231` → skip
- Within USD segment: ddate is sorted → zone maps are very effective.
  Expected: only blocks spanning ~27% of the ddate range qualify → skip ~73% of USD blocks.
- Row offset for block b = block_size × b (last block may be smaller).

### pre_atv_hash (hash: (adsh,tag,version) → pre row_id)
- File: `indexes/pre_atv_hash.bin` (536 MB)
- Layout: `[uint64_t cap=33554432] [PreATVSlot × 33554432]`
- `PreATVSlot`: `{ int32_t adsh; int32_t tag; int32_t version; int32_t row_id; }` empty: `adsh==INT32_MIN`
- Hash: `h = adsh*2654435761ULL ^ tag*40503ULL ^ version*48271ULL`
- Probe: `for (p=0; p<cap; ++p) { idx=(h+p)&(cap-1); if empty→not found; if match→found }`
- **P19 WARNING**: 536MB >> L3 (44MB) → cache thrashing if probed randomly for 9.3M rows.
  Mitigations: (a) bloom filter pre-filter (recommended); (b) prefetch with `__builtin_prefetch`.
- madvise with MADV_WILLNEED before query if loading from HDD.

## Zone Map Usage Pattern (Combined uom + ddate)

```cpp
// Phase 1: scan uom zone map to find USD range
const ZoneBlock<int16_t>* uom_blocks = ...;  // 395 blocks
for (uint32_t b = 0; b < 395; ++b) {
    uint32_t row_start = b * 100000;
    uint32_t row_end   = min(row_start + uom_blocks[b].row_count, N);
    if (uom_blocks[b].max_val < usd_code || uom_blocks[b].min_val > usd_code) continue;

    // Phase 2: within USD block, check ddate zone map
    const ZoneBlock<int32_t>* dd_block = &ddate_blocks[b];
    if (dd_block->max_val < 20230101 || dd_block->min_val > 20231231) continue;

    // Process rows [row_start, row_end)
    for (uint32_t i = row_start; i < row_end; ++i) {
        if (uom_col[i] != usd_code) continue;
        if (ddate_col[i] < 20230101 || ddate_col[i] > 20231231) continue;
        if (std::isnan(val_col[i])) continue;
        // Anti-join check: probe bloom then hash
        ...
    }
}
```

## Aggregation and Output (C29)

```cpp
// Group key: (tag_code, version_code) → uint64_t
uint64_t gkey = ((uint64_t)(uint32_t)tag_code << 32) | (uint32_t)version_code;

// Accumulate
group_map[gkey].cnt++;
group_map[gkey].sum_cents += llround(value * 100.0);

// Filter HAVING cnt > 10; collect to vector; partial_sort top 100
// Output: tag_dict[tag_code], version_dict[version_code], cnt, sum_cents/100.sum_cents%100
```
