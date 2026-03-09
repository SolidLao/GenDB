# Q24 Guide

```sql
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

### num.uom_code (dict_code, uint8_t, dictionary-encoded via uom_dict)
- File: `num/uom_code.bin` (39,401,761 rows, 1 byte each)
- Dictionary: `num/uom_dict_offsets.bin` + `num/uom_dict_data.bin` (201 entries)
- This query: `WHERE uom = 'USD'` → find usdCode, use uom_offsets for row range

### num.ddate (date_yyyymmdd, int32_t, fixed encoding)
- File: `num/ddate.bin` (39,401,761 rows, 4 bytes each)
- This query: `WHERE ddate BETWEEN 20230101 AND 20231231` → `ddate >= 20230101 && ddate <= 20231231`
- Selectivity ~27.2% → within USD range, ~9M rows pass the ddate filter
- **Zone map available** to skip blocks entirely outside this range

### num.sub_fk (foreign_key_to_sub, uint32_t, dense FK array)
- File: `num/sub_fk.bin` (39,401,761 rows, 4 bytes each)
- This query: anti-join key — `(sub_fk, tag_code, version_code)` probed against pre index

### num.tag_code (dict_code, uint32_t, dictionary-encoded via tag_dict)
- File: `num/tag_code.bin` (39,401,761 rows, 4 bytes each)
- Dictionary: `dicts/tag_dict_offsets.bin` + `dicts/tag_dict_data.bin` (198,311 entries)
- This query: anti-join key; `GROUP BY n.tag`; output column (decode for output)

### num.version_code (dict_code, uint32_t, dictionary-encoded via version_dict)
- File: `num/version_code.bin` (39,401,761 rows, 4 bytes each)
- Dictionary: `dicts/version_dict_offsets.bin` + `dicts/version_dict_data.bin` (83,815 entries)
- This query: anti-join key; `GROUP BY n.version`; output column (decode for output)

### num.value (numeric_value, double, fixed encoding, NaN = NULL)
- File: `num/value.bin` (39,401,761 rows, 8 bytes each)
- This query: `WHERE value IS NOT NULL` → `!std::isnan(value)`; `SUM(n.value)`, `COUNT(*)`

## Table Stats

| Table | Rows       | Role | Sort Order          | Block Size |
|-------|------------|------|---------------------|------------|
| num   | 39,401,761 | fact | (uom_code, sub_fk)  | 100,000    |
| pre   | 9,600,799  | fact | (stmt_code, sub_fk) | 100,000    |

## Query Analysis

### Strategy: Anti-join via pre index probe

This query finds num rows that have NO matching pre row (LEFT JOIN + IS NULL = anti-join).

**Recommended execution plan:**

1. **Find usdCode:** Load uom_dict, find code for "USD".
2. **Use uom_offsets** to get USD row range (~33.2M rows).
3. **Load pre_by_adsh_tag_ver index** into memory.
4. **Scan USD rows with zone map filtering:** For each block of 100,000 rows within the USD range:
   - Check zone map: if block's `[min_ddate, max_ddate]` doesn't overlap `[20230101, 20231231]`, skip entire block
   - For rows in non-skipped blocks: check `ddate >= 20230101 && ddate <= 20231231 && !isnan(value)`
   - For qualifying rows: probe **pre_by_adsh_tag_ver** index with `(sub_fk[i], tag_code[i], version_code[i])`
   - If **NO match found** in the index → this row passes the anti-join
   - Aggregate: group by `(tag_code, version_code)`, accumulate count and sum(value)
5. **HAVING filter:** Keep groups where `count > 10`.
6. **Decode and output:** Decode tag_code → tag string, version_code → version string. Sort by cnt DESC, limit 100.

### Zone Map Usage
The num table is sorted by `(uom_code, sub_fk)`, NOT by ddate. The zone map on ddate stores per-block min/max. Within the USD range, ddate values are not sorted, so zone map effectiveness depends on data distribution. Blocks where all ddate values are outside 2023 will be skipped.

### Anti-join Optimization
The pre_by_adsh_tag_ver index enables O(1) amortized anti-join probes. For each num row, compute the bucket and scan entries — if no exact key match exists, the row passes the anti-join. This avoids materializing any hash set from pre.

### Estimated costs
- USD range: ~33.2M rows
- After ddate filter (~27.2%): ~9.0M rows to probe
- Anti-join selectivity: "low" — most num rows DO have matching pre rows, so relatively few pass
- ~50,000 estimated groups before HAVING
- After HAVING (count > 10): ~2 result rows expected

## Indexes

### uom_offsets (offset_table on uom_code)
- File: `num/uom_offsets.bin`
- Layout: `uint32_t num_entries`, then `num_entries` pairs of `(uint64_t start, uint64_t end)`
- Usage: Get USD row range

### num_ddate_zonemap (zone_map on ddate)
- File: `indexes/num_ddate_zonemap.bin`
- Layout:
```
uint64_t num_blocks
uint64_t block_size        // = 100,000
// Then num_blocks entries:
int32_t min_ddate          // 4 bytes
int32_t max_ddate          // 4 bytes
```
- **Usage pattern:**
```cpp
std::ifstream f("indexes/num_ddate_zonemap.bin", std::ios::binary);
uint64_t numBlocks, blockSize;
f.read((char*)&numBlocks, 8);
f.read((char*)&blockSize, 8);
struct ZoneEntry { int32_t min_ddate, max_ddate; };
std::vector<ZoneEntry> zones(numBlocks);
f.read((char*)zones.data(), numBlocks * sizeof(ZoneEntry));
// For block b covering rows [b*blockSize, min((b+1)*blockSize, N)):
//   if (zones[b].max_ddate < 20230101 || zones[b].min_ddate > 20231231) skip block
```
- **Important:** Block indices are global (over all 39.4M num rows). When scanning within the USD range `[usdStart, usdEnd)`, compute the block index as `b = row / blockSize` using the global row index, not a range-relative index.

### pre_by_adsh_tag_ver (bucket hash on sub_fk, tag_code, version_code)
- File: `indexes/pre_by_adsh_tag_ver.idx`
- **Multi-value index (1:N)** — used here for anti-join existence check
- Layout:
```
uint64_t num_buckets
uint64_t total_entries
uint64_t bucket_offsets[num_buckets + 1]   // cumulative
Entry entries[total_entries]
```
- Entry struct:
```cpp
struct Entry {
    uint32_t sub_fk;        // 4 bytes
    uint32_t tag_code;      // 4 bytes
    uint32_t version_code;  // 4 bytes
    uint32_t row_idx;       // 4 bytes
};
// Total: 16 bytes per entry
```
- Hash function (verbatim from build_indexes.cpp):
```cpp
static inline uint64_t hashKey3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = (uint64_t)a * 2654435761ULL;
    h ^= (uint64_t)b * 2246822519ULL;
    h ^= (uint64_t)c * 3266489917ULL;
    h ^= h >> 16;
    h *= 0x45d9f3b37197344dULL;
    h ^= h >> 16;
    return h;
}
```
- Bucket lookup: `bucket = hashKey3(sfk, tc, vc) & (numBuckets - 1)`
- **Anti-join probe pattern:**
```cpp
uint64_t bucket = hashKey3(sfk, tc, vc) & (numBuckets - 1);
uint64_t bStart = bucketOffsets[bucket];
uint64_t bEnd = bucketOffsets[bucket + 1];
bool found = false;
for (uint64_t j = bStart; j < bEnd; j++) {
    if (entries[j].sub_fk == sfk && entries[j].tag_code == tc && entries[j].version_code == vc) {
        found = true;
        break;  // Any match means this num row is NOT in the anti-join result
    }
}
if (!found) {
    // Row passes anti-join — aggregate it
}
```
