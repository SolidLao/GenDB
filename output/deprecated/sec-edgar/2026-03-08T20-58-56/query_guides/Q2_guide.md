# Q2 Guide

## Column Reference
### num.uom (unit, uint16_t, dict_shared(uom_dict))
- File: `num/uom.bin` (39,401,761 rows)
- This query: `n.uom = 'pure'` and subquery `WHERE uom = 'pure'`.
- Runtime dictionary loading pattern (no hardcoded code): load `gendb/dicts/uom.dict` (`[n][len][bytes]...`), find code where value string is `"pure"`, then compare `uint16_t` code in scan/probe paths.

### num.adsh (filing_id_fk, uint32_t, dict_shared(adsh_dict))
- File: `num/adsh.bin` (39,401,761 rows)
- This query: joins `n.adsh = s.adsh`, `n.adsh = m.adsh`.

### num.tag (xbrl_tag, uint32_t, dict_shared(tag_dict))
- File: `num/tag.bin` (39,401,761 rows)
- This query: joins `n.tag = m.tag`, grouping `GROUP BY adsh, tag` in subquery.

### num.value (fact_value, double, plain_nan_null)
- File: `num/value.bin` (39,401,761 rows)
- This query: `value IS NOT NULL`, `MAX(value)`, `n.value = m.max_value`, `ORDER BY n.value DESC`.

### sub.adsh (filing_id, uint32_t, dict_shared(adsh_dict))
- File: `sub/adsh.bin` (86,135 rows)
- This query: join key to `num.adsh`.

### sub.fy (fiscal_year, int32_t, plain)
- File: `sub/fy.bin` (86,135 rows)
- This query: `s.fy = 2022`.

### sub.name (company_name, uint32_t, dictionary)
- File: `sub/name.bin` (86,135 rows)
- This query: projected output; decode via `sub/name.dict` after LIMIT/top-k.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| num | 39,401,761 | fact scan/filter/aggregate | none | 100000 |
| sub | 86,135 | dimension filter/join | none | 100000 |

## Query Analysis
- Estimated rows after `uom='pure'`: `39,401,761 x 0.001 ~= 39,402`.
- Estimated rows after `s.fy=2022`: `86,135 x 0.204 ~= 17,572`.
- Core plan shape:
  - Build/probe filtered `num` stream for `(adsh,tag)->max(value)`.
  - Re-join candidate `n` rows on `(adsh,tag,value=max_value)`.
  - Join to filtered `sub` on `adsh`.
  - Final sort on `(value DESC, name, tag)` and LIMIT 100.

## Indexes
### num_uom_hash (posting index on `num.uom`)
- File: `num/indexes/num_uom_hash.bin`
- Builder call is `build_posting_u32(gendb / "num", "uom.bin", "num_uom_hash.bin")`, so index build reads keys as `uint32_t` from `uom.bin`.
- Build code (verbatim key materialization): `for (uint32_t i = 0; i < n; ++i) p.emplace_back(key[i], i);`
- Build code (verbatim ordering):
  ```cpp
  std::sort(p.begin(), p.end(), [](const auto& a, const auto& b) {
    if (a.first != b.first) return a.first < b.first;
    return a.second < b.second;
  });
  ```
- Exact struct layout used during serialization:
  `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- On-disk layout:
  - `uint64_t entry_count`
  - `uint64_t rowid_count`
  - repeated entries: `(uint32_t key, uint64_t start, uint32_t count)`
  - trailing `uint32_t rowids[rowid_count]`
- Multi-value format: one key maps to contiguous slice `rowids[start ... start+count)`.
- Empty-slot sentinel: none (dense entry array + offsets, not open addressing).

### num_adsh_fk_hash (posting index on `num.adsh`)
- File: `num/indexes/num_adsh_fk_hash.bin`
- Build path: `build_posting_u32(gendb / "num", "adsh.bin", "num_adsh_fk_hash.bin")`.
- Verbatim row materialization: `p.emplace_back(key[i], i);`
- Exact struct layout:
  `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- On-disk layout:
  - `uint64_t entry_count`
  - `uint64_t rowid_count`
  - repeated `(uint32_t key, uint64_t start, uint32_t count)`
  - trailing `uint32_t rowids[rowid_count]`
- Multi-value format: one `adsh` can map to many `num` rowids.
- Empty-slot sentinel: none.

### sub_adsh_pk_hash (dense PK lookup on `sub.adsh`)
- File: `sub/indexes/sub_adsh_pk_hash.bin`
- Build logic: max code scan, then dense LUT fill by row id.
- Exact sentinel value:
  `std::numeric_limits<uint32_t>::max()`
- On-disk layout:
  - `uint64_t lut_size`
  - `uint32_t lut[lut_size]` where `lut[adsh_code] = sub_rowid` or sentinel if absent.
- Hash/key computation: direct index by encoded `adsh` value (no hash fold/mask).

### sub_fy_zonemap (zone map on `sub.fy`)
- File: `sub/indexes/sub_fy_zonemap.bin`
- Type instantiation: `build_zonemap_t<int32_t>(..., "fy.bin", ..., block_size=100000)`
- On-disk layout:
  - `uint64_t block_size`
  - `uint64_t blocks`
  - per block: `(int32_t min_fy, int32_t max_fy)`
- Usage: block pruning for `fy = 2022` before row-level compare.
