# Q24 Guide

## Column Reference
### num.tag / num.version / num.adsh
- Files: `num/tag.bin`, `num/version.bin`, `num/adsh.bin` (39,401,761 rows)
- This query: anti-join key `(n.tag, n.version, n.adsh)` against `pre`.

### num.uom (unit, uint16_t, dict_shared(uom_dict))
- File: `num/uom.bin` (39,401,761 rows)
- This query: `uom='USD'`; runtime resolves code from `gendb/dicts/uom.dict`.

### num.ddate (report_date_yyyymmdd, int32_t, plain)
- File: `num/ddate.bin` (39,401,761 rows)
- This query: `ddate BETWEEN 20230101 AND 20231231`.

### num.value (fact_value, double, plain_nan_null)
- File: `num/value.bin` (39,401,761 rows)
- This query: `value IS NOT NULL`, `SUM(value)`.

### pre.tag / pre.version / pre.adsh
- Files: `pre/tag.bin`, `pre/version.bin`, `pre/adsh.bin` (9,600,799 rows)
- This query: anti-join existence side (`LEFT JOIN ... WHERE p.adsh IS NULL`).

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| num | 39,401,761 | fact scan/filter/aggregate | none | 100000 |
| pre | 9,600,799 | anti-join lookup side | none | 100000 |

## Query Analysis
- Estimated row reductions from workload file:
  - `uom='USD'`: `39,401,761 x 0.887 ~= 34,949,362`
  - `ddate in [20230101,20231231]`: selectivity `0.0` in profile (dates in data end near 2022), so result expected empty/very small before anti-join.
- Anti-join pattern:
  - Probe pre-triple index for each surviving `num` row key `(adsh,tag,version)`.
  - Keep row only when posting list is absent (`p.adsh IS NULL` semantics).
- Aggregation:
  - Group by `(n.tag,n.version)`.
  - HAVING `COUNT(*) > 10` then order by `cnt DESC`, limit 100.

## Indexes
### num_uom_hash (posting index on `num.uom`)
- File: `num/indexes/num_uom_hash.bin`
- Builder call is `build_posting_u32(gendb / "num", "uom.bin", "num_uom_hash.bin")`, so build-time key reads use `uint32_t` elements from `uom.bin`.
- Verbatim materialization in builder: `p.emplace_back(key[i], i);`
- Entry struct: `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- Multi-value format: one `uom` key -> rowid span in trailing `uint32_t rowids[]`.
- Empty-slot sentinel: none.

### num_ddate_zonemap (zone map on `num.ddate`)
- File: `num/indexes/num_ddate_zonemap.bin`
- Type instantiation: `build_zonemap_t<int32_t>(gendb / "num", "ddate.bin", "num_ddate_zonemap.bin", block)`
- Layout:
  - `uint64_t block_size`
  - `uint64_t blocks`
  - per block `(int32_t min_ddate, int32_t max_ddate)`
- Usage: skip blocks where `[min_ddate,max_ddate]` does not overlap `[20230101,20231231]`.

### pre_adsh_tag_version_hash (triple posting index on `pre`)
- File: `pre/indexes/pre_adsh_tag_version_hash.bin`
- Exact key struct:
  `struct TripleKey { uint32_t a; uint32_t b; uint32_t c; };`
- Exact entry struct:
  `struct Entry { TripleKey k; uint64_t start; uint32_t count; };`
- Verbatim tuple materialization: `p.push_back({{a[i], b[i], c[i]}, i});`
- Verbatim comparator shape:
  ```cpp
  if (x.first.a != y.first.a) return x.first.a < y.first.a;
  if (x.first.b != y.first.b) return x.first.b < y.first.b;
  if (x.first.c != y.first.c) return x.first.c < y.first.c;
  return x.second < y.second;
  ```
- On-disk layout:
  - `uint64_t entry_count`
  - `uint64_t rowid_count`
  - repeated `(uint32_t a,uint32_t b,uint32_t c,uint64_t start,uint32_t count)`
  - trailing posting payload `uint32_t rowids[rowid_count]`
- Multi-value format: 1:N postings for duplicate triples.
- Empty-slot sentinel: none.
