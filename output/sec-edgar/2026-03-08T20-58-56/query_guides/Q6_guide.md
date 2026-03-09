# Q6 Guide

## Column Reference
### num.adsh / num.tag / num.version / num.uom / num.value
- Files: `num/adsh.bin`, `num/tag.bin`, `num/version.bin`, `num/uom.bin`, `num/value.bin` (39,401,761 rows)
- This query:
  - joins to `sub` on `adsh`
  - joins to `pre` on `(adsh,tag,version)`
  - filters `uom='USD'` and `value IS NOT NULL`
- Dictionary runtime loading: resolve USD via `gendb/dicts/uom.dict` each run.

### sub.adsh / sub.fy / sub.name
- Files: `sub/adsh.bin`, `sub/fy.bin`, `sub/name.bin` (86,135 rows)
- This query: join on `adsh`, filter `fy=2023`, group key/output includes `name`.

### pre.adsh / pre.tag / pre.version / pre.stmt / pre.plabel
- Files: `pre/adsh.bin`, `pre/tag.bin`, `pre/version.bin`, `pre/stmt.bin`, `pre/plabel.bin` (9,600,799 rows)
- This query: join triple, filter `stmt='IS'`, group/output uses `stmt` + `plabel`.
- Dictionary runtime loading: read `gendb/dicts/stmt.dict` to discover `'IS'` code; decode `pre/plabel.dict` for final output.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| num | 39,401,761 | fact driver | none | 100000 |
| sub | 86,135 | dimension filter | none | 100000 |
| pre | 9,600,799 | fact-side join/filter | none | 100000 |

## Query Analysis
- Estimated filters:
  - `num.uom='USD'`: `39,401,761 x 0.887 ~= 34,949,362`
  - `pre.stmt='IS'`: `9,600,799 x 0.194 ~= 1,862,555`
  - `sub.fy=2023`: workload analysis selectivity `0.0` (expected empty/near-empty join side).
- Group key: `(s.name, p.stmt, n.tag, p.plabel)` with `SUM(n.value)` and `COUNT(*)`.
- With `fy=2023` likely eliminating rows, fast-path behavior depends on early `sub` filter application.

## Indexes
### num_uom_hash (posting index on `num.uom`)
- File: `num/indexes/num_uom_hash.bin`
- Builder call is `build_posting_u32(gendb / "num", "uom.bin", "num_uom_hash.bin")`, so index build reads `uom.bin` keys as `uint32_t`.
- Verbatim build logic: `for (uint32_t i = 0; i < n; ++i) p.emplace_back(key[i], i);`
- Entry struct: `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- Multi-value layout: postings span in trailing `rowids[]` by `(start,count)`.
- Empty-slot sentinel: none.

### num_adsh_fk_hash (posting index on `num.adsh`)
- File: `num/indexes/num_adsh_fk_hash.bin`
- Build call: `build_posting_u32(gendb / "num", "adsh.bin", "num_adsh_fk_hash.bin")`.
- Struct layout: `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- On-disk layout: `[entry_count][rowid_count][(key,start,count)...][rowids...]`.
- Multi-value format: posting list per `adsh`.
- Empty-slot sentinel: none.

### num_adsh_tag_version_hash (triple posting index on `num`)
- File: `num/indexes/num_adsh_tag_version_hash.bin`
- Key struct: `struct TripleKey { uint32_t a; uint32_t b; uint32_t c; };`
- Entry struct: `struct Entry { TripleKey k; uint64_t start; uint32_t count; };`
- Verbatim key creation: `p.push_back({{a[i], b[i], c[i]}, i});`
- On-disk layout:
  - `uint64_t entry_count`
  - `uint64_t rowid_count`
  - entries `(a,b,c,start,count)`
  - trailing `uint32_t rowids[]`
- Multi-value format: contiguous posting spans by triple key.
- Empty-slot sentinel: none.

### sub_adsh_pk_hash (dense PK LUT on `sub.adsh`)
- File: `sub/indexes/sub_adsh_pk_hash.bin`
- Layout: `uint64_t size` then `uint32_t lut[size]`.
- Missing sentinel (exact): `std::numeric_limits<uint32_t>::max()`.

### sub_fy_zonemap (zone map on `sub.fy`)
- File: `sub/indexes/sub_fy_zonemap.bin`
- Layout: `[uint64_t block_size][uint64_t blocks][(int32_t mn, int32_t mx)...]`.
- Q6 usage: prune out blocks when probing `fy=2023`.

### pre_stmt_hash (posting index on `pre.stmt`)
- File: `pre/indexes/pre_stmt_hash.bin`
- Struct layout (exact): `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- Type note: key source is `uint8_t stmt` in column file, widened to `uint32_t` in index build.
- Empty-slot sentinel: none.

### pre_adsh_tag_version_hash (triple posting index on `pre`)
- File: `pre/indexes/pre_adsh_tag_version_hash.bin`
- Key struct: `struct TripleKey { uint32_t a; uint32_t b; uint32_t c; };`
- Entry struct: `struct Entry { TripleKey k; uint64_t start; uint32_t count; };`
- Verbatim key creation: `p.push_back({{a[i], b[i], c[i]}, i});`
- On-disk entry order: sorted lexicographically by `(a,b,c,rowid)`.
- Multi-value format: each triple key resolves to contiguous posting list in tail `rowids[]`.
- Empty-slot sentinel: none.
