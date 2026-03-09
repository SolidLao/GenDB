# Q4 Guide

## Column Reference
### num.adsh (filing_id_fk, uint32_t, dict_shared(adsh_dict))
- File: `num/adsh.bin` (39,401,761 rows)
- This query: joins to `sub.adsh` and `pre.adsh`.

### num.tag (xbrl_tag, uint32_t, dict_shared(tag_dict))
- File: `num/tag.bin` (39,401,761 rows)
- This query: joins to `tag.tag` and `pre.tag`.

### num.version (taxonomy_version, uint32_t, dict_shared(version_dict))
- File: `num/version.bin` (39,401,761 rows)
- This query: joins to `tag.version` and `pre.version`.

### num.uom (unit, uint16_t, dict_shared(uom_dict))
- File: `num/uom.bin` (39,401,761 rows)
- This query: `n.uom = 'USD'`; code loaded from `gendb/dicts/uom.dict` at runtime.

### num.value (fact_value, double, plain_nan_null)
- File: `num/value.bin` (39,401,761 rows)
- This query: `IS NOT NULL`, `SUM`, `AVG`, `ORDER BY total_value DESC`.

### sub.adsh / sub.sic / sub.cik (uint32_t, int32_t, int32_t)
- Files: `sub/adsh.bin`, `sub/sic.bin`, `sub/cik.bin` (86,135 rows each)
- This query: `n.adsh = s.adsh`, `s.sic BETWEEN 4000 AND 4999`, `COUNT(DISTINCT s.cik)`.

### tag.tag / tag.version / tag.abstract / tag.tlabel
- Files: `tag/tag.bin`, `tag/version.bin`, `tag/abstract.bin`, `tag/tlabel.bin` (1,070,662 rows)
- This query: `n.tag=n.tag AND n.version=t.version`, `t.abstract=0`, output `tlabel`.

### pre.adsh / pre.tag / pre.version / pre.stmt
- Files: `pre/adsh.bin`, `pre/tag.bin`, `pre/version.bin`, `pre/stmt.bin` (9,600,799 rows)
- This query: join triple with `num`, filter `p.stmt='EQ'`, output/group by `p.stmt`.
- Dictionary loading pattern: read `gendb/dicts/stmt.dict` at runtime, find `'EQ'` code, compare as `uint8_t`.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| num | 39,401,761 | fact driver | none | 100000 |
| sub | 86,135 | dimension | none | 100000 |
| tag | 1,070,662 | dimension | none | 100000 |
| pre | 9,600,799 | fact-side join/filter | none | 100000 |

## Query Analysis
- Selectivity estimates from workload:
  - `num.uom='USD'`: `39,401,761 x 0.887 ~= 34,949,362`
  - `sub.sic BETWEEN 4000 AND 4999`: `86,135 x 0.09 ~= 7,752`
  - `pre.stmt='EQ'`: `9,600,799 x 0.102 ~= 979,282`
  - `tag.abstract=0`: selectivity 1.0 (no reduction)
- Join pattern:
  - `num -> sub` on `adsh` (many-to-one)
  - `num -> tag` on `(tag,version)` (many-to-one)
  - `num -> pre` on `(adsh,tag,version)` (many-to-many)
- Aggregation:
  - Group key `(sic, tlabel, stmt)`
  - Metrics: `COUNT(DISTINCT cik)`, `SUM(value)`, `AVG(value)`; HAVING `distinct_cik >= 2`.

## Indexes
### num_uom_hash (posting index on `num.uom`)
- File: `num/indexes/num_uom_hash.bin`
- Builder call is `build_posting_u32(gendb / "num", "uom.bin", "num_uom_hash.bin")`, so this index is built by reading `uom.bin` as `uint32_t` keys.
- Verbatim key materialization: `p.emplace_back(key[i], i);`
- Struct layout (exact): `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- Multi-value format: entry points to a contiguous rowid span in trailing `uint32_t rowids[]`.
- Empty-slot sentinel: none.

### num_adsh_fk_hash (posting index on `num.adsh`)
- File: `num/indexes/num_adsh_fk_hash.bin`
- Build call: `build_posting_u32(gendb / "num", "adsh.bin", "num_adsh_fk_hash.bin")`.
- Exact struct layout: `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- On-disk layout:
  - `uint64_t entry_count`
  - `uint64_t rowid_count`
  - repeated `(uint32_t key, uint64_t start, uint32_t count)`
  - trailing `uint32_t rowids[rowid_count]`
- Multi-value format: one `adsh` maps to many `num` rowids.
- Empty-slot sentinel: none.

### num_adsh_tag_version_hash (triple posting index on `num`)
- File: `num/indexes/num_adsh_tag_version_hash.bin`
- Exact key struct:
  `struct TripleKey { uint32_t a; uint32_t b; uint32_t c; };`
- Exact entry struct:
  `struct Entry { TripleKey k; uint64_t start; uint32_t count; };`
- Verbatim materialization:
  `p.push_back({{a[i], b[i], c[i]}, i});`
- Verbatim comparator order: `(a,b,c,rowid)` ascending.
- On-disk layout:
  - `uint64_t entry_count`
  - `uint64_t rowid_count`
  - repeated `(uint32_t a,uint32_t b,uint32_t c,uint64_t start,uint32_t count)`
  - trailing `uint32_t rowids[rowid_count]`
- Multi-value format: 1:N list for duplicate triples.
- Empty-slot sentinel: none.

### sub_adsh_pk_hash (dense PK lookup on `sub.adsh`)
- File: `sub/indexes/sub_adsh_pk_hash.bin`
- Sentinel: `std::numeric_limits<uint32_t>::max()` for missing key.
- Layout: `[uint64_t lut_size][uint32_t lut[lut_size]]`.
- Hash formula: direct dense code indexing (no folding/mask).

### sub_sic_zonemap (zone map on `sub.sic`)
- File: `sub/indexes/sub_sic_zonemap.bin`
- Type: `int32_t` min/max pairs per `100000`-row block.
- Layout: `[uint64_t block_size][uint64_t blocks][(int32_t mn,int32_t mx)...]`.

### tag_tag_version_pk_hash (packed key PK map on `tag`)
- File: `tag/indexes/tag_tag_version_pk_hash.bin`
- Verbatim 64-bit key computation (authoritative):
  ```cpp
  uint64_t k = (static_cast<uint64_t>(tag[i]) << 32) | ver[i];
  ```
- Serialized layout:
  - `uint64_t pair_count`
  - repeated `(uint64_t k, uint32_t rowid)` pairs from `std::unordered_map<uint64_t,uint32_t>` iteration order.
- Struct layout in code: map value pair `(key:uint64_t, rowid:uint32_t)`.
- Empty-slot sentinel: none serialized; misses handled by probe-map lookup failure.

### pre_stmt_hash (posting index on `pre.stmt`)
- File: `pre/indexes/pre_stmt_hash.bin`
- Built with `build_posting_u32` over `stmt.bin`.
- Exact serialization entry struct:
  `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- Note on type widening: source `stmt.bin` is `uint8_t`; builder reads as `uint32_t` and stores `uint32_t key` in postings.
- Multi-value format and no-sentinel behavior are identical to `num_uom_hash`.

### pre_adsh_tag_version_hash (triple-key posting index on `pre`)
- File: `pre/indexes/pre_adsh_tag_version_hash.bin`
- Exact key struct:
  `struct TripleKey { uint32_t a; uint32_t b; uint32_t c; };`
- Exact entry struct:
  `struct Entry { TripleKey k; uint64_t start; uint32_t count; };`
- Verbatim key materialization:
  `p.push_back({{a[i], b[i], c[i]}, i});`
- Verbatim lexicographic comparator checks `a`, then `b`, then `c`, then rowid.
- On-disk layout:
  - `uint64_t entry_count`
  - `uint64_t rowid_count`
  - repeated `(uint32_t a,uint32_t b,uint32_t c,uint64_t start,uint32_t count)`
  - trailing `uint32_t rowids[rowid_count]`
- Multi-value format: 1:N row spans for duplicated `(adsh,tag,version)`.
- Empty-slot sentinel: none.
