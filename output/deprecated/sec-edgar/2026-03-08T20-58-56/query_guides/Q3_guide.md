# Q3 Guide

## Column Reference
### num.uom (unit, uint16_t, dict_shared(uom_dict))
- File: `num/uom.bin` (39,401,761 rows)
- This query: `n.uom = 'USD'` and subquery mirror filter.
- Runtime dictionary loading pattern: decode `gendb/dicts/uom.dict` into `string->code`; derive USD code at runtime.

### num.adsh (filing_id_fk, uint32_t, dict_shared(adsh_dict))
- File: `num/adsh.bin` (39,401,761 rows)
- This query: join key to `sub.adsh` in outer and subquery blocks.

### num.value (fact_value, double, plain_nan_null)
- File: `num/value.bin` (39,401,761 rows)
- This query: `IS NOT NULL`, `SUM(value)`, and inner AVG-of-SUM threshold.

### sub.adsh (filing_id, uint32_t, dict_shared(adsh_dict))
- File: `sub/adsh.bin` (86,135 rows)
- This query: join target for `num.adsh`.

### sub.fy (fiscal_year, int32_t, plain)
- File: `sub/fy.bin` (86,135 rows)
- This query: `s.fy = 2022` (outer + subquery).

### sub.cik (company_id, int32_t, plain)
- File: `sub/cik.bin` (86,135 rows)
- This query: group key (`GROUP BY s.name, s.cik`) and subquery grouping by `s2.cik`.

### sub.name (company_name, uint32_t, dictionary)
- File: `sub/name.bin` (86,135 rows)
- This query: output/group key; decode through `sub/name.dict` for final rows.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| num | 39,401,761 | fact filter/join/aggregate | none | 100000 |
| sub | 86,135 | dimension filter/join | none | 100000 |

## Query Analysis
- Estimated `num` rows after USD filter: `39,401,761 x 0.887 ~= 34,949,362`.
- Estimated `sub` rows after `fy=2022`: `86,135 x 0.204 ~= 17,572`.
- Execution shape:
  - Inner branch computes per-`cik` totals, then averages those totals.
  - Outer branch computes per-`(name,cik)` totals.
  - Apply `HAVING total_value > inner_avg`.
  - Sort `total_value DESC`, limit 100.

## Indexes
### num_uom_hash (posting index on `num.uom`)
- File: `num/indexes/num_uom_hash.bin`
- Builder call is `build_posting_u32(gendb / "num", "uom.bin", "num_uom_hash.bin")`, so keys are read as `uint32_t` from the column file during index construction.
- Verbatim key construction from builder: `p.emplace_back(key[i], i);`
- Verbatim sort comparator:
  ```cpp
  if (a.first != b.first) return a.first < b.first;
  return a.second < b.second;
  ```
- Exact struct layout:
  `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- Multi-value format: `rowids` tail array with each entry storing `(start,count)` slice.
- Empty-slot sentinel: none.

### num_adsh_fk_hash (posting index on `num.adsh`)
- File: `num/indexes/num_adsh_fk_hash.bin`
- Build call: `build_posting_u32(gendb / "num", "adsh.bin", "num_adsh_fk_hash.bin")`.
- Verbatim key construction: `p.emplace_back(key[i], i);`
- Exact struct layout:
  `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- On-disk layout: `[uint64_t entry_count][uint64_t rowid_count][entries...][uint32_t rowids...]`.
- Multi-value format: posting list per `adsh`.
- Empty-slot sentinel: none.

### sub_adsh_pk_hash (dense PK lookup on `sub.adsh`)
- File: `sub/indexes/sub_adsh_pk_hash.bin`
- Exact sentinel value: `std::numeric_limits<uint32_t>::max()`.
- Layout:
  - `uint64_t lut_size`
  - `uint32_t lut[]`
- Lookup: `rowid = lut[adsh_code]` (no hash arithmetic).

### sub_fy_zonemap (zone map on `sub.fy`)
- File: `sub/indexes/sub_fy_zonemap.bin`
- Layout:
  - `uint64_t block_size`
  - `uint64_t blocks`
  - repeated `(int32_t mn, int32_t mx)`
- Purpose in Q3: prune `sub` blocks that cannot contain `fy=2022`.
