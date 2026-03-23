
# Q2 Guide

## Column Reference
### num.adsh (filing_id, uint32_t, global_dict_u32)
- File: `num/adsh.bin` (39401761 rows)
- This query: `JOIN sub ON n.adsh = s.adsh` and `JOIN m ON n.adsh = m.adsh` → compare the stored global `uint32_t` codes directly.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.tag (xbrl_tag, uint32_t, global_dict_u32)
- File: `num/tag.bin` (39401761 rows)
- This query: `GROUP BY adsh, tag` inside `m` and `ORDER BY ..., n.tag` → aggregate and sort on the stored global tag code, then decode final output with `dicts/global_tag.*`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.uom (unit_of_measure, uint16_t, dict_u16)
- File: `num/uom.bin` (39401761 rows)
- This query: `uom = 'pure'` → C++ pattern `num_uom[rowid] == pure_code` where `pure_code` is loaded at runtime from `dicts/num_uom.*`.
- Dictionary files: `dicts/num_uom.offsets.bin` + `dicts/num_uom.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### num.value (numeric_fact, double, plain_f64)
- File: `num/value.bin` (39401761 rows)
- This query: `value IS NOT NULL` → `!std::isnan(num_value[rowid])`; `value = m.max_value` compares the stored `double` against the aggregated maximum for the same `(adsh, tag)` group.

### sub.adsh (filing_id, uint32_t, global_dict_u32)
- File: `sub/adsh.bin` (86135 rows)
- This query: join probe target for `n.adsh = s.adsh`; `sub` stores the same global filing code domain as `num` and `pre`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### sub.name (company_name, uint32_t, dict_u32)
- File: `sub/name.bin` (86135 rows)
- This query: `SELECT s.name` and secondary `ORDER BY s.name` → carry the stored code through the join and decode only for the final top-100 rows.
- Dictionary files: `dicts/sub_name.offsets.bin` + `dicts/sub_name.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### sub.fy (fiscal_year, int16_t, plain_i16)
- File: `sub/fy.bin` (86135 rows)
- This query: `s.fy = 2022` → C++ `sub_fy[rowid] == 2022`; null fiscal years use `std::numeric_limits<int16_t>::min()` and are excluded by the equality.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| `num` | 39401761 | fact | `[]` | 100000 |
| `sub` | 86135 | dimension | `[]` | 100000 |

## Query Analysis
- Filter derivation from `workload_analysis.json`: `num.uom = 'pure'` keeps about `39401761 × 0.0346 ≈ 1363301` rows; `sub.fy = 2022` keeps about `86135 × 0.3173 ≈ 27331` rows.
- The expensive part is the runtime aggregation `GROUP BY adsh, tag` to compute `MAX(value)`; there is no persisted `(adsh, tag)` index, so this stays a query-time hash aggregate.
- `sub` is small enough to filter first on `fy` and then use the dense `adsh -> rowid` lookup for the join from filtered `num` rows.
- Final ordering is by `n.value DESC, s.name, n.tag`; `value` sorts as `double`, while `name` and `tag` can remain coded until the top-100 set is materialized.

## Indexes
### num_uom_postings (value postings on `num.uom`)
- File set: `indexes/num/num_uom_postings.values.bin`, `indexes/num/num_uom_postings.offsets.bin`, `indexes/num/num_uom_postings.rowids.bin`
- Actual layout from `build_value_postings_no_null<uint16_t>`: sorted unique `uom` codes in `values.bin`, prefix-style `uint64_t` ranges in `offsets.bin`, and grouped row ids in `rowids.bin`.
- Cardinalities on disk: `201` unique values, `202` offsets entries, `39401761` grouped row ids.
- Group membership formula: `rowids[offsets[g] .. offsets[g + 1])` are the rows whose `num/uom.bin` code equals `values[g]`.
- Empty-slot sentinel: none; the structure is pure postings, not a hash table.
- Usage here: resolve `pure_code` from `dicts/num_uom.*`, binary search `values.bin` for that code, then scan only the `rowids` slice for `uom = 'pure'`.

### sub_fy_postings (value postings on `sub.fy`)
- File set: `indexes/sub/sub_fy_postings.values.bin`, `indexes/sub/sub_fy_postings.offsets.bin`, `indexes/sub/sub_fy_postings.rowids.bin`
- Actual layout from `build_value_postings<int16_t>`: sorted unique fiscal years excluding null sentinel `kInt16Null`, plus `offsets` and grouped `rowids`.
- Cardinalities on disk: `13` unique fiscal years, `14` offsets entries, `81473` non-null row ids.
- Group membership formula: `rowids[offsets[g] .. offsets[g + 1])` are the `sub` rows for `values[g]`.
- Empty-slot sentinel: none in the index payload; null `fy` values are skipped during build and therefore never appear in `values.bin`.
- Usage here: locate the postings group where `values[g] == 2022`, then either materialize a filtered `sub` row set or mark acceptable `adsh` codes before probing `num`.

### sub_adsh_dense_lookup (dense lookup on `sub.adsh`)
- File: `indexes/sub/sub_adsh_dense_lookup.bin`
- Exact builder logic:
```cpp
uint32_t max_code = 0;
for (uint32_t code : adsh) {
    max_code = std::max(max_code, code);
}
std::vector<uint32_t> lookup(static_cast<size_t>(max_code) + 1, std::numeric_limits<uint32_t>::max());
for (uint32_t rowid = 0; rowid < adsh.size(); ++rowid) {
    lookup[adsh[rowid]] = rowid;
}
```
- On-disk cardinality: `86136` `uint32_t` entries, so lookup is direct by global `adsh` code.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Usage here: after choosing a `num.adsh` code, probe `lookup[adsh_code]`; a value other than the sentinel is the matching `sub` row id for fetching `fy` and `name`.
