
# Q6 Guide

## Column Reference
### num.adsh (filing_id, uint32_t, global_dict_u32)
- File: `num/adsh.bin` (39401761 rows)
- This query: `JOIN sub ON n.adsh = s.adsh` and `JOIN pre ON (n.adsh, n.tag, n.version) = (p.adsh, p.tag, p.version)`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.tag (xbrl_tag, uint32_t, global_dict_u32)
- File: `num/tag.bin` (39401761 rows)
- This query: join to `pre.tag`, `GROUP BY n.tag`, and final `ORDER BY total_value DESC` on aggregated groups.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.version (taxonomy_version, uint32_t, global_dict_u32)
- File: `num/version.bin` (39401761 rows)
- This query: join to `pre.version` as part of the persisted triple key.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.uom (unit_of_measure, uint16_t, dict_u16)
- File: `num/uom.bin` (39401761 rows)
- This query: `n.uom = 'USD'` → `num_uom[rowid] == usd_code`, with runtime dictionary lookup through `dicts/num_uom.*`.
- Dictionary files: `dicts/num_uom.offsets.bin` + `dicts/num_uom.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### num.value (numeric_fact, double, plain_f64)
- File: `num/value.bin` (39401761 rows)
- This query: `n.value IS NOT NULL`, `SUM(n.value)`, `COUNT(*)` → `!std::isnan(num_value[rowid])` before updating aggregate state.

### sub.adsh (filing_id, uint32_t, global_dict_u32)
- File: `sub/adsh.bin` (86135 rows)
- This query: join target for `n.adsh = s.adsh`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### sub.name (company_name, uint32_t, dict_u32)
- File: `sub/name.bin` (86135 rows)
- This query: `SELECT s.name` and `GROUP BY s.name` → aggregate on the code and decode only for final output.
- Dictionary files: `dicts/sub_name.offsets.bin` + `dicts/sub_name.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### sub.fy (fiscal_year, int16_t, plain_i16)
- File: `sub/fy.bin` (86135 rows)
- This query: `s.fy = 2023` → `sub_fy[rowid] == 2023`.

### pre.adsh (filing_id, uint32_t, global_dict_u32)
- File: `pre/adsh.bin` (9600799 rows)
- This query: join side of `(adsh, tag, version)`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### pre.tag (xbrl_tag, uint32_t, global_dict_u32)
- File: `pre/tag.bin` (9600799 rows)
- This query: join side of `(adsh, tag, version)`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### pre.version (taxonomy_version, uint32_t, global_dict_u32)
- File: `pre/version.bin` (9600799 rows)
- This query: join side of `(adsh, tag, version)`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### pre.stmt (statement_type, uint16_t, dict_u16)
- File: `pre/stmt.bin` (9600799 rows)
- This query: `p.stmt = 'IS'` and `GROUP BY p.stmt` → `pre_stmt[rowid] == is_code`, with `is_code` resolved from `dicts/pre_stmt.*`.
- Dictionary files: `dicts/pre_stmt.offsets.bin` + `dicts/pre_stmt.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### pre.plabel (presentation_label, uint32_t, dict_u32)
- File: `pre/plabel.bin` (9600799 rows)
- This query: `SELECT p.plabel` and `GROUP BY p.plabel` → aggregate on the stored code; decode through `dicts/pre_plabel.*` only for the final result set.
- Dictionary files: `dicts/pre_plabel.offsets.bin` + `dicts/pre_plabel.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| `num` | 39401761 | fact | `[]` | 100000 |
| `sub` | 86135 | dimension | `[]` | 100000 |
| `pre` | 9600799 | fact | `[]` | 100000 |

## Query Analysis
- Filter derivation from `workload_analysis.json`: `num.uom = 'USD'` keeps about `33235385` rows, `pre.stmt = 'IS'` keeps about `9600799 × 0.1841 ≈ 1767507` rows, and `sub.fy = 2023` keeps about `86135 × 0.3090 ≈ 26616` rows.
- Runtime grouping key is `(sub.name_code, pre.stmt_code, num.tag_code, pre.plabel_code)`.
- The natural execution path is to prune `sub` by `fy`, prune `pre` by `stmt`, then stream filtered `num` rows through the persisted join structures.
- `COUNT(*)` counts joined fact rows, not distinct filings; every surviving `num` row contributes `1` to its aggregate bucket.

## Indexes
### num_uom_postings (value postings on `num.uom`)
- File set: `indexes/num/num_uom_postings.values.bin`, `indexes/num/num_uom_postings.offsets.bin`, `indexes/num/num_uom_postings.rowids.bin`
- Layout: sorted unique `uom` codes, `offsets`, grouped `rowids`.
- Cardinalities on disk: `201` unique values, `202` offsets entries, `39401761` row ids.
- Empty-slot sentinel: none.
- Usage here: find the `USD` code in `values.bin` and scan only its row-id slice.

### sub_fy_postings (value postings on `sub.fy`)
- File set: `indexes/sub/sub_fy_postings.values.bin`, `indexes/sub/sub_fy_postings.offsets.bin`, `indexes/sub/sub_fy_postings.rowids.bin`
- Layout: sorted non-null fiscal years, `offsets`, grouped `rowids`.
- Cardinalities on disk: `13` unique values, `14` offsets entries, `81473` non-null row ids.
- Empty-slot sentinel: none.
- Usage here: isolate the `2023` slice once, then build a permitted `sub.adsh` set or bitmap.

### sub_adsh_dense_lookup (dense lookup on `sub.adsh`)
- File: `indexes/sub/sub_adsh_dense_lookup.bin`
- Layout is a direct-addressed `uint32_t` vector keyed by global `adsh` code.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Usage here: for each filtered `num` row, reach `sub.name` and verify `fy == 2023` in O(1).

### pre_stmt_postings (value postings on `pre.stmt`)
- File set: `indexes/pre/pre_stmt_postings.values.bin`, `indexes/pre/pre_stmt_postings.offsets.bin`, `indexes/pre/pre_stmt_postings.rowids.bin`
- Layout: sorted statement codes, `offsets`, grouped `rowids`.
- Cardinalities on disk: `9` unique values, `10` offsets entries, `9600799` row ids.
- Empty-slot sentinel: none.
- Usage here: resolve `is_code` from `dicts/pre_stmt.*`, then limit `pre` participation to the `IS` postings slice.

### pre_adsh_tag_version_groups (open-addressed group hash on `(adsh, tag, version)`)
- File set: `indexes/pre/pre_adsh_tag_version_groups.rowids.bin`, `indexes/pre/pre_adsh_tag_version_groups.keys.bin`, `indexes/pre/pre_adsh_tag_version_groups.offsets.bin`, `indexes/pre/pre_adsh_tag_version_groups.hash.bin`
- Exact key and bucket layouts:
```cpp
struct TripleKey {
    uint32_t a;
    uint32_t b;
    uint32_t c;
};

struct TripleBucket {
    uint32_t a;
    uint32_t b;
    uint32_t c;
    uint64_t group_index;
};
```
- Exact hash computation copied from `build_indexes.cpp`:
```cpp
uint64_t mix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}

uint64_t hash_triple(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t seed = mix64(static_cast<uint64_t>(a) + 0x9e3779b97f4a7c15ull);
    seed ^= mix64((static_cast<uint64_t>(b) << 1) + 0x517cc1b727220a95ull);
    seed ^= mix64((static_cast<uint64_t>(c) << 7) + 0x94d049bb133111ebull);
    return mix64(seed);
}
```
- Exact probe-slot computation: `size_t slot = static_cast<size_t>(hash_triple(key.a, key.b, key.c) & (bucket_count - 1));` and collisions advance with `slot = (slot + 1) & (bucket_count - 1)`.
- Exact empty-slot sentinel: `kEmptyGroup = std::numeric_limits<uint64_t>::max()`.
- On-disk sizing: `8631890` groups, `8631891` offsets entries, `9600799` row ids, `33554432` hash buckets.
- Multi-value format: hash probe yields `group_index`; `keys[group_index]` is the exact triple and `rowids[offsets[group_index] .. offsets[group_index + 1])` are all matching `pre` rows.
- Usage here: each surviving `num` row probes the triple key to fetch one or more `pre` rows, then reads `stmt` and `plabel` from those row ids.
