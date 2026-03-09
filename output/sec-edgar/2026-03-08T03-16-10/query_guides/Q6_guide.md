# Q6 Guide

## Column Reference
### num.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `num/adsh.bin` (39401761 rows)
- Shared dictionary files: `shared/adsh.data.bin` + `shared/adsh.offsets.bin`
- This query: join from `num` to `sub` and to `pre`

### num.tag (shared_dict_id(tag), uint32_t, dictionary_shared)
- File: `num/tag.bin` (39401761 rows)
- Shared dictionary files: `shared/tag.data.bin` + `shared/tag.offsets.bin`
- This query: part of the `num -> pre` join and final `GROUP BY`

### num.version (shared_dict_id(version), uint32_t, dictionary_shared)
- File: `num/version.bin` (39401761 rows)
- Shared dictionary files: `shared/version.data.bin` + `shared/version.offsets.bin`
- This query: part of the `num -> pre` join

### num.uom (unit_of_measure, uint16_t, dictionary_local)
- File: `num/uom.bin` (39401761 rows)
- Dictionary files: `num/dict_uom.data.bin` + `num/dict_uom.offsets.bin`
- This query: `uom = 'USD'` -> runtime dictionary resolution, then integer comparison

### num.value (numeric_fact, double, plain)
- File: `num/value.bin` (39401761 rows)
- This query: `SUM(value)` and `COUNT(*)`
- Null semantics: `value IS NOT NULL` is a storage no-op because ingest writes `0.0` for empty CSV fields

### sub.fy (fiscal_year, int32_t, plain)
- File: `sub/fy.bin` (86135 rows)
- This query: `s.fy = 2023`

### sub.name (registrant_name, uint32_t, dictionary_local)
- File: `sub/name.bin` (86135 rows)
- Dictionary files: `sub/dict_name.data.bin` + `sub/dict_name.offsets.bin`
- This query: `GROUP BY s.name`

### pre.stmt (statement_type, uint16_t, dictionary_local)
- File: `pre/stmt.bin` (9600799 rows)
- Dictionary files: `pre/dict_stmt.data.bin` + `pre/dict_stmt.offsets.bin`
- This query: `p.stmt = 'IS'` and `GROUP BY p.stmt`; resolve `"IS"` at runtime, no hardcoded code

### pre.tag (shared_dict_id(tag), uint32_t, dictionary_shared)
- File: `pre/tag.bin` (9600799 rows)
- This query: part of the `(adsh,tag,version)` join key

### pre.version (shared_dict_id(version), uint32_t, dictionary_shared)
- File: `pre/version.bin` (9600799 rows)
- This query: part of the `(adsh,tag,version)` join key

### pre.plabel (presentation_label, uint32_t, dictionary_local)
- File: `pre/plabel.bin` (9600799 rows)
- Dictionary files: `pre/dict_plabel.data.bin` + `pre/dict_plabel.offsets.bin` (698148 values)
- This query: `GROUP BY p.plabel`; keep `uint32_t` codes during aggregation, decode final rows only

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| num | 39401761 | fact | none on base columns; separate sorted rowid index on `(adsh,tag,version)` | 100000 |
| sub | 86135 | dimension | none on base columns | 100000 |
| pre | 9600799 | fact | none on base columns; separate sorted rowid index on `(adsh,tag,version)` | 100000 |

## Query Analysis
- Q6 is a three-way aggregation: filtered `num`, `sub` lookup by `adsh`, and multi-match `pre` lookup by `(adsh,tag,version)`.
- Workload selectivities: `pre.stmt='IS'` is `0.194`; `sub.fy=2023` is listed as `0.0` in `workload_analysis.json` for this materialized dataset.
- If `fy=2023` truly has no rows in `sub`, the dense lookup plus `fy` test short-circuits the query quickly after the `num` scan.
- The aggregation key is `(sub.name_id, pre.stmt_id, num.tag_id, pre.plabel_id)`.
- Both `name` and `plabel` are dictionary IDs during execution and decode only in the final output.

## Indexes
### uom_zone_map (zone_map on `num.uom`)
- File: `num/indexes/uom.zone_map.bin`
- Exact struct layout:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type: `uint16_t`
- Empty-slot sentinel: none
- Usage: resolve `usd_code`, then prune `num` blocks by zone containment

### adsh_to_rowid (dense lookup on `sub.adsh`)
- File: `sub/indexes/adsh_to_rowid.bin`
- Layout and sentinel:
```cpp
std::vector<uint32_t> lookup(adsh_offsets.size() - 1, kEmpty32);
for (uint32_t row = 0; row < sub_adsh.size(); ++row) {
    lookup[sub_adsh[row]] = row;
}
constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
```
- Hash function: none
- Usage: `sub_row = lookup[num_adsh[row]]`, then test `fy[sub_row] == 2023`

### fy_zone_map (zone_map on `sub.fy`)
- File: `sub/indexes/fy.zone_map.bin`
- Row coverage: 1 block
- Exact struct layout:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type: `int32_t`
- Empty-slot sentinel: none
- Usage: with one block, this is a dataset-level confirmation rather than a pruning accelerator

### stmt_zone_map (zone_map on `pre.stmt`)
- File: `pre/indexes/stmt.zone_map.bin`
- Row coverage: 97 blocks
- Exact struct layout:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type: `uint16_t`
- Empty-slot sentinel: none
- Usage: resolve `is_code` from `pre/dict_stmt.*`, then discard blocks whose `[min,max]` excludes that code

### adsh_tag_version_sorted (sorted multi-value index on `pre`)
- File: `pre/indexes/adsh_tag_version.rowids.bin`
- Exact build logic:
```cpp
std::vector<uint32_t> rowids(adsh.size());
std::iota(rowids.begin(), rowids.end(), 0);
std::sort(rowids.begin(), rowids.end(), [&](uint32_t lhs, uint32_t rhs) {
    return std::tie(adsh[lhs], tag[lhs], version[lhs], lhs) < std::tie(adsh[rhs], tag[rhs], version[rhs], rhs);
});
```
- Struct layout: none
- Empty-slot sentinel: none
- Multi-value format: all matching `pre` rowids for one `(adsh,tag,version)` key occupy one contiguous run in the permutation file; no offset/count sidecar exists
- Usage: probe by the `num` row’s shared-ID triple, then scan the contiguous run and keep only rows whose `stmt == is_code`

## Runtime Dictionary Loading Pattern
- Resolve `"USD"` from `num/dict_uom.*`.
- Resolve `"IS"` from `pre/dict_stmt.*`.
- Keep `sub.name`, `pre.stmt`, `num.tag`, and `pre.plabel` as integer IDs while aggregating.
- Decode `name` and `plabel` only for the top 200 output rows.

## Implementation Notes
- The guide uses the actual on-disk type for `pre.stmt`: `uint16_t`, not the smaller nominal type in `storage_design.json`.
- Q6 does not use the `tag` table, so the `tag_version_hash` index is intentionally omitted.
