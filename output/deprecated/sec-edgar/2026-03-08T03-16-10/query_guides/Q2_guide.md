# Q2 Guide

## Column Reference
### num.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `num/adsh.bin` (39401761 rows)
- Shared dictionary files: `shared/adsh.data.bin` + `shared/adsh.offsets.bin` (86135 values)
- This query: `n.adsh = s.adsh`, `n.adsh = m.adsh` -> direct integer equality across `num` and `sub` because both tables use the same shared dictionary IDs

### num.tag (shared_dict_id(tag), uint32_t, dictionary_shared)
- File: `num/tag.bin` (39401761 rows)
- Shared dictionary files: `shared/tag.data.bin` + `shared/tag.offsets.bin` (198311 values)
- This query: `n.tag = m.tag`, `GROUP BY adsh, tag`, final `ORDER BY ... n.tag` -> aggregate and join on shared `uint32_t` IDs, decode for output ordering tie-break only if needed after top-k materialization

### num.uom (unit_of_measure, uint16_t, dictionary_local)
- File: `num/uom.bin` (39401761 rows)
- Dictionary files: `num/dict_uom.data.bin` + `num/dict_uom.offsets.bin` (201 values)
- This query: `uom = 'pure'` -> resolve the code for `"pure"` at runtime by scanning the local dictionary, then compare `uom[row] == pure_code`

### num.value (numeric_fact, double, plain)
- File: `num/value.bin` (39401761 rows)
- This query: `MAX(value)`, `n.value = m.max_value`, `ORDER BY n.value DESC`
- Null semantics: `fields[7].empty() ? 0.0 : std::stod(fields[7])`; storage has no null bitmap, so SQL `value IS NOT NULL` is a no-op on this materialization

### sub.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `sub/adsh.bin` (86135 rows)
- Shared dictionary files: `shared/adsh.data.bin` + `shared/adsh.offsets.bin`
- This query: `s.adsh = n.adsh` -> probe `sub` row by shared `adsh` ID

### sub.fy (fiscal_year, int32_t, plain)
- File: `sub/fy.bin` (86135 rows)
- This query: `s.fy = 2022` -> plain `int32_t` comparison after row lookup or after single-block zone check

### sub.name (registrant_name, uint32_t, dictionary_local)
- File: `sub/name.bin` (86135 rows)
- Dictionary files: `sub/dict_name.data.bin` + `sub/dict_name.offsets.bin` (9646 values)
- This query: `SELECT s.name` and `ORDER BY ... s.name` -> keep dictionary ID during execution, decode for the final 100 rows

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| num | 39401761 | fact | none on base columns; separate sorted rowid index on `(adsh,tag,version)` | 100000 |
| sub | 86135 | dimension | none on base columns | 100000 |

## Query Analysis
- The expensive part is the filtered `num` scan for `uom = 'pure'`, then grouping by `(adsh, tag)` to compute `MAX(value)`.
- The rewritten self-join uses `(adsh, tag, value)` equality against the grouped maxima; no prebuilt index matches that exact key.
- `sub` is best treated as a dense dimension lookup keyed by shared `adsh` ID.
- Workload selectivities: `num.uom = 'pure'` is `0.001`; `sub.fy = 2022` is `0.204`.
- Because `value IS NOT NULL` is not represented separately on disk, execution should not spend cycles on a null check for `value`.

## Indexes
### uom_zone_map (zone_map on `uom`)
- File: `num/indexes/uom.zone_map.bin`
- Row coverage: `ceil(39401761 / 100000) = 395` blocks, file contains 395 records
- Exact struct layout from `build_indexes.cpp`:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type here: `T = uint16_t`
- Binary layout per record: `uint16_t min; uint16_t max;`
- Empty-slot sentinel: none
- Exact build call:
```cpp
auto num_uom_zone = std::async(std::launch::async, build_zone_map<uint16_t>, base_dir / "num" / "uom.bin", base_dir / "num" / "indexes" / "uom.zone_map.bin");
```
- This query usage: after runtime resolution of `"pure"` to `pure_code`, keep only blocks where `min <= pure_code && pure_code <= max`

### adsh_to_rowid (dense lookup on `sub.adsh`)
- File: `sub/indexes/adsh_to_rowid.bin`
- Supporting cardinality file: `shared/adsh.offsets.bin` determines the lookup array length `adsh_offsets.size() - 1`
- Physical layout: one `uint32_t` per shared `adsh` ID; file contains 86135 entries
- Exact implementation:
```cpp
std::vector<uint32_t> lookup(adsh_offsets.size() - 1, kEmpty32);
for (uint32_t row = 0; row < sub_adsh.size(); ++row) {
    lookup[sub_adsh[row]] = row;
}
```
- Empty-slot sentinel:
```cpp
constexpr uint32_t kEmpty32 = std::numeric_limits<uint32_t>::max();
```
- Hash function: none; this is direct array addressing
- This query usage: `sub_row = lookup[num_adsh[row]]`; then test `sub_row != kEmpty32 && fy[sub_row] == 2022`

### fy_zone_map (zone_map on `fy`)
- File: `sub/indexes/fy.zone_map.bin`
- Row coverage: `ceil(86135 / 100000) = 1` block, file contains 1 record
- Exact struct layout:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type here: `T = int32_t`
- Binary layout per record: `int32_t min; int32_t max;`
- Empty-slot sentinel: none
- Exact build call:
```cpp
auto sub_fy_zone = std::async(std::launch::async, build_zone_map<int32_t>, base_dir / "sub" / "fy.bin", base_dir / "sub" / "indexes" / "fy.zone_map.bin");
```
- This query usage: confirms the only `sub` block may contain `2022`; it does not partition `sub` further because there is one block total

## Runtime Dictionary Loading Pattern
- Do not hardcode the code for `"pure"`.
- Load `num/dict_uom.offsets.bin` as `uint64_t[]` and `num/dict_uom.data.bin` as bytes.
- Resolve `"pure"` by scanning entries:
```cpp
std::string_view v(data.data() + offsets[i], offsets[i + 1] - offsets[i]);
if (v == "pure") pure_code = static_cast<uint16_t>(i);
```
- Shared `adsh` and `tag` values already align across tables, so joins stay on integers.

## Implementation Notes
- The sorted `(adsh,tag,version)` index on `num` is not directly usable for Q2 because the grouped self-join key is `(adsh, tag, value)` and omits `version`.
- `sub` row lookup is O(1) by dense ID, not by hashing.
