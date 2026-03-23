# Q3 Guide

## Column Reference
### num.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `num/adsh.bin` (39401761 rows)
- Shared dictionary files: `shared/adsh.data.bin` + `shared/adsh.offsets.bin` (86135 values)
- This query: `n.adsh = s.adsh` -> direct shared-ID equality, then dense lookup into `sub`

### num.uom (unit_of_measure, uint16_t, dictionary_local)
- File: `num/uom.bin` (39401761 rows)
- Dictionary files: `num/dict_uom.data.bin` + `num/dict_uom.offsets.bin` (201 values)
- This query: `uom = 'USD'` -> resolve `"USD"` at runtime, then compare `uom[row] == usd_code`

### num.value (numeric_fact, double, plain)
- File: `num/value.bin` (39401761 rows)
- This query: `SUM(value)` in both the outer aggregation and the correlated average-of-sums subquery
- Null semantics: empty CSV values are written as `0.0`, so `value IS NOT NULL` is a no-op in storage

### sub.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `sub/adsh.bin` (86135 rows)
- Shared dictionary files: `shared/adsh.data.bin` + `shared/adsh.offsets.bin`
- This query: join target for every qualifying `num` row

### sub.fy (fiscal_year, int32_t, plain)
- File: `sub/fy.bin` (86135 rows)
- This query: `s.fy = 2022` and `s2.fy = 2022` -> `int32_t` equality after lookup

### sub.name (registrant_name, uint32_t, dictionary_local)
- File: `sub/name.bin` (86135 rows)
- Dictionary files: `sub/dict_name.data.bin` + `sub/dict_name.offsets.bin` (9646 values)
- This query: `GROUP BY s.name, s.cik` -> group on `name` dictionary ID plus `cik`, decode `name` only for final rows

### sub.cik (cik, int32_t, plain)
- File: `sub/cik.bin` (86135 rows)
- This query: `GROUP BY s.name, s.cik` in the outer query and `GROUP BY s2.cik` inside the subquery

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| num | 39401761 | fact | none on base columns; separate sorted rowid index on `(adsh,tag,version)` | 100000 |
| sub | 86135 | dimension | none on base columns | 100000 |

## Query Analysis
- Outer plan: scan `num` filtered to `uom = 'USD'`, join to `sub` on `adsh`, filter `fy = 2022`, aggregate by `(name_id, cik)`.
- Inner threshold subquery: repeat the same filtered join shape, but aggregate by `cik`, then average those per-`cik` totals.
- Workload selectivities: `num.uom = 'USD'` is `0.887`; `sub.fy = 2022` is reused on both passes.
- Because both outer and inner passes share the same `sub` probe pattern, the dense `adsh_to_rowid` lookup is the key reusable index.
- `value IS NOT NULL` is not selective in storage because missing CSV values became `0.0`.

## Indexes
### uom_zone_map (zone_map on `uom`)
- File: `num/indexes/uom.zone_map.bin`
- Row coverage: `ceil(39401761 / 100000) = 395` blocks
- Exact struct layout:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type here: `uint16_t`
- Empty-slot sentinel: none
- Exact build call:
```cpp
auto num_uom_zone = std::async(std::launch::async, build_zone_map<uint16_t>, base_dir / "num" / "uom.bin", base_dir / "num" / "indexes" / "uom.zone_map.bin");
```
- This query usage: resolve `usd_code` at runtime, then retain blocks whose zone contains `usd_code`

### adsh_to_rowid (dense lookup on `sub.adsh`)
- File: `sub/indexes/adsh_to_rowid.bin`
- Physical layout: one `uint32_t` rowid per shared `adsh` ID
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
- Hash function: none
- This query usage: used in both the outer aggregate and the inner aggregate-by-`cik` subquery

### fy_zone_map (zone_map on `fy`)
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
- Instantiated type here: `int32_t`
- Empty-slot sentinel: none
- This query usage: only confirms the single `sub` block spans the requested fiscal year; there is no multi-block pruning opportunity

## Runtime Dictionary Loading Pattern
- `uom='USD'` must be resolved from `num/dict_uom.*` at runtime, never by a baked-in code.
- `name` stays as `uint32_t` dictionary ID during grouping:
```cpp
group_key = {sub_name[sub_row], sub_cik[sub_row]};
```
- Decode `name` only for the final `ORDER BY total_value DESC LIMIT 100` output rows.

## Implementation Notes
- The materialized storage makes the subquery threshold cheaper than text-key grouping because both `name` and `adsh` are integer-coded.
- No prebuilt index helps the `HAVING SUM(value) > AVG(sub_total)` comparison; that threshold is computed at query time.
