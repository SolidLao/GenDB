# Q24 Guide

## Column Reference
### num.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `num/adsh.bin` (39401761 rows)
- Shared dictionary files: `shared/adsh.data.bin` + `shared/adsh.offsets.bin`
- This query: part of the anti-join key `(tag,version,adsh)` against `pre`

### num.tag (shared_dict_id(tag), uint32_t, dictionary_shared)
- File: `num/tag.bin` (39401761 rows)
- Shared dictionary files: `shared/tag.data.bin` + `shared/tag.offsets.bin`
- This query: `GROUP BY n.tag, n.version` and anti-join to `pre`

### num.version (shared_dict_id(version), uint32_t, dictionary_shared)
- File: `num/version.bin` (39401761 rows)
- Shared dictionary files: `shared/version.data.bin` + `shared/version.offsets.bin`
- This query: `GROUP BY n.tag, n.version` and anti-join to `pre`

### num.uom (unit_of_measure, uint16_t, dictionary_local)
- File: `num/uom.bin` (39401761 rows)
- Dictionary files: `num/dict_uom.data.bin` + `num/dict_uom.offsets.bin`
- This query: `uom = 'USD'` -> resolve `usd_code` at runtime

### num.ddate (date, int32_t, days_since_epoch_1970)
- File: `num/ddate.bin` (39401761 rows)
- This query SQL: `n.ddate BETWEEN 20230101 AND 20231231`
- Storage comparison: query code must convert those literals to day counts before comparing against `ddate.bin`
- Exact ingest path: `cols.ddate.push_back(parse_yyyymmdd_days(fields[3]));`
- Runtime bounds for this predicate: `20230101 -> 19358`, `20231231 -> 19722`

### num.value (numeric_fact, double, plain)
- File: `num/value.bin` (39401761 rows)
- This query: `SUM(n.value)`
- Null semantics: `value IS NOT NULL` is a storage no-op because empty input becomes `0.0`

### pre.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `pre/adsh.bin` (9600799 rows)
- This query: anti-join probe target

### pre.tag (shared_dict_id(tag), uint32_t, dictionary_shared)
- File: `pre/tag.bin` (9600799 rows)
- This query: anti-join probe target

### pre.version (shared_dict_id(version), uint32_t, dictionary_shared)
- File: `pre/version.bin` (9600799 rows)
- This query: anti-join probe target

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| num | 39401761 | fact | none on base columns; separate sorted rowid index on `(adsh,tag,version)` | 100000 |
| pre | 9600799 | fact | none on base columns; separate sorted rowid index on `(adsh,tag,version)` | 100000 |

## Query Analysis
- Q24 is an anti-join: keep `num` rows that have no matching `pre` row on `(adsh,tag,version)`.
- Filters are applied on `num` first: `uom='USD'`, `ddate` in 2023, and the storage no-op `value IS NOT NULL`.
- Workload analysis records `num.ddate BETWEEN ...` selectivity as `0.0` for this materialized dataset.
- That matches the sampled min/max metadata: observed source dates top out at `20220131`, while stored dates are day counts and the 2023 predicate maps to `[19358, 19722]`.
- Grouping happens on shared `(tag, version)` integer IDs and final decode is deferred to result emission.

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
- Usage: resolve `usd_code` from `num/dict_uom.*`, then keep only blocks whose zone contains it

### ddate_zone_map (zone_map on `num.ddate`)
- File: `num/indexes/ddate.zone_map.bin`
- Row coverage: `ceil(39401761 / 100000) = 395` blocks
- Exact struct layout:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type: `int32_t`
- Binary layout per record: `int32_t min; int32_t max;`
- Empty-slot sentinel: none
- Exact build call:
```cpp
auto num_ddate_zone = std::async(std::launch::async, build_zone_map<int32_t>, base_dir / "num" / "ddate.bin", base_dir / "num" / "indexes" / "ddate.zone_map.bin");
```
- Usage: prune blocks by overlap with `[19358, 19722]`; on this materialized dataset that range should eliminate all blocks if the stored max remains below `19358`

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
- Multi-value format: all matching `pre` rowids for a given `(adsh,tag,version)` key appear contiguously in the rowid permutation; there is no postings header or end marker
- Anti-join usage: for each qualifying `num` row, binary-search the sorted permutation for the key triple; if the contiguous run is empty, the `num` row survives

## Runtime Dictionary Loading Pattern
- Resolve `"USD"` by scanning `num/dict_uom.offsets.bin` + `num/dict_uom.data.bin`.
- Keep `(tag_id, version_id)` as integer group keys.
- Decode tag/version strings only for final grouped rows if the query returns any.

## Implementation Notes
- The SQL literals `20230101` and `20231231` are not stored directly; they must be converted with the same calendar-to-days logic used by `parse_yyyymmdd_days`.
- No hash index exists for the anti-join key; the query uses the sorted `pre` permutation as the 1:N membership structure.
