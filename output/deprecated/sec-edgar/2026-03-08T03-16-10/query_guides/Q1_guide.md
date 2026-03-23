# Q1 Guide

## Column Reference
### pre.stmt (statement_type, uint16_t, dictionary_local)
- File: `pre/stmt.bin` (9600799 rows)
- Dictionary files: `pre/dict_stmt.data.bin` + `pre/dict_stmt.offsets.bin` (9 values; runtime must scan offsets/data to resolve strings such as `stmt`)
- Storage design cross-check: `storage_design.json` marks `stmt` as `dictionary_local`; ingest writes `uint16_t` IDs via `cols.stmt.push_back(static_cast<uint16_t>(stmt_dict.get_or_add(fields[3])));`
- This query: `stmt IS NOT NULL` -> no storage-level filter; ingestion does not materialize nulls for `stmt`, it always stores a dictionary ID

### pre.rfile (render_source, uint16_t, dictionary_local)
- File: `pre/rfile.bin` (9600799 rows)
- Dictionary files: `pre/dict_rfile.data.bin` + `pre/dict_rfile.offsets.bin` (2 values; runtime decodes final groups by loading the dictionary)
- Storage design cross-check: `storage_design.json` marks `rfile` as `dictionary_local`; ingest writes `uint16_t` IDs via `cols.rfile.push_back(static_cast<uint16_t>(rfile_dict.get_or_add(fields[5])));`
- This query: `GROUP BY rfile` -> aggregate on stored `uint16_t` code, decode after grouping

### pre.adsh (shared_dict_id(adsh), uint32_t, dictionary_shared)
- File: `pre/adsh.bin` (9600799 rows)
- Shared dictionary files: `shared/adsh.data.bin` + `shared/adsh.offsets.bin` (86135 values)
- This query: `COUNT(DISTINCT adsh)` -> distinct-count on shared `uint32_t` IDs; decode only for presentation if needed

### pre.line (line_number, int32_t, plain)
- File: `pre/line.bin` (9600799 rows)
- This query: `AVG(line)` -> sum `int32_t`, divide by count per `(stmt, rfile)` group

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| pre | 9600799 | fact | none on base columns; separate sorted rowid index on `(adsh,tag,version)` | 100000 |

## Query Analysis
- Single-table aggregation on `pre`.
- `stmt IS NOT NULL` has workload selectivity `1.0`; that matches ingestion semantics because blank strings still get dictionary codes and there is no null sentinel column.
- Aggregate key is compact: `(uint16_t stmt_id, uint16_t rfile_id)`.
- `COUNT(DISTINCT adsh)` uses shared `uint32_t` IDs, so the per-group distinct set can stay on integer keys.
- Final decoding path is dictionary lookup against `pre/dict_stmt*`, `pre/dict_rfile*`, and no decode is required for `line`.

## Indexes
### stmt_zone_map (zone_map on `stmt`)
- File: `pre/indexes/stmt.zone_map.bin`
- Row coverage: `ceil(9600799 / 100000) = 97` blocks, file contains 97 records
- Exact struct layout from `build_indexes.cpp`:
```cpp
template <typename T>
struct ZoneRecord {
    T min;
    T max;
};
```
- Instantiated type here: `T = uint16_t`
- Binary layout per record: `uint16_t min; uint16_t max;` in that field order, repeated 97 times
- Empty-slot sentinel: none
- Exact build call:
```cpp
auto pre_stmt_zone = std::async(std::launch::async, build_zone_map<uint16_t>, base_dir / "pre" / "stmt.bin", base_dir / "pre" / "indexes" / "stmt.zone_map.bin");
```
- This query usage: not selective for `IS NOT NULL`; every block must be scanned because there is no null encoding to exclude

## Runtime Dictionary Loading Pattern
- Do not hardcode statement or render-file codes.
- Load `offsets` as `uint64_t[]` and `data` as a byte blob.
- For dictionary entry `i`, materialize:
```cpp
std::string_view value(data.data() + offsets[i], offsets[i + 1] - offsets[i]);
```
- For grouping, keep integer IDs in the hash table.
- For output, decode only the final sorted result rows.

## Implementation Notes
- The guide follows the implementation, not the nominal type labels in `storage_design.json`.
- `pre.stmt` and `pre.rfile` are materially `uint16_t` on disk because the file sizes are `19201598` bytes each for `9600799` rows.
- No other built index on `pre` is relevant to Q1.
