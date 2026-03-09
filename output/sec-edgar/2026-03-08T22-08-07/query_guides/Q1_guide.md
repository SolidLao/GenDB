
# Q1 Guide

## Column Reference
### pre.stmt (statement_type, uint16_t, dict_u16)
- File: `pre/stmt.bin` (9600799 rows)
- This query: `stmt IS NOT NULL` → C++ `pre_stmt[rowid] != 0`; `LocalStringDict` reserves code `0` for the empty string.
- Dictionary files: `dicts/pre_stmt.offsets.bin` + `dicts/pre_stmt.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### pre.rfile (render_source, uint16_t, dict_u16)
- File: `pre/rfile.bin` (9600799 rows)
- This query: `GROUP BY rfile` → aggregate on the stored `uint16_t` code, then decode result groups through `dicts/pre_rfile.*`.
- Dictionary files: `dicts/pre_rfile.offsets.bin` + `dicts/pre_rfile.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### pre.adsh (filing_id, uint32_t, global_dict_u32)
- File: `pre/adsh.bin` (9600799 rows)
- This query: `COUNT(DISTINCT adsh)` → maintain distinctness on the stored global `uint32_t` filing code.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### pre.line (line_number, int32_t, plain_i32)
- File: `pre/line.bin` (9600799 rows)
- This query: `AVG(line)` → accumulate `int64_t sum_line += pre_line[rowid]` and divide by `cnt` at finalize time.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| `pre` | 9600799 | fact | `[]` | 100000 |

## Query Analysis
- Filter selectivity from `workload_analysis.json`: `stmt IS NOT NULL` = `1.0`, so the query effectively scans all `pre` rows.
- Grouping is on dictionary code pair `(stmt_code, rfile_code)`; estimated result groups in the workload model = `14`.
- `COUNT(DISTINCT adsh)` is cheapest on the stored `uint32_t` global code because `pre.adsh` and `sub.adsh` share the same dictionary domain.
- Output decoding happens only for final groups: `stmt` via `dicts/pre_stmt.*`, `rfile` via `dicts/pre_rfile.*`.

## Indexes
### pre_stmt_postings (value postings on `pre.stmt`)
- File set: `indexes/pre/pre_stmt_postings.values.bin`, `indexes/pre/pre_stmt_postings.offsets.bin`, `indexes/pre/pre_stmt_postings.rowids.bin`
- Actual layout from `build_value_postings_no_null<uint16_t>`: `values.bin` stores sorted unique `stmt` codes, `offsets.bin` stores one more `uint64_t` than the number of unique codes, and `rowids.bin` stores row ids grouped by code.
- Cardinalities on disk: `9` unique values, `10` offsets entries, `9600799` grouped row ids.
- Group membership formula: for postings position `g`, matching rows are `rowids[offsets[g] .. offsets[g + 1])` and the matching code is `values[g]`.
- Builder sort order is exact: rows are sorted by `(values[left] < values[right])`, then by `left < right` for ties.
- Empty-slot sentinel: none; this index has no hash table and no open-addressed empty marker.
- Usage here: optional fast path for `stmt IS NOT NULL` by iterating every postings group; because all rows are non-null, the main benefit is that rows arrive clustered by `stmt` code before the query also reads `pre/rfile.bin` and `pre/line.bin`.
