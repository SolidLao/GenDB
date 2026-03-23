# Q1 Guide

## Column Reference
### pre.stmt (statement_type, uint8_t, dict_shared(stmt_dict))
- File: `pre/stmt.bin` (9,600,799 rows)
- This query: `stmt IS NOT NULL` -> C++ runtime compares encoded `uint8_t` values only when grouping.
- Runtime dictionary loading pattern (no hardcoded code): load `gendb/dicts/stmt.dict` as `[uint32_t n][uint32_t len][bytes]...`, build `string -> uint32_t` map, then cast to `uint8_t` if needed.

### pre.rfile (render_source, uint8_t, dict_shared(rfile_dict))
- File: `pre/rfile.bin` (9,600,799 rows)
- This query: `GROUP BY rfile` -> hash/group key component is raw `uint8_t` code.
- Runtime dictionary loading pattern: load `gendb/dicts/rfile.dict`, decode only for final output rows.

### pre.adsh (filing_id_fk, uint32_t, dict_shared(adsh_dict))
- File: `pre/adsh.bin` (9,600,799 rows)
- This query: `COUNT(DISTINCT adsh)` -> distinct-set key is raw `uint32_t` code.

### pre.line (line_num, int16_t, plain)
- File: `pre/line.bin` (9,600,799 rows)
- This query: `AVG(line)` -> running sum/count over `int16_t` values.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| pre | 9,600,799 | fact scan + aggregate | none | 100000 |

## Query Analysis
- Access pattern: full scan of `pre` because predicate `stmt IS NOT NULL` is not selective for this encoding path.
- Grouping key: `(stmt, rfile)` both fixed-width dictionary codes.
- Aggregate state per group:
  - `cnt += 1`
  - `distinct_adsh` set over `uint32_t`
  - `line_sum += line`, `line_count += 1`, `avg_line_num = line_sum / line_count`
- Workload estimate in analysis: ~8 groups, so compute cost is scan-dominant.

## Indexes
### pre_stmt_hash (posting index on `pre.stmt`)
- File: `pre/indexes/pre_stmt_hash.bin`
- Builder call: `build_posting_u32(gendb / "pre", "stmt.bin", "pre_stmt_hash.bin")`.
- Verbatim key materialization: `p.emplace_back(key[i], i);`
- Verbatim ordering comparator:
  ```cpp
  if (a.first != b.first) return a.first < b.first;
  return a.second < b.second;
  ```
- Exact serialization struct layout:
  `struct Entry { uint32_t key; uint64_t start; uint32_t count; };`
- On-disk layout:
  - `uint64_t entry_count`
  - `uint64_t rowid_count`
  - repeated entries `(uint32_t key, uint64_t start, uint32_t count)`
  - trailing `uint32_t rowids[rowid_count]`
- Multi-value format: each `stmt` key maps to contiguous posting span `[start, start+count)`.
- Empty-slot sentinel: none.
- Q1 usage note: this index can locate specific statement codes, but `IS NOT NULL` does not identify a subset key list in this storage design.

### pre_stmt_zonemap (zone map on `pre.stmt`)
- File: `pre/indexes/pre_stmt_zonemap.bin`
- Builder instantiation:
  `build_zonemap_t<uint8_t>(gendb / "pre", "stmt.bin", "pre_stmt_zonemap.bin", block)`
- On-disk layout:
  - `uint64_t block_size`
  - `uint64_t blocks`
  - per block `(uint8_t min_stmt, uint8_t max_stmt)`
- Empty-slot sentinel: none (min/max metadata only).
- Q1 usage note: zonemap helps for bounded/equality code predicates; `IS NOT NULL` gives no block-pruning predicate on encoded `uint8_t`.

### Index Selection Summary
- Although both stmt indexes are structurally relevant to the query column set, neither materially prunes this predicate.
- Runtime still performs full-table aggregation scan over `stmt`, `rfile`, `adsh`, and `line`.
