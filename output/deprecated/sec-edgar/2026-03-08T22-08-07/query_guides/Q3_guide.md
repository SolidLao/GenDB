
# Q3 Guide

## Column Reference
### num.adsh (filing_id, uint32_t, global_dict_u32)
- File: `num/adsh.bin` (39401761 rows)
- This query: `JOIN sub ON n.adsh = s.adsh` → compare shared global filing codes directly.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### num.uom (unit_of_measure, uint16_t, dict_u16)
- File: `num/uom.bin` (39401761 rows)
- This query: `uom = 'USD'` → `num_uom[rowid] == usd_code`, where `usd_code` is resolved at runtime from `dicts/num_uom.*`.
- Dictionary files: `dicts/num_uom.offsets.bin` + `dicts/num_uom.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### num.value (numeric_fact, double, plain_f64)
- File: `num/value.bin` (39401761 rows)
- This query: `value IS NOT NULL`, outer `SUM(value)`, and inner `SUM(value)` → `!std::isnan(num_value[rowid])` before adding to the relevant group accumulator.

### sub.adsh (filing_id, uint32_t, global_dict_u32)
- File: `sub/adsh.bin` (86135 rows)
- This query: join target for `n.adsh = s.adsh`; codes are compatible with `num.adsh` because both use `dicts/global_adsh.*`.
- Dictionary files: `dicts/global_*.offsets.bin` + `dicts/global_*.data.bin`; load codes at runtime and decode with `std::string_view(data + offsets[code], offsets[code + 1] - offsets[code])`.

### sub.name (company_name, uint32_t, dict_u32)
- File: `sub/name.bin` (86135 rows)
- This query: outer `GROUP BY s.name, s.cik` and final `SELECT s.name` → keep the `uint32_t` code in the aggregate key; decode only after top-100 selection.
- Dictionary files: `dicts/sub_name.offsets.bin` + `dicts/sub_name.data.bin`; runtime filters must resolve string literals by loading the dictionary, never by hardcoding category codes.

### sub.cik (company_id, int32_t, plain_i32)
- File: `sub/cik.bin` (86135 rows)
- This query: outer `GROUP BY s.name, s.cik` and inner `GROUP BY s2.cik` → compare and hash the stored `int32_t` directly.

### sub.fy (fiscal_year, int16_t, plain_i16)
- File: `sub/fy.bin` (86135 rows)
- This query: `s.fy = 2022` and `s2.fy = 2022` → `sub_fy[rowid] == 2022` for both outer and inner passes.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| `num` | 39401761 | fact | `[]` | 100000 |
| `sub` | 86135 | dimension | `[]` | 100000 |

## Query Analysis
- Filter derivation from `workload_analysis.json`: `num.uom = 'USD'` keeps about `39401761 × 0.8435 ≈ 33235385` rows; `sub.fy = 2022` keeps about `86135 × 0.3173 ≈ 27331` rows.
- Both the outer aggregation and the inner average-of-sums subquery operate on the same filtered join domain, so a shared filtered `sub` map and a shared `USD` row slice are high-value runtime structures.
- The outer grouping key is `(sub.name_code, sub.cik)`; the inner grouping key is `sub.cik` alone.
- `HAVING SUM(n.value) > AVG(sub_total)` means query code must finish the inner aggregation first (or in a coordinated two-phase pipeline) before applying the outer threshold.

## Indexes
### num_uom_postings (value postings on `num.uom`)
- File set: `indexes/num/num_uom_postings.values.bin`, `indexes/num/num_uom_postings.offsets.bin`, `indexes/num/num_uom_postings.rowids.bin`
- Actual layout from `build_value_postings_no_null<uint16_t>`: `values.bin` stores sorted unique `uom` codes, `offsets.bin` stores `uint64_t` range boundaries, and `rowids.bin` stores grouped `num` row ids.
- Cardinalities on disk: `201` unique values, `202` offsets entries, `39401761` grouped row ids.
- Empty-slot sentinel: none.
- Usage here: find `usd_code` in `values.bin`, then drive both outer and inner query phases from the matching `rowids` slice rather than scanning all `num` rows.

### sub_fy_postings (value postings on `sub.fy`)
- File set: `indexes/sub/sub_fy_postings.values.bin`, `indexes/sub/sub_fy_postings.offsets.bin`, `indexes/sub/sub_fy_postings.rowids.bin`
- Actual layout from `build_value_postings<int16_t>`: sorted non-null fiscal years, `offsets`, and grouped `rowids`.
- Cardinalities on disk: `13` unique fiscal years, `14` offsets entries, `81473` non-null row ids.
- Empty-slot sentinel: none in index storage; null fiscal years are omitted during construction.
- Usage here: locate the `2022` postings group once and build the admissible `sub` row/adsh set used by both the outer and inner subquery aggregates.

### sub_adsh_dense_lookup (dense lookup on `sub.adsh`)
- File: `indexes/sub/sub_adsh_dense_lookup.bin`
- Layout is a direct-addressed `std::vector<uint32_t>` indexed by global `adsh` code.
- Empty-slot sentinel: `std::numeric_limits<uint32_t>::max()`.
- Usage here: for each filtered `num` row, probe `lookup[num_adsh[rowid]]` to reach the `sub` row and then validate `fy == 2022` before adding `value` into either aggregate.
