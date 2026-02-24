# Q1 Guide

## SQL
```sql
SELECT stmt, rfile, COUNT(*) AS cnt,
       COUNT(DISTINCT adsh) AS num_filings,
       AVG(line) AS avg_line_num
FROM pre
WHERE stmt IS NOT NULL
GROUP BY stmt, rfile
ORDER BY cnt DESC;
```
Expected output: ~14 rows (all stmt×rfile combinations).

## Column Reference

### pre/stmt.bin (dict_string, int16_t)
- File: `pre/stmt.bin` (9,600,799 rows, 18.3 MB)
- Dict: `pre/stmt_dict.txt` — load at runtime. NEVER hardcode codes (C2).
  - Known codes at ingest time: BS=0, IS=1, CF=2, EQ=3, CI=4, UN=5, "empty"=6, CP=7, SI=8
  - At runtime: `std::vector<std::string> stmt_dict = load_dict(db + "/pre/stmt_dict.txt");`
- Filter: `WHERE stmt IS NOT NULL` → exclude rows where stmt code maps to empty string `""`
  - Find null_code: `int16_t null_code = find_code(stmt_dict, ""); // -1 if no empty entry`
  - Filter: `if (stmt_code == null_code) skip;`  (or skip if stmt string is empty)
- GROUP BY: stmt is a GROUP BY key. Use stmt code as part of hash key.

### pre/rfile.bin (dict_string, int16_t)
- File: `pre/rfile.bin` (9,600,799 rows, 18.3 MB)
- Dict: `pre/rfile_dict.txt` — load at runtime.
  - Known values: H=0, X=1 (or R=1 depending on data version)
- GROUP BY: rfile is a GROUP BY key. Use rfile code as part of hash key.

### pre/adsh.bin (dict_string, int32_t, global dict)
- File: `pre/adsh.bin` (9,600,799 rows, 36.6 MB)
- Dict: `adsh_global_dict.txt` (86,135 entries)
- This query: `COUNT(DISTINCT adsh)` — track distinct adsh codes per (stmt, rfile) group.
  - Only 86,135 distinct values → use a small bitset or hash set per group.
  - Since groups are small (~14 total), use per-group `std::unordered_set<int32_t>`.

### pre/line.bin (integer, int32_t)
- File: `pre/line.bin` (9,600,799 rows, 36.6 MB)
- This query: `AVG(line)` → accumulate sum_line (int64_t) and count per group.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| pre   | 9,600,799 | sole table | none | 65,536 |

## Query Analysis
- Single table scan of pre (9.6M rows)
- Filter: stmt IS NOT NULL (exclude rows with empty stmt string)
- GROUP BY (stmt, rfile): extremely low cardinality (~14 groups max — 9 stmt values × 2 rfile values)
- Aggregates: COUNT(*), COUNT(DISTINCT adsh), AVG(line)
- No joins, no indexes needed
- ORDER BY cnt DESC (just sort 14 groups at the end — trivial)

## Implementation Strategy
1. Load `pre/stmt_dict.txt` and `pre/rfile_dict.txt`
2. Find code for empty string `""` → that's the NULL sentinel to exclude
3. mmap all 4 columns: stmt, rfile, adsh, line
4. Use a fixed-size aggregation array indexed by `(stmt_code * max_rfile + rfile_code)` for O(1) group lookup
   - Or: use a flat array of 16 slots (covers all possible stmt×rfile combos)
5. Per slot: `int64_t count`, `int64_t sum_line`, `int64_t count_for_avg`, `std::unordered_set<int32_t> distinct_adsh`
6. Parallel scan with thread-local aggregation arrays (P20), merge at end
7. Compute AVG = sum_line / count_for_avg
8. Sort 14 groups by cnt DESC, print

## No Indexes Used
- No indexes required for Q1 (single table, low-cardinality group-by)
- Direct columnar scan is optimal

## Output Format
```
stmt,rfile,cnt,num_filings,avg_line_num
BS,H,3150624,45872,10.23
...
```
Print as CSV. avg_line_num: use printf("%.6f", avg) or similar.
