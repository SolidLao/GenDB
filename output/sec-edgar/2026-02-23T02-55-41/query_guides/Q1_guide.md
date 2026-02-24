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

## Column Reference

### stmt (dict_local, int32_t)
- File: `pre/stmt.bin` (9,600,799 rows)
- Dict: `pre/stmt_dict.txt` (8 distinct values: BS, CF, IS, EQ, CI, UN, …)
- This query: `stmt IS NOT NULL` → load dict at runtime, find code for `""` (empty/null entry) if present; skip rows where stmt_code == null_code. In practice stmt is nearly always non-null (workload: 99.9% non-null), so skip check if null_code not found in dict.
- Group-by key (part 1 of 2)

### rfile (dict_local, int32_t)
- File: `pre/rfile.bin` (9,600,799 rows)
- Dict: `pre/rfile_dict.txt` (2 distinct: H, X)
- Group-by key (part 2 of 2)

### adsh (dict_shared_adsh, int32_t)
- File: `pre/adsh.bin` (9,600,799 rows)
- Dict: `pre/adsh_dict.txt` (shared; same codes as sub/adsh.bin)
- This query: `COUNT(DISTINCT adsh)` — count distinct adsh codes per (stmt, rfile) group

### line (int32_t)
- File: `pre/line.bin` (9,600,799 rows)
- This query: `AVG(line)` — accumulate sum(line) and count per group

## Table Stats
| Table | Rows       | Role | Sort Order | Block Size |
|-------|------------|------|------------|------------|
| pre   | 9,600,799  | fact | adsh       | 100,000    |

## Query Analysis
- Single table, full scan of pre
- Group by (stmt, rfile): only 8 × 2 = 16 possible groups → use direct array aggregation indexed by (stmt_code * num_rfile_codes + rfile_code)
- COUNT(DISTINCT adsh): use a bitset or small hash set per group (only 16 groups, each can hold a `std::unordered_set<int32_t>`)
- AVG(line): accumulate `sum_line` and `count` per group, divide at end
- Selectivity of `stmt IS NOT NULL`: ~99.9% → almost no filtering needed
- Output: 14 rows, ORDER BY cnt DESC → trivial sort on 16 elements

## Indexes
No indexes used — full sequential scan of pre.stmt, pre.rfile, pre.adsh, pre.line.

## Implementation Notes
- Load stmt_dict.txt: find if any entry is empty string → that code is the null sentinel
- 16-element arrays for aggregation (indexed by combined group code):
  ```cpp
  // After loading dicts:
  int n_stmt = stmt_dict.size();  // ≤ 8
  int n_rfile = rfile_dict.size(); // ≤ 2
  // Per group accumulators:
  std::vector<int64_t> cnt(n_stmt * n_rfile, 0);
  std::vector<int64_t> sum_line(n_stmt * n_rfile, 0);
  std::vector<std::unordered_set<int32_t>> distinct_adsh(n_stmt * n_rfile);
  // For each row i:
  int grp = stmt[i] * n_rfile + rfile[i];
  cnt[grp]++;
  sum_line[grp] += line[i];
  distinct_adsh[grp].insert(adsh[i]);
  ```
- mmap all 4 columns: stmt.bin, rfile.bin, adsh.bin, line.bin
- MADV_SEQUENTIAL on all columns
