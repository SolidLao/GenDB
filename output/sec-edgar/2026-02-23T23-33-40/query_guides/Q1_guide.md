# Q1 Guide

```sql
SELECT stmt, rfile, COUNT(*) AS cnt,
       COUNT(DISTINCT adsh) AS num_filings,
       AVG(line) AS avg_line_num
FROM pre
WHERE stmt IS NOT NULL
GROUP BY stmt, rfile
ORDER BY cnt DESC;
```

## Table Stats
| Table | Rows      | Role | Sort Order | Block Size |
|-------|-----------|------|------------|------------|
| pre   | 9,600,799 | fact | (none)     | 65536      |

## Query Analysis
- **Single-table scan** over `pre` (9.6M rows); no joins
- Filter: `stmt IS NOT NULL` — excludes rows where stmt encodes empty/null string
- Group by (stmt, rfile): ~12 distinct groups (very low cardinality — direct array lookup viable)
- Aggregates: COUNT(*), COUNT(DISTINCT adsh), AVG(line)
- Output: 14 rows, sorted by cnt DESC (no LIMIT — full sort of 12 groups is trivial)
- No indexes needed — full sequential scan

## Column Reference

### stmt (dict_string, int16_t)
- File: `pre/stmt.bin` (9,600,799 rows × 2 bytes = ~18MB)
- Dict: `pre/stmt_dict.txt` — line N (0-indexed) is the string for code N
- This query: `stmt IS NOT NULL` → at runtime, find null code:
  ```cpp
  // Load dict at runtime — NEVER hardcode codes
  std::vector<std::string> stmt_dict; // load pre/stmt_dict.txt line-by-line
  int16_t null_stmt_code = -1;
  for (int16_t c = 0; c < (int16_t)stmt_dict.size(); ++c)
      if (stmt_dict[c].empty()) { null_stmt_code = c; break; }
  // Filter: skip row if stmt_code == null_stmt_code
  ```
- Also used as GROUP BY key and output column (decode via `stmt_dict[code]`)
- Known distinct values (from workload analysis): BS, CF, IS, EQ, CI, UN (~6 values + possible empty)

### rfile (dict_string, int16_t)
- File: `pre/rfile.bin` (9,600,799 rows × 2 bytes = ~18MB)
- Dict: `pre/rfile_dict.txt` — load at runtime
- This query: GROUP BY rfile — used as second group key and output column
- Known approx distinct: 2 (dominant value: "H")
- Decode for output: `rfile_dict[rfile_code].c_str()`

### adsh (dict_string, int32_t)
- File: `pre/adsh.bin` (9,600,799 rows × 4 bytes = ~37MB)
- Dict: `pre/adsh_dict.txt` — global dict shared with num/adsh_dict.txt and sub/adsh_dict.txt
- This query: `COUNT(DISTINCT adsh)` — use a per-group hash set or bitset over adsh codes

### line (integer, int32_t)
- File: `pre/line.bin` (9,600,799 rows × 4 bytes = ~37MB)
- This query: `AVG(line)` → accumulate `int64_t line_sum` and `int64_t line_count` per group;
  output `(double)line_sum / line_count`

## Indexes
No pre-built indexes are used for Q1. This is a full sequential scan.

## Aggregation Strategy
- ~12 distinct (stmt, rfile) groups → use a direct flat array or small open-addressing map
- Composite group key: `int32_t key = (int32_t)stmt_code << 16 | (uint16_t)rfile_code`
  (valid since each component fits in int16_t)
- Per-group state:
  ```cpp
  struct Q1Group {
      int64_t  cnt;          // COUNT(*)
      int64_t  line_sum;     // for AVG(line)
      int64_t  line_count;   // for AVG(line)
      // COUNT(DISTINCT adsh): use std::unordered_set<int32_t> or thread-local bitset
  };
  ```
- Thread-local aggregation then merge (P17/P20): with only ~12 groups, merging is trivial

## Output
- Decode stmt and rfile codes to strings via their respective dicts
- Double-quote all string output columns (C31): `printf("\"%s\"", stmt_dict[code].c_str())`
- Sort by cnt DESC (no LIMIT — all rows returned)
- date_encoding note: `pre` has no date columns — no epoch conversion needed
