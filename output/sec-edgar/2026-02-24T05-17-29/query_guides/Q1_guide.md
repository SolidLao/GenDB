# Q1 Guide

## Query
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
| Table | Rows      | Role | Sort Order                                | Block Size |
|-------|-----------|------|-------------------------------------------|------------|
| pre   | 9,600,799 | fact | adsh_code ASC, tag_code ASC, ver_code ASC | 100,000    |

## Column Reference

### pre.stmt (string_dict, int16_t — dict-encoded)
- File: `sf3.gendb/pre/stmt.bin` — 9,600,799 × 2 bytes = 19,201,598 bytes
- Dict: `sf3.gendb/pre/stmt_dict.txt` — 6 distinct values: BS, IS, CF, EQ, CI, UN
- Null sentinel: code = -1 (NULL stmt)
- This query: `WHERE stmt IS NOT NULL` → C++ `stmt_code != -1`
- This query: `GROUP BY stmt` — use stmt_code as group key component
- Selectivity: ~98% of rows pass (2% are NULL)
- Dict loading pattern:
  ```cpp
  std::vector<std::string> stmt_dict;
  // read sf3.gendb/pre/stmt_dict.txt line by line
  // stmt_dict[code] → string for output
  ```

### pre.rfile (string_dict, int16_t — dict-encoded)
- File: `sf3.gendb/pre/rfile.bin` — 9,600,799 × 2 bytes = 19,201,598 bytes
- Dict: `sf3.gendb/pre/rfile_dict.txt` — 2 distinct values: H, R
- This query: `GROUP BY rfile` — use rfile_code as group key component
- Dict loading pattern:
  ```cpp
  std::vector<std::string> rfile_dict;
  // read sf3.gendb/pre/rfile_dict.txt line by line
  // rfile_dict[code] → "H" or "R" for output
  ```

### pre.adsh (string_dict, int32_t — dict-encoded, shared)
- File: `sf3.gendb/pre/adsh.bin` — 9,600,799 × 4 bytes = 38,403,196 bytes
- Dict: `sf3.gendb/shared/adsh_dict.txt` — 86,135 distinct values
- This query: `COUNT(DISTINCT adsh)` — count distinct adsh_code values per (stmt, rfile) group

### pre.line (integer, int32_t — raw)
- File: `sf3.gendb/pre/line.bin` — 9,600,799 × 4 bytes = 38,403,196 bytes
- This query: `AVG(line)` — accumulate sum_line and count per group, output sum/count

## Query Analysis
- **Single table scan**: full scan of `pre` (9.6M rows), no joins
- **Filter**: `stmt IS NOT NULL` → skip rows where stmt_code == -1 (~2% excluded)
- **Group cardinality**: (stmt × rfile) → up to 6 × 2 = 12 groups (all fit trivially in any hash map)
- **Aggregations per group**:
  - `COUNT(*)` → increment counter
  - `COUNT(DISTINCT adsh)` → maintain a `std::unordered_set<int32_t>` or bitset per group
  - `AVG(line)` → accumulate sum_line (int64_t) + count, compute at output time
- **Output ordering**: `ORDER BY cnt DESC` — sort 12 rows, trivial
- **No indexes needed**: full sequential scan of pre columns (stmt, rfile, adsh, line)

## Implementation Notes

### Aggregation struct (12 groups max)
```cpp
struct Q1Group {
    int64_t  cnt      = 0;
    int64_t  sum_line = 0;
    std::unordered_set<int32_t> adsh_set;  // for COUNT(DISTINCT)
};
// Key: (stmt_code << 16) | (uint16_t)rfile_code
std::unordered_map<int32_t, Q1Group> agg;
```

### Main scan
```cpp
const int16_t* stmt_col  = ...; // mmap pre/stmt.bin
const int16_t* rfile_col = ...; // mmap pre/rfile.bin
const int32_t* adsh_col  = ...; // mmap pre/adsh.bin
const int32_t* line_col  = ...; // mmap pre/line.bin

for (int32_t i = 0; i < N; i++) {
    int16_t sc = stmt_col[i];
    if (sc == -1) continue;            // WHERE stmt IS NOT NULL
    int16_t rc = rfile_col[i];
    int32_t key = ((int32_t)sc << 16) | (uint16_t)rc;
    auto& g = agg[key];
    g.cnt++;
    g.sum_line += line_col[i];
    g.adsh_set.insert(adsh_col[i]);    // COUNT(DISTINCT adsh)
}
```

### Output
- Decode: `stmt_dict[stmt_code]`, `rfile_dict[rfile_code]`
- avg_line = (double)sum_line / (double)count
- Sort by cnt DESC
- C31: quote string columns if they contain commas (stmt values are short codes, safe)
