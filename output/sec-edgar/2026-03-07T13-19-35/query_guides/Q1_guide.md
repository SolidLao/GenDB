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

## Column Reference

### stmt_code (dict_code, int8_t)
- File: `pre/stmt_code.bin` (9,600,799 rows × 1 byte)
- This query: `WHERE stmt IS NOT NULL` → exclude rows where `stmt_code` equals the code
  assigned to the empty string `""`, if any. Load at startup:
  ```cpp
  // Load indexes/stmt_codes.bin → map<string,int8_t>
  // uint8_t N; then N × { int8_t code, uint8_t slen, char[slen] }
  int8_t null_stmt_code = -99; // sentinel: "no null code"
  auto stmt_map = load_dict("indexes/stmt_codes.bin");
  auto it = stmt_map.find("");
  if (it != stmt_map.end()) null_stmt_code = it->second;
  // Filter: stmt_code != null_stmt_code (or null_stmt_code == -99 → accept all)
  ```
- GROUP BY stmt → decode: `stmt_map_inv[stmt_code]` (build inverse at startup)
- Output: human-readable stmt string from the inverse map

### rfile_code (dict_code, int8_t)
- File: `pre/rfile_code.bin` (9,600,799 rows × 1 byte)
- This query: GROUP BY rfile → decode using `indexes/rfile_codes.bin`
  ```cpp
  // uint8_t N; then N × { int8_t code, uint8_t slen, char[slen] }
  // Load at startup: build code→string array rfile_strings[256]
  ```
- Note: 100% of rows have rfile='H' in this dataset → only 1 group per stmt

### adsh_code (dict_code, int32_t)
- File: `pre/adsh_code.bin` (9,600,799 rows × 4 bytes)
- This query: `COUNT(DISTINCT adsh)` → collect unique adsh_code values per (stmt_code,
  rfile_code) group. Since adsh_code ∈ [0, 86134], use a `vector<bool>` or bitset of
  size 86135 per group, or accumulate into `unordered_set<int32_t>`.
- No string decode needed for counting; adsh_code is already a unique identifier.

### line (integer, int8_t)
- File: `pre/line.bin` (9,600,799 rows × 1 byte)
- This query: `AVG(line)` → accumulate `sum_line` (int64_t) and `count` per group;
  output `(double)sum_line / count`.
- Range: 1–115 (fits int8_t; cast to int before accumulation to avoid overflow).

## Table Stats

| Table | Rows      | Role  | Sort Order              | Block Size |
|-------|-----------|-------|-------------------------|------------|
| pre   | 9,600,799 | scan  | (adsh_code, tagver_code)| 100,000    |

## Query Analysis
- **Single-table scan**: no joins required.
- **Filter**: `stmt IS NOT NULL` — 97% selectivity (pass-through for ~9.3M rows).
- **Aggregation**: GROUP BY (stmt_code, rfile_code). At most 7×2 = 14 groups
  (stmt has 7 distinct values; rfile has 1 distinct value 'H'). Expected output: 14 rows.
- **COUNT(DISTINCT adsh)**: per group, collect distinct adsh_code values. With only
  14 groups and 9.3M passing rows, use a per-group `unordered_set<int32_t>`.
- **AVG(line)**: sum + count per group, divide at output time.
- **ORDER BY cnt DESC**: sort 14 result rows; trivial.
- **Execution pattern**: single forward scan of pre columns simultaneously.

## Indexes

### stmt_codes (dict_file on stmt)
- File: `indexes/stmt_codes.bin`
- Layout:
  ```
  uint8_t N_stmt
  N_stmt × { int8_t code, uint8_t slen, char[slen] }
  ```
- Usage: load at startup to build `string→code` and `code→string` maps.
  The empty-string code (if any) identifies IS-NULL rows to exclude.

### rfile_codes (dict_file on rfile)
- File: `indexes/rfile_codes.bin`
- Layout:
  ```
  uint8_t N_rfile
  N_rfile × { int8_t code, uint8_t slen, char[slen] }
  ```
- Usage: load at startup to decode rfile_code → string for GROUP BY output.

## Loading Pattern for Dict Files
```cpp
// Applies to uom_codes.bin, stmt_codes.bin, rfile_codes.bin, form_codes.bin
unordered_map<string,int8_t> load_dict(const string& path) {
    unordered_map<string,int8_t> m;
    FILE* f = fopen(path.c_str(), "rb");
    uint8_t n; fread(&n, 1, 1, f);
    for (int i = 0; i < (int)n; ++i) {
        int8_t code;  fread(&code, 1, 1, f);
        uint8_t slen; fread(&slen, 1, 1, f);
        char buf[32]{}; fread(buf, 1, slen, f);
        m[string(buf, slen)] = code;
    }
    fclose(f); return m;
}
```
