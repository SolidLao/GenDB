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

### pre.stmt (dict_string, int16_t, dict_int16)
- File: `pre/stmt.bin` (9,600,799 rows × 2 bytes = 19.2 MB)
- Dict: `pre/stmt_dict.txt` — 8 entries. Load as `vector<string> stmt_dict`; `stmt_dict[code]` = string value.
- This query: `stmt IS NOT NULL` → `stmt_code != -1` (C2: do NOT hardcode; all rows in sorted data have valid stmt)
- **Table is sorted by stmt code** → each zone-map block has min=max=one code. Use `pre_stmt_zone_map.bin` to skip entirely if block has only the NULL sentinel (-1).
- GROUP BY output: decode via `stmt_dict[code]`

### pre.rfile (dict_string, int16_t, dict_int16)
- File: `pre/rfile.bin` (9,600,799 rows × 2 bytes = 19.2 MB)
- Dict: `pre/rfile_dict.txt` — 1 entry (all "H"). Load as `vector<string> rfile_dict`.
- This query: GROUP BY rfile → decode via `rfile_dict[code]`

### pre.adsh (dict_string, int32_t, dict_int32)
- File: `pre/adsh.bin` (9,600,799 rows × 4 bytes = 38.4 MB)
- Dict: `pre/adsh_dict.txt` — 86,135 entries.
- This query: COUNT(DISTINCT adsh) → count distinct int32_t codes; no string decode needed.

### pre.line (integer, int32_t)
- File: `pre/line.bin` (9,600,799 rows × 4 bytes = 38.4 MB)
- This query: AVG(line) → accumulate as `int64_t sum_line` + `int64_t count`; output `(double)sum_line / count`.

## Table Stats

| Table | Rows      | Role | Sort Order  | Block Size |
|-------|-----------|------|-------------|------------|
| pre   | 9,600,799 | fact | stmt (asc)  | 100,000    |

## Query Analysis
- Single-table scan of `pre` with filter `stmt IS NOT NULL`
- GROUP BY (stmt, rfile) → at most 8 × 1 = 8 distinct groups (observed 14 from workload analysis — some stmt+rfile combos)
- Output: ORDER BY cnt DESC (no LIMIT)
- Aggregation: low cardinality → use a direct array indexed by composite (stmt_code, rfile_code): `agg[stmt_code * rfile_dict.size() + rfile_code]`
- Since rfile has 1 distinct value (code 0), the array has at most 8 slots: `double avg_line[8]; int64_t sum_line[8], cnt[8]; int64_t adsh_distinct[8][...];`
- COUNT(DISTINCT adsh): use a per-group `unordered_set<int32_t>` or bitset over 86K possible adsh codes (86135 bits ≈ 11KB per group — feasible, 8 groups = 88KB total)

## Indexes

### pre_stmt_zone_map (zone_map on pre.stmt)
- File: `indexes/pre_stmt_zone_map.bin`
- Layout: `[uint32_t num_blocks=97] [ZoneBlock<int16_t> × 97]`
- `ZoneBlock<int16_t>`: `{ int16_t min_val; int16_t max_val; uint32_t row_count; }`
- Since pre is sorted by stmt and stmt IS NOT NULL is true for all (or nearly all) rows, this zone map primarily helps avoid the rare NULL-stmt blocks.
- Usage: iterate blocks; since stmt IS NOT NULL filter accepts all non-(-1) codes, only skip blocks where `max_val == -1` (all NULL). In practice all blocks pass → full scan.
- Row offset for block i = sum of row_count[0..i-1]

## Performance Notes
- Full scan of pre (9.6M rows): ~4 binary columns = ~115 MB total I/O on HDD
- Thread-local aggregation: each thread maintains local `cnt[8], sum_line[8], adsh_sets[8]` → merge after barrier (P17, P20)
- Parallelism: `#pragma omp parallel for` over row blocks, thread-local group arrays, merge at end
- For COUNT(DISTINCT adsh): use a thread-local `unordered_set<int32_t>` per (stmt,rfile) group; merge sets after parallel phase

## C++ Pattern

```cpp
// Load dicts (C2: NEVER hardcode codes)
vector<string> stmt_dict = load_dict(db + "/pre/stmt_dict.txt");
vector<string> rfile_dict = load_dict(db + "/pre/rfile_dict.txt");

// mmap columns
const int16_t* stmt_col  = mmap_col<int16_t>(db + "/pre/stmt.bin", N);
const int16_t* rfile_col = mmap_col<int16_t>(db + "/pre/rfile.bin", N);
const int32_t* adsh_col  = mmap_col<int32_t>(db + "/pre/adsh.bin", N);
const int32_t* line_col  = mmap_col<int32_t>(db + "/pre/line.bin", N);

// Thread-local aggregation
int S = stmt_dict.size(), R = rfile_dict.size();
// per-group: cnt, sum_line, adsh_set
...
#pragma omp parallel for
for (int64_t i = 0; i < N; ++i) {
    int16_t sc = stmt_col[i], rc = rfile_col[i];
    if (sc == -1) continue;  // stmt IS NOT NULL
    int key = sc * R + rc;
    local_cnt[key]++;
    local_sum_line[key] += line_col[i];
    local_adsh_set[key].insert(adsh_col[i]);
}
// merge thread-locals, decode, order by cnt desc
// output: stmt_dict[sc], rfile_dict[rc], cnt, num_filings, avg_line
```
