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

### stmt (dict_int8, int8_t — pre-seeded codes)
- File: `pre/stmt.bin` (9,600,799 rows × 1 byte)
- Dict: `shared/stmt.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- Pre-seeded insertion order (ingest.cpp lines 495–499):
  - BS=0, IS=1, CF=2, EQ=3, CI=4
- This query: `WHERE stmt IS NOT NULL` → `stmt_code != -1`
- GROUP BY stmt → group key is `int8_t stmt_code`
- Output decode: load `shared/stmt.dict` at runtime; `strs[stmt_code]` gives string

### rfile (dict_int8, int8_t)
- File: `pre/rfile.bin` (9,600,799 rows × 1 byte)
- Dict: `shared/rfile.dict` — format `[n:uint32][len:uint16, bytes...]*n`
- No filter applied; GROUP BY rfile → group key is `int8_t rfile_code`
- Output decode: load `shared/rfile.dict` at runtime; `strs[rfile_code]` gives string

### adsh (dict_int32, int32_t — FK into sub)
- File: `pre/adsh.bin` (9,600,799 rows × 4 bytes)
- Dict: `sub/adsh.bin` — N×char[20] sorted array; code = row index
- This query: `COUNT(DISTINCT adsh)` only — no join, no filter; used as grouping value
  for distinct counting per (stmt, rfile) group

### line (int32_t, raw)
- File: `pre/line.bin` (9,600,799 rows × 4 bytes)
- This query: `AVG(line)` — accumulate `sum_line` and `count_line` per group;
  output `(double)sum_line / count_line`

## Table Stats

| Table | Rows      | Role | Sort Order  | Block Size |
|-------|-----------|------|-------------|------------|
| pre   | 9,600,799 | fact | (stmt, adsh)| 100,000    |

## Query Analysis

- Single-table scan of `pre`
- Filter: `stmt != -1` (skip null stmt rows — ~1% of rows per workload_analysis)
- GROUP BY (stmt_code, rfile_code) — at most ~8 stmt values × small rfile cardinality ≈ ~10–50 groups
- Aggregates per group: COUNT(*), COUNT(DISTINCT adsh) [use HyperLogLog or a small hash set],
  SUM(line) + COUNT for AVG
- No join required
- ORDER BY cnt DESC — sort ≤ 50 groups, trivial

## Indexes

### pre_zonemaps (zone_map on stmt, adsh)
- File: `indexes/pre_zonemaps.bin`
- Layout (from build_indexes.cpp lines 136–170):
  ```
  [n_blocks : int32]
  per block (100,000 rows):
    stmt_min  : int8_t
    stmt_max  : int8_t
    adsh_min  : int32_t
    adsh_max  : int32_t
  ```
  Total: 4 + n_blocks × 10 bytes; n_blocks = ceil(9,600,799 / 100,000) = 97 blocks
- Usage: pre is sorted by (stmt, adsh). All NULL-stmt rows (code -1) cluster in the first
  bucket (bucket 0 of counting sort). Zone maps allow skipping the null-stmt prefix blocks
  quickly: if a block's `stmt_max == -1`, all rows are NULL — skip entirely.
  Once `stmt_min > -1`, all remaining blocks pass the `stmt IS NOT NULL` filter.
- For Q1 this means: scan blocks with `stmt_max != -1` only (i.e., skip null-stmt blocks).

## Execution Strategy

1. Load `shared/stmt.dict` and `shared/rfile.dict` once.
2. Read `pre_zonemaps.bin`; skip blocks where `stmt_max == -1`.
3. Scan qualifying blocks of `pre/stmt.bin` and `pre/rfile.bin` together.
   For each row: if `stmt == -1` skip; otherwise accumulate into group map keyed by
   `(int8_t stmt_code, int8_t rfile_code)`.
4. Also read `pre/adsh.bin` and `pre/line.bin` in the same pass for distinct-adsh counting
   and line sum.
5. After full scan, decode group keys using dicts and emit rows sorted by cnt DESC.
