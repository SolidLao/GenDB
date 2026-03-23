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

### stmt (category, uint8_t, dict_stmt)
- File: `pre/stmt.bin` (9,600,799 rows of uint8_t)
- Dictionary: `dictionaries/stmt.dict` (~8 entries)
- This query: `WHERE stmt IS NOT NULL` — filter out null/empty stmt codes
- Also: `GROUP BY stmt` — group key component
- Dict format: uint32_t count, uint32_t[count+1] offsets, char[] string_data
- Load dictionary to decode stmt codes to strings for output

### rfile (category, uint8_t, dict_rfile)
- File: `pre/rfile.bin` (9,600,799 rows of uint8_t)
- Dictionary: `dictionaries/rfile.dict` (~2 entries)
- This query: `GROUP BY rfile` — group key component
- Load dictionary to decode rfile codes to strings for output

### adsh (fk_code, uint32_t, dict_shared_adsh)
- File: `pre/adsh.bin` (9,600,799 rows of uint32_t)
- This query: `COUNT(DISTINCT adsh)` — count distinct adsh codes per group
- No need to decode dictionary — just count distinct uint32_t codes

### line (ordinal, int32_t, raw)
- File: `pre/line.bin` (9,600,799 rows of int32_t)
- This query: `AVG(line)` — compute SUM(line)/COUNT(line) per group

## Table Stats

| Table | Rows      | Role | Sort Order | Block Size |
|-------|-----------|------|------------|------------|
| pre   | 9,600,799 | fact | none       | 100,000    |

## Query Analysis

- **Pattern**: Single-table scan with filter, grouped aggregation, ordered output
- **Filter**: `stmt IS NOT NULL` — selectivity ~0.9999 (almost all rows pass). During ingestion, empty stmt fields get encoded as dictionary code 0 (the empty string ""). Filter by checking if the stmt string is empty after dict lookup, or identify the code for "" at load time.
- **Group by**: (stmt, rfile) — both are small uint8_t codes. Max groups = 8 * 2 = 16 (estimated 16 groups)
- **Aggregations per group**:
  - `COUNT(*)`: simple counter
  - `COUNT(DISTINCT adsh)`: need a set/bitset per group tracking distinct uint32_t adsh codes. With ~86K max adsh codes, a bitset of 86K bits (~11KB) per group is feasible (16 groups * 11KB = 176KB total)
  - `AVG(line)`: track SUM(line) as int64_t and COUNT(line) as uint32_t, divide at end
- **Order**: by cnt DESC, no LIMIT — output all groups (~16 rows)
- **Optimization**: group key = (stmt << 8 | rfile) fits in uint16_t, use flat array of 256 aggregation structs indexed by this composite key for O(1) lookup (no hash table needed)

## Indexes

No indexes needed — this is a single full-table scan of pre with trivial grouping.

## Execution Strategy

1. Read `pre/row_count.bin` to get exact row count
2. Load `dictionaries/stmt.dict` and `dictionaries/rfile.dict`
3. Identify the dictionary code for the empty string "" in stmt dict (this is the NULL sentinel)
4. Mmap `pre/stmt.bin`, `pre/rfile.bin`, `pre/adsh.bin`, `pre/line.bin`
5. Scan all rows:
   - Skip rows where stmt == null_code (empty string code)
   - Compute flat index = (stmt_code << 8) | rfile_code
   - Increment count, add line to sum, insert adsh into distinct-tracking structure
6. Collect non-empty groups, decode stmt/rfile codes via dictionary, compute AVG
7. Sort by cnt DESC
8. Output all rows
