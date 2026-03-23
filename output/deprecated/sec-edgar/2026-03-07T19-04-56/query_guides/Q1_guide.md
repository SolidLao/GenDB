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

### stmt_code (dict_code, uint8_t, dictionary-encoded via stmt_dict)
- File: `pre/stmt_code.bin` (9,600,799 rows, 1 byte each)
- Dictionary: `pre/stmt_dict_offsets.bin` + `pre/stmt_dict_data.bin` (9 entries)
- This query: `WHERE stmt IS NOT NULL` → filter out rows where the original stmt field was empty
- Also: `GROUP BY stmt` → group by stmt_code, decode to string for output
- **Loading the dictionary:**
```cpp
// Read offsets: (N+1) uint64_t values where N = number of dictionary entries
auto offsets = readColumn<uint64_t>("pre/stmt_dict_offsets.bin"); // size = N+1
// Read data blob
auto data = readFile("pre/stmt_dict_data.bin");
// Decode entry i: string_view(data + offsets[i], offsets[i+1] - offsets[i])
// The code that encoded empty strings as a dict entry — check which code maps to ""
// to implement IS NOT NULL filter
```
- **IS NOT NULL handling:** During ingestion, empty stmt fields were encoded as a dictionary entry. Load the dictionary, find which code maps to the empty string `""`, and exclude rows with that code.

### rfile_code (dict_code, uint8_t, dictionary-encoded via rfile_dict)
- File: `pre/rfile_code.bin` (9,600,799 rows, 1 byte each)
- Dictionary: `pre/rfile_dict_offsets.bin` + `pre/rfile_dict_data.bin` (2 entries)
- This query: `GROUP BY rfile` → group by rfile_code, decode to string for output

### sub_fk (foreign_key_to_sub, uint32_t, dense FK array)
- File: `pre/sub_fk.bin` (9,600,799 rows, 4 bytes each)
- This query: `COUNT(DISTINCT adsh)` → count distinct sub_fk values per group
- sub_fk is a direct index into the sub table (0-based row index), so distinct sub_fk = distinct adsh

### line (ordinal, int16_t, fixed encoding)
- File: `pre/line.bin` (9,600,799 rows, 2 bytes each)
- This query: `AVG(line)` → accumulate sum and count per group, divide for average

## Table Stats

| Table | Rows      | Role | Sort Order            | Block Size |
|-------|-----------|------|-----------------------|------------|
| pre   | 9,600,799 | fact | (stmt_code, sub_fk)   | 100,000    |

## Query Analysis

- **No joins** — single-table scan on pre
- **Filter:** `stmt IS NOT NULL` — selectivity ~0.9999 (almost all rows pass). Load stmt_dict, find the code for empty string, skip those rows.
- **Grouping:** `(stmt_code, rfile_code)` — at most 9 * 2 = 18 groups. Use a small flat array or map keyed by `(uint8_t, uint8_t)`.
- **Aggregation per group:**
  - `COUNT(*)` — simple counter
  - `COUNT(DISTINCT adsh)` — count distinct sub_fk values. With ~18 groups, can use a `std::unordered_set<uint32_t>` per group or a bitset (86,135 possible sub_fk values → ~10.5 KB bitset per group, 18 groups → ~189 KB total).
  - `AVG(line)` — track sum_line (int64_t) and count, divide at end
- **Output:** Decode stmt_code and rfile_code back to strings via their dictionaries. ~14 result rows.
- **Sort:** ORDER BY cnt DESC — sort ~18 result tuples, trivial.

### Optimization: Exploit Sort Order
The pre table is sorted by `(stmt_code, sub_fk)`. This means all rows with the same stmt_code are contiguous. You could use `stmt_offsets` to jump directly to each stmt_code range, but since the filter passes ~99.99% of rows, a full scan is simpler and equally fast.

## Indexes

### stmt_offsets (offset_table on stmt_code)
- File: `pre/stmt_offsets.bin`
- Layout: `uint32_t num_entries`, then `num_entries` pairs of `(uint64_t start, uint64_t end)`
  - Entry at index `c` gives the row range `[start, end)` for `stmt_code == c`
  - If a code has no rows: start=0, end=0
- Usage: Can iterate over each stmt_code's range separately. Useful if you want to skip the empty-string code entirely without checking every row.
- **Loading pattern:**
```cpp
std::ifstream f("pre/stmt_offsets.bin", std::ios::binary);
uint32_t numEntries; f.read((char*)&numEntries, 4);
struct Range { uint64_t start, end; };
std::vector<Range> ranges(numEntries);
f.read((char*)ranges.data(), numEntries * sizeof(Range));
// For each code c: rows [ranges[c].start, ranges[c].end) have stmt_code == c
```
