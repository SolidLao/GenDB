# Q1 Guide — Pricing Summary Report

## Query
```sql
SELECT l_returnflag, l_linestatus,
       SUM(l_quantity), SUM(l_extendedprice),
       SUM(l_extendedprice*(1-l_discount)), SUM(l_extendedprice*(1-l_discount)*(1+l_tax)),
       AVG(l_quantity), AVG(l_extendedprice), AVG(l_discount), COUNT(*)
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows × 4 bytes)
- **lineitem is physically sorted by l_shipdate ascending**
- Encoding: raw int32_t; value = calendar days since 1970-01-01
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY` = `l_shipdate <= DATE '1998-09-02'`
  - Epoch constant: `1998-09-02` → `raw <= 10471`
  - Derivation: YEAR_START[1998-1970=28]=10227; months Jan-Aug = 31+28+31+30+31+30+31+31 = 243; day offset = 2-1 = 1; total = 10227+243+1 = **10471**

### l_returnflag (STRING, int8_t, dictionary-encoded)
- File: `lineitem/l_returnflag.bin` (59,986,052 rows × 1 byte)
- Dictionary: `lineitem/l_returnflag_dict.txt` — load as `std::vector<std::string>` (one entry per line = one code)
- Actual dict contents (code → value): `0="R"`, `1="N"`, `2="A"`
- Filtering: load dict, match entry to target string, compare stored int8_t code
- Output: `dict[code]` → std::string

### l_linestatus (STRING, int8_t, dictionary-encoded)
- File: `lineitem/l_linestatus.bin` (59,986,052 rows × 1 byte)
- Dictionary: `lineitem/l_linestatus_dict.txt`
- Actual dict contents (code → value): `0="F"`, `1="O"`
- Same load/filter/decode pattern as l_returnflag

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- Stored as native double — values match SQL directly. No scaling needed.
- Range: 1.0 – 50.0. Used in SUM and AVG aggregations.

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- Stored as native double — values match SQL directly. No scaling needed.
- Used in SUM(l_extendedprice), SUM(l_extendedprice*(1-l_discount)), etc.

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Stored as native double — values match SQL directly (range 0.00–0.10). No scaling needed.
- Used in discount expressions and AVG(l_discount).

### l_tax (DECIMAL, double)
- File: `lineitem/l_tax.bin` (59,986,052 rows × 8 bytes)
- Stored as native double — values match SQL directly. No scaling needed.
- Used in SUM(l_extendedprice*(1-l_discount)*(1+l_tax)).

## Table Stats
| Table    | Rows       | Role | Sort Order  | Block Size |
|----------|------------|------|-------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate↑ | 100,000    |

## Query Analysis
- **Access pattern**: full scan of lineitem with single date predicate; 98.8% selectivity (59,316,666 rows pass)
- **Filter**: `l_shipdate <= 10471` — nearly all rows qualify; zone map can only skip the last ~7 blocks (those with min > 10471)
- **Aggregation**: `GROUP BY l_returnflag, l_linestatus` → only 4 distinct groups (R/F, R/O, N/F, N/O, A/F — TPC-H has ≤6 combinations). Use a fixed 6-slot array indexed by `(rf_code, ls_code)` — direct array lookup, zero hash overhead.
- **Output**: 4–6 rows, ORDER BY l_returnflag, l_linestatus — trivial sort on result set.
- **Parallelism**: divide 600 blocks across 32 threads (18–19 blocks/thread); local partial aggregates per thread, single merge at end.
- **Optimization**: fuse scan+filter+aggregate into one pass; load only 7 columns (skip l_partkey, l_suppkey, l_orderkey); SIMD-friendly inner loop on double columns.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600]` then per block: `[int32_t min, int32_t max, uint32_t block_size]` (12 bytes each)
- Total file size: 4 + 600×12 = 7,204 bytes
- Skip logic: skip block if `block_min > 10471` (block entirely after cutoff)
- Since lineitem is sorted by l_shipdate ascending, only the last few blocks will be skipped for Q1 (cutoff 10471, max shipdate ~10561). Approximately 7 blocks out of 600 skipped (1.2%). **Zone map provides minor benefit for Q1 — the real payoff is for Q6.**
- `row_offset` = block index × block_size (first row of block). Access as `col[row_offset + i]`.
- mmap the zone map file; iterate all 600 block entries; skip those where `min > predicate_bound`.

### No hash indexes needed for Q1 (single-table scan).
