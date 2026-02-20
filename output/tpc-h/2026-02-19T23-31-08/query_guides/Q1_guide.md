# Q1 Guide — Pricing Summary Report

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows × 4 bytes)
- lineitem is **sorted by l_shipdate ascending** — enables highly effective zone-map pruning
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
  = `l_shipdate <= epoch('1998-09-02')` = `l_shipdate <= 10471`
- Use `gendb::date_str_to_epoch_days("1998-09-02")` or constant `10471` at compile time

### l_returnflag (STRING, uint8_t, dictionary-encoded)
- File: `lineitem/l_returnflag.bin` (59,986,052 rows × 1 byte)
- Dictionary: `lineitem/l_returnflag_dict.txt` → load as `std::vector<std::string>`
  - Format: `0=A\n1=N\n2=R\n` → dict[0]="A", dict[1]="N", dict[2]="R"
- This query: used as GROUP BY key — compare codes directly, decode for output
- Output: `dict[code]` to get the character string

### l_linestatus (STRING, uint8_t, dictionary-encoded)
- File: `lineitem/l_linestatus.bin` (59,986,052 rows × 1 byte)
- Dictionary: `lineitem/l_linestatus_dict.txt` → dict[0]="F", dict[1]="O"
- This query: used as GROUP BY key — compare codes directly, decode for output

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows × 8 bytes)
- Stored as native IEEE-754 double; values match SQL directly (e.g., 17.0, 36.0)
- This query: SUM(l_quantity), AVG(l_quantity) — accumulate as double

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows × 8 bytes)
- This query: SUM, SUM*(1-disc), SUM*(1-disc)*(1+tax), AVG — accumulate as double

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows × 8 bytes)
- Range in data: [0.00, 0.10]
- This query: used in `(1 - l_discount)` factor and AVG(l_discount)

### l_tax (DECIMAL, double)
- File: `lineitem/l_tax.bin` (59,986,052 rows × 8 bytes)
- This query: used in `(1 + l_tax)` factor for sum_charge computation

## Table Stats
| Table    | Rows       | Role | Sort Order  | Block Size |
|----------|------------|------|-------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate ↑| 100,000    |

## Query Analysis
- **Join pattern**: Single table scan — no joins
- **Filter**: `l_shipdate <= 10471` — estimated **96.5% of rows qualify** (selectivity 0.965)
  - Zone-map prunes the last ~3.5% of blocks (those with all dates after 1998-09-02)
  - Since lineitem is sorted ascending by l_shipdate, the tail blocks get skipped
- **Aggregation**: 4 groups (l_returnflag × l_linestatus = {A,F}, {N,O}, {R,F} typically 4 combos)
  - Use a fixed 4-entry aggregation array indexed by `(rflag_code * 2 + lstatus_code)`
  - Very low cardinality → perfect for sorted aggregation or direct array lookup
- **Output**: 4 rows, ordered by l_returnflag ASC, l_linestatus ASC (sort agg groups)
- **Multi-aggregation**: fuse SUM(qty), SUM(price), SUM(disc_price), SUM(charge), AVG(qty), AVG(price), AVG(disc), COUNT in a single pass

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600]` then 600 × `[int32_t min_val, int32_t max_val, uint32_t row_count]` (12 bytes each)
- Total file: 4 + 600×12 = 7,204 bytes
- Skip logic: skip block b if `zone.min_val > 10471` (all dates in block are after threshold)
  - Since data is sorted, once min_val > 10471 all subsequent blocks also skip
- `row_offset` for block b = `b * 100000` (row index, not byte offset); last block may be partial (use `zone.row_count`)
- **This query**: skip trailing blocks where min_val > 10471. ~3.5% of 600 blocks ≈ 21 blocks skipped → binary search to find cutoff point
- Accessing column data: `reinterpret_cast<const double*>(col_mmap)[row_idx]` for doubles, `reinterpret_cast<const int32_t*>(col_mmap)[row_idx]` for dates, `col_mmap[row_idx]` for uint8_t

## Execution Strategy
1. Load zone map → binary search for last block where max_val <= 10471 → determine scan range
2. mmap all 7 columns (l_shipdate, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus)
3. Use 64 threads: each thread processes a morsel of rows, maintains local agg accumulators
4. Filter row: `l_shipdate[i] <= 10471` (no zone-map skipped rows in first N-21 blocks always qualify)
5. Aggregate into `agg[rflag_code * 2 + lstatus_code]` per thread
6. Merge thread-local results into global accumulators
7. Compute AVG = SUM/count, decode dict codes for output, sort 4 rows by (rflag, lstatus)
