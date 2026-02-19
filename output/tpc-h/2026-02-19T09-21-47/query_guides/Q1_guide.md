# Q1 Guide — Pricing Summary Report

## Column Reference

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 rows, **sorted ascending**)
- Encoding: days since epoch. `parse_date("1970-01-01")` = 0.
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
  - `'1998-12-01' - 90 days` = `1998-09-02`
  - Epoch: `YEAR_DAYS[28] + MONTH_STARTS[0][8] + 1` = `10227 + 243 + 1` = **10471**
  - C++ filter: `raw <= 10471`
  - Since data is sorted ascending, once a block's `min_val > 10471` all subsequent blocks fail.

### l_returnflag (STRING, int8_t, dictionary-encoded)
- File: `lineitem/l_returnflag.bin` (59,986,052 rows, 1 byte/row)
- Dictionary: `lineitem/l_returnflag_dict.txt` → `A=0, N=1, R=2` (sorted alphabetically)
- This query: used in GROUP BY. No filter predicate on this column.
- Load dict at runtime as `std::vector<std::string>`, decode code to string via `dict[code]`.

### l_linestatus (STRING, int8_t, dictionary-encoded)
- File: `lineitem/l_linestatus.bin` (59,986,052 rows, 1 byte/row)
- Dictionary: `lineitem/l_linestatus_dict.txt` → `F=0, O=1` (sorted alphabetically)
- This query: used in GROUP BY. No filter predicate.

### l_quantity (DECIMAL, double)
- File: `lineitem/l_quantity.bin` (59,986,052 rows, 8 bytes/row)
- Stored as native double. Values match SQL directly. No scaling needed.
- This query: `SUM(l_quantity)`, `AVG(l_quantity)` — accumulate as double.

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 rows, 8 bytes/row)
- Stored as native double. Values match SQL directly.
- This query: `SUM(l_extendedprice)`, `SUM(l_extendedprice * (1 - l_discount))`, etc.

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59,986,052 rows, 8 bytes/row)
- Stored as native double. Values in `[0.00, 0.10]`.
- This query: `SUM(l_extendedprice * (1 - l_discount))`, `AVG(l_discount)`.

### l_tax (DECIMAL, double)
- File: `lineitem/l_tax.bin` (59,986,052 rows, 8 bytes/row)
- Stored as native double. Values in `[0.00, 0.08]`.
- This query: `SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))`.

---

## Table Stats
| Table    | Rows       | Role | Sort Order   | Block Size |
|----------|------------|------|--------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate ↑ | 100,000    |

---

## Query Analysis
- **Access pattern**: Full scan of lineitem with a single range filter on the sort key.
- **Filter**: `l_shipdate <= 10471` (selectivity ~0.987 — nearly all rows qualify)
  - ~13 blocks at the tail (out of 600) can be skipped via zone map.
- **Aggregation**: 4 distinct groups (`(l_returnflag, l_linestatus)` = 4 combos: AF, NF, NO, RF).
  Sorted aggregation is efficient because only 4 groups exist.
- **Output**: SUM/AVG over multiple columns per group, 4 rows total.
- **ORDER BY**: `l_returnflag ASC, l_linestatus ASC` — natural order of the 4 groups.
- **No joins**, no subqueries.
- **Parallelism**: Partition lineitem rows into N_THREADS morsels. Each thread accumulates
  per-group partial sums/counts. Merge at the end.

---

## Indexes

### l_shipdate_zonemap (zone_map on l_shipdate)
- File: `lineitem/indexes/l_shipdate_zonemap.bin`
- Layout: `[uint32_t num_blocks=600]` then per block: `[double min_val, double max_val, uint32_t row_count, uint32_t _pad]` = 24 bytes/entry
- Access: `mmap` the file. Cast to `struct ZoneMapEntry { double min_val, max_val; uint32_t row_count, _pad; }`.
- `row_offset` of block `i` = `i * block_size` (row index, NOT byte offset). Access column data as `col_ptr[row_offset]`.
- This query: skip block `i` if `entry[i].min_val > 10471.0`
  - Since data is sorted ascending, early exit once `entry[i].min_val > 10471.0`.
  - ~21 tail blocks skippable (~3.5% skip rate). Low but free to check.

### l_discount_zonemap (zone_map on l_discount)
- File: `lineitem/indexes/l_discount_zonemap.bin`
- Same layout as above (double min/max).
- This query: **not useful** — no l_discount filter in Q1. Ignore.

### l_quantity_zonemap (zone_map on l_quantity)
- File: `lineitem/indexes/l_quantity_zonemap.bin`
- Same layout as above (double min/max).
- This query: **not useful** — no l_quantity filter in Q1. Ignore.

---

## Aggregation Output Schema
```
GROUP KEY: (l_returnflag: int8_t code, l_linestatus: int8_t code)
sum_qty:        double  (SUM of l_quantity)
sum_base_price: double  (SUM of l_extendedprice)
sum_disc_price: double  (SUM of l_extendedprice * (1 - l_discount))
sum_charge:     double  (SUM of l_extendedprice * (1 - l_discount) * (1 + l_tax))
count_order:    int64_t (COUNT(*))
avg_qty:        double  = sum_qty / count_order
avg_price:      double  = sum_base_price / count_order
avg_disc:       double  = SUM(l_discount) / count_order  [keep a separate sum_discount]
```
Output: sort 4 groups by (l_returnflag asc, l_linestatus asc), decode codes via dict.
