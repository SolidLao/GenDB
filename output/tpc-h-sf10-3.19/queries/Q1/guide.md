# Q1 Guide — Pricing Summary Report

## Column Reference

### l_shipdate (date, int32_t, days_since_epoch_1970)
- File: `storage/lineitem/l_shipdate.bin` (59986052 rows × 4 bytes)
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY` → C++ `l_shipdate[i] <= 10471`
- Date encoding: days since 1970-01-01. `1998-09-02` = 10471 days since epoch.

### l_returnflag (flag, int8_t, char_as_byte)
- File: `storage/lineitem/l_returnflag.bin` (59986052 rows × 1 byte)
- This query: GROUP BY key. Values are ASCII chars stored as int8_t: 'A'=65, 'N'=78, 'R'=82
- No dictionary — raw ASCII byte.

### l_linestatus (flag, int8_t, char_as_byte)
- File: `storage/lineitem/l_linestatus.bin` (59986052 rows × 1 byte)
- This query: GROUP BY key. Values are ASCII chars stored as int8_t: 'F'=70, 'O'=79
- No dictionary — raw ASCII byte.

### l_quantity (measure, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59986052 rows × 8 bytes)
- This query: `SUM(l_quantity)`, `AVG(l_quantity)` — accumulate per group

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59986052 rows × 8 bytes)
- This query: `SUM(l_extendedprice)`, `AVG(l_extendedprice)`, and in expressions

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59986052 rows × 8 bytes)
- This query: `SUM(l_extendedprice * (1 - l_discount))`, `AVG(l_discount)` — used in computation

### l_tax (measure, double, raw)
- File: `storage/lineitem/l_tax.bin` (59986052 rows × 8 bytes)
- This query: `SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))` — used in computation

## Table Stats
| Table    | Rows       | Role | Sort Order  | Block Size |
|----------|------------|------|-------------|------------|
| lineitem | 59,986,052 | fact | l_orderkey  | 65536      |

## Query Analysis

### Pattern
Single-table scan with date filter, grouped aggregation, ordered output.

### Filter
- `l_shipdate <= 10471` (1998-09-02): selectivity ~0.988 → ~59.3M rows pass
- Zone map on l_shipdate can skip blocks where `min_date > 10471` (very few blocks skipped at 98.8% selectivity)

### Aggregation
- GROUP BY (l_returnflag, l_linestatus): only ~4 groups (3 returnflag × 2 linestatus, but only 4 valid combos)
- Aggregate struct per group: sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_disc, count (6 doubles + 1 int64)
- Tiny group count → use array or small hash map keyed by (returnflag, linestatus) pair

### Aggregation Key Strategy
- l_returnflag has 3 values: 'A'(65), 'N'(78), 'R'(82)
- l_linestatus has 2 values: 'F'(70), 'O'(79)
- Efficient: map to small index. E.g., `((flag - 'A') * 2 + (status == 'O'))` or use a 256×256 lookup

### Output
- ORDER BY l_returnflag ASC, l_linestatus ASC — trivial sort of 4 rows
- Compute AVGs as sum/count in final output

## Indexes

### lineitem_l_shipdate_zonemap (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout:
  - Header: `uint64_t num_blocks` (= ceil(59986052 / 65536) = 916 blocks), `uint32_t block_size` (= 65536)
  - Body: `int32_t[num_blocks * 2]` — pairs of (min_date, max_date) per block
- Usage: For each block `b`, read `zonemap[b*2]` (min) and `zonemap[b*2+1]` (max).
  Skip block if `min_date > 10471`. Process block if `max_date <= 10471` (all rows pass, no per-row check needed).
  Otherwise scan rows individually.
- At 98.8% selectivity, most blocks will fully pass — zone map mainly helps skip the last ~1.2% of blocks.

## Recommended Execution Strategy
1. Load zone map header, then zone map body
2. Load all 7 lineitem columns (shipdate, returnflag, linestatus, quantity, extendedprice, discount, tax)
3. For each block: check zone map. If max <= 10471, aggregate all rows without per-row date check. If min > 10471, skip entirely. Otherwise, check per-row.
4. Use a fixed-size array of ~6 aggregation structs indexed by (returnflag, linestatus) compact key
5. Sort 4 result groups and output
