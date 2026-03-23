# Q6 Guide

## Column Reference
### l_shipdate (date_filter, int32_t, plain, days_since_epoch_1970)
- File: `lineitem/l_shipdate.bin` (59986052 rows)
- SQL: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR`
- Date derivation:
  - lower bound: `1994-01-01 -> 8766`
  - upper bound: `1995-01-01 -> 9131`
- C++ compare: `l_shipdate >= 8766 && l_shipdate < 9131`

### l_discount (filter_measure, double, plain)
- File: `lineitem/l_discount.bin` (59986052 rows)
- SQL: `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01`
- Bound derivation: `[0.05, 0.07]`
- C++ compare: `l_discount >= 0.05 && l_discount <= 0.07`

### l_quantity (filter_measure, double, plain)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- SQL: `l_quantity < 24`
- C++ compare: `l_quantity < 24.0`

### l_extendedprice (measure, double, plain)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- SQL: `SUM(l_extendedprice * l_discount)`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 100000 |

## Query Analysis
- Single-table filtered aggregate; no joins.
- Combined selectivity from workload file: `0.024`.
- Expected qualifying rows formula: `N_pass = 59986052 * 0.024 = 1439665.248`.
- Execution strategy:
  1. Read three zone maps.
  2. Intersect candidate blocks by block-level predicates.
  3. For candidate rows, evaluate exact predicates and accumulate `revenue += l_extendedprice * l_discount`.

## Indexes
### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `lineitem/lineitem_shipdate_zonemap.idx`
- Built by `write_zone_map<int32_t>`.
- Layout (exact):
  1. `uint32_t block_size`
  2. `uint64_t n`
  3. `uint64_t blocks`
  4. `int32_t mins[blocks]`
  5. `int32_t maxs[blocks]`
- For this run: `n=59986052`, `block_size=100000`, `blocks=600`.
- Keep block iff `maxs[b] >= 8766 && mins[b] < 9131`.
- Empty-slot sentinel value: none.

### lineitem_discount_zonemap (zone_map on l_discount)
- File: `lineitem/lineitem_discount_zonemap.idx`
- Built by `write_zone_map<double>`.
- Layout (exact):
  1. `uint32_t block_size`
  2. `uint64_t n`
  3. `uint64_t blocks`
  4. `double mins[blocks]`
  5. `double maxs[blocks]`
- For this run: `n=59986052`, `block_size=100000`, `blocks=600`.
- Keep block iff `maxs[b] >= 0.05 && mins[b] <= 0.07`.
- Empty-slot sentinel value: none.

### lineitem_quantity_zonemap (zone_map on l_quantity)
- File: `lineitem/lineitem_quantity_zonemap.idx`
- Built by `write_zone_map<double>`.
- Layout (exact):
  1. `uint32_t block_size`
  2. `uint64_t n`
  3. `uint64_t blocks`
  4. `double mins[blocks]`
  5. `double maxs[blocks]`
- For this run: `n=59986052`, `block_size=100000`, `blocks=600`.
- Keep block iff `mins[b] < 24.0`.
- Empty-slot sentinel value: none.
