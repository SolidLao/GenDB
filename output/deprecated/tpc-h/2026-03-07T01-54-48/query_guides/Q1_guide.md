# Q1 Guide

## Column Reference
### l_returnflag (group_key, uint32_t, dictionary)
- File: `lineitem/l_returnflag.bin` (59986052 rows), dictionary: `lineitem/l_returnflag.dict`
- This query: `GROUP BY l_returnflag`, `ORDER BY l_returnflag`
- Dictionary file layout from ingest path:
  1. `uint32_t n`
  2. For each dictionary value: `uint32_t len`, then `len` raw bytes
- Runtime loading pattern: parse `.dict` at runtime, build `code->string` for final output decode, and optional `string->code` lookup for comparisons.
- No hardcoded dictionary numeric values are used.

### l_linestatus (group_key, uint32_t, dictionary)
- File: `lineitem/l_linestatus.bin` (59986052 rows), dictionary: `lineitem/l_linestatus.dict`
- This query: `GROUP BY l_linestatus`, `ORDER BY l_linestatus`
- Dictionary layout and loading pattern: same as `l_returnflag`.

### l_quantity (filter_measure, double, plain)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- This query: `SUM(l_quantity)`, `AVG(l_quantity)`

### l_extendedprice (measure, double, plain)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- This query: `SUM(l_extendedprice)`, `SUM(l_extendedprice * (1 - l_discount))`, `SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))`, `AVG(l_extendedprice)`

### l_discount (filter_measure, double, plain)
- File: `lineitem/l_discount.bin` (59986052 rows)
- This query: arithmetic `(1 - l_discount)` and `AVG(l_discount)`

### l_tax (measure, double, plain)
- File: `lineitem/l_tax.bin` (59986052 rows)
- This query: arithmetic `(1 + l_tax)`

### l_shipdate (date_filter, int32_t, plain, days_since_epoch_1970)
- File: `lineitem/l_shipdate.bin` (59986052 rows)
- SQL: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
- Date derivation: `1998-12-01 - 90 days = 1998-09-02`
- Encoded compare value from ingest date encoding: `1998-09-02 -> 10471`
- C++ predicate: `l_shipdate <= 10471`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|---|---:|---|---|---:|
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 100000 |

## Query Analysis
- Logical flow:
  1. Apply shipdate predicate.
  2. Aggregate all nine output measures by `(l_returnflag, l_linestatus)`.
  3. Decode dictionary codes for final sorted output.
- Selectivity from workload file: `0.988`.
- Expected qualifying rows formula: `N_pass = 59986052 * 0.988 = 59266219.376`.
- Group count expectation from workload: `6` groups.
- Average derivation pattern:
  - `avg_qty = sum_qty / count_order`
  - `avg_price = sum_base_price / count_order`
  - `avg_disc = sum_disc / count_order`

## Indexes
### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `lineitem/lineitem_shipdate_zonemap.idx`
- Built by `write_zone_map<int32_t>` in `build_indexes.cpp`.
- On-disk layout (exact write order):
  1. `uint32_t block_size`
  2. `uint64_t n`
  3. `uint64_t blocks`
  4. `int32_t mins[blocks]`
  5. `int32_t maxs[blocks]`
- Parameter derivation:
  - `n = 59986052`
  - `block_size = 100000`
  - `blocks = (n + block_size - 1) / block_size = (59986052 + 99999) / 100000 = 600`
- Pruning rule for this query:
  - Skip block when `mins[b] > 10471`
  - Scan block when `mins[b] <= 10471`
- Row-range mapping for block `b`:
  - `start = b * block_size`
  - `end = min(n, start + block_size)`
- Empty-slot sentinel value: none (zone map is dense arrays, not hash slots).
