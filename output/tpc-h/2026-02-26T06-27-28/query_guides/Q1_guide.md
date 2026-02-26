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

## Table Stats
| Table    | Rows       | Role | Sort Order  | Block Size |
|----------|------------|------|-------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate  | 100,000    |

Number of blocks = ceil(59,986,052 / 100,000) = 600

## Query Analysis
- **Single-table scan** on lineitem (no joins).
- **Filter selectivity**: ~97% of rows pass (`l_shipdate <= 10471`). Almost all rows survive.
- **Group-by**: `(l_returnflag, l_linestatus)` → at most 3 × 2 = 6 groups. Use a 6-entry fixed array keyed by `(rflag_code * 2 + lstatus_code)`.
- **Zone map usage**: Because lineitem is sorted by `l_shipdate` ascending, blocks with `min_val > 10471` can be skipped. Since the threshold is 1998-09-02 and the data max is 1998-11-29, only the last ~2–3 blocks can be skipped; most blocks pass.
- **Output**: Sort 6 rows by `(l_returnflag_string, l_linestatus_string)` — trivially cheap.

## Date Threshold
```
DATE '1998-12-01' - INTERVAL '90' DAY = 1998-09-02
date_to_days(1998, 9, 2) = 10471   // Howard Hinnant days_from_civil
```
C++ predicate: `l_shipdate[i] <= 10471`

## Column Reference

### l_shipdate (date_filter, int32_t, days_since_epoch_1970)
- File: `lineitem/l_shipdate.bin` — int32_t[59,986,052]
- Sorted ascending (table sort order = l_shipdate)
- Predicate: `l_shipdate[i] <= 10471`

### l_returnflag (group_by_low_card, int8_t, dict)
- File: `lineitem/l_returnflag.bin` — int8_t[59,986,052]
- Dict file: `lineitem/l_returnflag.dict` — 3 lines (0-indexed)
  - Load at runtime: read line i → code i is that string
  - Encoding (from ingest.cpp `enc_returnflag`):
    ```cpp
    if (c == 'A') return 0;
    if (c == 'N') return 1;
    return 2; // R
    ```
- GROUP BY key component: use code directly as array index [0..2]

### l_linestatus (group_by_low_card, int8_t, dict)
- File: `lineitem/l_linestatus.bin` — int8_t[59,986,052]
- Dict file: `lineitem/l_linestatus.dict` — 2 lines (0-indexed)
  - Encoding (from ingest.cpp `enc_linestatus`):
    ```cpp
    return c == 'O' ? 1 : 0; // F=0, O=1
    ```
- GROUP BY key component: use code directly as array index [0..1]

### l_quantity (measure, double, raw)
- File: `lineitem/l_quantity.bin` — double[59,986,052]
- Used in: SUM, AVG accumulators

### l_extendedprice (measure, double, raw)
- File: `lineitem/l_extendedprice.bin` — double[59,986,052]
- Used in: SUM(l_extendedprice), SUM(l_extendedprice*(1-l_discount)), SUM(l_extendedprice*(1-l_discount)*(1+l_tax)), AVG

### l_discount (measure_filter, double, raw)
- File: `lineitem/l_discount.bin` — double[59,986,052]
- Used in: SUM(l_extendedprice*(1-l_discount)), SUM(...*(1+l_tax)), AVG(l_discount)

### l_tax (measure, double, raw)
- File: `lineitem/l_tax.bin` — double[59,986,052]
- Used in: SUM(l_extendedprice*(1-l_discount)*(1+l_tax))

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout (from build_indexes.cpp `build_zone_map`):
  ```
  uint64_t  n_blocks               // = 600
  { int32_t min_val; int32_t max_val; } [n_blocks]
  ```
- Usage for Q1:
  ```cpp
  // Load header
  uint64_t n_blocks;
  fread(&n_blocks, 8, 1, f);
  // For each block b:
  //   rows [b*100000 .. min((b+1)*100000, N)-1]
  //   read {int32_t mn, mx}
  //   skip block if mn > 10471   // all rows in block fail the filter
  ```
- Since table is sorted by l_shipdate, once `mn > 10471` for block b, all subsequent blocks also fail → early termination.

## Aggregation Strategy
Use a flat array of 6 accumulators indexed by `rflag_code * 2 + lstatus_code`:
```cpp
struct Acc {
    double sum_qty, sum_base, sum_disc_price, sum_charge, sum_disc;
    int64_t cnt;
};
Acc agg[6] = {};
// For each passing row i:
int key = (int)rflag[i] * 2 + (int)lstatus[i];  // 0..5
agg[key].sum_qty         += qty[i];
agg[key].sum_base        += ext[i];
agg[key].sum_disc_price  += ext[i] * (1.0 - disc[i]);
agg[key].sum_charge      += ext[i] * (1.0 - disc[i]) * (1.0 + tax[i]);
agg[key].sum_disc        += disc[i];
agg[key].cnt++;
```

## Output Ordering
Sort the (up to 6) result rows by `(l_returnflag_string ASC, l_linestatus_string ASC)`.
Load dict files at startup to map codes back to strings for final output.
