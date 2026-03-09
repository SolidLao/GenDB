# Q6 Guide

## Column Reference
### l_shipdate (date, int32_t, days_since_epoch_1970)
- File: `gendb/lineitem/l_shipdate.bin` (59986052 rows)
- Ingest parse: `l_shipdate.push_back(parse_date(cur.next()));`
- This query: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'`.
- Constant derivation: `1994-01-01 -> 8766`, `1995-01-01 -> 9131`.
- C++ comparisons: `l_shipdate[row] >= 8766` and `l_shipdate[row] < 9131`.

### l_discount (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_discount.bin` (59986052 rows)
- Ingest parse: `l_discount.push_back(parse_scaled_2(cur.next()));`
- This query: `l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01`.
- Constant derivation under scale-2 ingest: `0.05 -> 5`, `0.07 -> 7`.
- C++ comparison: `l_discount[row] >= 5 && l_discount[row] <= 7`.

### l_quantity (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_quantity.bin` (59986052 rows)
- Ingest parse: `l_quantity.push_back(parse_scaled_2(cur.next()));`
- This query: `l_quantity < 24`.
- Constant derivation under scale-2 ingest: `24.00 -> 2400`.
- C++ comparison: `l_quantity[row] < 2400`.

### l_extendedprice (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_extendedprice.bin` (59986052 rows)
- Ingest parse: `l_extendedprice.push_back(parse_scaled_2(cur.next()));`
- This query: `SUM(l_extendedprice * l_discount)` → scale-2 arithmetic in `int64_t`.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| lineitem | 59986052 | fact | `l_orderkey`, `l_linenumber` | 100000 |

## Query Analysis
- Filter selectivity from `workload_analysis.json`: `l_shipdate` year range → `0.161`; combined `l_discount,l_quantity` range → `0.024`.
- Aggregation estimate from `workload_analysis.json`: `1` output group.
- This query is entirely scan/filter/accumulate on `lineitem`, so the three zone maps are the only persistent accelerators.
- Because the indexes are independent zone maps, the execution code should derive candidate blocks from all three and then apply row-level predicates inside surviving blocks.

## Indexes
### lineitem_shipdate_zone_map (zone_map on `l_shipdate`)
- Files: `gendb/lineitem/indexes/lineitem_shipdate_zone_map.mins.bin`, `gendb/lineitem/indexes/lineitem_shipdate_zone_map.maxs.bin`
- Build call:
```cpp
auto shipdate = read_vector<int32_t>(table_dir / "l_shipdate.bin");
write_zone_map(index_dir, "lineitem_shipdate_zone_map", shipdate, 100000);
```
- Exact on-disk layout: `std::vector<int32_t> mins`, then `std::vector<int32_t> maxs`.
- Empty-slot sentinel: none.
- Actual block count: `ceil(59986052 / 100000) = 600`.
- Query usage:
  - Skip block `b` when `maxs[b] < 8766` or `mins[b] >= 9131`.
  - Otherwise scan the block and apply the two row-level date comparisons.

### lineitem_discount_zone_map (zone_map on `l_discount`)
- Files: `gendb/lineitem/indexes/lineitem_discount_zone_map.mins.bin`, `gendb/lineitem/indexes/lineitem_discount_zone_map.maxs.bin`
- Build call:
```cpp
auto discount = read_vector<int64_t>(table_dir / "l_discount.bin");
write_zone_map(index_dir, "lineitem_discount_zone_map", discount, 100000);
```
- Exact on-disk layout: `std::vector<int64_t> mins`, then `std::vector<int64_t> maxs`.
- Empty-slot sentinel: none.
- Actual block count: `600`.
- Query usage:
  - Skip block `b` when `maxs[b] < 5` or `mins[b] > 7`.
  - Otherwise scan the block and apply `5 <= l_discount[row] && l_discount[row] <= 7`.

### lineitem_quantity_zone_map (zone_map on `l_quantity`)
- Files: `gendb/lineitem/indexes/lineitem_quantity_zone_map.mins.bin`, `gendb/lineitem/indexes/lineitem_quantity_zone_map.maxs.bin`
- Build call:
```cpp
auto quantity = read_vector<int64_t>(table_dir / "l_quantity.bin");
write_zone_map(index_dir, "lineitem_quantity_zone_map", quantity, 100000);
```
- Exact on-disk layout: `std::vector<int64_t> mins`, then `std::vector<int64_t> maxs`.
- Empty-slot sentinel: none.
- Actual block count: `600`.
- Query usage:
  - Skip block `b` when `mins[b] >= 2400`.
  - Accept block `b` without row-level quantity checks when `maxs[b] < 2400`.
  - Otherwise test `l_quantity[row] < 2400` inside the block.

## Implementation Notes
- There is no composite `(shipdate, discount, quantity)` index in the generated storage; guide consumers must combine three independent block-pruning signals.
- All four referenced Q6 columns are fixed-width and row-aligned, so a surviving row id can read every needed value without extra indirection.
