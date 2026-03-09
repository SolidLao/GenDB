# Q1 Guide

## Column Reference
### l_returnflag (group_key, uint8_t, plain_u8)
- File: `gendb/lineitem/l_returnflag.bin` (59986052 rows)
- Ingest layout: fixed-width `std::vector<uint8_t>` written by `write_vector`.
- This query: `GROUP BY l_returnflag` and `ORDER BY l_returnflag` → byte equality / byte ordering.

### l_linestatus (group_key, uint8_t, plain_u8)
- File: `gendb/lineitem/l_linestatus.bin` (59986052 rows)
- Ingest layout: fixed-width `std::vector<uint8_t>` written by `write_vector`.
- This query: `GROUP BY l_linestatus` and `ORDER BY l_linestatus` → byte equality / byte ordering.

### l_quantity (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_quantity.bin` (59986052 rows)
- Ingest parse: `l_quantity.push_back(parse_scaled_2(cur.next()));`
- Stored unit: cents-like scale-2 integer; `12.34` becomes `1234`.
- This query: `SUM(l_quantity)` and `AVG(l_quantity)` → sum `int64_t`, divide by `COUNT(*)` at the end.

### l_extendedprice (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_extendedprice.bin` (59986052 rows)
- Ingest parse: `l_extendedprice.push_back(parse_scaled_2(cur.next()));`
- This query: `SUM(l_extendedprice)` and `AVG(l_extendedprice)` → scale-2 arithmetic in `int64_t`.

### l_discount (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_discount.bin` (59986052 rows)
- Ingest parse: `l_discount.push_back(parse_scaled_2(cur.next()));`
- This query: `l_extendedprice * (1 - l_discount)` and `AVG(l_discount)` → row arithmetic uses encoded scale-2 values.

### l_tax (decimal_scale_2, int64_t, scaled_i64)
- File: `gendb/lineitem/l_tax.bin` (59986052 rows)
- Ingest parse: `l_tax.push_back(parse_scaled_2(cur.next()));`
- This query: `l_extendedprice * (1 - l_discount) * (1 + l_tax)` → row arithmetic uses encoded scale-2 values.

### l_shipdate (date, int32_t, days_since_epoch_1970)
- File: `gendb/lineitem/l_shipdate.bin` (59986052 rows)
- Ingest parse: `l_shipdate.push_back(parse_date(cur.next()));`
- Exact encoding path in `ingest.cpp`:
```cpp
static inline int32_t parse_date(std::string_view sv) {
    int y = static_cast<int>(parse_int64(sv.substr(0, 4)));
    unsigned m = static_cast<unsigned>(parse_int64(sv.substr(5, 2)));
    unsigned d = static_cast<unsigned>(parse_int64(sv.substr(8, 2)));
    return days_from_civil(y, m, d);
}
```
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`.
- Constant derivation: `1998-12-01 - 90 days = 1998-09-02`; encoded cutoff is `10471` days since `1970-01-01`.
- C++ comparison for row filtering: `l_shipdate[row] <= 10471`.

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| lineitem | 59986052 | fact | `l_orderkey`, `l_linenumber` | 100000 |

## Query Analysis
- Filter selectivity from `workload_analysis.json`: `0.988` on `l_shipdate <= cutoff`; this is almost a full-table scan.
- Aggregation estimate from `workload_analysis.json`: `6` groups on `(l_returnflag, l_linestatus)`.
- Because the filter is weak, the main win is block pruning near the tail of `l_shipdate`; surviving rows still need per-row arithmetic on `l_quantity`, `l_extendedprice`, `l_discount`, and `l_tax`.
- The table sort order is on `l_orderkey`, not `l_shipdate`, so the zone map is advisory pruning rather than a contiguous range scan guarantee.

## Indexes
### lineitem_shipdate_zone_map (zone_map on `l_shipdate`)
- Files: `gendb/lineitem/indexes/lineitem_shipdate_zone_map.mins.bin`, `gendb/lineitem/indexes/lineitem_shipdate_zone_map.maxs.bin`
- Authoritative build call in `build_indexes.cpp`:
```cpp
auto shipdate = read_vector<int32_t>(table_dir / "l_shipdate.bin");
write_zone_map(index_dir, "lineitem_shipdate_zone_map", shipdate, 100000);
```
- Exact on-disk layout: no explicit struct; two parallel arrays written in this order by `write_zone_map`:
  1. `std::vector<int32_t> mins`
  2. `std::vector<int32_t> maxs`
- Exact block builder in `build_indexes.cpp`:
```cpp
size_t blocks = (data.size() + block_size - 1) / block_size;
std::vector<T> mins(blocks);
std::vector<T> maxs(blocks);
for (size_t block = 0; block < blocks; ++block) {
    size_t begin = block * block_size;
    size_t end = std::min(begin + block_size, data.size());
    T min_v = data[begin];
    T max_v = data[begin];
    for (size_t i = begin + 1; i < end; ++i) {
        min_v = std::min(min_v, data[i]);
        max_v = std::max(max_v, data[i]);
    }
    mins[block] = min_v;
    maxs[block] = max_v;
}
```
- Empty-slot sentinel: none.
- Actual index cardinality: `ceil(59986052 / 100000) = 600` blocks.
- Query usage pattern:
  - Skip block `b` when `mins[b] > 10471`.
  - Accept block `b` without row-level date checks when `maxs[b] <= 10471`.
  - Otherwise scan rows `b*100000 .. min((b+1)*100000, 59986052)` and apply `l_shipdate[row] <= 10471`.

## Implementation Notes
- Every referenced Q1 column is fixed-width and written one row per position, so row id alignment across all `lineitem/*.bin` files is exact.
- Decimal math must respect the scale-2 ingest encoding from `parse_scaled_2`; no column in Q1 is dictionary encoded.
- The guide intentionally does not invent a struct for the zone map because the implementation writes plain vectors, not structs.
