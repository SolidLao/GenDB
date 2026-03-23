# Q1 Guide

## Column Reference
### l_returnflag (return_flag, uint8_t, plain)
- File: `lineitem/l_returnflag.bin` (59986052 rows)
- Storage: one byte per row; ingest stores `static_cast<uint8_t>(returnflag[0])`
- This query: `GROUP BY l_returnflag` and `ORDER BY l_returnflag`

### l_linestatus (line_status, uint8_t, plain)
- File: `lineitem/l_linestatus.bin` (59986052 rows)
- Storage: one byte per row; ingest stores `static_cast<uint8_t>(linestatus[0])`
- This query: `GROUP BY l_linestatus` and `ORDER BY l_linestatus`

### l_quantity (decimal, double, plain)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- This query: `SUM(l_quantity)` and `AVG(l_quantity)`

### l_extendedprice (decimal, double, plain)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- This query: `SUM(l_extendedprice)`, `AVG(l_extendedprice)`, and arithmetic payload for discounted sums

### l_discount (decimal, double, plain)
- File: `lineitem/l_discount.bin` (59986052 rows)
- This query: `l_extendedprice * (1 - l_discount)` and `AVG(l_discount)`

### l_tax (decimal, double, plain)
- File: `lineitem/l_tax.bin` (59986052 rows)
- This query: `l_extendedprice * (1 - l_discount) * (1 + l_tax)`

### l_shipdate (date, int32_t, plain)
- File: `lineitem/l_shipdate.bin` (59986052 rows)
- Encoding: `parse_date()` in ingest writes `days_since_epoch_1970`
- This query: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY`
- Runtime literal handling should use the same date conversion path as ingest, then subtract 90 days from the encoded `1998-12-01`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 131072 |

## Query Analysis
- Predicate selectivity from workload analysis: `l_shipdate <= cutoff` keeps `0.985939` of `lineitem`
- Grouping cardinality is tiny: estimated `6` groups on `(l_returnflag, l_linestatus)`
- The table is not sorted by `l_shipdate`, so the zone map can only skip blocks whose max ship date is below or min ship date is above the cutoff
- The aggregation state can stay in a tiny fixed map keyed by the two `uint8_t` codes

## Indexes
### lineitem_shipdate_zonemap (zone_map on `l_shipdate`)
- Files:
  - `lineitem/indexes/lineitem_shipdate_zonemap.bin`
- Struct layout from `build_indexes.cpp`:
```cpp
struct ZoneMap1I32 {
    int32_t min_value;
    int32_t max_value;
};
```
- Build loop from `build_indexes.cpp`:
```cpp
constexpr size_t block_size = 131072;
for (size_t base = 0; base < l_shipdate.size(); base += block_size) {
    size_t end = std::min(base + block_size, l_shipdate.size());
    int32_t min_v = std::numeric_limits<int32_t>::max();
    int32_t max_v = std::numeric_limits<int32_t>::min();
    for (size_t i = base; i < end; ++i) {
        min_v = std::min(min_v, l_shipdate[i]);
        max_v = std::max(max_v, l_shipdate[i]);
    }
    zones.push_back({min_v, max_v});
}
```
- Exact format:
  - one `ZoneMap1I32` per contiguous 131072-row block
  - `458` entries because `ceil(59986052 / 131072) = 458`
  - block `b` covers row range `[b * 131072, min((b + 1) * 131072, 59986052))`
- Empty-slot sentinel: none; every block emits exactly one struct
- Query use:
  - keep block `b` if `zones[b].min_value <= cutoff`
  - skip block `b` only when `zones[b].min_value > cutoff`
  - rows in kept blocks still require row-level `l_shipdate <= cutoff`

## Dictionary Handling
- This query does not touch dictionary-encoded columns

## Formula Notes
- Date literals are not stored as strings anywhere in `gendb`
- Use the same ingest-side calendar conversion:
  - `parse_date("1998-12-01") -> days_from_civil(1998, 12, 1)`
  - cutoff is that encoded integer minus `90`
