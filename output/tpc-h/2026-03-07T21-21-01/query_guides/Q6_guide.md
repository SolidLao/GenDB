# Q6 Guide

## Column Reference
### l_shipdate (date, int32_t, plain)
- File: `lineitem/l_shipdate.bin` (59986052 rows)
- Encoding: ingest uses `parse_date()` and stores `days_since_epoch_1970`
- This query: `l_shipdate >= DATE '1994-01-01'` and `l_shipdate < DATE '1995-01-01'`

### l_discount (decimal, double, plain)
- File: `lineitem/l_discount.bin` (59986052 rows)
- This query: `l_discount BETWEEN 0.05 AND 0.07`

### l_quantity (decimal, double, plain)
- File: `lineitem/l_quantity.bin` (59986052 rows)
- This query: `l_quantity < 24`

### l_extendedprice (decimal, double, plain)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- This query: `SUM(l_extendedprice * l_discount)`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
| --- | ---: | --- | --- | ---: |
| lineitem | 59986052 | fact | `l_orderkey, l_linenumber` | 131072 |

## Query Analysis
- Workload analysis gives combined selectivity `0.0189921` for the three predicates together
- The dedicated Q6 zone map stores all three filtered columns per block, so block pruning can use ship date, discount, and quantity before touching the base columns
- The aggregation is scalar: no group-by state, only one running `double` sum

## Indexes
### lineitem_q6_zonemap (zone_map on `l_shipdate, l_discount, l_quantity`)
- File: `lineitem/indexes/lineitem_q6_zonemap.bin`
- Struct layout from `build_indexes.cpp`:
```cpp
struct ZoneMap3 {
    int32_t shipdate_min;
    int32_t shipdate_max;
    double discount_min;
    double discount_max;
    double quantity_min;
    double quantity_max;
};
```
- Build loop from `build_indexes.cpp`:
```cpp
constexpr size_t block_size = 131072;
for (size_t base = 0; base < l_shipdate.size(); base += block_size) {
    size_t end = std::min(base + block_size, l_shipdate.size());
    int32_t ship_min = std::numeric_limits<int32_t>::max();
    int32_t ship_max = std::numeric_limits<int32_t>::min();
    double disc_min = std::numeric_limits<double>::infinity();
    double disc_max = -std::numeric_limits<double>::infinity();
    double qty_min = std::numeric_limits<double>::infinity();
    double qty_max = -std::numeric_limits<double>::infinity();
    for (size_t i = base; i < end; ++i) {
        ship_min = std::min(ship_min, l_shipdate[i]);
        ship_max = std::max(ship_max, l_shipdate[i]);
        disc_min = std::min(disc_min, l_discount[i]);
        disc_max = std::max(disc_max, l_discount[i]);
        qty_min = std::min(qty_min, l_quantity[i]);
        qty_max = std::max(qty_max, l_quantity[i]);
    }
    q6_zones.push_back({ship_min, ship_max, disc_min, disc_max, qty_min, qty_max});
}
```
- Exact format:
  - one `ZoneMap3` per contiguous 131072-row block
  - `458` entries because `ceil(59986052 / 131072) = 458`
  - block `b` covers row range `[b * 131072, min((b + 1) * 131072, 59986052))`
- Empty-slot sentinel: none
- Query use:
  - ship-date overlap test: `shipdate_max >= lower && shipdate_min < upper`
  - discount overlap test: `discount_max >= 0.05 && discount_min <= 0.07`
  - quantity overlap test: `quantity_min < 24`
  - only blocks passing all three overlap tests need row-level predicate checks

### lineitem_shipdate_zonemap (zone_map on `l_shipdate`)
- File: `lineitem/indexes/lineitem_shipdate_zonemap.bin`
- Struct layout:
```cpp
struct ZoneMap1I32 {
    int32_t min_value;
    int32_t max_value;
};
```
- Relevance:
  - this index exists, but the Q6-specific zone map is strictly richer for this query because it contains ship date plus both other filtered columns
  - use this only if query code chooses a shipdate-only prepass
- Empty-slot sentinel: none

## Formula Notes
- Date range endpoints should be encoded using the same ingest-side date conversion
- `DATE '1994-01-01' + INTERVAL '1' YEAR` should become the encoded `1995-01-01`; do not compare textual dates against `l_shipdate.bin`
