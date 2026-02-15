# Q6 Storage Guide

## Query Summary
Single-table selective scan on lineitem with compound filters on l_shipdate, l_discount, l_quantity; scalar aggregation SUM(l_extendedprice * l_discount).

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t

## Table: lineitem
- Rows: 59,986,052
- Block size: 100,000
- Sort order: l_shipdate (ascending)

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | — |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### lineitem_shipdate_zonemap
- File: indexes/lineitem_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
- Column: l_shipdate (100K rows/zone)

## Query Filtering and Aggregation
Predicates (compound selectivity ≈1.3% of 60M rows):
- l_shipdate >= 1994-01-01 (8766 days) AND l_shipdate < 1995-01-01 (9131 days): use zone maps to prune blocks outside [8766, 9131)
- l_discount BETWEEN 0.05 AND 0.07 (scaled: 5 ≤ l_discount ≤ 7): branch-free range check
- l_quantity < 24 (scaled: l_quantity < 2400): branch-free comparison

After filtering: aggregate SUM(l_extendedprice * l_discount) over matching rows (~780K rows).
Parallelized: partition rows by core, local sum per thread, final merge.

## Date References
- 1994-01-01: 8766 days since epoch
- 1995-01-01: 9131 days since epoch
