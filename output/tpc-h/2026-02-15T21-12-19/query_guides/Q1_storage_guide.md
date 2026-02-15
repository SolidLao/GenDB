# Q1 Storage Guide

## Query Summary
Single-table scan on lineitem with range filter on l_shipdate and aggregation by l_returnflag, l_linestatus.

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100 (e.g., 0.05 → 5)
- String columns: dictionary-encoded as int32_t codes
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→int32_t (dict codes)

## Table: lineitem
- Rows: 59,986,052
- Block size: 100,000
- Sort order: l_shipdate (ascending)

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_tax | lineitem/l_tax.bin | int64_t | DECIMAL | none | 100 |
| l_returnflag | lineitem/l_returnflag.bin | int32_t | STRING | dictionary | — |
| l_linestatus | lineitem/l_linestatus.bin | int32_t | STRING | dictionary | — |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | — |

- Dictionary files:
  - l_returnflag → lineitem/l_returnflag_dict.txt (3 values: N, A, R)
  - l_linestatus → lineitem/l_linestatus_dict.txt (2 values: F, O)

## Indexes

### lineitem_shipdate_zonemap
- File: indexes/lineitem_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
- Column: l_shipdate (100K rows/zone)

### lineitem_orderkey_hash
- File: indexes/lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [key:int32_t, offset:uint32_t, count:uint32_t × num_unique] [uint32_t pos_count] [positions...]
- Column: l_orderkey (not used in Q1, available for multi-query optimization)

## Column Access
Q1 filters l_shipdate with range predicate and aggregates l_quantity, l_extendedprice, l_discount, l_tax by low-cardinality (l_returnflag, l_linestatus). Use zone maps to prune blocks where l_shipdate range does not overlap predicate. Vectorize aggregation over remaining rows.
