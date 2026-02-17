# Q12 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### orders
- Rows: 15000000, Block size: 131072, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_orderpriority | orders/o_orderpriority_dict.txt | int32_t | STRING | dictionary | → |

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_shipmode | lineitem/l_shipmode_dict.txt | int32_t | STRING | dictionary | → |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | → |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | → |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

## Indexes

### l_shipdate_zone
- File: indexes/l_shipdate_zone.bin
- Type: zone_map
- Column: l_shipdate (range filter 1994-01-01 to 1994-12-31)

### o_orderkey_hash, l_orderkey_hash
- See Q3 for layouts
