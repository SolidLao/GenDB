# Q14 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

### part
- Rows: 2000000, Block size: 65536, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_type | part/p_type_dict.txt | int32_t | STRING | dictionary | → |

## Indexes

### l_shipdate_zone
- File: indexes/l_shipdate_zone.bin
- Type: zone_map
- Column: l_shipdate (range filter 1995-09-01 to 1995-09-30)

### l_partkey_hash, p_partkey_hash
- See Q3, Q2 for layouts
