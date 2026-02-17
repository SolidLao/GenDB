# Q15 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

### supplier
- Rows: 100000, Block size: 32768, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_name | supplier/s_name_dict.txt | int32_t | STRING | dictionary | → |
| s_address | supplier/s_address_dict.txt | int32_t | STRING | dictionary | → |
| s_phone | supplier/s_phone_dict.txt | int32_t | STRING | dictionary | → |

## Indexes

### l_shipdate_zone
- File: indexes/l_shipdate_zone.bin
- Type: zone_map
- Column: l_shipdate (range filter 1996-01-01 to 1996-03-31)

### l_suppkey_hash, s_suppkey_hash
- See Q3, Q2 for layouts
