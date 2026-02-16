# Q1 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor = 100
- String encoding: Dictionary-encoded as int8_t

## Tables

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_tax | lineitem/l_tax.bin | int64_t | DECIMAL | none | 100 |
| l_returnflag | lineitem/l_returnflag.bin | int8_t | STRING | dictionary | - |
| l_linestatus | lineitem/l_linestatus.bin | int8_t | STRING | dictionary | - |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |

- Dictionary files: l_returnflag → lineitem/l_returnflag_dict.txt, l_linestatus → lineitem/l_linestatus_dict.txt

## Indexes

### lineitem_l_shipdate_zone
- File: indexes/lineitem_l_shipdate_zone.bin
- Type: zone_map
- Layout: [uint32_t num_zones=300] then [min_value:int32_t, max_value:int32_t, row_count:uint32_t] per zone (12 bytes/zone)
- Column: l_shipdate
