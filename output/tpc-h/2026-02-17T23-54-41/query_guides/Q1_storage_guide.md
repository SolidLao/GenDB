# Q1 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100 for monetary columns
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_returnflag | lineitem/l_returnflag.bin | int32_t | STRING | dictionary | → |
| l_linestatus | lineitem/l_linestatus.bin | int32_t | STRING | dictionary | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_tax | lineitem/l_tax.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

- Dictionary files: lineitem/l_returnflag_dict.txt, lineitem/l_linestatus_dict.txt

## Indexes

### lineitem_l_shipdate (zone_map)
- File: indexes/lineitem_l_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count] (12 bytes each)
- Zones: 300 blocks, block size 200,000 rows
- Column: l_shipdate (range filter for WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY)
