# Q6 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t

## Tables

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### lineitem_l_shipdate_zonemap
- File: indexes/lineitem_l_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_blocks] then [min:int32_t, max:int32_t] per block
- Column: l_shipdate
