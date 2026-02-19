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
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### idx_lineitem_shipdate (Zone Map)
- File: indexes/idx_lineitem_shipdate.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then [min:int32_t, max:int32_t, count:uint32_t] per zone (12B/zone)
- 600 zones (100K rows/zone)
- Column: l_shipdate
- Use to skip zones where max < 1994-01-01 (8035 days) or min >= 1995-01-01 (10592 days)

### idx_lineitem_discount_qty (Zone Map)
- File: indexes/idx_lineitem_discount_qty.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then [min:int64_t, max:int64_t, count:uint32_t] per zone (20B/zone)
- 600 zones
- Columns: l_discount, l_quantity
- Use to skip zones where max discount < 500 (0.05*100) or min > 700 (0.07*100)
- Use to skip zones where max quantity >= 2400 (24*100)
