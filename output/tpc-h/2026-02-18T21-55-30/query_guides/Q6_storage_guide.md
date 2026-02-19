# Q6 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=2 for DECIMAL columns
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t

## Tables

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 2 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 2 |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 2 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

## Indexes

### zone_map_l_shipdate
- File: indexes/zone_map_l_shipdate.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=600] then [int32_t min, int32_t max, uint32_t count] × 600 (12 bytes per block)
- Column: l_shipdate

### zone_map_l_discount_qty
- File: indexes/zone_map_l_discount_qty.bin
- Type: zone_map (multi-column)
- Layout: [uint32_t num_blocks=600] then [int32_t min1, int32_t max1, int32_t min2, int32_t max2, uint32_t count] × 600 (20 bytes per block)
- Columns: l_discount, l_quantity
