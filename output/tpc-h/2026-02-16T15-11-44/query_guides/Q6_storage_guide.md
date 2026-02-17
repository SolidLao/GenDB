# Q6 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### idx_lineitem_shipdate_zmap
- File: indexes/idx_lineitem_shipdate_zmap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min_val, int32_t max_val, uint32_t row_count] (12B/zone)
- Column: l_shipdate
- Purpose: Skip blocks where l_shipdate < 1994-01-01 or >= 1995-01-01 (epoch days)

### idx_lineitem_discount_zmap
- File: indexes/idx_lineitem_discount_zmap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int64_t min_val, int64_t max_val, uint32_t row_count] (20B/zone)
- Column: l_discount
- Purpose: Skip blocks where discount BETWEEN 0.05 AND 0.07 (scaled 5-7 * 100)

### idx_lineitem_quantity_zmap
- File: indexes/idx_lineitem_quantity_zmap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int64_t min_val, int64_t max_val, uint32_t row_count] (20B/zone)
- Column: l_quantity
- Purpose: Skip blocks where l_quantity >= 24 (scaled 2400)
