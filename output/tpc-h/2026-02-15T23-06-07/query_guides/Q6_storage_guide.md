# Q6 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

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
- Layout: [uint32_t num_entries] then per entry: [min_val:int32_t, max_val:int32_t, start_row:uint32_t, row_count:uint32_t] (16 bytes/entry)
- Column: l_shipdate

### lineitem_l_discount_zonemap
- File: indexes/lineitem_l_discount_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_entries] then per entry: [min_val:int64_t, max_val:int64_t, start_row:uint32_t, row_count:uint32_t] (24 bytes/entry)
- Column: l_discount

### lineitem_l_quantity_zonemap
- File: indexes/lineitem_l_quantity_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_entries] then per entry: [min_val:int64_t, max_val:int64_t, start_row:uint32_t, row_count:uint32_t] (24 bytes/entry)
- Column: l_quantity
