# Q6 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (100 for TPC-H)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded uint8_t or std::string

## Tables

### lineitem
- Rows: 59986052, Block size: 256000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### zone_map_l_shipdate
- File: indexes/zone_map_l_shipdate.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=235] then per block: [int32_t min, int32_t max, uint32_t count, uint32_t null_count] (16 bytes/block)
- Column: l_shipdate (range filter: 1994-01-01 to 1994-12-31)

