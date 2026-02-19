# Q6 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int16_t

## Tables

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | -> |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 1 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### lineitem_l_shipdate
- File: indexes/lineitem_l_shipdate.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count, uint32_t offset] (16B/zone)
- Column: l_shipdate

## Query Notes
Q6 applies conjunctive filters: l_shipdate >= 1994-01-01 AND l_shipdate < 1995-01-01 (combined 25% selectivity), l_discount BETWEEN 0.05 AND 0.07 (27% selectivity), l_quantity < 24 (46% selectivity). Zone map enables block pruning on date range. Vectorized filter for qty and discount produces ~6% final selectivity.
