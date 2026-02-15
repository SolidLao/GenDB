# Q1 Storage Guide

## Data Encoding

- Date encoding: days_since_epoch (1970-01-01), stored as int32_t (range: 8036–10561 for this data)
- Decimal encoding: scaled integers (int64_t, scale_factor=100)
  - l_quantity: int64_t, divide by 100 for actual value
  - l_extendedprice: int64_t, divide by 100 for actual value
  - l_discount: int64_t, divide by 100 for actual value
  - l_tax: int64_t, divide by 100 for actual value
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate
- Filtering predicate: `l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY` (epoch day ≤ 10516)

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_returnflag | lineitem/l_returnflag.bin | int32_t | STRING | dictionary | – |
| l_linestatus | lineitem/l_linestatus.bin | int32_t | STRING | dictionary | – |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | – |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_tax | lineitem/l_tax.bin | int64_t | DECIMAL | none | 100 |

- Dictionary files:
  - l_returnflag → lineitem/l_returnflag_dict.txt (values: N, O, R)
  - l_linestatus → lineitem/l_linestatus_dict.txt (values: F, O)

## Indexes

### zone_map_l_shipdate
- File: indexes/zone_map_l_shipdate.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=600] then [ZoneMap min_val, max_val] per block (8B each)
- Column: l_shipdate (int32_t)
- Purpose: Skip blocks where max(l_shipdate) < 10516 (predicate `l_shipdate <= 10516`)

### zone_map_l_discount
- File: indexes/zone_map_l_discount.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=600] then [ZoneMapDecimal min_val, max_val] per block (16B each)
- Column: l_discount (int64_t)
- Purpose: Future filtering on discount ranges

### zone_map_l_quantity
- File: indexes/zone_map_l_quantity.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=600] then [ZoneMapDecimal min_val, max_val] per block (16B each)
- Column: l_quantity (int64_t)
- Purpose: Future filtering on quantity ranges
