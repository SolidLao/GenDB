# Q6 Storage Guide

## Data Encoding

- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
  - 1994-01-01 = epoch day 8766
  - 1995-01-01 = epoch day 9131
- Decimal encoding: scaled integers (int64_t, scale_factor=100)
  - l_discount: int64_t, divide by 100 for actual value (0.00–0.10 range)
  - l_quantity: int64_t, divide by 100 for actual value (1–50 range)
  - l_extendedprice: int64_t, divide by 100 for actual value
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t

## Tables

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate
- Filtering predicates:
  - `l_shipdate >= 1994-01-01` (epoch day ≥ 8766)
  - `l_shipdate < 1995-01-01` (epoch day < 9131)
  - `l_discount BETWEEN 0.05 AND 0.07` (scaled: 5–7)
  - `l_quantity < 24` (scaled: < 2400)

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | – |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### zone_map_l_shipdate
- File: indexes/zone_map_l_shipdate.bin
- Type: zone_map
- Layout: [ZoneMap min_val, max_val] per block (8B each, 600 blocks)
- Column: l_shipdate (int32_t)
- Purpose: Skip blocks where max(l_shipdate) < 8766 or min(l_shipdate) >= 9131 (predicate `8766 ≤ l_shipdate < 9131`)

### zone_map_l_discount
- File: indexes/zone_map_l_discount.bin
- Type: zone_map
- Layout: [ZoneMapDecimal min_val, max_val] per block (16B each, 600 blocks)
- Column: l_discount (int64_t, scaled)
- Purpose: Skip blocks where max(l_discount) < 5 or min(l_discount) > 7 (predicate `5 ≤ l_discount ≤ 7`)

### zone_map_l_quantity
- File: indexes/zone_map_l_quantity.bin
- Type: zone_map
- Layout: [ZoneMapDecimal min_val, max_val] per block (16B each, 600 blocks)
- Column: l_quantity (int64_t, scaled)
- Purpose: Skip blocks where min(l_quantity) >= 2400 (predicate `l_quantity < 2400`)

## Query Selectivity

Combined selectivity of all predicates: ~2.4% of 59.9M rows ≈ ~1.4M rows to aggregate.
