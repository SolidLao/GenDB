# Q12 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_orderpriority | orders/o_orderpriority.bin | int32_t | STRING | dictionary | → |

- Dictionary files: o_orderpriority → orders/o_orderpriority_dict.txt

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_shipmode | lineitem/l_shipmode.bin | int32_t | STRING | dictionary | → |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | → |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | → |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

- Dictionary files: l_shipmode → lineitem/l_shipmode_dict.txt

## Indexes

### idx_orders_orderkey_hash
- File: indexes/idx_orders_orderkey_hash.bin
- Type: hash_multi_value
- Column: o_orderkey

### idx_lineitem_orderkey_hash
- File: indexes/idx_lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey

### idx_lineitem_shipdate_zmap
- File: indexes/idx_lineitem_shipdate_zmap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min_val, int32_t max_val, uint32_t row_count] (12B/zone)
- Column: l_shipdate
- Purpose: Skip blocks where l_receiptdate < 1994-01-01 or >= 1995-01-01 (epoch days)
