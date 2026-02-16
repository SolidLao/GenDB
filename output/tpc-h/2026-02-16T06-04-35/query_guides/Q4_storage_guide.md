# Q4 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- String encoding: Dictionary-encoded as int8_t

## Tables

### orders
- Rows: 15,000,000, Block size: 150,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |
| o_orderpriority | orders/o_orderpriority.bin | int8_t | STRING | dictionary | - |

- Dictionary files: o_orderpriority → orders/o_orderpriority_dict.txt

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | - |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | - |

## Indexes

### orders_o_orderdate_zone
- File: indexes/orders_o_orderdate_zone.bin
- Type: zone_map
- Layout: [uint32_t num_zones=100] then [min_value:int32_t, max_value:int32_t, row_count:uint32_t] per zone (12 bytes/zone)
- Column: o_orderdate

### lineitem_l_orderkey_hash
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count] [positions...]
- Column: l_orderkey
