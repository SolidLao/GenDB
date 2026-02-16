# Q12 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- String encoding: Dictionary-encoded as int8_t

## Tables

### orders
- Rows: 15,000,000, Block size: 150,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_orderpriority | orders/o_orderpriority.bin | int8_t | STRING | dictionary | - |

- Dictionary files: o_orderpriority → orders/o_orderpriority_dict.txt

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_shipmode | lineitem/l_shipmode.bin | int8_t | STRING | dictionary | - |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | - |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | - |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |

- Dictionary files: l_shipmode → lineitem/l_shipmode_dict.txt

## Indexes

### lineitem_l_orderkey_hash
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count] [positions...]
- Column: l_orderkey

### lineitem_l_shipdate_zone
- File: indexes/lineitem_l_shipdate_zone.bin
- Type: zone_map
- Column: l_shipdate
