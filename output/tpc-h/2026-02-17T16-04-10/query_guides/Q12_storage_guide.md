# Q12 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_orderpriority | orders/o_orderpriority.bin | int32_t | STRING | dictionary | - |

- Dictionary files: o_orderpriority → orders/o_orderpriority_dict.txt

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_shipmode | lineitem/l_shipmode.bin | int32_t | STRING | dictionary | - |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | - |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | - |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |

- Dictionary files: l_shipmode → lineitem/l_shipmode_dict.txt

## Indexes

### orders_o_orderkey_hash
- File: indexes/orders_o_orderkey_hash.bin
- Type: hash
- Column: o_orderkey

### lineitem_l_orderkey_hash
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey

### lineitem_l_shipdate_zonemap
- File: indexes/lineitem_l_shipdate_zonemap.bin
- Type: zone_map
- Column: l_shipdate
