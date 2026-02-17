# Q12 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_orderpriority | orders/o_orderpriority.bin | int32_t | STRING | dictionary | - |

- Dictionary files: o_orderpriority → orders/o_orderpriority_dict.txt (load at runtime)

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_shipmode | lineitem/l_shipmode.bin | int32_t | STRING | dictionary | - |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | - |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | - |

- Dictionary files: l_shipmode → lineitem/l_shipmode_dict.txt (load at runtime)

## Indexes

### lineitem_orderkey_hash
- File: indexes/lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12 bytes/entry), then [uint32_t pos_count][uint32_t positions...]
- Column: l_orderkey

### orders_orderkey_hash
- File: indexes/orders_orderkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [key:int32_t, position:uint32_t] per slot (8 bytes/entry)
- Column: o_orderkey
