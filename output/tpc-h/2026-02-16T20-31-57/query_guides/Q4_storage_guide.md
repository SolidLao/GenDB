# Q4 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor: 2
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR→dictionary-encoded int32_t

## Tables

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_orderpriority | orders/o_orderpriority.bin | int32_t | STRING | dictionary | → |

- Dictionary files: o_orderpriority → orders/o_orderpriority_dict.txt

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | → |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | → |

## Indexes

### orders_orderkey_hash
- File: indexes/orders_orderkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=15000000] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: o_orderkey

### orders_orderdate_zonemap
- File: indexes/orders_orderdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=150] then [int32_t min, int32_t max, uint32_t row_count] per block (12 bytes/block)
- Column: o_orderdate
