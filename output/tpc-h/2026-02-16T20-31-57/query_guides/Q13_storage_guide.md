# Q13 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor: 2
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1500000, Block size: 100000, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_comment | orders/o_comment.bin | int32_t | STRING | dictionary | → |

- Dictionary files: o_comment → orders/o_comment_dict.txt

## Indexes

### customer_custkey_hash
- File: indexes/customer_custkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=1500000] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: c_custkey

### orders_custkey_hash
- File: indexes/orders_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=999982][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot), then [uint32_t positions_count][uint32_t positions...]
- Column: o_custkey
