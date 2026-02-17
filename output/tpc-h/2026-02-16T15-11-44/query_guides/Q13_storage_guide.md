# Q13 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

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

### idx_customer_custkey_hash
- File: indexes/idx_customer_custkey_hash.bin
- Type: hash_multi_value
- Column: c_custkey

### idx_orders_custkey_hash
- File: indexes/idx_orders_custkey_hash.bin
- Type: hash_multi_value
- Column: o_custkey
