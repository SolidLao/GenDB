# Q13 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Type mappings: INTEGER→int32_t, CHAR/VARCHAR→raw strings

## Tables

### customer
- Rows: 1,500,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |
| o_comment | orders/o_comment.bin | std::string | STRING | none | - |

## Indexes

### customer_c_custkey_hash
- File: indexes/customer_c_custkey_hash.bin
- Type: hash
- Column: c_custkey

### orders_o_custkey_hash
- File: indexes/orders_o_custkey_hash.bin
- Type: hash_multi_value
- Column: o_custkey
