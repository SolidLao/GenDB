# Q22 Storage Guide

## Data Encoding
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t (scale_factor=100), CHAR/VARCHAR→raw strings

## Tables

### customer
- Rows: 1,500,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |
| c_phone | customer/c_phone.bin | std::string | STRING | none | - |
| c_acctbal | customer/c_acctbal.bin | int64_t | DECIMAL | none | 100 |

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |

## Indexes

### customer_c_custkey_hash
- File: indexes/customer_c_custkey_hash.bin
- Type: hash
- Column: c_custkey

### orders_o_custkey_hash
- File: indexes/orders_o_custkey_hash.bin
- Type: hash_multi_value
- Column: o_custkey
