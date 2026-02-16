# Q22 Storage Guide

## Data Encoding
- Decimal encoding: scaled integers (int64_t), scale_factor = 100

## Tables

### customer
- Rows: 1,500,000, Block size: 150,000, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |
| c_acctbal | customer/c_acctbal.bin | int64_t | DECIMAL | none | 100 |

### orders
- Rows: 15,000,000, Block size: 150,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |

## Indexes

### orders_o_custkey_hash
- File: indexes/orders_o_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count] [positions...]
- Column: o_custkey
