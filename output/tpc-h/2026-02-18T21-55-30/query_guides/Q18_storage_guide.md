# Q18 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=2 for DECIMAL columns
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, VARCHAR→raw string (c_name), INTEGER→int32_t

## Tables

### customer
- Rows: 1,500,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_name | customer/c_name.bin | std::string | STRING | none | → |

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 2 |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 2 |

## Indexes

### hash_c_custkey
- File: indexes/hash_c_custkey.bin
- Type: hash_single
- Layout: [uint32_t table_size=2097152] then per slot [int32_t key, uint32_t position] (8B × 2097152)
- Column: c_custkey

### hash_o_custkey
- File: indexes/hash_o_custkey.bin
- Type: hash_multi_value
- Layout: [uint32_t table_size=2097152] then per slot [int32_t key, uint32_t offset, uint32_t count] (12B × 2097152), then [uint32_t positions...]
- Column: o_custkey

### hash_l_orderkey
- File: indexes/hash_l_orderkey.bin
- Type: hash_multi_value
- Layout: [uint32_t table_size=33554432] then per slot [int32_t key, uint32_t offset, uint32_t count] (12B × 33554432), then [uint32_t positions...]
- Column: l_orderkey
