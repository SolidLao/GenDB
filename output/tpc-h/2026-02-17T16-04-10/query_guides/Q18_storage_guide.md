# Q18 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→raw strings

## Tables

### customer
- Rows: 1,500,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |
| c_name | customer/c_name.bin | std::string | STRING | none | - |

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 100 |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### customer_c_custkey_hash
- File: indexes/customer_c_custkey_hash.bin
- Type: hash
- Column: c_custkey

### orders_o_custkey_hash
- File: indexes/orders_o_custkey_hash.bin
- Type: hash_multi_value
- Column: o_custkey

### lineitem_l_orderkey_hash
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey
