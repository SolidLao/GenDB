# Q21 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Type mappings: INTEGER→int32_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t or raw strings

## Tables

### supplier
- Rows: 100,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | - |
| s_name | supplier/s_name.bin | std::string | STRING | none | - |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | - |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | - |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | - |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | - |

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_orderstatus | orders/o_orderstatus.bin | int32_t | STRING | dictionary | - |

- Dictionary files: o_orderstatus → orders/o_orderstatus_dict.txt

### nation
- Rows: 25, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | - |
| n_name | nation/n_name.bin | std::string | STRING | none | - |

## Indexes

### supplier_s_suppkey_hash
- File: indexes/supplier_s_suppkey_hash.bin
- Type: hash
- Column: s_suppkey

### supplier_s_nationkey_hash
- File: indexes/supplier_s_nationkey_hash.bin
- Type: hash_multi_value
- Column: s_nationkey

### lineitem_l_suppkey_hash
- File: indexes/lineitem_l_suppkey_hash.bin
- Type: hash_multi_value
- Column: l_suppkey

### lineitem_l_orderkey_hash
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey

### orders_o_orderkey_hash
- File: indexes/orders_o_orderkey_hash.bin
- Type: hash
- Column: o_orderkey

### nation_n_nationkey_hash
- File: indexes/nation_n_nationkey_hash.bin
- Type: hash
- Column: n_nationkey
