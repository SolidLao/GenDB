# Q9 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t or raw strings

## Tables

### part
- Rows: 2,000,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | - |
| p_name | part/p_name.bin | std::string | STRING | none | - |

### supplier
- Rows: 100,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | - |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | - |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | - |
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | - |
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |

### partsupp
- Rows: 8,000,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | - |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | - |
| ps_supplycost | partsupp/ps_supplycost.bin | int64_t | DECIMAL | none | 100 |

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |

### nation
- Rows: 25, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | - |
| n_name | nation/n_name.bin | std::string | STRING | none | - |

## Indexes

### lineitem_l_suppkey_hash
- File: indexes/lineitem_l_suppkey_hash.bin
- Type: hash_multi_value
- Column: l_suppkey

### lineitem_l_orderkey_hash
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey

### partsupp_ps_partkey_hash
- File: indexes/partsupp_ps_partkey_hash.bin
- Type: hash_multi_value
- Column: ps_partkey

### partsupp_ps_suppkey_hash
- File: indexes/partsupp_ps_suppkey_hash.bin
- Type: hash_multi_value
- Column: ps_suppkey

### supplier_s_suppkey_hash
- File: indexes/supplier_s_suppkey_hash.bin
- Type: hash
- Column: s_suppkey

### supplier_s_nationkey_hash
- File: indexes/supplier_s_nationkey_hash.bin
- Type: hash_multi_value
- Column: s_nationkey

### nation_n_nationkey_hash
- File: indexes/nation_n_nationkey_hash.bin
- Type: hash
- Column: n_nationkey
