# Q5 Storage Guide

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
| c_nationkey | customer/c_nationkey.bin | int32_t | INTEGER | none | → |

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |

### supplier
- Rows: 100000, Block size: 100000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | → |

### nation
- Rows: 25, Block size: 100000, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name.bin | int32_t | STRING | dictionary | → |
| n_regionkey | nation/n_regionkey.bin | int32_t | INTEGER | none | → |

- Dictionary files: n_name → nation/n_name_dict.txt

### region
- Rows: 5, Block size: 100000, Sort order: r_regionkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| r_regionkey | region/r_regionkey.bin | int32_t | INTEGER | none | → |
| r_name | region/r_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: r_name → region/r_name_dict.txt

## Indexes

### idx_customer_custkey_hash
- File: indexes/idx_customer_custkey_hash.bin
- Type: hash_multi_value
- Column: c_custkey

### idx_customer_nationkey_hash
- File: indexes/idx_customer_nationkey_hash.bin
- Type: hash_multi_value
- Column: c_nationkey

### idx_orders_custkey_hash
- File: indexes/idx_orders_custkey_hash.bin
- Type: hash_multi_value
- Column: o_custkey

### idx_lineitem_orderkey_hash
- File: indexes/idx_lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey

### idx_supplier_suppkey_hash
- File: indexes/idx_supplier_suppkey_hash.bin
- Type: hash_multi_value
- Column: s_suppkey

### idx_supplier_nationkey_hash
- File: indexes/idx_supplier_nationkey_hash.bin
- Type: hash_multi_value
- Column: s_nationkey

### idx_nation_nationkey_hash
- File: indexes/idx_nation_nationkey_hash.bin
- Type: hash_multi_value
- Column: n_nationkey

### idx_nation_regionkey_hash
- File: indexes/idx_nation_regionkey_hash.bin
- Type: hash_multi_value
- Column: n_regionkey
