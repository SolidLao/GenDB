# Q21 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### supplier
- Rows: 100000, Block size: 100000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_name | supplier/s_name.bin | int32_t | STRING | dictionary | → |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | → |

- Dictionary files: s_name → supplier/s_name_dict.txt

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | → |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | → |

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_orderstatus | orders/o_orderstatus.bin | int32_t | STRING | dictionary | → |

- Dictionary files: o_orderstatus → orders/o_orderstatus_dict.txt

### nation
- Rows: 25, Block size: 100000, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: n_name → nation/n_name_dict.txt

## Indexes

### idx_supplier_suppkey_hash
- File: indexes/idx_supplier_suppkey_hash.bin
- Type: hash_multi_value
- Column: s_suppkey

### idx_supplier_nationkey_hash
- File: indexes/idx_supplier_nationkey_hash.bin
- Type: hash_multi_value
- Column: s_nationkey

### idx_lineitem_orderkey_hash
- File: indexes/idx_lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey

### idx_nation_nationkey_hash
- File: indexes/idx_nation_nationkey_hash.bin
- Type: hash_multi_value
- Column: n_nationkey
