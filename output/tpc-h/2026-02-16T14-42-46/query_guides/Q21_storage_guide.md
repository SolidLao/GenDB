# Q21 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### supplier
- Rows: 100000, Block size: 32768, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_name | supplier/s_name_dict.txt | int32_t | STRING | dictionary | → |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | → |

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | → |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | → |

### orders
- Rows: 15000000, Block size: 131072, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_orderstatus | orders/o_orderstatus_dict.txt | int32_t | STRING | dictionary | → |

### nation
- Rows: 25, Block size: 8192, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name_dict.txt | int32_t | STRING | dictionary | → |

## Indexes

### s_suppkey_hash, l_suppkey_hash, l_orderkey_hash, o_orderkey_hash, n_nationkey_hash
- See Q2, Q3 for layouts
