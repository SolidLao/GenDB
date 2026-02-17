# Q18 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1500000, Block size: 131072, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_name | customer/c_name_dict.txt | int32_t | STRING | dictionary | → |
| c_acctbal | customer/c_acctbal.bin | int64_t | DECIMAL | none | 100 |

### orders
- Rows: 15000000, Block size: 131072, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 100 |

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### c_custkey_hash, o_orderkey_hash, o_custkey_hash, l_orderkey_hash
- See Q3 for layouts
