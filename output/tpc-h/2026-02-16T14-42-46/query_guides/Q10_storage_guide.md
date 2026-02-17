# Q10 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1500000, Block size: 131072, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_name | customer/c_name_dict.txt | int32_t | STRING | dictionary | → |
| c_address | customer/c_address_dict.txt | int32_t | STRING | dictionary | → |
| c_acctbal | customer/c_acctbal.bin | int64_t | DECIMAL | none | 100 |
| c_phone | customer/c_phone_dict.txt | int32_t | STRING | dictionary | → |
| c_nationkey | customer/c_nationkey.bin | int32_t | INTEGER | none | → |

### orders
- Rows: 15000000, Block size: 131072, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_returnflag | lineitem/l_returnflag.bin | int32_t | STRING | dictionary | → |

### nation
- Rows: 25, Block size: 8192, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name_dict.txt | int32_t | STRING | dictionary | → |

## Indexes

### o_orderdate_zone
- File: indexes/o_orderdate_zone.bin
- Type: zone_map
- Column: o_orderdate (range filter 1993-10-01 to 1993-12-31)

### c_custkey_hash, o_orderkey_hash, o_custkey_hash, l_orderkey_hash, n_nationkey_hash
- See Q3 for layouts
