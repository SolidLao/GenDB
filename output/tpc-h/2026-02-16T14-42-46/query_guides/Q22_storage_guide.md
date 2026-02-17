# Q22 Storage Guide

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
| c_phone | customer/c_phone_dict.txt | int32_t | STRING | dictionary | → |
| c_acctbal | customer/c_acctbal.bin | int64_t | DECIMAL | none | 100 |

### orders
- Rows: 15000000, Block size: 131072, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |

## Indexes

### c_custkey_hash, o_custkey_hash
- See Q3 for layouts
