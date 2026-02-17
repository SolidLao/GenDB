# Q22 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1500000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |
| c_phone | customer/c_phone.bin | std::string | STRING | none | - |
| c_acctbal | customer/c_acctbal.bin | int64_t | DECIMAL | none | 100 |

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |

## Indexes

### orders_custkey_hash
- File: indexes/orders_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12 bytes/entry), then [uint32_t pos_count][uint32_t positions...]
- Column: o_custkey
