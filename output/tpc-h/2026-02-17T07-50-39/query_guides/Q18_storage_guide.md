# Q18 Storage Guide

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
| c_name | customer/c_name.bin | std::string | STRING | none | - |

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 100 |

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### customer_custkey_hash
- File: indexes/customer_custkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [key:int32_t, position:uint32_t] per slot (8 bytes/entry)
- Column: c_custkey

### lineitem_orderkey_hash
- File: indexes/lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12 bytes/entry), then [uint32_t pos_count][uint32_t positions...]
- Column: l_orderkey
