# Q18 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 65536, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |

### orders
- Rows: 15000000, Block size: 65536, Sort order: o_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 100 |

### customer
- Rows: 1500000, Block size: 65536, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |
| c_name | customer/c_name.bin | int32_t | STRING | dictionary | - |

- Dictionary files: c_name → customer/c_name_dict.txt

## Indexes

### lineitem_orderkey_hash
- File: indexes/lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t total_positions][uint32_t positions...]
- Column: l_orderkey
- Unique keys: 15000000, table_size: 33554432
- Usage: subquery grouping SUM(l_quantity) per l_orderkey, then semi-join filter HAVING > 300*100 (scaled)

### orders_orderkey_hash
- File: indexes/orders_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t total_positions][uint32_t positions...]
- Column: o_orderkey
- Unique keys: 15000000, table_size: 33554432

### orders_custkey_hash
- File: indexes/orders_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t total_positions][uint32_t positions...]
- Column: o_custkey
- Unique keys: 999982, table_size: 2097152

### customer_custkey_hash
- File: indexes/customer_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t total_positions][uint32_t positions...]
- Column: c_custkey
- Unique keys: 1500000, table_size: 4194304
