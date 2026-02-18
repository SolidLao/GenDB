# Q9 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### part
- Rows: 2000000, Block size: 65536, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | - |
| p_name | part/p_name.bin | int32_t | STRING | dictionary | - |

- Dictionary files: p_name → part/p_name_dict.txt

### supplier
- Rows: 100000, Block size: 65536, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | - |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | - |

### lineitem
- Rows: 59986052, Block size: 65536, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | - |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | - |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |

### partsupp
- Rows: 8000000, Block size: 65536, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | - |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | - |
| ps_supplycost | partsupp/ps_supplycost.bin | int64_t | DECIMAL | none | 100 |

### orders
- Rows: 15000000, Block size: 65536, Sort order: o_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |

### nation
- Rows: 25, Block size: 65536, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | - |
| n_name | nation/n_name.bin | int32_t | STRING | dictionary | - |

- Dictionary files: n_name → nation/n_name_dict.txt

## Indexes

### orders_orderkey_hash
- File: indexes/orders_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t total_positions][uint32_t positions...]
- Column: o_orderkey
- Unique keys: 15000000, table_size: 33554432
