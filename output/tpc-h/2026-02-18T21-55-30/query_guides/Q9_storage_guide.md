# Q9 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=2 for all DECIMAL columns
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, VARCHAR→raw string (p_name), VARCHAR→dictionary (others)

## Tables

### part
- Rows: 2,000,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_name | part/p_name.bin | std::string | STRING | none | → |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 2 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 2 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 2 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

### partsupp
- Rows: 8,000,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | → |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | → |
| ps_supplycost | partsupp/ps_supplycost.bin | int64_t | DECIMAL | none | 2 |

### supplier
- Rows: 100,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | → |

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |

### nation
- Rows: 25, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name.bin | std::string | STRING | none | → |

## Indexes

### hash_p_partkey
- File: indexes/hash_p_partkey.bin
- Type: hash_single
- Layout: [uint32_t table_size=2097152] then per slot [int32_t key, uint32_t position] (8B × 2097152)
- Column: p_partkey

### hash_ps_partkey
- File: indexes/hash_ps_partkey.bin
- Type: hash_multi_value
- Layout: [uint32_t table_size=4194304] then per slot [int32_t key, uint32_t offset, uint32_t count] (12B × 4194304), then [uint32_t positions...]
- Column: ps_partkey

### hash_ps_suppkey
- File: indexes/hash_ps_suppkey.bin
- Type: hash_multi_value
- Layout: [uint32_t table_size=262144] then per slot [int32_t key, uint32_t offset, uint32_t count] (12B × 262144), then [uint32_t positions...]
- Column: ps_suppkey

### hash_l_orderkey
- File: indexes/hash_l_orderkey.bin
- Type: hash_multi_value
- Layout: [uint32_t table_size=33554432] then per slot [int32_t key, uint32_t offset, uint32_t count] (12B × 33554432), then [uint32_t positions...]
- Column: l_orderkey
