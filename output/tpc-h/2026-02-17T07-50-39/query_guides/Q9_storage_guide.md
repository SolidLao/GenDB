# Q9 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### part
- Rows: 2000000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | - |
| p_name | part/p_name.bin | std::string | STRING | none | - |

### partsupp
- Rows: 8000000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | - |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | - |
| ps_supplycost | partsupp/ps_supplycost.bin | int64_t | DECIMAL | none | 100 |

### supplier
- Rows: 100000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | - |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | - |

### nation
- Rows: 25, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | - |
| n_name | nation/n_name.bin | std::string | STRING | none | - |

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | - |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | - |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### part_partkey_hash
- File: indexes/part_partkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [key:int32_t, position:uint32_t] per slot (8 bytes/entry)
- Column: p_partkey

### partsupp_partkey_suppkey_hash
- File: indexes/partsupp_partkey_suppkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [key1:int32_t, key2:int32_t, offset:uint32_t, count:uint32_t] per slot (16 bytes/entry)
- Column: ps_partkey, ps_suppkey

### supplier_suppkey_hash
- File: indexes/supplier_suppkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [key:int32_t, position:uint32_t] per slot (8 bytes/entry)
- Column: s_suppkey

### lineitem_partkey_suppkey_hash
- File: indexes/lineitem_partkey_suppkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [key1:int32_t, key2:int32_t, offset:uint32_t, count:uint32_t] per slot (16 bytes/entry), then [uint32_t pos_count][uint32_t positions...]
- Column: l_partkey, l_suppkey
