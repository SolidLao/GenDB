# Q9 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor: 2
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR→dictionary-encoded int32_t

## Tables

### part
- Rows: 2000000, Block size: 100000, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_name | part/p_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: p_name → part/p_name_dict.txt

### supplier
- Rows: 100000, Block size: 100000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | → |

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 2 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 2 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 2 |

### partsupp
- Rows: 8000000, Block size: 100000, Sort order: ps_partkey, ps_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | → |
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | → |
| ps_supplycost | partsupp/ps_supplycost.bin | int64_t | DECIMAL | none | 2 |

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |

### nation
- Rows: 25, Block size: 10000, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: n_name → nation/n_name_dict.txt

## Indexes

### part_partkey_hash
- File: indexes/part_partkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=2000000] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: p_partkey

### supplier_suppkey_hash
- File: indexes/supplier_suppkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=100000] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: s_suppkey

### lineitem_suppkey_hash
- File: indexes/lineitem_suppkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=100000][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot), then [uint32_t positions_count][uint32_t positions...]
- Column: l_suppkey

### lineitem_partkey_hash (implicit from l_partkey column)
- Uses lineitem/l_partkey.bin, would be indexed similarly if needed

### partsupp_suppkey_hash
- File: indexes/partsupp_suppkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=100000][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot), then [uint32_t positions_count][uint32_t positions...]
- Column: ps_suppkey

### partsupp_partkey_hash
- File: indexes/partsupp_partkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=2000000][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot), then [uint32_t positions_count][uint32_t positions...]
- Column: ps_partkey

### orders_orderkey_hash
- File: indexes/orders_orderkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=15000000] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: o_orderkey

### nation_nationkey_hash
- File: indexes/nation_nationkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=25] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: n_nationkey
