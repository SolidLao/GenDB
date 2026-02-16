# Q20 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor = 100

## Tables

### partsupp
- Rows: 8,000,000, Block size: 150,000, Sort order: ps_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | - |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | - |
| ps_availqty | partsupp/ps_availqty.bin | int32_t | INTEGER | none | - |

### part
- Rows: 2,000,000, Block size: 150,000, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | - |

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | - |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | - |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |

### supplier
- Rows: 100,000, Block size: 100,000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | - |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | - |

### nation
- Rows: 25, Block size: 100,000, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | - |

## Indexes

### partsupp_ps_partkey_hash
- File: indexes/partsupp_ps_partkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count] [positions...]
- Column: ps_partkey

### partsupp_ps_suppkey_hash
- File: indexes/partsupp_ps_suppkey_hash.bin
- Type: hash_multi_value
- Column: ps_suppkey

### lineitem_l_partkey_hash
- File: indexes/lineitem_l_partkey_hash.bin
- Type: hash_multi_value
- Column: l_partkey

### lineitem_l_suppkey_hash
- File: indexes/lineitem_l_suppkey_hash.bin
- Type: hash_multi_value
- Column: l_suppkey

### lineitem_l_shipdate_zone
- File: indexes/lineitem_l_shipdate_zone.bin
- Type: zone_map
- Column: l_shipdate

### supplier_s_nationkey_hash
- File: indexes/supplier_s_nationkey_hash.bin
- Type: hash_multi_value
- Column: s_nationkey
