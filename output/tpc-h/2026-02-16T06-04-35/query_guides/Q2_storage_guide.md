# Q2 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor = 100
- String encoding: Dictionary-encoded as int8_t or int16_t

## Tables

### part
- Rows: 2,000,000, Block size: 150,000, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | - |
| p_brand | part/p_brand.bin | int8_t | STRING | dictionary | - |
| p_type | part/p_type.bin | int16_t | STRING | dictionary | - |
| p_size | part/p_size.bin | int32_t | INTEGER | none | - |

- Dictionary files: p_brand → part/p_brand_dict.txt, p_type → part/p_type_dict.txt

### supplier
- Rows: 100,000, Block size: 100,000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | - |
| s_acctbal | supplier/s_acctbal.bin | int64_t | DECIMAL | none | 100 |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | - |

### partsupp
- Rows: 8,000,000, Block size: 150,000, Sort order: ps_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | - |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | - |
| ps_supplycost | partsupp/ps_supplycost.bin | int64_t | DECIMAL | none | 100 |

### nation
- Rows: 25, Block size: 100,000, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | - |
| n_regionkey | nation/n_regionkey.bin | int32_t | INTEGER | none | - |

### region
- Rows: 5, Block size: 100,000, Sort order: r_regionkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| r_regionkey | region/r_regionkey.bin | int32_t | INTEGER | none | - |

## Indexes

### part_p_brand_type_hash
- File: indexes/part_p_brand_hash.bin (prototype uses p_brand only)
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [uint32_t table_size] then [key:int8_t, offset:uint32_t, count:uint32_t] per slot (12 bytes/entry), then [uint32_t pos_count] [positions...]
- Column: p_brand

### nation_n_regionkey_hash
- File: indexes/nation_n_regionkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count] [positions...]
- Column: n_regionkey

### supplier_s_nationkey_hash
- File: indexes/supplier_s_nationkey_hash.bin
- Type: hash_multi_value
- Column: s_nationkey

### partsupp_ps_suppkey_hash
- File: indexes/partsupp_ps_suppkey_hash.bin
- Type: hash_multi_value
- Column: ps_suppkey

### partsupp_ps_partkey_hash
- File: indexes/partsupp_ps_partkey_hash.bin
- Type: hash_multi_value
- Column: ps_partkey
