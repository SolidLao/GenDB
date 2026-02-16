# Q16 Storage Guide

## Data Encoding
- String encoding: Dictionary-encoded as int8_t or int16_t

## Tables

### partsupp
- Rows: 8,000,000, Block size: 150,000, Sort order: ps_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | - |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | - |

### part
- Rows: 2,000,000, Block size: 150,000, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | - |
| p_brand | part/p_brand.bin | int8_t | STRING | dictionary | - |
| p_type | part/p_type.bin | int16_t | STRING | dictionary | - |
| p_size | part/p_size.bin | int32_t | INTEGER | none | - |

- Dictionary files: p_brand → part/p_brand_dict.txt, p_type → part/p_type_dict.txt

## Indexes

### part_p_brand_hash
- File: indexes/part_p_brand_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [uint32_t table_size] then [key:int8_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count] [positions...]
- Column: p_brand

### partsupp_ps_partkey_hash
- File: indexes/partsupp_ps_partkey_hash.bin
- Type: hash_multi_value
- Column: ps_partkey
