# Q16 Storage Guide

## Data Encoding
- Type mappings: INTEGER→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t or raw strings

## Tables

### partsupp
- Rows: 8,000,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | - |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | - |

### part
- Rows: 2,000,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | - |
| p_brand | part/p_brand.bin | int32_t | STRING | dictionary | - |
| p_type | part/p_type.bin | int32_t | STRING | dictionary | - |
| p_size | part/p_size.bin | int32_t | INTEGER | none | - |

- Dictionary files: p_brand → part/p_brand_dict.txt, p_type → part/p_type_dict.txt

### supplier
- Rows: 100,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | - |
| s_comment | supplier/s_comment.bin | std::string | STRING | none | - |

## Indexes

### partsupp_ps_partkey_hash
- File: indexes/partsupp_ps_partkey_hash.bin
- Type: hash_multi_value
- Column: ps_partkey

### part_p_partkey_hash
- File: indexes/part_p_partkey_hash.bin
- Type: hash
- Column: p_partkey

### part_p_brand_hash
- File: indexes/part_p_brand_hash.bin
- Type: hash_multi_value
- Column: p_brand

### supplier_s_suppkey_hash
- File: indexes/supplier_s_suppkey_hash.bin
- Type: hash
- Column: s_suppkey
