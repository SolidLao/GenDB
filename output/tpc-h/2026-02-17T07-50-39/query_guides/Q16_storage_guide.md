# Q16 Storage Guide

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
| p_brand | part/p_brand.bin | int32_t | STRING | dictionary | - |
| p_type | part/p_type.bin | std::string | STRING | none | - |
| p_size | part/p_size.bin | int32_t | INTEGER | none | - |

- Dictionary files: p_brand → part/p_brand_dict.txt (load at runtime)

### partsupp
- Rows: 8000000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | - |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | - |

### supplier
- Rows: 100000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | - |
| s_comment | supplier/s_comment.bin | std::string | STRING | none | - |

## Indexes

### part_partkey_hash
- File: indexes/part_partkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [key:int32_t, position:uint32_t] per slot (8 bytes/entry)
- Column: p_partkey

### supplier_suppkey_hash
- File: indexes/supplier_suppkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [key:int32_t, position:uint32_t] per slot (8 bytes/entry)
- Column: s_suppkey
