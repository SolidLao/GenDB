# Q16 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### partsupp
- Rows: 8000000, Block size: 131072, Sort order: ps_partkey, ps_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | → |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | → |

### part
- Rows: 2000000, Block size: 65536, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_brand | part/p_brand_dict.txt | int32_t | STRING | dictionary | → |
| p_type | part/p_type_dict.txt | int32_t | STRING | dictionary | → |
| p_size | part/p_size.bin | int32_t | INTEGER | none | → |

### supplier
- Rows: 100000, Block size: 32768, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_comment | supplier/s_comment_dict.txt | int32_t | STRING | dictionary | → |

## Indexes

### ps_partkey_hash, ps_suppkey_hash, p_partkey_hash, s_suppkey_hash
- See Q2, Q3 for layouts
