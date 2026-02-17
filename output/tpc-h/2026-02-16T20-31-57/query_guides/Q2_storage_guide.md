# Q2 Storage Guide

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
| p_type | part/p_type.bin | int32_t | STRING | dictionary | → |
| p_brand | part/p_brand.bin | int32_t | STRING | dictionary | → |
| p_mfgr | part/p_mfgr.bin | int32_t | STRING | dictionary | → |
| p_name | part/p_name.bin | int32_t | STRING | dictionary | → |
| p_size | part/p_size.bin | int32_t | INTEGER | none | → |

- Dictionary files: p_type → part/p_type_dict.txt, p_brand → part/p_brand_dict.txt, p_mfgr → part/p_mfgr_dict.txt, p_name → part/p_name_dict.txt

### supplier
- Rows: 100000, Block size: 100000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | → |
| s_acctbal | supplier/s_acctbal.bin | int64_t | DECIMAL | none | 2 |
| s_name | supplier/s_name.bin | int32_t | STRING | dictionary | → |
| s_address | supplier/s_address.bin | int32_t | STRING | dictionary | → |
| s_phone | supplier/s_phone.bin | int32_t | STRING | dictionary | → |
| s_comment | supplier/s_comment.bin | int32_t | STRING | dictionary | → |

- Dictionary files: s_name → supplier/s_name_dict.txt, s_address → supplier/s_address_dict.txt, s_phone → supplier/s_phone_dict.txt, s_comment → supplier/s_comment_dict.txt

### partsupp
- Rows: 8000000, Block size: 100000, Sort order: ps_partkey, ps_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | → |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | → |
| ps_supplycost | partsupp/ps_supplycost.bin | int64_t | DECIMAL | none | 2 |

### nation
- Rows: 25, Block size: 10000, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name.bin | int32_t | STRING | dictionary | → |
| n_regionkey | nation/n_regionkey.bin | int32_t | INTEGER | none | → |

- Dictionary files: n_name → nation/n_name_dict.txt

### region
- Rows: 5, Block size: 10000, Sort order: r_regionkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| r_regionkey | region/r_regionkey.bin | int32_t | INTEGER | none | → |
| r_name | region/r_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: r_name → region/r_name_dict.txt

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

### partsupp_partkey_hash
- File: indexes/partsupp_partkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=2000000][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot), then [uint32_t positions_count][uint32_t positions...]
- Column: ps_partkey

### partsupp_suppkey_hash
- File: indexes/partsupp_suppkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=100000][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot), then [uint32_t positions_count][uint32_t positions...]
- Column: ps_suppkey

### nation_nationkey_hash
- File: indexes/nation_nationkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=25] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: n_nationkey

### region_regionkey_hash
- File: indexes/region_regionkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=5] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: r_regionkey
