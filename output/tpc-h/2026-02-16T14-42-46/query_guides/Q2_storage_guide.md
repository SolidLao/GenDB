# Q2 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### part
- Rows: 2000000, Block size: 65536, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_mfgr | part/p_mfgr.bin | int32_t | STRING | dictionary | → |
| p_type | part/p_type.bin | int32_t | STRING | dictionary | → |
| p_size | part/p_size.bin | int32_t | INTEGER | none | → |

- Dictionary files: p_mfgr → part/p_mfgr_dict.txt, p_type → part/p_type_dict.txt (150 values)

### supplier
- Rows: 100000, Block size: 32768, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_name | supplier/s_name.bin | int32_t | STRING | dictionary | → |
| s_address | supplier/s_address.bin | int32_t | STRING | dictionary | → |
| s_acctbal | supplier/s_acctbal.bin | int64_t | DECIMAL | none | 100 |

### partsupp
- Rows: 8000000, Block size: 131072, Sort order: ps_partkey, ps_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | → |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | → |
| ps_supplycost | partsupp/ps_supplycost.bin | int64_t | DECIMAL | none | 100 |

### nation
- Rows: 25, Block size: 8192, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name_dict.txt | int32_t | STRING | dictionary | → |
| n_regionkey | nation/n_regionkey.bin | int32_t | INTEGER | none | → |

### region
- Rows: 5, Block size: 8192, Sort order: r_regionkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| r_regionkey | region/r_regionkey.bin | int32_t | INTEGER | none | → |
| r_name | region/r_name_dict.txt | int32_t | STRING | dictionary | → |

## Indexes

### p_partkey_hash
- File: indexes/p_partkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [int32_t key, uint32_t pos] per slot (8B)

### s_suppkey_hash
- File: indexes/s_suppkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [int32_t key, uint32_t pos] per slot (8B)

### ps_partkey_hash
- File: indexes/ps_partkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then positions array

### ps_suppkey_hash
- File: indexes/ps_suppkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then positions array

### n_nationkey_hash
- File: indexes/n_nationkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [int32_t key, uint32_t pos] per slot (8B)

### r_regionkey_hash
- File: indexes/r_regionkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [int32_t key, uint32_t pos] per slot (8B)
