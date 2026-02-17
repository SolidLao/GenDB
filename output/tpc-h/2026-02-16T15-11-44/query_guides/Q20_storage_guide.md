# Q20 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### supplier
- Rows: 100000, Block size: 100000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_name | supplier/s_name.bin | int32_t | STRING | dictionary | → |
| s_address | supplier/s_address.bin | int32_t | STRING | dictionary | → |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | → |

- Dictionary files: s_name → supplier/s_name_dict.txt, s_address → supplier/s_address_dict.txt

### nation
- Rows: 25, Block size: 100000, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: n_name → nation/n_name_dict.txt

### part
- Rows: 2000000, Block size: 100000, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_name | part/p_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: p_name → part/p_name_dict.txt

### partsupp
- Rows: 8000000, Block size: 100000, Sort order: ps_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | → |
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | → |
| ps_availqty | partsupp/ps_availqty.bin | int32_t | INTEGER | none | → |

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

## Indexes

### idx_supplier_suppkey_hash
- File: indexes/idx_supplier_suppkey_hash.bin
- Type: hash_multi_value
- Column: s_suppkey

### idx_supplier_nationkey_hash
- File: indexes/idx_supplier_nationkey_hash.bin
- Type: hash_multi_value
- Column: s_nationkey

### idx_nation_nationkey_hash
- File: indexes/idx_nation_nationkey_hash.bin
- Type: hash_multi_value
- Column: n_nationkey

### idx_part_partkey_hash
- File: indexes/idx_part_partkey_hash.bin
- Type: hash_multi_value
- Column: p_partkey

### idx_partsupp_partkey_hash
- File: indexes/idx_partsupp_partkey_hash.bin
- Type: hash_multi_value
- Column: ps_partkey

### idx_partsupp_suppkey_hash
- File: indexes/idx_partsupp_suppkey_hash.bin
- Type: hash_multi_value
- Column: ps_suppkey
