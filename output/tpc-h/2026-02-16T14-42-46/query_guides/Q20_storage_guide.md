# Q20 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### supplier
- Rows: 100000, Block size: 32768, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_name | supplier/s_name_dict.txt | int32_t | STRING | dictionary | → |
| s_address | supplier/s_address_dict.txt | int32_t | STRING | dictionary | → |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | → |

### nation
- Rows: 25, Block size: 8192, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name_dict.txt | int32_t | STRING | dictionary | → |

### partsupp
- Rows: 8000000, Block size: 131072, Sort order: ps_partkey, ps_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| ps_suppkey | partsupp/ps_suppkey.bin | int32_t | INTEGER | none | → |
| ps_partkey | partsupp/ps_partkey.bin | int32_t | INTEGER | none | → |
| ps_availqty | partsupp/ps_availqty.bin | int32_t | INTEGER | none | → |

### part
- Rows: 2000000, Block size: 65536, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_name | part/p_name_dict.txt | int32_t | STRING | dictionary | → |

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

## Indexes

### l_shipdate_zone
- File: indexes/l_shipdate_zone.bin
- Type: zone_map
- Column: l_shipdate (range filter 1994-01-01 to 1994-12-31)

### s_suppkey_hash, n_nationkey_hash, ps_suppkey_hash, ps_partkey_hash, p_partkey_hash, l_suppkey_hash
- See Q2, Q3 for layouts
