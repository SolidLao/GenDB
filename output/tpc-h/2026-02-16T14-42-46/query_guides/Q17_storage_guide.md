# Q17 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |

### part
- Rows: 2000000, Block size: 65536, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|---|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_brand | part/p_brand_dict.txt | int32_t | STRING | dictionary | → |
| p_container | part/p_container_dict.txt | int32_t | STRING | dictionary | → |

## Indexes

### l_partkey_hash, p_partkey_hash
- See Q2, Q3 for layouts
