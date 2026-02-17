# Q19 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipmode | lineitem/l_shipmode.bin | int32_t | STRING | dictionary | → |
| l_shipinstruct | lineitem/l_shipinstruct.bin | int32_t | STRING | dictionary | → |

- Dictionary files: l_shipmode → lineitem/l_shipmode_dict.txt, l_shipinstruct → lineitem/l_shipinstruct_dict.txt

### part
- Rows: 2000000, Block size: 100000, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_brand | part/p_brand.bin | int32_t | STRING | dictionary | → |
| p_size | part/p_size.bin | int32_t | INTEGER | none | → |
| p_container | part/p_container.bin | int32_t | STRING | dictionary | → |

- Dictionary files: p_brand → part/p_brand_dict.txt, p_container → part/p_container_dict.txt

## Indexes

### idx_part_partkey_hash
- File: indexes/idx_part_partkey_hash.bin
- Type: hash_multi_value
- Column: p_partkey
