# Q19 Storage Guide

## Data Encoding
- Decimal encoding: scaled integers (int64_t), scale_factor = 100
- String encoding: Dictionary-encoded as int8_t

## Tables

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | - |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipmode | lineitem/l_shipmode.bin | int8_t | STRING | dictionary | - |
| l_shipinstruct | lineitem/l_shipinstruct.bin | int8_t | STRING | dictionary | - |

- Dictionary files: l_shipmode → lineitem/l_shipmode_dict.txt, l_shipinstruct → lineitem/l_shipinstruct_dict.txt

### part
- Rows: 2,000,000, Block size: 150,000, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|----------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | - |
| p_brand | part/p_brand.bin | int8_t | STRING | dictionary | - |
| p_container | part/p_container.bin | int8_t | STRING | dictionary | - |
| p_size | part/p_size.bin | int32_t | INTEGER | none | - |

- Dictionary files: p_brand → part/p_brand_dict.txt, p_container → part/p_container_dict.txt

## Indexes

### lineitem_l_partkey_hash
- File: indexes/lineitem_l_partkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count] [positions...]
- Column: l_partkey
