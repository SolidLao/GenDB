# Q14 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor: 2
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_partkey | lineitem/l_partkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 2 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 2 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

### part
- Rows: 2000000, Block size: 100000, Sort order: p_partkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| p_partkey | part/p_partkey.bin | int32_t | INTEGER | none | → |
| p_type | part/p_type.bin | int32_t | STRING | dictionary | → |

- Dictionary files: p_type → part/p_type_dict.txt

## Indexes

### part_partkey_hash
- File: indexes/part_partkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=2000000] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: p_partkey

### lineitem_shipdate_zonemap
- File: indexes/lineitem_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=600] then [int32_t min, int32_t max, uint32_t row_count] per block (12 bytes/block)
- Column: l_shipdate
