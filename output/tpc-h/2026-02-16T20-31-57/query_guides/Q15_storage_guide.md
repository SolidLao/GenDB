# Q15 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor: 2
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 2 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 2 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

### supplier
- Rows: 100000, Block size: 100000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | → |
| s_name | supplier/s_name.bin | int32_t | STRING | dictionary | → |
| s_address | supplier/s_address.bin | int32_t | STRING | dictionary | → |
| s_phone | supplier/s_phone.bin | int32_t | STRING | dictionary | → |

- Dictionary files: s_name → supplier/s_name_dict.txt, s_address → supplier/s_address_dict.txt, s_phone → supplier/s_phone_dict.txt

## Indexes

### lineitem_suppkey_hash
- File: indexes/lineitem_suppkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=100000][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot), then [uint32_t positions_count][uint32_t positions...]
- Column: l_suppkey

### supplier_suppkey_hash
- File: indexes/supplier_suppkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries=100000] then [key:int32_t, pos:uint32_t] (8B/entry)
- Column: s_suppkey

### lineitem_shipdate_zonemap
- File: indexes/lineitem_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=600] then [int32_t min, int32_t max, uint32_t row_count] per block (12 bytes/block)
- Column: l_shipdate
