# Q15 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
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

### idx_lineitem_shipdate_zmap
- File: indexes/idx_lineitem_shipdate_zmap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min_val, int32_t max_val, uint32_t row_count] (12B/zone)
- Column: l_shipdate
- Purpose: Skip blocks where l_shipdate < 1996-01-01 or >= 1996-04-01 (epoch days)

### idx_supplier_suppkey_hash
- File: indexes/idx_supplier_suppkey_hash.bin
- Type: hash_multi_value
- Column: s_suppkey
