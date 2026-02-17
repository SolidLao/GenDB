# Q10 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1500000, Block size: 100000, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_name | customer/c_name.bin | int32_t | STRING | dictionary | → |
| c_address | customer/c_address.bin | int32_t | STRING | dictionary | → |
| c_phone | customer/c_phone.bin | int32_t | STRING | dictionary | → |
| c_acctbal | customer/c_acctbal.bin | int64_t | DECIMAL | none | 100 |
| c_nationkey | customer/c_nationkey.bin | int32_t | INTEGER | none | → |
| c_comment | customer/c_comment.bin | int32_t | STRING | dictionary | → |

- Dictionary files: c_name → customer/c_name_dict.txt, c_address → customer/c_address_dict.txt, c_phone → customer/c_phone_dict.txt, c_comment → customer/c_comment_dict.txt

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_returnflag | lineitem/l_returnflag.bin | int32_t | STRING | dictionary | → |

- Dictionary files: l_returnflag → lineitem/l_returnflag_dict.txt

### nation
- Rows: 25, Block size: 100000, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | → |
| n_name | nation/n_name.bin | int32_t | STRING | dictionary | → |

- Dictionary files: n_name → nation/n_name_dict.txt

## Indexes

### idx_customer_custkey_hash
- File: indexes/idx_customer_custkey_hash.bin
- Type: hash_multi_value
- Column: c_custkey

### idx_customer_nationkey_hash
- File: indexes/idx_customer_nationkey_hash.bin
- Type: hash_multi_value
- Column: c_nationkey

### idx_orders_custkey_hash
- File: indexes/idx_orders_custkey_hash.bin
- Type: hash_multi_value
- Column: o_custkey

### idx_orders_orderdate_zmap
- File: indexes/idx_orders_orderdate_zmap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min_val, int32_t max_val, uint32_t row_count] (12B/zone)
- Column: o_orderdate
- Purpose: Skip blocks where o_orderdate < 1993-10-01 or >= 1994-01-01 (epoch days)

### idx_lineitem_orderkey_hash
- File: indexes/idx_lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey

### idx_nation_nationkey_hash
- File: indexes/idx_nation_nationkey_hash.bin
- Type: hash_multi_value
- Column: n_nationkey
