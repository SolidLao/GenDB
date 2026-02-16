# Q7 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor = 100

## Tables

### supplier
- Rows: 100,000, Block size: 100,000, Sort order: s_suppkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| s_suppkey | supplier/s_suppkey.bin | int32_t | INTEGER | none | - |
| s_nationkey | supplier/s_nationkey.bin | int32_t | INTEGER | none | - |

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_suppkey | lineitem/l_suppkey.bin | int32_t | INTEGER | none | - |
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |

### orders
- Rows: 15,000,000, Block size: 150,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |

### customer
- Rows: 1,500,000, Block size: 150,000, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |
| c_nationkey | customer/c_nationkey.bin | int32_t | INTEGER | none | - |

### nation
- Rows: 25, Block size: 100,000, Sort order: n_nationkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| n_nationkey | nation/n_nationkey.bin | int32_t | INTEGER | none | - |

## Indexes

### supplier_s_nationkey_hash
- File: indexes/supplier_s_nationkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique] [uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count] [positions...]
- Column: s_nationkey

### lineitem_l_suppkey_hash
- File: indexes/lineitem_l_suppkey_hash.bin
- Type: hash_multi_value
- Column: l_suppkey

### lineitem_l_orderkey_hash
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey

### lineitem_l_shipdate_zone
- File: indexes/lineitem_l_shipdate_zone.bin
- Type: zone_map
- Column: l_shipdate

### orders_o_custkey_hash
- File: indexes/orders_o_custkey_hash.bin
- Type: hash_multi_value
- Column: o_custkey

### customer_c_nationkey_hash
- File: indexes/customer_c_nationkey_hash.bin
- Type: hash_multi_value
- Column: c_nationkey
