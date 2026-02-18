# Q3 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1,500,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | - |

- Dictionary files: c_mktsegment → customer/c_mktsegment_dict.txt (load at runtime)

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | - |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |

## Indexes

### customer_c_mktsegment_hash
- File: indexes/customer_c_mktsegment_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t num_positions] then [key:int32_t, offset:uint32_t, count:uint32_t] per unique key, then [uint32_t positions...]
- Column: c_mktsegment

### orders_o_custkey_hash
- File: indexes/orders_o_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t num_positions] then [key:int32_t, offset:uint32_t, count:uint32_t] per unique key, then [uint32_t positions...]
- Column: o_custkey

### lineitem_l_orderkey_hash
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t num_positions] then [key:int32_t, offset:uint32_t, count:uint32_t] per unique key, then [uint32_t positions...]
- Column: l_orderkey

### lineitem_l_shipdate_zonemap
- File: indexes/lineitem_l_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_blocks] then [min:int32_t, max:int32_t] per block
- Column: l_shipdate
