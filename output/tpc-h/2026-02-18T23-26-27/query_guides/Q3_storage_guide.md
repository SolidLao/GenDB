# Q3 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100 per DECIMAL column
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1500000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | → |

- Dictionary files: c_mktsegment → customer/c_mktsegment_dict.txt (5 entries)

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | → |

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

## Indexes

### customer_c_custkey
- File: indexes/customer_c_custkey.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [uint8_t occupied, key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count][uint32_t positions...]
- Column: c_custkey

### orders_o_custkey
- File: indexes/orders_o_custkey.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [uint8_t occupied, key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count][uint32_t positions...]
- Column: o_custkey

### orders_o_orderdate_zonemap
- File: indexes/orders_o_orderdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=150] then per block: [int32_t min, int32_t max, uint32_t block_size]
- Column: o_orderdate

### lineitem_l_orderkey
- File: indexes/lineitem_l_orderkey.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [uint8_t occupied, key:int32_t, offset:uint32_t, count:uint32_t] per slot, then [uint32_t pos_count][uint32_t positions...]
- Column: l_orderkey

### lineitem_l_shipdate_zonemap
- File: indexes/lineitem_l_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=600] then per block: [int32_t min, int32_t max, uint32_t block_size]
- Column: l_shipdate
