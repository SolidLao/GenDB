# Q3 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1500000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | - |

- Dictionary files: c_mktsegment → customer/c_mktsegment_dict.txt

### orders
- Rows: 15000000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | - |

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### customer_c_custkey_hash
- File: indexes/customer_c_custkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_unique][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12 bytes/entry)
- Column: c_custkey

### orders_o_orderkey_hash
- File: indexes/orders_o_orderkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_unique][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12 bytes/entry)
- Column: o_orderkey

### orders_o_custkey_hash
- File: indexes/orders_o_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12 bytes/entry), then [uint32_t pos_count][uint32_t positions...]
- Column: o_custkey

### orders_o_orderdate_zonemap
- File: indexes/orders_o_orderdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_entries] then per entry: [min_val:int32_t, max_val:int32_t, start_row:uint32_t, row_count:uint32_t] (16 bytes/entry)
- Column: o_orderdate

### lineitem_l_orderkey_hash
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12 bytes/entry), then [uint32_t pos_count][uint32_t positions...]
- Column: l_orderkey

### lineitem_l_shipdate_zonemap
- File: indexes/lineitem_l_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_entries] then per entry: [min_val:int32_t, max_val:int32_t, start_row:uint32_t, row_count:uint32_t] (16 bytes/entry)
- Column: l_shipdate
