# Q3 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1500000, Block size: 131072, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | → |

- Dictionary files: c_mktsegment → customer/c_mktsegment_dict.txt (5 values)

### orders
- Rows: 15000000, Block size: 131072, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | → |

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

## Indexes

### o_orderdate_zone
- File: indexes/o_orderdate_zone.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=115] then per block: [int32_t min, int32_t max, uint32_t count] (12B each)
- Column: o_orderdate (range filter for date < 1995-03-15)

### c_custkey_hash
- File: indexes/c_custkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [int32_t key, uint32_t pos] per slot (8B)

### o_orderkey_hash
- File: indexes/o_orderkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [int32_t key, uint32_t pos] per slot (8B)

### o_custkey_hash
- File: indexes/o_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then positions array

### l_orderkey_hash
- File: indexes/l_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then positions array
