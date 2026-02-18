# Q3 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1500000, Block size: 65536, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | - |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | - |

- Dictionary files: c_mktsegment → customer/c_mktsegment_dict.txt

### orders
- Rows: 15000000, Block size: 65536, Sort order: o_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | - |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | - |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | - |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | - |

### lineitem
- Rows: 59986052, Block size: 65536, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | - |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | - |

## Indexes

### lineitem_shipdate_zonemap
- File: indexes/lineitem_shipdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_blocks] then per block: [int32_t min, int32_t max, uint64_t row_start, uint32_t row_count] (20 bytes/entry)
- Column: l_shipdate
- Num blocks: 916 (65536 rows/block)
- Usage: predicate l_shipdate > DATE '1995-03-15'; skip blocks where max <= threshold

### lineitem_orderkey_hash
- File: indexes/lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t total_positions][uint32_t positions...]
- Column: l_orderkey
- Unique keys: 15000000, table_size: 33554432

### orders_custkey_hash
- File: indexes/orders_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t total_positions][uint32_t positions...]
- Column: o_custkey
- Unique keys: 999982, table_size: 2097152

### customer_custkey_hash
- File: indexes/customer_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t total_positions][uint32_t positions...]
- Column: c_custkey
- Unique keys: 1500000, table_size: 4194304
