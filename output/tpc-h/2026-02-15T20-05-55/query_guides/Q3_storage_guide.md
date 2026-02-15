# Q3 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (100 for TPC-H)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded uint8_t or std::string

## Tables

### customer
- Rows: 1500000, Block size: 256000, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_mktsegment | customer/c_mktsegment.bin | uint8_t | STRING | dictionary | → |

- Dictionary files: c_mktsegment → customer/c_mktsegment_dict.txt

### orders
- Rows: 15000000, Block size: 256000, Sort order: o_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | → |

### lineitem
- Rows: 59986052, Block size: 256000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### hash_c_custkey
- File: indexes/hash_c_custkey.bin
- Type: hash_single (one-value lookup)
- Layout: [uint32_t num_unique=1500000, uint32_t table_size=4194304] then hash table [int32_t key, uint32_t offset, uint32_t count] (12 bytes per slot), then positions array [uint32_t count, uint32_t pos...]
- Column: c_custkey (equality join)

### zone_map_o_orderdate
- File: indexes/zone_map_o_orderdate.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=59] then per block: [int32_t min, int32_t max, uint32_t count, uint32_t null_count] (16 bytes/block)
- Column: o_orderdate (range predicate filtering)

### hash_o_custkey
- File: indexes/hash_o_custkey.bin
- Type: hash_multi_value (many rows per key)
- Layout: [uint32_t num_unique=999982, uint32_t table_size=2097152] then hash table [int32_t key, uint32_t offset, uint32_t count] (12 bytes per slot), then positions array [uint32_t count, uint32_t pos...]
- Column: o_custkey (join probe from customer)

### hash_l_orderkey
- File: indexes/hash_l_orderkey.bin
- Type: hash_multi_value (many rows per order)
- Layout: [uint32_t num_unique=15000000, uint32_t table_size=33554432] then hash table [int32_t key, uint32_t offset, uint32_t count] (12 bytes per slot), then positions array [uint32_t count, uint32_t pos...]
- Column: l_orderkey (join probe from orders)

