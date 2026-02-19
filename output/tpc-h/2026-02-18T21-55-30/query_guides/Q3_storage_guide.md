# Q3 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=2 for all DECIMAL columns
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, VARCHAR→dictionary or raw string

## Tables

### customer
- Rows: 1,500,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | → |

- Dictionary files: c_mktsegment → customer/c_mktsegment_dict.txt

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | → |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 2 |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 2 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 2 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

## Indexes

### zone_map_o_orderdate
- File: indexes/zone_map_o_orderdate.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=150] then [int32_t min, int32_t max, uint32_t count] × 150
- Column: o_orderdate

### hash_o_custkey
- File: indexes/hash_o_custkey.bin
- Type: hash_multi_value
- Layout: [uint32_t table_size=2097152] then per slot [int32_t key, uint32_t offset, uint32_t count] (12B × 2097152 slots), then [uint32_t positions...] (15M positions)
- Column: o_custkey

### hash_l_orderkey
- File: indexes/hash_l_orderkey.bin
- Type: hash_multi_value
- Layout: [uint32_t num_entries=59986052, uint32_t table_size=33554432] then per slot [int32_t key, uint32_t offset, uint32_t count] (12B × 33554432), then [uint32_t positions...] (60M positions)
- Column: l_orderkey
