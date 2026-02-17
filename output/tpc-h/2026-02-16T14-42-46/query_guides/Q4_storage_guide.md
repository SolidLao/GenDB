# Q4 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### orders
- Rows: 15000000, Block size: 131072, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_orderpriority | orders/o_orderpriority.bin | int32_t | STRING | dictionary | → |

- Dictionary files: o_orderpriority → orders/o_orderpriority_dict.txt (5 values)

### lineitem
- Rows: 59986052, Block size: 262144, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_commitdate | lineitem/l_commitdate.bin | int32_t | DATE | none | → |
| l_receiptdate | lineitem/l_receiptdate.bin | int32_t | DATE | none | → |

## Indexes

### o_orderdate_zone
- File: indexes/o_orderdate_zone.bin
- Type: zone_map
- Layout: [uint32_t num_blocks=115] then per block: [int32_t min, int32_t max, uint32_t count] (12B each)
- Column: o_orderdate (range filter for 1993-07-01 to 1993-09-30)

### o_orderkey_hash
- File: indexes/o_orderkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries][uint32_t table_size] then [int32_t key, uint32_t pos] per slot (8B)

### l_orderkey_hash
- File: indexes/l_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then positions array
