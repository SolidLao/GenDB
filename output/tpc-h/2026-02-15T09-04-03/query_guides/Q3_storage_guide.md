# Q3 Storage Guide

## Tables

### customer
- Rows: 1,500,000, Block size: 200,000, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_name | customer/c_name.bin | std::string | STRING | none | → |
| c_address | customer/c_address.bin | std::string | STRING | none | → |
| c_nationkey | customer/c_nationkey.bin | int32_t | INTEGER | none | → |
| c_phone | customer/c_phone.bin | std::string | STRING | none | → |
| c_acctbal | customer/c_acctbal.bin | int64_t | DECIMAL | none | 100 |
| c_mktsegment | customer/c_mktsegment.bin | uint8_t | STRING | dictionary | → |
| c_comment | customer/c_comment.bin | std::string | STRING | none | → |

- Dictionary files: c_mktsegment → customer/c_mktsegment_dict.txt

### orders
- Rows: 15,000,000, Block size: 300,000, Sort order: o_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderstatus | orders/o_orderstatus.bin | uint8_t | STRING | dictionary | → |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 100 |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_orderpriority | orders/o_orderpriority.bin | std::string | STRING | none | → |
| o_clerk | orders/o_clerk.bin | std::string | STRING | none | → |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | → |
| o_comment | orders/o_comment.bin | std::string | STRING | none | → |

- Dictionary files: o_orderstatus → orders/o_orderstatus_dict.txt

### lineitem
- Rows: 59,986,052, Block size: 500,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### customer_custkey_hash
- File: indexes/customer_custkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries (1500000)] then [int32_t key, uint32_t position] per entry (8B)
- Column: c_custkey

### orders_custkey_hash
- File: indexes/orders_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique (999982)][uint32_t table_size (1999964)] then [int32_t key, uint32_t offset, uint32_t count] per hash slot (12B), then [uint32_t positions...] array
- Column: o_custkey

### orders_orderkey_hash
- File: indexes/orders_orderkey_hash.bin
- Type: hash_single
- Layout: [uint32_t num_entries (15000000)] then [int32_t key, uint32_t position] per entry (8B)
- Column: o_orderkey

### orders_orderdate_zone
- File: indexes/orders_orderdate_zone.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min_val, int32_t max_val, uint32_t block_num, uint32_t row_count] (16B/entry)
- 50 zones total
- Column: o_orderdate

### lineitem_orderkey_hash
- File: indexes/lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique (15000000)][uint32_t table_size (30000000)] then [int32_t key, uint32_t offset, uint32_t count] per hash slot (12B), then [uint32_t positions...] array
- Column: l_orderkey
