# Q18 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor per column (see table below)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int16_t

## Tables

### customer
- Rows: 1500000, Block size: 100000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | -> |
| c_name | customer/c_name.bin | int16_t | STRING | dictionary | -> |

- Dictionary files: c_name → customer/c_name_dict.txt

### orders
- Rows: 15000000, Block size: 100000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | -> |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | -> |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | -> |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 100 |

### lineitem
- Rows: 59986052, Block size: 100000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | -> |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 1 |

## Indexes

### customer_c_custkey
- File: indexes/customer_c_custkey.bin
- Type: hash_single
- Layout: [uint32_t table_size] then [int32_t key, uint32_t row_pos] per slot (8B/slot)
- Column: c_custkey

### orders_o_custkey
- File: indexes/orders_o_custkey.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t pos_count][uint32_t positions...]
- Column: o_custkey

### lineitem_l_orderkey
- File: indexes/lineitem_l_orderkey.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique][uint32_t table_size] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t pos_count][uint32_t positions...]
- Column: l_orderkey

## Query Notes
Q18 uses a subquery that filters orders with SUM(l_quantity) > 300. Hash aggregation on l_orderkey computes subquery result (~500K qualifying orders). Hash semi-join with customer/orders filters rows. Final top-100 result sorted by o_totalprice DESC, o_orderdate ASC using partial sort or radix sort.
