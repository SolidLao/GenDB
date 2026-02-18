# Q3 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100 for monetary columns
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1,500,000, Block size: 50,000, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | → |

- Dictionary files: customer/c_mktsegment_dict.txt

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | → |

### lineitem
- Rows: 59,986,052, Block size: 200,000, Sort order: l_shipdate, l_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

## Indexes

### customer_c_mktsegment (hash_multi_value)
- File: indexes/customer_c_mktsegment_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_entries][uint32_t table_size] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12B)
- For hash (multi-value): [uint32_t pos_count][uint32_t positions...]
- Column: c_mktsegment (equi-filter for c_mktsegment = 'BUILDING')

### orders_o_custkey (hash_multi_value)
- File: indexes/orders_o_custkey_hash.bin
- Type: hash_multi_value
- Column: o_custkey (FK join probe: customer.c_custkey = orders.o_custkey)

### orders_o_orderdate (zone_map)
- File: indexes/orders_o_orderdate_zonemap.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then per zone: [int32_t min, int32_t max, uint32_t count]
- Zones: 150 blocks for range filter WHERE o_orderdate < DATE '1995-03-15'

### lineitem_l_orderkey (hash_multi_value)
- File: indexes/lineitem_l_orderkey_hash.bin
- Type: hash_multi_value
- Column: l_orderkey (FK join probe: orders.o_orderkey = lineitem.l_orderkey)

### lineitem_l_shipdate (zone_map)
- File: indexes/lineitem_l_shipdate_zonemap.bin
- Type: zone_map
- Zones: 300 blocks for range filter WHERE l_shipdate > DATE '1995-03-15'
