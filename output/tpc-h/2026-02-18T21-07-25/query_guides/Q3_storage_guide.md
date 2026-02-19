# Q3 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1,500,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | → |

- Dictionary files:
  - c_mktsegment → customer/c_mktsegment_dict.txt

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | → |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | → |

## Indexes

### idx_customer_custkey (Hash)
- File: indexes/idx_customer_custkey.bin
- Type: hash
- Layout: [uint32_t capacity] then [key:int32_t, position:uint32_t] per slot (8B/slot)
- Capacity: 4,194,304
- Column: c_custkey (PK)

### idx_orders_custkey (Hash Multi-Value)
- File: indexes/idx_orders_custkey.bin
- Type: hash_multi_value
- Layout: [uint32_t capacity][uint32_t num_unique] then [key:int32_t, offset:uint32_t, count:uint32_t] per slot (12B), then [uint32_t pos_count][uint32_t positions...]
- Capacity: 2,097,152
- Column: o_custkey (FK)

### idx_orders_orderdate (Zone Map)
- File: indexes/idx_orders_orderdate.bin
- Type: zone_map
- Layout: [uint32_t num_zones] then [min:int32_t, max:int32_t, count:uint32_t] per zone (12B/zone)
- 150 zones
- Column: o_orderdate

### idx_lineitem_orderkey (Hash Multi-Value)
- File: indexes/idx_lineitem_orderkey.bin
- Type: hash_multi_value
- Capacity: 33,554,432
- Column: l_orderkey (FK)

### idx_lineitem_shipdate (Zone Map)
- File: indexes/idx_lineitem_shipdate.bin
- Type: zone_map
- 600 zones
- Column: l_shipdate
