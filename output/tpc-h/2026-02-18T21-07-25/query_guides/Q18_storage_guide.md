# Q18 Storage Guide

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor=100
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→none

## Tables

### customer
- Rows: 1,500,000, Block size: 100,000, Sort order: none

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | → |
| c_name | customer/c_name.bin | std::string | STRING | none | → |

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | → |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | → |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | → |
| o_totalprice | orders/o_totalprice.bin | int64_t | DECIMAL | none | 100 |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | → |
| l_quantity | lineitem/l_quantity.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### idx_customer_custkey (Hash)
- File: indexes/idx_customer_custkey.bin
- Type: hash
- Capacity: 4,194,304
- Column: c_custkey (PK)

### idx_orders_custkey (Hash Multi-Value)
- File: indexes/idx_orders_custkey.bin
- Type: hash_multi_value
- Capacity: 2,097,152
- Column: o_custkey (FK)

### idx_orders_orderkey (Hash)
- File: indexes/idx_orders_orderkey.bin
- Type: hash
- Capacity: 33,554,432
- Column: o_orderkey (PK)

### idx_lineitem_orderkey (Hash Multi-Value)
- File: indexes/idx_lineitem_orderkey.bin
- Type: hash_multi_value
- Capacity: 33,554,432
- Column: l_orderkey (FK)
- Use to find all lines for a given orderkey in correlated subquery: SUM(l_quantity) > 300

### idx_orders_orderdate (Zone Map)
- File: indexes/idx_orders_orderdate.bin
- Type: zone_map
- 150 zones
- Column: o_orderdate
