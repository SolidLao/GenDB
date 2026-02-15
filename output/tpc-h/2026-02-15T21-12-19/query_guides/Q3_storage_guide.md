# Q3 Storage Guide

## Query Summary
Three-way join: customer.c_mktsegment = 'BUILDING' → orders.o_custkey join lineitem.l_orderkey, with filters on o_orderdate and l_shipdate.

## Data Encoding
- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
- Decimal encoding: scaled integers (int64_t), scale_factor 100
- String columns: dictionary-encoded as int32_t codes
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, STRING→int32_t (dict codes)

## Tables

### customer
- Rows: 1,500,000
- Block size: 100,000
- Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | — |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | — |

- Dictionary file: c_mktsegment → customer/c_mktsegment_dict.txt (5 segments: AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY)

### orders
- Rows: 15,000,000
- Block size: 100,000
- Sort order: o_orderkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | — |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | — |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | — |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | — |

### lineitem
- Rows: 59,986,052
- Block size: 100,000
- Sort order: l_shipdate

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | — |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | — |

## Indexes

### customer_mktsegment_hash
- File: indexes/customer_mktsegment_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=5] [key:int32_t, offset:uint32_t, count:uint32_t × 5] [uint32_t pos_count] [positions...]
- Column: c_mktsegment (hash on encoded segment code, e.g., 1 for BUILDING)

### orders_custkey_hash
- File: indexes/orders_custkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique≈1M] [key:int32_t, offset:uint32_t, count:uint32_t × num_unique] [uint32_t pos_count=15M] [positions...]
- Column: o_custkey (FK→customer.c_custkey)

### lineitem_orderkey_hash
- File: indexes/lineitem_orderkey_hash.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=15M] [key:int32_t, offset:uint32_t, count:uint32_t × num_unique] [uint32_t pos_count=60M] [positions...]
- Column: l_orderkey (FK→orders.o_orderkey)

## Join Execution
1. Filter customer by c_mktsegment = 'BUILDING' (dict code 1): use customer_mktsegment_hash to scan matching customers (~300K rows).
2. Build hash table on filtered customers (c_custkey).
3. Filter orders by o_orderdate < 1995-03-15 (9204 days): full scan, extract matching orders (~7.2M rows).
4. Probe orders-to-customer hash join: output joined (c_custkey, o_orderkey, o_shippriority).
5. Filter lineitem by l_shipdate > 1995-03-15: use zone maps on l_shipdate to prune blocks, output matching rows (~3.1M rows).
6. Probe lineitem-to-orders hash join: use lineitem_orderkey_hash for O(1) lookup per lineitem row.
7. Group by l_orderkey, o_orderdate, o_shippriority; compute SUM(l_extendedprice * (1 - l_discount)); limit 10 by revenue DESC.
