# Q3 Storage Guide

## Data Encoding

- Date encoding: days_since_epoch (1970-01-01), stored as int32_t
  - 1995-03-15 = epoch day 9204
  - 1992–1998 range: 8036–10561
- Decimal encoding: scaled integers (int64_t, scale_factor=100)
- Type mappings: INTEGER→int32_t, DECIMAL→int64_t, DATE→int32_t, CHAR/VARCHAR→dictionary-encoded int32_t

## Tables

### customer
- Rows: 1,500,000, Block size: 100,000, Sort order: c_custkey

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| c_custkey | customer/c_custkey.bin | int32_t | INTEGER | none | – |
| c_mktsegment | customer/c_mktsegment.bin | int32_t | STRING | dictionary | – |

- Dictionary files:
  - c_mktsegment → customer/c_mktsegment_dict.txt (values: AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY)

### orders
- Rows: 15,000,000, Block size: 100,000, Sort order: o_orderkey
- Filtering predicate: `o_orderdate < 1995-03-15` (epoch day < 9204)

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| o_orderkey | orders/o_orderkey.bin | int32_t | INTEGER | none | – |
| o_custkey | orders/o_custkey.bin | int32_t | INTEGER | none | – |
| o_orderdate | orders/o_orderdate.bin | int32_t | DATE | none | – |
| o_shippriority | orders/o_shippriority.bin | int32_t | INTEGER | none | – |

### lineitem
- Rows: 59,986,052, Block size: 100,000, Sort order: l_shipdate
- Filtering predicate: `l_shipdate > 1995-03-15` (epoch day > 9204)

| Column | File | C++ Type | Semantic | Encoding | Scale Factor |
|--------|------|----------|----------|----------|--------------|
| l_orderkey | lineitem/l_orderkey.bin | int32_t | INTEGER | none | – |
| l_shipdate | lineitem/l_shipdate.bin | int32_t | DATE | none | – |
| l_extendedprice | lineitem/l_extendedprice.bin | int64_t | DECIMAL | none | 100 |
| l_discount | lineitem/l_discount.bin | int64_t | DECIMAL | none | 100 |

## Indexes

### hash_c_custkey
- File: indexes/hash_c_custkey.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=1,500,000][uint32_t table_size=4,194,304] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t positions...]
- Column: c_custkey (int32_t)
- Purpose: Probe-side hash table for customer filter → orders join (c_custkey = o_custkey)

### hash_o_custkey
- File: indexes/hash_o_custkey.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique≈1,000,000][uint32_t table_size=2,097,152] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t positions...]
- Column: o_custkey (int32_t)
- Purpose: Build-side hash table for filtered orders (c_mktsegment='BUILDING', o_orderdate < 9204)

### hash_l_orderkey
- File: indexes/hash_l_orderkey.bin
- Type: hash_multi_value
- Layout: [uint32_t num_unique=15,000,000][uint32_t table_size=33,554,432] then [int32_t key, uint32_t offset, uint32_t count] per slot (12B), then [uint32_t positions...]
- Column: l_orderkey (int32_t)
- Purpose: Build-side hash table for filtered lineitem (l_shipdate > 9204), probe with orders.o_orderkey

### zone_map_o_orderdate
- File: indexes/zone_map_o_orderdate.bin
- Type: zone_map
- Layout: [ZoneMap min_val, max_val] per block (8B each, 150 blocks)
- Column: o_orderdate (int32_t)
- Purpose: Skip blocks where min(o_orderdate) >= 9204 (predicate `o_orderdate < 9204`)

### zone_map_l_shipdate
- File: indexes/zone_map_l_shipdate.bin
- Type: zone_map
- Layout: [ZoneMap min_val, max_val] per block (8B each, 600 blocks)
- Column: l_shipdate (int32_t)
- Purpose: Skip blocks where max(l_shipdate) <= 9204 (predicate `l_shipdate > 9204`)
