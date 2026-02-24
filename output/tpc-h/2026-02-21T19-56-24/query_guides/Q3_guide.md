# Q3 Guide — Shipping Priority

## Column Reference

### c_custkey (INTEGER, int32_t)
- File: `customer/c_custkey.bin` (1500000 rows)
- This query: join key `c_custkey = o_custkey`; used to probe hash index after applying c_mktsegment filter.

### c_mktsegment (STRING, uint8_t, byte_pack)
- File: `customer/c_mktsegment.bin` (1500000 rows, uint8_t)
- Lookup: `customer/c_mktsegment_lookup.txt`
  - Codes (alphabetical): 0="AUTOMOBILE", 1="BUILDING", 2="FURNITURE", 3="HOUSEHOLD", 4="MACHINERY"
- This query: `c_mktsegment = 'BUILDING'` → C++ filter: `raw_code == 1`
  - Estimated selectivity: ~20% of customers → ~300K qualifying customers

### o_orderkey (INTEGER, int32_t)
- File: `orders/o_orderkey.bin` (15000000 rows)
- This query: GROUP BY l_orderkey = o_orderkey; used as join key with lineitem.
- Index: `indexes/orders_orderkey_hash.bin` (single-value hash, PK)

### o_custkey (INTEGER, int32_t)
- File: `orders/o_custkey.bin` (15000000 rows)
- This query: join condition `o_custkey = c_custkey`
- Index: `indexes/orders_custkey_hash.bin` (multi-value hash)

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `orders/o_orderdate.bin` (15000000 rows)
- Sorted column — orders is sorted by o_orderdate ascending. Zone map available.
- This query: `o_orderdate < DATE '1995-03-15'` → epoch **9204**
  - C++ filter: `raw_orderdate < 9204`
- Zone map file: `indexes/orders_orderdate_zonemap.bin`
  - num_blocks=150; range 1992-01-01 (8035) to 1998-08-02 (10440)
  - Skip blocks where `block_min >= 9204` (blocks where all dates ≥ 1995-03-15)
  - Estimated: ~49% of orders qualify (dates before 1995-03-15); ~51% of blocks can be skipped

### o_shippriority (INTEGER, int32_t)
- File: `orders/o_shippriority.bin` (15000000 rows)
- This query: SELECT o_shippriority (passed through to output)

### l_orderkey (INTEGER, int32_t)
- File: `lineitem/l_orderkey.bin` (59986052 rows)
- This query: join key `l_orderkey = o_orderkey`; GROUP BY l_orderkey
- Index: `indexes/lineitem_orderkey_hash.bin` (multi-value hash, 15M unique keys)

### l_extendedprice (DECIMAL, double)
- File: `lineitem/l_extendedprice.bin` (59986052 rows)
- Stored as native double. Values match SQL directly.
- This query: `SUM(l_extendedprice * (1 - l_discount)) AS revenue`

### l_discount (DECIMAL, double)
- File: `lineitem/l_discount.bin` (59986052 rows)
- Stored as native double.
- This query: used in revenue formula: `l_extendedprice * (1 - l_discount)`

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59986052 rows)
- Sorted column. Zone map available.
- This query: `l_shipdate > DATE '1995-03-15'` → epoch **9204**
  - C++ filter: `raw_shipdate > 9204`
- Zone map: `indexes/lineitem_shipdate_zonemap.bin` (600 blocks)
  - Skip blocks where `block_max <= 9204` (blocks entirely before 1995-03-15)
  - ~54% of rows qualify (shipdate > 1995-03-15); ~46% of blocks skippable

## Table Stats
| Table    | Rows     | Role      | Sort Order  | Block Size |
|----------|----------|-----------|-------------|------------|
| lineitem | 59986052 | fact      | l_shipdate  | 100000     |
| orders   | 15000000 | fact      | o_orderdate | 100000     |
| customer | 1500000  | dimension | none        | 100000     |

## Query Analysis
- **Join pattern**: customer → orders → lineitem (PK-FK chain)
  - Build side: filtered customers (c_mktsegment='BUILDING' → ~300K rows)
  - Probe: orders filtered by o_orderdate < 9204 (~7.4M qualifying orders)
  - Probe: lineitem filtered by l_shipdate > 9204 (~32M qualifying lineitems)
- **Recommended execution plan**:
  1. Scan `customer/c_mktsegment.bin`; collect qualifying `c_custkey` values (~300K).
  2. Build hash set of qualifying c_custkey.
  3. Scan `orders/o_orderdate.bin` + `orders/o_custkey.bin` with zone map guidance;
     filter rows where `orderdate < 9204` AND `custkey` in hash set → ~7.4M × 0.20 = ~1.5M orders.
  4. Collect qualifying o_orderkey values → build hash set.
  5. Scan `lineitem/l_shipdate.bin` + `lineitem/l_orderkey.bin` with zone map guidance;
     filter rows where `shipdate > 9204` AND `l_orderkey` in hash set.
  6. For qualifying lineitem rows: load l_extendedprice, l_discount; compute revenue.
  7. Group by (l_orderkey, o_orderdate, o_shippriority); accumulate revenue.
  8. Sort by revenue DESC, o_orderdate; output top 10.
- **Filters and selectivities**:
  - `c_mktsegment='BUILDING'`: 20% of 1.5M = ~300K customers
  - `o_orderdate < 9204`: 49% of 15M = ~7.35M orders; with c_custkey filter: ~7.35M × 0.20 = ~1.47M
  - `l_shipdate > 9204`: 54% of 60M = ~32.4M lineitems; combined with orderkey filter: much fewer
- **Aggregation**: GROUP BY (l_orderkey, o_orderdate, o_shippriority) → up to ~3.5M groups; use hash aggregation.
- **Output**: Top 10 by revenue DESC, then o_orderdate ASC. Use a partial sort / top-k heap.

## Indexes

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout: `uint32_t num_blocks (600)`, then `[int32_t min, int32_t max] × 600`
- row_offset is ROW index. Access column value as `col_data[row_idx]`.
- This query: skip blocks where `block_max <= 9204` (all lineitems before 1995-03-15).
  - Roughly first ~46% of blocks (the early years) can be skipped entirely.

### orders_orderdate_zonemap (zone_map on o_orderdate)
- File: `indexes/orders_orderdate_zonemap.bin`
- Layout: `uint32_t num_blocks (150)`, then `[int32_t min, int32_t max] × 150`
- row_offset is ROW index.
- This query: skip blocks where `block_min >= 9204`. Since orders sorted by o_orderdate, the later half of blocks can be skipped entirely. ~51% block skip rate.

### orders_custkey_hash (multi-value hash on o_custkey)
- File: `indexes/orders_custkey_hash.bin`
- Layout:
  ```
  uint32_t num_positions  (= 15000000)
  uint32_t num_unique     (= 999982)
  uint32_t capacity       (= 2097152, power of 2)
  uint32_t positions[15000000]    // row indices sorted by o_custkey
  MvEntry[2097152]: { int32_t key; uint32_t offset; uint32_t count; }
  // empty slot: key == INT32_MIN (-2147483648)
  ```
- Probe: hash(c_custkey) & (capacity-1) using multiply-shift `(key*2654435761ULL)>>32`; linear probe for match; get positions[offset..offset+count].
- This query: for each qualifying customer, look up their orders and check o_orderdate.

### lineitem_orderkey_hash (multi-value hash on l_orderkey)
- File: `indexes/lineitem_orderkey_hash.bin`
- Layout:
  ```
  uint32_t num_positions  (= 59986052)
  uint32_t num_unique     (= 15000000)
  uint32_t capacity       (= 33554432)
  uint32_t positions[59986052]
  MvEntry[33554432]: { int32_t key; uint32_t offset; uint32_t count; }
  // empty slot: key == INT32_MIN
  ```
- Average ~4 lineitem rows per orderkey.
- This query: for each qualifying orderkey, look up lineitem rows; load shipdate to filter, then load extendedprice + discount for revenue.

### customer_custkey_hash (single-value hash on c_custkey)
- File: `indexes/customer_custkey_hash.bin`
- Layout:
  ```
  uint32_t num_rows  (= 1500000)
  uint32_t capacity  (= 4194304)
  SvEntry[4194304]: { int32_t key; uint32_t row_idx; }
  // empty slot: key == INT32_MIN
  ```
- Probe: multiply-shift hash on c_custkey → get row_idx in customer table.
- This query: used to verify/fetch customer data if scanning orders and joining back to customers (alternative execution plan).
