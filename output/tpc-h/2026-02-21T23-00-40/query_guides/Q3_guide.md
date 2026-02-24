# Q3 Guide — Shipping Priority

## Column Reference

### c_mktsegment (STRING, uint8_t, dictionary-encoded)
- File: customer/c_mktsegment.bin (1500000 rows)
- Dictionary: customer/c_mktsegment_dict.txt (load at runtime as std::vector<std::string>; line number = code)
- Known values: AUTOMOBILE, BUILDING, FURNITURE, HOUSEHOLD, MACHINERY (5 segments, codes 0–4)
- Filtering: load dict, find code where dict[code] == "BUILDING", then filter rows where mktsegment_code == that_code.
- This query: `c_mktsegment = 'BUILDING'` → find building_code from dict, filter rows: `mktseg_byte == building_code`
- Estimated selectivity: 21% of 1.5M = ~315,000 qualifying customers.

### c_custkey (INTEGER, int32_t)
- File: customer/c_custkey.bin (1500000 rows)
- This query: join key for `c_custkey = o_custkey`. After filtering by mktsegment, collect qualifying custkeys
  into a hash set, then probe orders.o_custkey.
- Also used in GROUP BY output (not needed since Q3 groups by l_orderkey).

### o_custkey (INTEGER, int32_t)
- File: orders/o_custkey.bin (15000000 rows)
- This query: join probe key — `o_custkey = c_custkey`. Filter orders where o_custkey is in the BUILDING customer set.

### o_orderdate (DATE, int32_t, epoch days since 1970-01-01)
- File: orders/o_orderdate.bin (15000000 rows)
- Column is sorted ascending (orders table sorted by o_orderdate).
- This query: `o_orderdate < DATE '1995-03-15'` → `raw < 9204`
  (derivation: 1995-01-01 = 9131; Jan=31, Feb=28, Mar 1–14 = 14 days more → 9131+31+28+14 = 9204)
- Zone map: skip blocks where `block_min >= 9204`. Selectivity ~49% of 15M = ~7.35M qualifying orders.
- Combined with customer join: orders joined with BUILDING customers AND o_orderdate < 9204.

### o_orderkey (INTEGER, int32_t)
- File: orders/o_orderkey.bin (15000000 rows)
- This query: join key for `l_orderkey = o_orderkey` (GROUP BY output key) and hash index lookup.
- After filtering orders, collect qualifying (o_orderkey, o_orderdate, o_shippriority) tuples into a hash map.

### o_shippriority (INTEGER, int32_t)
- File: orders/o_shippriority.bin (15000000 rows)
- This query: included in GROUP BY and output. Retrieve for qualifying orders.

### l_orderkey (INTEGER, int32_t)
- File: lineitem/l_orderkey.bin (59986052 rows)
- This query: join key `l_orderkey = o_orderkey`. For each lineitem row passing shipdate filter,
  probe the orders hash map with l_orderkey.

### l_shipdate (DATE, int32_t, epoch days since 1970-01-01)
- File: lineitem/l_shipdate.bin (59986052 rows)
- Column is sorted ascending. Zone map available.
- This query: `l_shipdate > DATE '1995-03-15'` → `raw > 9204`
  Selectivity ~55% of 59.9M = ~33M rows qualify.
- Zone map: skip blocks where `block_max <= 9204`. Since column is sorted, skip all blocks before the
  first block where block_max > 9204 (find first qualifying block via binary search on zone map).

### l_extendedprice (DECIMAL, double)
- File: lineitem/l_extendedprice.bin (59986052 rows)
- Stored as native double. Values match SQL directly.
- This query: `SUM(l_extendedprice * (1 - l_discount))` per (l_orderkey, o_orderdate, o_shippriority) group.

### l_discount (DECIMAL, uint8_t, byte_pack compression)
- File: lineitem/l_discount.bin (59986052 rows)
- Compression: byte_pack. code = round(value × 100). Lookup: lineitem/l_discount_lookup.bin (256 doubles).
- Actual value: `disc_lut[discount_code]`
- This query: `(1 - l_discount)` → `1.0 - disc_lut[disc_code]`
  Revenue per row: `extprice * (1.0 - disc_lut[disc_code])`

## Table Stats

| Table    | Rows     | Role      | Sort Order  | Block Size |
|----------|----------|-----------|-------------|------------|
| customer | 1500000  | dimension | none        | 100000     |
| orders   | 15000000 | fact      | o_orderdate | 100000     |
| lineitem | 59986052 | fact      | l_shipdate  | 100000     |

## Query Analysis

- **Join pattern**: customer ⋈ orders ⋈ lineitem (star join)
  - customer → orders: PK-FK on c_custkey = o_custkey (build on customer, probe orders)
  - orders → lineitem: PK-FK on o_orderkey = l_orderkey (build on filtered orders, probe lineitem)
- **Filters**:
  1. `c_mktsegment = 'BUILDING'` on customer: ~21% → ~315K rows
  2. `o_orderdate < 9204` on orders: ~49% → ~7.35M rows
  3. `l_shipdate > 9204` on lineitem: ~55% → ~33M rows
- **Recommended execution plan**:
  1. Scan customer (1.5M), filter by mktsegment=BUILDING → ~315K custkeys → build hash set
  2. Scan orders, filter `o_orderdate < 9204` (use zone maps), probe customer hash set →
     ~315K × 10 orders/customer ≈ 3.5M qualifying orders → build hash map: orderkey → (orderdate, shippriority)
  3. Scan lineitem (zone-map guided: skip blocks where block_max <= 9204 on shipdate),
     filter `raw > 9204`, probe orders hash map with l_orderkey →
     accumulate revenue = SUM(extprice * (1 - disc_lut[dc])) per orderkey group
  4. Sort top-10 by revenue DESC, o_orderdate ASC
- **Aggregation**: ~3M groups (unique qualifying orderkeys), but only top-10 needed → use top-K heap.
- **Output**: 10 rows — l_orderkey, revenue, o_orderdate, o_shippriority.
- **Combined selectivity**: ~0.55 × 0.49 × 0.21 ≈ 5.7% of the cross product, but sequential join reduces it further.
- **Optimization hint**: Bloom filter on qualifying o_orderkeys speeds up lineitem probe (Q3 hint from workload analysis).
  Zone maps on l_shipdate skip the early portion of lineitem (entries before 1995-03-15).

## Indexes

### shipdate_zonemap (zone_map on l_shipdate) — lineitem
- File: lineitem/indexes/shipdate_zonemap.bin
- Layout: `[uint32_t num_blocks=600]` then `[int32_t min, int32_t max, uint32_t start_row]` × 600
- This query: skip blocks where `block_max <= 9204` (shipdate ≤ 1995-03-15).
  Since column sorted, find first block where block_max > 9204 and scan from there.
- row_offset is ROW index, not byte offset.

### orderdate_zonemap (zone_map on o_orderdate) — orders
- File: orders/indexes/orderdate_zonemap.bin
- Layout: `[uint32_t num_blocks=150]` then `[int32_t min, int32_t max, uint32_t start_row]` × 150
- This query: skip blocks where `block_min >= 9204` (all orders on/after 1995-03-15).
  Since orders sorted by o_orderdate, once block_min >= 9204, break.
- row_offset is ROW index, not byte offset.

### orderkey_hash (hash on o_orderkey) — orders
- File: orders/indexes/orderkey_hash.bin
- Layout: `[uint32_t capacity=33554432]` then `[int32_t key, uint32_t row_idx]` × capacity
- Empty slot: key == INT32_MIN.
- Hash function: `slot = ((uint32_t)key * 2654435761u) & (capacity - 1)`, linear probe on collision.
- Usage: given l_orderkey from lineitem, find matching order row:
  `h = hash(l_orderkey, mask); while(table[h].key != l_orderkey && table[h].key != INT32_MIN) h=(h+1)&mask;`
  Then read o_orderdate[table[h].row_idx] and o_shippriority[table[h].row_idx].
- This query: probe with each qualifying l_orderkey to retrieve orderdate, shippriority, and verify
  the order was in the BUILDING-customer + orderdate-filter set.

### custkey_hash (hash on c_custkey) — customer
- File: customer/indexes/custkey_hash.bin
- Layout: `[uint32_t capacity=4194304]` then `[int32_t key, uint32_t row_idx]` × capacity
- Empty slot: key == INT32_MIN. Hash function: `((uint32_t)key * 2654435761u) & (capacity-1)`.
- This query: optional — can build in-memory hash set of BUILDING customer custkeys instead of
  using this persistent index. The persistent index is useful if scanning orders and probing customer.
