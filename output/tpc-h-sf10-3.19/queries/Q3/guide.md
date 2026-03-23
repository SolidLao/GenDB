# Q3 Guide — Shipping Priority

## Column Reference

### c_custkey (pk, int32_t, raw)
- File: `storage/customer/c_custkey.bin` (1500000 rows × 4 bytes)
- This query: JOIN key `c_custkey = o_custkey`

### c_mktsegment (category, int8_t, dictionary)
- File: `storage/customer/c_mktsegment.bin` (1500000 rows × 1 byte)
- Dictionary: `storage/customer/c_mktsegment_dict.bin`
- Dictionary format: `uint32_t count`, then `count` entries of `uint16_t len` + `char[len]`
- This query: `c_mktsegment = 'BUILDING'` → load dict, find code for "BUILDING", then `c_mktsegment[i] == building_code`
- NEVER hardcode the dictionary code. Load at runtime:
  ```cpp
  // Load dictionary
  std::ifstream df("storage/customer/c_mktsegment_dict.bin", std::ios::binary);
  uint32_t dict_count; df.read((char*)&dict_count, 4);
  int8_t building_code = -1;
  for (uint32_t d = 0; d < dict_count; d++) {
      uint16_t len; df.read((char*)&len, 2);
      std::string val(len, '\0'); df.read(val.data(), len);
      if (val == "BUILDING") building_code = (int8_t)d;
  }
  ```
- Selectivity: ~0.2 (5 segments) → ~300K qualifying customers

### o_custkey (fk, int32_t, raw)
- File: `storage/orders/o_custkey.bin` (15000000 rows × 4 bytes)
- This query: JOIN key `c_custkey = o_custkey`

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15000000 rows × 4 bytes)
- This query: JOIN key `l_orderkey = o_orderkey`, GROUP BY key

### o_orderdate (date, int32_t, days_since_epoch_1970)
- File: `storage/orders/o_orderdate.bin` (15000000 rows × 4 bytes)
- This query: `o_orderdate < DATE '1995-03-15'` → C++ `o_orderdate[i] < 9204`
- Also GROUP BY key and ORDER BY key (secondary, ASC)

### o_shippriority (attribute, int32_t, raw)
- File: `storage/orders/o_shippriority.bin` (15000000 rows × 4 bytes)
- This query: GROUP BY key, SELECT output

### l_orderkey (fk, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59986052 rows × 4 bytes)
- This query: JOIN key `l_orderkey = o_orderkey`, GROUP BY key

### l_shipdate (date, int32_t, days_since_epoch_1970)
- File: `storage/lineitem/l_shipdate.bin` (59986052 rows × 4 bytes)
- This query: `l_shipdate > DATE '1995-03-15'` → C++ `l_shipdate[i] > 9204`
- Selectivity: ~0.54

### l_extendedprice (measure, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59986052 rows × 8 bytes)
- This query: `SUM(l_extendedprice * (1 - l_discount))` — revenue computation

### l_discount (measure, double, raw)
- File: `storage/lineitem/l_discount.bin` (59986052 rows × 8 bytes)
- This query: `SUM(l_extendedprice * (1 - l_discount))` — revenue computation

## Table Stats
| Table    | Rows       | Role      | Sort Order  | Block Size |
|----------|------------|-----------|-------------|------------|
| customer | 1,500,000  | dimension | c_custkey   | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey  | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey  | 65536      |

## Query Analysis

### Join Pattern
- customer → orders: `c_custkey = o_custkey` (1:N, customer is PK side)
- orders → lineitem: `o_orderkey = l_orderkey` (1:N, orders is PK side)

### Filters & Selectivities
- `c_mktsegment = 'BUILDING'`: sel ~0.2 → ~300K customers
- `o_orderdate < 9204` (1995-03-15): sel ~0.48 → ~7.2M orders
- `l_shipdate > 9204` (1995-03-15): sel ~0.54 → ~32.4M lineitems
- Combined: ~300K customers × ~48% orders = ~3.5M qualifying orders, then filter lineitems

### Aggregation
- GROUP BY (l_orderkey, o_orderdate, o_shippriority): ~500K groups estimated
- Aggregate: `SUM(l_extendedprice * (1 - l_discount))` as revenue
- Since l_orderkey uniquely determines o_orderdate and o_shippriority (via the join), the effective group key is just l_orderkey

### Output
- ORDER BY revenue DESC, o_orderdate ASC
- LIMIT 10 → use partial sort or priority queue

## Indexes

### orders_o_orderkey_lookup (dense_pk_array on o_orderkey)
- File: `storage/indexes/orders_o_orderkey_lookup.bin`
- Layout:
  - Header: `uint64_t num_entries` (= max_orderkey + 1)
  - Body: `int32_t[num_entries]` where `arr[orderkey] = row_id`, `-1` if missing
- Usage: Given `l_orderkey`, look up orders row: `int32_t order_row = lookup[l_orderkey]`
- O(1) random access. No hash function needed.

### lineitem_l_orderkey_grouped (sorted_grouped on l_orderkey)
- File: `storage/indexes/lineitem_l_orderkey_grouped.bin`
- Layout:
  - Header: `uint64_t num_entries` (= max_orderkey + 1)
  - Body: interleaved `uint32_t[num_entries * 2]` — `[start0, count0, start1, count1, ...]`
  - For orderkey `k`: `start = body[k*2]`, `count = body[k*2+1]`
  - If count == 0, no lineitems for that orderkey
- Usage: For a qualifying order with orderkey `k`, read lineitems from row `start` for `count` rows.
- Lineitem is sorted by l_orderkey, so these rows are contiguous.

### lineitem_l_shipdate_zonemap (zone_map on l_shipdate)
- File: `storage/indexes/lineitem_l_shipdate_zonemap.bin`
- Layout:
  - Header: `uint64_t num_blocks`, `uint32_t block_size` (65536)
  - Body: `int32_t[num_blocks * 2]` — (min_date, max_date) per block
- Usage: Can skip blocks where `max_date <= 9204` (all dates too early).

## Recommended Execution Strategy
1. **Build customer filter**: Load c_mktsegment dict, find 'BUILDING' code. Scan customer, collect qualifying c_custkey values into a hash set or dense bitmap (max custkey ~1.5M, bitmap feasible)
2. **Scan orders**: For each order row, check `o_orderdate < 9204` AND `c_custkey` is in the customer set. Collect qualifying (o_orderkey, o_orderdate, o_shippriority) tuples.
3. **Probe lineitem**: For each qualifying orderkey, use `lineitem_l_orderkey_grouped` index to find contiguous lineitem rows. For each lineitem row, check `l_shipdate > 9204`. If passes, accumulate `l_extendedprice * (1 - l_discount)` into aggregation keyed by l_orderkey.
4. **Top-10**: Use partial sort or min-heap to find top 10 by (revenue DESC, o_orderdate ASC).
