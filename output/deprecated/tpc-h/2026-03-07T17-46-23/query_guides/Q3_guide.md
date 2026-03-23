# Q3 Guide

## SQL
```sql
SELECT l_orderkey, SUM(l_extendedprice * (1 - l_discount)) AS revenue,
       o_orderdate, o_shippriority
FROM customer, orders, lineitem
WHERE c_mktsegment = 'BUILDING'
  AND c_custkey = o_custkey
  AND l_orderkey = o_orderkey
  AND o_orderdate < DATE '1995-03-15'
  AND l_shipdate > DATE '1995-03-15'
GROUP BY l_orderkey, o_orderdate, o_shippriority
ORDER BY revenue DESC, o_orderdate
LIMIT 10;
```

## Column Reference

### c_custkey (PK, int32_t, native_binary)
- File: `customer/c_custkey.bin` (~1,500,000 rows, 4 bytes each)
- This query: join key `c_custkey = o_custkey`

### c_mktsegment (attribute, uint8_t, dictionary encoded)
- File: `customer/c_mktsegment.bin` (~1,500,000 rows, 1 byte each)
- Dictionary: `customer/c_mktsegment_dict.bin` — text format: `code|string\n`
- Encoding: uint8_t dictionary codes. ~5 distinct values.
- This query: `WHERE c_mktsegment = 'BUILDING'`
- **Runtime loading pattern** (DO NOT hardcode code values):
  ```cpp
  // Load dictionary to find BUILDING code
  uint8_t building_code = 255; // sentinel
  FILE* df = fopen("customer/c_mktsegment_dict.bin", "r");
  char line[256];
  while (fgets(line, sizeof(line), df)) {
      int code; char name[64];
      sscanf(line, "%d|%s", &code, name);
      if (strcmp(name, "BUILDING") == 0) { building_code = (uint8_t)code; break; }
  }
  fclose(df);
  // Then filter: c_mktsegment[i] == building_code
  ```
- Selectivity: ~20.98% of customers match BUILDING

### o_orderkey (PK, int32_t, native_binary)
- File: `orders/o_orderkey.bin` (~15,000,000 rows, 4 bytes each)
- This query: join key `l_orderkey = o_orderkey`, GROUP BY key, output column

### o_custkey (FK, int32_t, native_binary)
- File: `orders/o_custkey.bin` (~15,000,000 rows, 4 bytes each)
- This query: join key `c_custkey = o_custkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `orders/o_orderdate.bin` (~15,000,000 rows, 4 bytes each)
- This query: `WHERE o_orderdate < DATE '1995-03-15'`, GROUP BY key, output column, ORDER BY
- Date constant 1995-03-15:
  ```cpp
  // days_from_civil(1995, 3, 15):
  // yr=1995, mn=3, dy=15 → yr -= (3<=2)=0 → yr=1995
  // era = 1995/400 = 4, yoe = 1995-1600=395
  // doy = (153*(3-3)+2)/5 + 15 - 1 = 2/5 + 14 = 0+14 = 14
  // doe = 395*365 + 395/4 - 395/100 + 14 = 144175+98-3+14 = 144284
  // result = 4*146097 + 144284 - 719468 = 584388+144284-719468 = 9204
  int32_t date_19950315 = 9204;
  ```
- C++ filter: `o_orderdate[i] < 9204`
- Selectivity: ~48.6% of orders

### o_shippriority (attribute, int32_t, native_binary)
- File: `orders/o_shippriority.bin` (~15,000,000 rows, 4 bytes each)
- This query: GROUP BY key, output column

### l_orderkey (FK, int32_t, native_binary)
- File: `lineitem/l_orderkey.bin` (~59,986,052 rows, 4 bytes each)
- This query: join key `l_orderkey = o_orderkey`

### l_shipdate (date, int32_t, days_since_epoch)
- File: `lineitem/l_shipdate.bin` (~59,986,052 rows, 4 bytes each)
- This query: `WHERE l_shipdate > DATE '1995-03-15'`
- C++ filter: `l_shipdate[i] > 9204` (same date constant as o_orderdate)
- Selectivity: ~53.9% of lineitem rows

### l_extendedprice (measure, double, native_binary)
- File: `lineitem/l_extendedprice.bin` (~59,986,052 rows, 8 bytes each)
- This query: in `SUM(l_extendedprice * (1 - l_discount))`

### l_discount (measure, double, native_binary)
- File: `lineitem/l_discount.bin` (~59,986,052 rows, 8 bytes each)
- This query: in `(1 - l_discount)` expression

## Table Stats

| Table    | Rows        | Role      | Sort Order | Block Size |
|----------|-------------|-----------|------------|------------|
| customer | ~1,500,000  | dimension | c_custkey  | 100,000    |
| orders   | ~15,000,000 | fact      | o_orderkey | 100,000    |
| lineitem | ~59,986,052 | fact      | l_orderkey | 100,000    |

## Query Analysis

### Join Pattern
Three-way join: customer → orders → lineitem
- customer.c_custkey = orders.o_custkey (1:N, up to 46 orders per customer)
- orders.o_orderkey = lineitem.l_orderkey (1:N, up to 7 lineitems per order)

### Recommended Execution Strategy
1. **Build BUILDING customer set**: Scan customer, load dictionary to find BUILDING code,
   collect custkeys where `c_mktsegment[i] == building_code`. ~314K customers.
   Use `customer_custkey_lookup` to know customer is sorted by custkey — or simply
   build a bitset/hash set of qualifying custkeys.

2. **Filter orders**: Scan orders checking:
   - `o_orderdate[i] < 9204` (~48.6% pass → ~7.3M orders)
   - `customer_custkey_lookup` to check if `o_custkey[i]` belongs to BUILDING segment
   - Combined selectivity: ~48.6% × 20.98% ≈ 10.2% → ~1.53M qualifying orders

3. **Probe lineitem via index**: For each qualifying order, use `lineitem_orderkey_index`
   to find lineitem rows. Filter `l_shipdate > 9204`. Aggregate `l_extendedprice * (1 - l_discount)`.

4. **Aggregate & Top-10**: Group by `(l_orderkey, o_orderdate, o_shippriority)`.
   Since l_orderkey determines o_orderdate and o_shippriority, can group by l_orderkey alone.
   Estimated ~1M groups. Use a hash map or maintain a top-10 heap.

5. **Output**: ORDER BY revenue DESC, o_orderdate ASC, LIMIT 10.
   A partial sort / priority queue of size 10 suffices.

### Alternative Strategy (lineitem-driven)
Scan lineitem filtering `l_shipdate > 9204`, then probe orders via `orders_orderkey_lookup`,
filter `o_orderdate < 9204`, then check customer segment. This avoids building a customer set
but scans the larger table first.

## Indexes

### customer_custkey_lookup (dense_lookup on c_custkey)
- File: `indexes/customer_custkey_lookup.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t max_custkey
  Byte 4+:    int32_t[max_custkey + 1] — lookup[custkey] = row_index, or -1 if absent
  ```
- Sentinel: `-1` (int32_t) means no customer with that custkey
- Usage: Given a custkey, `lookup[custkey]` gives the row index into customer columns.
  Then check `c_mktsegment[row_index] == building_code`.
- Since customer is sorted by c_custkey and custkeys are dense (1..1500000),
  `lookup[k] = k - 1` approximately. Can also build a bitset of BUILDING custkeys directly.

### orders_orderkey_lookup (dense_lookup on o_orderkey)
- File: `indexes/orders_orderkey_lookup.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t max_orderkey
  Byte 4+:    int32_t[max_orderkey + 1] — lookup[orderkey] = row_index, or -1 if absent
  ```
- Sentinel: `-1` (int32_t) means no order with that orderkey
- Usage: Given an orderkey from lineitem, `lookup[orderkey]` gives the row index in orders.
  Then access `o_orderdate[row_index]`, `o_custkey[row_index]`, `o_shippriority[row_index]`.

### lineitem_orderkey_index (dense_range on l_orderkey)
- File: `indexes/lineitem_orderkey_index.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t max_orderkey
  Byte 4+:    struct { uint32_t start; uint32_t count; }[max_orderkey + 1]
  ```
  Each entry is 8 bytes (two uint32_t).
- Sentinel: `{0, 0}` means no lineitem rows for that orderkey (count=0)
- Usage: For a qualifying orderkey, `index[ok].start` is the first row and `index[ok].count`
  is the number of lineitem rows. Access rows `[start, start+count)` in all lineitem columns.
- Lineitem is sorted by l_orderkey, so these rows are contiguous.

### lineitem_shipdate_zonemap (zone_map on l_shipdate)
- File: `indexes/lineitem_shipdate_zonemap.bin`
- Layout:
  ```
  Byte 0-3:   uint32_t num_blocks
  Byte 4-7:   uint32_t block_size (= 100000)
  Byte 8+:    struct { int32_t min_date; int32_t max_date; }[num_blocks]
  ```
- Usage: If driving from lineitem side, can skip blocks where `max_date <= 9204`.
  Not useful if using the orderkey index to probe individual orderkey ranges.

## Performance Notes
- Customer is small (1.5M rows, ~1.4MB for mktsegment). Building a BUILDING bitset is cheap.
- Orders scan: 15M rows, reading o_orderdate(57MB) + o_custkey(57MB) = 114MB.
- Lineitem probes via index: ~1.53M qualifying orders × ~4 lineitems/order = ~6.1M lineitem rows.
  Random access pattern through lineitem columns — read l_shipdate, l_extendedprice, l_discount.
- With 64 cores: parallelize the orders scan, then parallelize lineitem probes.
- LIMIT 10 means only a small priority queue needed for final output.
