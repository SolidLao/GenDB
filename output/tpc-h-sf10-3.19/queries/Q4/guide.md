# Q4 Guide — Order Priority Checking

## Column Reference

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows)
- This query: `l_orderkey = o_orderkey` → join key for EXISTS subquery

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows)
- This query: `o_orderdate >= '1993-07-01' AND o_orderdate < '1993-10-01'`
  → C++ `o_orderdate[i] >= 8582 && o_orderdate[i] < 8674`
  (1993-07-01 = 8582 days since 1970-01-01, 1993-10-01 = 8674)

### o_orderpriority (category, int8_t, dictionary)
- File: `storage/orders/o_orderpriority.bin` (15,000,000 rows), dict: `storage/orders/o_orderpriority_dict.bin`
- This query: GROUP BY o_orderpriority, ORDER BY o_orderpriority
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0
- NEVER hardcode dict codes — load dict at runtime to map code→string for output

### l_orderkey (fk, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: `l_orderkey = o_orderkey` → EXISTS subquery join key

### l_commitdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_commitdate.bin` (59,986,052 rows)
- This query: `l_commitdate < l_receiptdate` → C++ `l_commitdate[i] < l_receiptdate[i]`

### l_receiptdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_receiptdate.bin` (59,986,052 rows)
- This query: `l_commitdate < l_receiptdate` → C++ `l_commitdate[i] < l_receiptdate[i]`

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| orders | 15,000,000 | Filtered fact | o_orderkey | 65536 |
| lineitem | 59,986,052 | EXISTS probe | l_orderkey | 65536 |

## Query Analysis
- **Date filter**: o_orderdate in [1993-07-01, 1993-10-01) → ~3.8% selectivity → ~570K orders.
- **EXISTS subquery**: For each qualifying order, check if ANY lineitem has l_commitdate < l_receiptdate.
  - ~62.5% of lineitems satisfy this condition.
  - Since most orders have 1-7 lineitems, the EXISTS check usually succeeds quickly.
- **Aggregation**: GROUP BY o_orderpriority → 5 groups, COUNT(*).
- **Output**: Ordered by o_orderpriority (ascending string order).

## Indexes

### orders_o_orderdate_zonemap (zone_map on o_orderdate)
- File: `storage/indexes/orders_o_orderdate_zonemap.bin`
- Layout:
  - Header: `uint64_t num_blocks`, `uint32_t block_size` (=65536)
  - Body: `int32_t[num_blocks * 2]` as `[min_0, max_0, min_1, max_1, ...]`
  - Block `b` covers rows `[b*65536, min((b+1)*65536, N))`
  - Skip block if `max < 8582 || min >= 8674`
- Usage: Skip blocks whose o_orderdate range doesn't overlap [8582, 8674).

### lineitem_l_orderkey_grouped (sorted_grouped on l_orderkey)
- File: `storage/indexes/lineitem_l_orderkey_grouped.bin`
- Layout:
  - Header: `uint64_t num_entries` (= max_orderkey + 1)
  - Body: `uint32_t[num_entries * 2]` interleaved as `[start_0, count_0, start_1, count_1, ...]`
  - For orderkey `ok`: `start = body[ok * 2]`, `count = body[ok * 2 + 1]`
- Usage: For each date-qualifying order, lookup its lineitems by o_orderkey → check EXISTS condition.

## Recommended Approach
1. Load orderdate zonemap. For each block overlapping [8582, 8674):
   - Scan o_orderdate in that block range, collect qualifying order row_ids and their o_orderkey values.
2. Load lineitem_l_orderkey_grouped index.
3. For each qualifying order, use grouped index to find its lineitem rows.
4. Scan those lineitem rows: if ANY has `l_commitdate[row] < l_receiptdate[row]`, the order qualifies → increment count for its o_orderpriority code.
5. Load o_orderpriority dict to map codes to strings. Sort by string value, output.
