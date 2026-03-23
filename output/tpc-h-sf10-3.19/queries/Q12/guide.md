# Q12 Guide — Shipping Modes and Order Priority

## Column Reference

### l_orderkey (fk, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows)
- This query: `o_orderkey = l_orderkey` → join key

### l_shipmode (category, int8_t, dictionary)
- File: `storage/lineitem/l_shipmode.bin` (59,986,052 rows), dict: `storage/lineitem/l_shipmode_dict.bin`
- This query: `l_shipmode IN ('MAIL', 'SHIP')` → load dict, find codes for MAIL and SHIP, then check `l_shipmode[i] == mail_code || l_shipmode[i] == ship_code`
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0
- GROUP BY l_shipmode → 2 groups

### l_commitdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_commitdate.bin` (59,986,052 rows)
- This query: `l_commitdate < l_receiptdate` → C++ `l_commitdate[i] < l_receiptdate[i]`

### l_receiptdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_receiptdate.bin` (59,986,052 rows)
- This query: `l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'`
  → C++ `l_receiptdate[i] >= 8766 && l_receiptdate[i] < 9131`
- Also: `l_commitdate < l_receiptdate`

### l_shipdate (date, int32_t, days_since_epoch)
- File: `storage/lineitem/l_shipdate.bin` (59,986,052 rows)
- This query: `l_shipdate < l_commitdate` → C++ `l_shipdate[i] < l_commitdate[i]`

### o_orderkey (pk, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows)
- This query: `o_orderkey = l_orderkey` → join key

### o_orderpriority (category, int8_t, dictionary)
- File: `storage/orders/o_orderpriority.bin` (15,000,000 rows), dict: `storage/orders/o_orderpriority_dict.bin`
- This query: CASE on `o_orderpriority IN ('1-URGENT', '2-HIGH')` → load dict, find codes for '1-URGENT' and '2-HIGH'
- Dict format: uint32_t count, then [uint16_t len, char[len]] per entry; codes are int8_t sequential from 0

## Table Stats
| Table | Rows | Role | Sort Order | Block Size |
|-------|------|------|------------|------------|
| lineitem | 59,986,052 | Filtered fact | l_orderkey | 65536 |
| orders | 15,000,000 | Dimension | o_orderkey | 65536 |

## Query Analysis
- **Lineitem filters** (all must pass):
  1. `l_shipmode IN ('MAIL', 'SHIP')` → ~28% (~2/7 modes)
  2. `l_receiptdate >= '1994-01-01' AND l_receiptdate < '1995-01-01'` → ~15%
  3. `l_commitdate < l_receiptdate` → ~62.5%
  4. `l_shipdate < l_commitdate` → ~48%
  - Combined: ~28% × 15% × 62.5% × 48% ≈ ~1.3% → ~780K qualifying lineitems
- **Join**: For each qualifying lineitem, lookup order to get o_orderpriority.
- **Aggregation**: GROUP BY l_shipmode → 2 groups (MAIL, SHIP). Two counters per group (high_line_count, low_line_count).
- **Output**: ORDER BY l_shipmode (ascending).

## Indexes

### lineitem_l_receiptdate_zonemap (zone_map on l_receiptdate)
- File: `storage/indexes/lineitem_l_receiptdate_zonemap.bin`
- Layout: Header: `uint64_t num_blocks`, `uint32_t block_size` (=65536). Body: `int32_t[num_blocks*2]` (min, max pairs).
- Usage: Skip blocks where `max < 8766 || min >= 9131`.

### orders_o_orderkey_lookup (dense_pk_array on o_orderkey)
- File: `storage/indexes/orders_o_orderkey_lookup.bin`
- Layout: Header: `uint64_t num_entries`. Body: `int32_t[num_entries]`, `arr[orderkey] = row_id`, `-1` if missing.
- Usage: For each qualifying lineitem, lookup order row_id by l_orderkey → get o_orderpriority.

## Recommended Approach
1. Load l_shipmode dict. Find codes for 'MAIL' and 'SHIP'.
2. Load o_orderpriority dict. Find codes for '1-URGENT' and '2-HIGH'.
3. Load receiptdate zonemap. For each block potentially in [8766, 9131):
   - Scan l_receiptdate, l_commitdate, l_shipdate, l_shipmode for that block.
   - Apply all 4 filters. For qualifying rows:
     a. Get l_orderkey. Use orders_o_orderkey_lookup to get order row_id.
     b. Read o_orderpriority[row_id]. Check if '1-URGENT' or '2-HIGH' code.
     c. Increment appropriate counter (high_line_count or low_line_count) for the shipmode group.
4. Output 2 rows, sorted by l_shipmode (decode from dict).
