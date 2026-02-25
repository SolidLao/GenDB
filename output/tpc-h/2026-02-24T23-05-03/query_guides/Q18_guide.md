# Q18 Guide — Large Volume Customer

## Query
```sql
SELECT c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice,
       SUM(l_quantity) AS sum_qty
FROM customer, orders, lineitem
WHERE o_orderkey IN (
    SELECT l_orderkey FROM lineitem
    GROUP BY l_orderkey
    HAVING SUM(l_quantity) > 300
  )
  AND c_custkey = o_custkey
  AND o_orderkey = l_orderkey
GROUP BY c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice
ORDER BY o_totalprice DESC, o_orderdate ASC
LIMIT 100;
```

## Table Stats
| Table    | Rows       | Role      | Sort Order | Block Size |
|----------|------------|-----------|------------|------------|
| customer | 1,500,000  | dimension | (none)     | 65536      |
| orders   | 15,000,000 | fact      | (none)     | 65536      |
| lineitem | 59,986,052 | fact      | (none)     | 65536      |

## Column Reference

### l_orderkey (key, int32_t — direct) — subquery and main scan
- File: `lineitem/l_orderkey.bin` (59,986,052 × 4 bytes = 239,944,208 bytes)
- **Subquery**: GROUP BY l_orderkey, HAVING SUM(l_quantity) > 300 → ~624 qualifying orderkeys
  (selectivity: 624 / 15,000,000 ≈ 0.0042%)
- **Main query**: join key l_orderkey = o_orderkey + filter by subquery result set

### l_quantity (measure, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes = 479,888,416 bytes)
- Encoding: IEEE 754 double; values ∈ [1, 50] (integer-valued, stored as double)
- **Subquery**: SUM(l_quantity) per l_orderkey; HAVING > 300
- **Main query**: SUM(l_quantity) per group — output column sum_qty
- double precision is sufficient (max group sum: ~7 lines × 50 = 350 < 10^10)

### o_orderkey (key, int32_t — direct)
- File: `orders/o_orderkey.bin` (15,000,000 × 4 bytes = 60,000,000 bytes)
- This query: filter `IN (subquery result)`; join key l_orderkey = o_orderkey; GROUP BY key; output

### o_custkey (foreign key, int32_t — direct)
- File: `orders/o_custkey.bin` (15,000,000 × 4 bytes = 60,000,000 bytes)
- This query: join key c_custkey = o_custkey → probe customer for c_name (or reconstruct)

### o_orderdate (date, int32_t — days_since_epoch_1970)
- File: `orders/o_orderdate.bin` (15,000,000 × 4 bytes = 60,000,000 bytes)
- Encoding: Howard Hinnant algorithm; values confirmed > 3000
- This query: GROUP BY key; ORDER BY o_orderdate ASC (tiebreaker after o_totalprice DESC)
- **C1**: Stored as int32_t epoch days; output as date string using `date_utils.h`:
  ```cpp
  gendb::init_date_tables();  // C11: call once at top of main()
  char buf[11];
  gendb::epoch_to_date_str(o_orderdate[row], buf);  // formats YYYY-MM-DD
  ```

### o_totalprice (measure, double)
- File: `orders/o_totalprice.bin` (15,000,000 × 8 bytes = 120,000,000 bytes)
- Encoding: IEEE 754 double
- This query: GROUP BY key (unique per order); ORDER BY o_totalprice DESC; output column

### c_custkey (key, int32_t — direct)
- File: `customer/c_custkey.bin` (1,500,000 × 4 bytes = 6,000,000 bytes)
- This query: join key c_custkey = o_custkey; GROUP BY key; output column

### c_name — NOT stored in binary
- **Important**: c_name is NOT stored in the binary columnar store (ingest.cpp skips col 1)
- **Q18 must reconstruct**: `sprintf(buf, "Customer#%09d", c_custkey)` — matches TPC-H format
- GROUP BY c_name is equivalent to GROUP BY c_custkey (1:1 mapping)
- Output: `"Customer#%09d"` formatted string; quote for CSV (C31):
  ```cpp
  char cname[32];
  snprintf(cname, sizeof(cname), "Customer#%09d", c_custkey);
  printf("\"%s\"", cname);  // C31: always double-quote string output columns
  ```

## Query Analysis

### Two-Phase Execution

#### Phase 1: Subquery — find high-volume orderkeys
- Scan ALL 60M lineitem rows: accumulate SUM(l_quantity) per l_orderkey
- Total distinct l_orderkey values ≈ 15M (one per order)
- After aggregation: filter HAVING SUM(l_quantity) > 300 → ~624 qualifying orderkeys
- **Key design decision**: 15M groups → partitioned aggregation recommended (C36/P36/P37):
  - NUM_PARTITIONS = 64; P1_CAP = next_power_of_2(15M/64 × 2) = 65536 (C36/P37)
  - p1_part MUST use HIGH bits: `(key * KNUTH) >> (32 - 6)` (C36 — orthogonal bit ranges)
  - p1_hash MUST use LOW bits: `(key * KNUTH) & P1_MASK`
- Alternatively: shared XADD int64_t accumulation (P38) — viable at 15M groups
- Collect qualifying orderkeys into a dense bitset (orderkey max ≈ 60M in TPC-H sf10):
  ```cpp
  // TPC-H sf10: o_orderkey max value = ~60,000,000
  // bitmap: 60M / 8 = 7.5MB — fits in L3 cache
  std::vector<uint8_t> orderkey_bitmap((60000000/8) + 1, 0);
  // set bit: orderkey_bitmap[ok >> 3] |= (1u << (ok & 7));
  // test bit: orderkey_bitmap[ok >> 3] & (1u << (ok & 7))
  ```

#### Phase 2: Main query — join filtered orders with customer + lineitem
- **Scan orders** (15M rows): filter `orderkey_bitmap[o_orderkey]` → ~624 qualifying orders
- For each qualifying order: probe customer_pk_hash to get c_custkey row (or reconstruct c_name directly)
- **Re-scan lineitem** (60M rows): filter `orderkey_bitmap[l_orderkey]` → small hit set
- Accumulate SUM(l_quantity) per (c_custkey, o_orderkey, o_orderdate, o_totalprice) group — ~624 groups

### Selectivities
| Filter                            | Qualifying Rows |
|-----------------------------------|-----------------|
| HAVING SUM(l_quantity) > 300      | ~624 orders     |
| orders matching subquery          | ~624 rows       |
| lineitem matching subquery orderkeys | ~4,368 rows (624 × avg 7 lines) |

### GROUP BY Key (C15)
All five GROUP BY columns are required: `(c_name, c_custkey, o_orderkey, o_orderdate, o_totalprice)`
- In practice, o_orderkey is unique → (c_name, c_custkey, o_orderdate, o_totalprice) follow from it
- Can simplify: key = o_orderkey; carry other columns as payload

### Output
- ORDER BY o_totalprice DESC, o_orderdate ASC; LIMIT 100 (only 624 groups → sort all)
- **C33**: No non-deterministic ties expected (o_totalprice + o_orderdate combination is unique per order)
- **C31**: Quote c_name in output: `printf("\"%s\",", cname)`

## Indexes

### orders_pk_hash (hash on o_orderkey)
- File: `orders/indexes/orders_pk_hash.bin`
- File size: 4 + 33,554,432 × 8 = 268,435,460 bytes (~256MB)
- Layout: `[uint32_t cap][PKSlot[cap]]`
  ```cpp
  struct PKSlot { int32_t key; uint32_t row_idx; };
  ```
- Cap: 33,554,432 (2^25 = next_power_of_2(15,000,000 × 2))
- Mask: 33,554,431 (cap - 1)
- Empty sentinel: `key == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  static uint32_t pk_hash(int32_t key, uint32_t mask) {
      return ((uint32_t)key * 2654435761u) & mask;
  }
  ```
- Probe pattern (C24 bounded):
  ```cpp
  uint32_t h = ((uint32_t)o_orderkey * 2654435761u) & mask;
  for (uint32_t pr = 0; pr < cap; pr++) {
      uint32_t idx = (h + pr) & mask;
      if (ht[idx].key == INT32_MIN) break;
      if (ht[idx].key == o_orderkey) {
          uint32_t orow = ht[idx].row_idx;
          // access o_custkey[orow], o_orderdate[orow], o_totalprice[orow]
          break;
      }
  }
  ```
- **Q18 usage**: In Phase 2, after finding qualifying l_orderkey values from lineitem,
  probe orders_pk_hash to get o_custkey/o_orderdate/o_totalprice.
  Only ~624 probes needed — negligible cost.

### customer_pk_hash (hash on c_custkey)
- File: `customer/indexes/customer_pk_hash.bin`
- File size: 4 + 4,194,304 × 8 = 33,554,436 bytes (~32MB)
- Layout: `[uint32_t cap][PKSlot[cap]]`
  ```cpp
  struct PKSlot { int32_t key; uint32_t row_idx; };
  ```
- Cap: 4,194,304 (2^22 = next_power_of_2(1,500,000 × 2))
- Mask: 4,194,303 (cap - 1)
- Empty sentinel: `key == INT32_MIN`
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  static uint32_t pk_hash(int32_t key, uint32_t mask) {
      return ((uint32_t)key * 2654435761u) & mask;
  }
  ```
- Probe pattern (C24 bounded):
  ```cpp
  uint32_t h = ((uint32_t)c_custkey * 2654435761u) & mask;
  for (uint32_t pr = 0; pr < cap; pr++) {
      uint32_t idx = (h + pr) & mask;
      if (ht[idx].key == INT32_MIN) break;
      if (ht[idx].key == c_custkey) {
          // uint32_t crow = ht[idx].row_idx;  // row in customer table
          // c_name NOT in binary — reconstruct: "Customer#%09d" from c_custkey
          break;
      }
  }
  ```
- **Q18 usage**: Since c_name is reconstructed from c_custkey, the customer_pk_hash is NOT
  needed for Q18 (no customer columns are fetched from binary). The hash index is listed for
  completeness but the query can skip mmap'ing it entirely.

## Subquery Aggregation Notes (C36 critical)

### Partitioned hash aggregation for 15M groups
```
NUM_PARTITIONS = 64
KNUTH = 2654435761u  (same Knuth multiplier used throughout)

// C36: orthogonal bit ranges — MANDATORY
uint32_t p1_part(int32_t key) {
    return ((uint32_t)key * KNUTH) >> 26;          // HIGH 6 bits → partition 0-63
}
uint32_t p1_hash(int32_t key, uint32_t mask) {
    return ((uint32_t)key * KNUTH) & mask;          // LOW bits → slot within partition
}

// P37: per-partition capacity
// 15M keys / 64 partitions = ~234,375 → next_power_of_2(234375*2) = 524288
// Total: 64 × 524288 × slot_size (8 bytes) = 256MB — check vs L3 (44MB)
// If total > L3: reduce by using scan-time partitioned approach
```

## Date Output Summary
| Column       | Storage       | Output Format  | C++ Pattern                              |
|--------------|---------------|----------------|------------------------------------------|
| o_orderdate  | int32_t epoch | YYYY-MM-DD     | `gendb::epoch_to_date_str(val, buf)`     |
