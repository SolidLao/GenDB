# Q9 Guide — Product Type Profit Measure

## Column Reference

### p_partkey (PK, int32_t, raw)
- File: `storage/part/p_partkey.bin` (2,000,000 rows of int32_t)
- This query: join key `p_partkey = l_partkey`

### p_name (string, varlen)
- File: `storage/part/p_name.bin` (offsets, uint32_t[2000001]), `storage/part/p_name_data.bin`
- This query: `p_name LIKE '%green%'` — scan varlen strings for substring "green"
- Selectivity: ~0.007

### l_partkey (FK, int32_t, raw)
- File: `storage/lineitem/l_partkey.bin` (59,986,052 rows of int32_t)
- This query: join keys `p_partkey = l_partkey`, `ps_partkey = l_partkey`

### l_suppkey (FK, int32_t, raw)
- File: `storage/lineitem/l_suppkey.bin` (59,986,052 rows of int32_t)
- This query: join keys `s_suppkey = l_suppkey`, `ps_suppkey = l_suppkey`

### l_orderkey (FK, int32_t, raw)
- File: `storage/lineitem/l_orderkey.bin` (59,986,052 rows of int32_t)
- This query: join key `o_orderkey = l_orderkey`

### l_quantity (decimal, double, raw)
- File: `storage/lineitem/l_quantity.bin` (59,986,052 rows of double)
- This query: `ps_supplycost * l_quantity` in amount calculation

### l_extendedprice (decimal, double, raw)
- File: `storage/lineitem/l_extendedprice.bin` (59,986,052 rows of double)
- This query: `l_extendedprice * (1 - l_discount)` in amount calculation

### l_discount (decimal, double, raw)
- File: `storage/lineitem/l_discount.bin` (59,986,052 rows of double)
- This query: amount = `l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`

### ps_partkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_partkey.bin` (8,000,000 rows of int32_t)
- This query: join key `ps_partkey = l_partkey`

### ps_suppkey (FK, int32_t, raw)
- File: `storage/partsupp/ps_suppkey.bin` (8,000,000 rows of int32_t)
- This query: join key `ps_suppkey = l_suppkey`

### ps_supplycost (decimal, double, raw)
- File: `storage/partsupp/ps_supplycost.bin` (8,000,000 rows of double)
- This query: `ps_supplycost * l_quantity` in amount calculation

### o_orderkey (PK, int32_t, raw)
- File: `storage/orders/o_orderkey.bin` (15,000,000 rows of int32_t)
- This query: join key `o_orderkey = l_orderkey`

### o_orderdate (date, int32_t, days_since_epoch)
- File: `storage/orders/o_orderdate.bin` (15,000,000 rows of int32_t)
- This query: `EXTRACT(YEAR FROM o_orderdate)` for grouping

### s_suppkey (PK, int32_t, raw)
- File: `storage/supplier/s_suppkey.bin` (100,000 rows of int32_t)
- This query: join key `s_suppkey = l_suppkey`

### s_nationkey (FK, int32_t, raw)
- File: `storage/supplier/s_nationkey.bin` (100,000 rows of int32_t)
- This query: `s_nationkey = n_nationkey`

### n_nationkey (PK, int32_t, raw)
- File: `storage/nation/n_nationkey.bin` (25 rows of int32_t)

### n_name (string, uint8_t, dictionary)
- File: `storage/nation/n_name.bin` (25 rows of uint8_t)
- Dict: `storage/nation/n_name_dict.bin`
- This query: GROUP BY (as "nation"), output — group by nationkey (25 values), decode for output

## Table Stats
| Table    | Rows       | Role      | Sort Order              | Block Size |
|----------|------------|-----------|-------------------------|------------|
| part     | 2,000,000  | dimension | p_partkey               | 65536      |
| lineitem | 59,986,052 | fact      | l_orderkey              | 65536      |
| partsupp | 8,000,000  | fact      | (ps_partkey, ps_suppkey) | 65536      |
| orders   | 15,000,000 | fact      | o_orderkey              | 65536      |
| supplier | 100,000    | dimension | s_suppkey               | 65536      |
| nation   | 25         | dimension | n_nationkey             | 65536      |

## Query Analysis
- **Pattern**: 6-way join, substring filter on p_name, no date filter on lineitem, group by (nation, year)
- **Strategy**:
  1. Build set of qualifying partkeys: scan p_name varlen, find rows where name contains "green" → ~14K partkeys. Store as bitset (max 2M bits = 250KB).
  2. Build `suppkey → nationkey` dense array (100K entries)
  3. Scan lineitem: check `l_partkey` in qualifying set. For qualifying rows:
     - Look up ps_supplycost via **partsupp_composite_hash** using (l_partkey, l_suppkey)
     - Look up o_orderdate via **orders_pk_idx** using l_orderkey → extract year
     - Look up nation via `supp_nationkey[l_suppkey]`
     - Compute amount = `l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity`
     - Accumulate SUM(amount) by (nationkey, year)
  4. Output: ~175 groups (25 nations * ~7 years), ORDER BY nation ASC, o_year DESC

## Indexes

### partsupp_composite_hash (hash on (ps_partkey, ps_suppkey))
- File: `storage/indexes/partsupp_composite_hash.bin`
- Meta: `storage/indexes/partsupp_composite_hash_meta.txt`
- Layout: open-addressing hash table, each entry 16 bytes:
  ```cpp
  struct Entry {
      int32_t partkey;   // offset 0
      int32_t suppkey;   // offset 4
      int32_t row_id;    // offset 8
      int32_t pad;       // offset 12 (alignment)
  };
  ```
- Capacity: next power of 2 above `2 * 8000000` = 16,777,216
- Mask: `capacity - 1` = 16,777,215
- Hash function (verbatim from build_indexes.cpp):
  ```cpp
  uint64_t h = (uint64_t)(uint32_t)pk * 2654435761ULL ^ (uint64_t)(uint32_t)sk * 40503ULL;
  ```
- Probe: `slot = h & mask`, linear probing `slot = (slot + 1) & mask`
- Empty sentinel: `row_id == -1`
- Lookup pattern:
  ```cpp
  uint64_t h = (uint64_t)(uint32_t)pk * 2654435761ULL ^ (uint64_t)(uint32_t)sk * 40503ULL;
  uint64_t slot = h & mask;
  while (table[slot].row_id != -1) {
      if (table[slot].partkey == pk && table[slot].suppkey == sk)
          return table[slot].row_id;  // found
      slot = (slot + 1) & mask;
  }
  return -1;  // not found
  ```
- Usage: Given (l_partkey, l_suppkey), look up partsupp row_id → read ps_supplycost[row_id]

### orders_pk_idx (dense_pk on o_orderkey)
- File: `storage/indexes/orders_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by orderkey → row_id, sentinel -1
- Usage: Given l_orderkey, look up order row_id → read o_orderdate[row_id]

### part_pk_idx (dense_pk on p_partkey)
- File: `storage/indexes/part_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by partkey → row_id, sentinel -1

### supplier_pk_idx (dense_pk on s_suppkey)
- File: `storage/indexes/supplier_pk_index.bin`
- Layout: `int32_t[max_key+1]`, indexed by suppkey → row_id, sentinel -1
