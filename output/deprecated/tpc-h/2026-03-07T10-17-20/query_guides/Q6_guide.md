# Q6 Guide — Forecasting Revenue Change

## Query
```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE l_shipdate >= DATE '1994-01-01'
  AND l_shipdate <  DATE '1994-01-01' + INTERVAL '1' YEAR
  AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
  AND l_quantity < 24;
```

## Table Stats
| Table    | Rows       | Role | Sort Order   | Block Size |
|----------|------------|------|--------------|------------|
| lineitem | 59,986,052 | fact | l_shipdate ↑ | 100,000    |

## Column Reference

### l_shipdate (date, int32_t — days since 1970-01-01)
- File: `lineitem/l_shipdate.bin` (59,986,052 × 4 bytes, sorted ascending)
- Filter: `l_shipdate >= DATE '1994-01-01' AND l_shipdate < DATE '1995-01-01'`
- Compute both thresholds at query startup using `ingest.cpp::parse_date` logic:

  **Lower bound — 1994-01-01:**
  ```cpp
  // parse_date("1994-01-01")
  int y=1994, m=1, d=1;
  // m <= 2: y → 1993, m → 13
  int A = 1993/100;                        // 19
  int B = 2 - 19 + 19/4;                  // 2-19+4 = -13
  int jdn = (int)(365.25*(1993+4716))      // (int)(365.25*6709) = 2449272
           + (int)(30.6001*(13+1))         // (int)(30.6001*14)  = 428
           + 1 + (-13) - 1524;
  // jdn = 2448164  →  days = 2448164 - 2440588 = 7576
  const int32_t kShipdateLo = 7576;        // inclusive
  ```

  **Upper bound — 1995-01-01:**
  ```cpp
  // parse_date("1995-01-01")
  int y=1995, m=1, d=1;
  // m <= 2: y → 1994, m → 13
  int A = 1994/100;                        // 19
  int B = 2 - 19 + 19/4;                  // -13
  int jdn = (int)(365.25*(1994+4716))      // (int)(365.25*6710) = 2449637
           + (int)(30.6001*14)             // 428
           + 1 + (-13) - 1524;
  // jdn = 2448529  →  days = 2448529 - 2440588 = 7941
  const int32_t kShipdateHi = 7941;        // exclusive
  ```
  Filter: `l_shipdate >= 7576 && l_shipdate < 7941` (one calendar year)
- Selectivity: ~14% of rows → ~8.4M rows survive the date filter

### l_discount (decimal_15_2, double)
- File: `lineitem/l_discount.bin` (59,986,052 × 8 bytes)
- Filter: `l_discount BETWEEN 0.05 AND 0.07`
  ```cpp
  const double kDiscLo = 0.05;
  const double kDiscHi = 0.07;
  // predicate: l_discount >= kDiscLo && l_discount <= kDiscHi
  ```
  Selectivity: ~30% of rows (discount range [0.05, 0.07] out of [0.00, 0.10])
- Also the multiplicand: `revenue += l_extendedprice[i] * l_discount[i]`

### l_quantity (decimal_15_2, double)
- File: `lineitem/l_quantity.bin` (59,986,052 × 8 bytes)
- Filter: `l_quantity < 24.0`
  ```cpp
  const double kQuantityMax = 24.0;
  // predicate: l_quantity < kQuantityMax
  ```
  Selectivity: ~50% of rows (quantity range [1, 50], uniform; 23/50 values pass)

### l_extendedprice (decimal_15_2, double)
- File: `lineitem/l_extendedprice.bin` (59,986,052 × 8 bytes)
- This query: SUM accumulator multiplied by l_discount

## Indexes

### l_shipdate_zone_map (zone_map on l_shipdate)
- File: `lineitem/l_shipdate_zone_map.bin`
- Layout (all int32_t, packed):
  ```
  [0]           int32_t  num_blocks        // = ceil(59986052 / 100000) = 600
  [1]           int32_t  block_size        // = 100000
  [2 .. 601]    int32_t  min[600]
  [602 .. 1201] int32_t  max[600]
  ```
  Column sorted ascending → `min[b] = col[b*100000]`, `max[b] = col[min(..., nrows-1)]`.
- Usage for Q6: skip blocks entirely outside `[kShipdateLo, kShipdateHi)`:
  ```cpp
  // Load zone map
  int32_t num_blocks, block_size;
  // (read from file as shown in layout above)
  int32_t* zm_min = ...; // points into mmap/buffer at offset 2
  int32_t* zm_max = ...; // points at offset 2 + num_blocks

  for (int b = 0; b < num_blocks; b++) {
      if (zm_max[b] <  kShipdateLo) continue;  // block entirely before range
      if (zm_min[b] >= kShipdateHi) break;      // sorted: all later blocks also past range
      // process block b (may need per-row date check if partially overlapping)
      bool full_block = (zm_min[b] >= kShipdateLo && zm_max[b] < kShipdateHi);
  }
  ```
- Impact: ~14% pass rate. The date range covers ~1 year out of ~6.7 years in the table.
  Approximately (1/6.7) × 600 ≈ 90 blocks fully inside the range; roughly 508 blocks
  are entirely skippable. This is Q6's biggest performance win.

## Query Analysis

### Execution Strategy
1. Memory-map or read `l_shipdate_zone_map.bin` (1,201 int32_t = ~5KB, fits in L1).
2. For each block b (block_size = 100,000 rows):
   - Skip if `zm_max[b] < 7576 || zm_min[b] >= 7941`.
   - Determine if block is fully inside date range: `full = (zm_min[b] >= 7576 && zm_max[b] < 7941)`.
3. For each row i in qualifying block:
   - If not `full`: check `l_shipdate[i] >= 7576 && l_shipdate[i] < 7941`
   - Check `l_discount[i] >= 0.05 && l_discount[i] <= 0.07`
   - Check `l_quantity[i] < 24.0`
   - If all pass: `revenue += l_extendedprice[i] * l_discount[i]`
4. Output: single double `revenue`.

### Predicate Ordering for Short-Circuit
Conjunctive selectivity: 0.14 × 0.30 × 0.50 ≈ 1.9% of rows survive all filters.
Optimal predicate order (cheapest filter first within a SIMD pass):
- l_shipdate (int32 range): evaluate first — only ~14% of rows in qualifying blocks pass
- l_discount (double range): ~30% pass; test second
- l_quantity (double upper bound): ~50% pass; test last
With branchless SIMD on AVX-512, all three predicates can be applied simultaneously
using masked operations on 8 doubles per cycle.

### Single-Threaded vs Multi-Threaded
With zone-map skipping, only ~90 out of 600 blocks need processing (~15M rows).
Reading 15M × (4+8+8+8) bytes = ~420MB from HDD may dominate runtime. Consider
parallelizing over blocks with std::async or a thread pool (64 cores available).

### Column Read Sizes (qualifying blocks only)
| Column          | Type    | Per-row bytes | ~Qualifying rows | ~MB  |
|-----------------|---------|---------------|-----------------|------|
| l_shipdate      | int32_t | 4             | 8,400,000       |  34  |
| l_discount      | double  | 8             | 8,400,000       |  67  |
| l_quantity      | double  | 8             | 8,400,000       |  67  |
| l_extendedprice | double  | 8             | 8,400,000       |  67  |

Total qualifying data: ~235MB (before zone-map skipping of 86% of blocks).
