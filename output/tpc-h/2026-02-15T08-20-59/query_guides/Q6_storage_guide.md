# Q6 Storage Guide: Forecasting Revenue Change

## Query Summary
```sql
SELECT SUM(l_extendedprice * l_discount) AS revenue
FROM lineitem
WHERE
    l_shipdate >= DATE '1994-01-01'
    AND l_shipdate < DATE '1994-01-01' + INTERVAL '1' YEAR
    AND l_discount BETWEEN 0.06 - 0.01 AND 0.06 + 0.01
    AND l_quantity < 24;
```

Q6 is a **single-table scan with complex range predicates** (date range + BETWEEN + <). Highly selective (~5.2% pass rate).

---

## Data Files

### Lineitem Table
- **Total rows:** 59,986,052
- **Block size:** 131,072 rows per block (458 blocks)

| Column | File Path | C++ Type | Semantic Type | Encoding | Scale Factor |
|--------|-----------|----------|---------------|----------|--------------|
| `l_shipdate` | `lineitem/l_shipdate.bin` | int32_t | DATE | none | N/A |
| `l_discount` | `lineitem/l_discount.bin` | int64_t | DECIMAL | none | 100 |
| `l_quantity` | `lineitem/l_quantity.bin` | int64_t | DECIMAL | none | 100 |
| `l_extendedprice` | `lineitem/l_extendedprice.bin` | int64_t | DECIMAL | none | 100 |

---

## Available Indexes

### Zone Maps (Critical for Selectivity)

#### l_shipdate_zone_map
- **File:** `indexes/l_shipdate_zone_map.bin`
- **Index type:** zone_map
- **Cardinality:** 458 blocks
- **Binary Layout:**
  - Header: `uint32_t num_blocks` (4 bytes)
  - For each block: `int32_t min_date, int32_t max_date` (8 bytes)
  - Total file size: 4 + (458 × 8) = 3,668 bytes
- **Date encoding:** Epoch days (1970-01-01 = day 0)
  - Q6 range: 1994-01-01 (8036 days) to 1994-12-31 (8400 days)
  - Predicates: `l_shipdate >= 8036 AND l_shipdate < 8401`
- **Expected block skip rate:** ~85–90% (only ~1 year of 7-year dataset)
- **Usage:** **HIGHLY RECOMMENDED** - blocks outside 1994 are skipped entirely

#### l_discount_zone_map
- **File:** `indexes/l_discount_zone_map.bin`
- **Index type:** zone_map
- **Cardinality:** 458 blocks
- **Binary Layout:**
  - Header: `uint32_t num_blocks`
  - For each block: `int64_t min_discount, int64_t max_discount` (16 bytes)
  - Total file size: 4 + (458 × 16) = 7,332 bytes
- **Discount encoding:** Stored as scaled integers (0–10, multiplied by scale_factor=100)
  - Example: 0.05 is stored as 5, 0.07 is stored as 7
  - Q6 range: 0.05 to 0.07 (stored as 5 to 7)
- **Expected block skip rate:** ~70–80% (discount is uniformly distributed 0–10)
- **Usage:** **RECOMMENDED** - blocks with no discounts in [5,7] are skipped

#### l_quantity_zone_map
- **File:** `indexes/l_quantity_zone_map.bin`
- **Index type:** zone_map
- **Cardinality:** 458 blocks
- **Binary Layout:**
  - Header: `uint32_t num_blocks`
  - For each block: `int64_t min_quantity, int64_t max_quantity` (16 bytes)
- **Quantity encoding:** Stored as scaled integers (scale_factor=100)
  - Example: 24.00 is stored as 2400
  - Q6 predicate: `l_quantity < 2400` (i.e., < 24.00)
- **Expected block skip rate:** ~40–50% (quantity range is 1–50; many blocks are >= 24)
- **Usage:** **RECOMMENDED** - blocks with min_quantity >= 2400 are skipped

---

## Index Usage Strategy

### Recommended Multi-Stage Filtering (SIMD-friendly)

Q6 benefits from **applying zone maps in order of selectivity** (most selective first) to maximize early termination.

#### Stage 1: l_shipdate Zone Map (85–90% block skip)
```
for each block B in lineitem:
    if B.max_date < 8036 or B.min_date >= 8401:
        skip_block(B)
    else:
        // Proceed to Stage 2
```

#### Stage 2: l_discount Zone Map (70–80% block skip, conditional on Stage 1)
```
for each row in active_blocks:
    load l_discount[row]
    if l_discount[row] < 500 or l_discount[row] > 700:  // 5 to 7
        skip_row()
    else:
        // Proceed to Stage 3
```

#### Stage 3: l_quantity Zone Map (40–50% skip, conditional on Stages 1–2)
```
for each row in active_rows:
    load l_quantity[row]
    if l_quantity[row] >= 2400:  // >= 24.00
        skip_row()
    else:
        // Proceed to Stage 4
```

#### Stage 4: Final Computation
```
for each remaining row:
    load l_shipdate[row], l_discount[row], l_extendedprice[row]
    revenue += l_extendedprice[row] * l_discount[row] / 100  // discount is 0-10
    (no grouping needed)
```

### Expected Selectivity Chain
- Input: 59,986,052 rows
- After date filter (Stage 1): ~8.4M rows (14% of input, 85% block skip)
- After discount filter (Stage 2): ~1.6M rows (27% of date-filtered, 70% of remaining)
- After quantity filter (Stage 3): ~312K rows (20% of discount-filtered, 50% of remaining)
- **Final selectivity:** ~0.52% (312K / 60M)

---

## Storage Properties

### Binary Columnar Layout

**lineitem/l_shipdate.bin**
```
[9569] [9599] [9525] ... [9000]  (int32_t, 4 bytes each)
Total: 59.99M × 4 bytes = 240 MB
```

**lineitem/l_discount.bin**
```
[4] [9] [10] ... [6]  (int64_t, 8 bytes each; stored as 100× the decimal value)
Total: 59.99M × 8 bytes = 480 MB
```

**lineitem/l_quantity.bin**
```
[1700] [2200] [3600] ... [1200]  (int64_t, 8 bytes each; stored as 100× the decimal value)
Total: 59.99M × 8 bytes = 480 MB
```

**lineitem/l_extendedprice.bin**
```
[10000] [50000] [90000] ... [20000]  (int64_t, 8 bytes each; stored as 100× the decimal value)
Total: 59.99M × 8 bytes = 480 MB
```

### Column Pruning Benefit
Q6 only accesses 4 columns (l_shipdate, l_discount, l_quantity, l_extendedprice) out of 16 lineitem columns.
- **Accessed:** 240 + 480 + 480 + 480 = **1.68 GB**
- **Avoided:** 16 columns - 4 columns = 12 columns ≈ **5 GB** (savings: ~75% I/O reduction)

---

## Zone Map Layout Details

### l_shipdate_zone_map Binary Format
```
Byte layout:
[0-3]:     uint32_t num_blocks = 458
[4-11]:    Block 0: int32_t min_date = 8037, int32_t max_date = 10562
[12-19]:   Block 1: int32_t min_date = 8100, int32_t max_date = 10500
...
[3664-3671]: Block 457: (last block min/max)
```

**Block skip logic:**
```cpp
uint32_t num_blocks = read_u32(zone_map, 0);
for (uint32_t b = 0; b < num_blocks; ++b) {
    int32_t min_date = read_i32(zone_map, 4 + b*8);
    int32_t max_date = read_i32(zone_map, 8 + b*8);

    // Q6: l_shipdate >= 8036 AND l_shipdate < 8401
    if (max_date < 8036 || min_date >= 8401) {
        skip_block(b);
    }
}
```

### l_discount_zone_map Binary Format
```
Byte layout:
[0-3]:     uint32_t num_blocks = 458
[4-19]:    Block 0: int64_t min_discount = 0, int64_t max_discount = 1000
[20-35]:   Block 1: int64_t min_discount = 0, int64_t max_discount = 1000
...
```

**Row-level check (SIMD-friendly):**
```cpp
// After loading a block's discount values:
const int64_t* discounts = mmap_block(l_discount, block_id);
for (size_t i = 0; i < block_size; ++i) {
    if (discounts[i] >= 500 && discounts[i] <= 700) {
        // Pass to next stage
    }
}
```

### l_quantity_zone_map Binary Format
Same structure as l_discount_zone_map (int64_t min/max per block).

---

## Query Execution Summary

```
1. Load l_shipdate_zone_map (3.7 KB, negligible)
2. Load l_discount_zone_map (7.3 KB, negligible)
3. Load l_quantity_zone_map (7.3 KB, negligible)

4. For each block B in lineitem (458 blocks):
   a. Check B against l_shipdate_zone_map:
      if B.max_date < 8036 or B.min_date >= 8401: skip

   b. For remaining blocks, load columns:
      - l_shipdate (4 bytes × 131K rows)
      - l_discount (8 bytes × 131K rows)
      - l_quantity (8 bytes × 131K rows)
      - l_extendedprice (8 bytes × 131K rows)

   c. For each row r in block:
      if (r.l_shipdate >= 8036 and r.l_shipdate < 8401 and
          r.l_discount >= 500 and r.l_discount <= 700 and
          r.l_quantity < 2400):
          revenue += r.l_extendedprice * r.l_discount / 100

5. Return revenue (single aggregate)
```

---

## Performance Characteristics

### I/O Reduction via Zone Maps
- **Without zone maps:** Must read all 458 blocks = ~1.68 GB (4 columns × 240–480 MB)
- **With zone maps (estimated):**
  - Block skip (date): 85% of 458 = 391 blocks skipped, 67 blocks read
  - Effective I/O: 67 blocks × (240+480+480+480)/458 ≈ **240 MB**
  - **Savings: ~85% I/O reduction**

### CPU Filtering
- Input to row-level filter: ~8.4M rows (14% of 60M)
- Output after discount + quantity filters: ~312K rows (0.52% of 60M)
- **SIMD vectorization potential:** Discount and quantity filters are amenable to SIMD (batch predicates on 8+ rows in parallel)

### Memory Usage
- **Hash aggregation:** Not needed (single aggregate SUM)
- **Working set:** 4 column buffers + zone maps ≈ **1–2 GB RAM** (minimal)

---

## Zone Map Effectiveness

### Selectivity Analysis

| Filter | Selectivity | Blocks Affected | Block Skip Rate |
|--------|-------------|-----------------|-----------------|
| Date (1994 only) | 14% | 67/458 | **85%** |
| Discount (0.05–0.07) | ~25% | Conditional | ~70% of active |
| Quantity (< 24) | ~50% | Conditional | ~50% of active |
| **Combined** | **~0.52%** | **~3 blocks** | **>99%** |

The **combined effect is multiplicative**: date filter alone skips 85%, discount filter skips ~70% of the remaining 67 blocks, quantity filter skips ~50% of those.

---

## Post-Execution Verification

After Q6 execution:
1. All intermediate row dates are in [8036, 8400] ✓
2. All intermediate discount values are in [500, 700] / 100 = [5, 7] ✓
3. All intermediate quantity values are < 2400 / 100 = < 24.00 ✓
4. Final SUM(revenue) is non-negative and reasonable ✓
5. Result is a single scalar (no GROUP BY) ✓

---

## Index Construction Summary

All three zone maps were built during the index building phase:

```
Built l_shipdate_zone_map: 458 blocks
Built l_discount_zone_map: 458 blocks
Built l_quantity_zone_map: 458 blocks
```

Each zone map file is <10 KB and provides dramatic filtering benefits due to the low cardinality of the date range (1 year of 7-year dataset).
