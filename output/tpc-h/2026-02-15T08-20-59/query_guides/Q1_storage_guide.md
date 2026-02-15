# Q1 Storage Guide: Pricing Summary Report

## Query Summary
```sql
SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty, ...
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

Q1 is a **single-table scan with a date range filter** and **simple group-by aggregation** over 6 groups.

---

## Data Files

### Lineitem Table (Primary Access)
- **Total rows:** 59,986,052
- **Block size:** 131,072 rows per block

#### Required Columns

| Column | File Path | C++ Type | Semantic Type | Encoding | Storage Format | Scale Factor |
|--------|-----------|----------|---------------|----------|-----------------|--------------|
| `l_shipdate` | `lineitem/l_shipdate.bin` | int32_t | DATE | none | Binary, days since 1970-01-01 | N/A |
| `l_quantity` | `lineitem/l_quantity.bin` | int64_t | DECIMAL | none | Binary, scaled integers | 100 |
| `l_extendedprice` | `lineitem/l_extendedprice.bin` | int64_t | DECIMAL | none | Binary, scaled integers | 100 |
| `l_discount` | `lineitem/l_discount.bin` | int64_t | DECIMAL | none | Binary, scaled integers | 100 |
| `l_tax` | `lineitem/l_tax.bin` | int64_t | DECIMAL | none | Binary, scaled integers | 100 |
| `l_returnflag` | `lineitem/l_returnflag.bin` | uint8_t | CHAR | dictionary | Binary codes (0, 1, 2) | N/A |
| `l_linestatus` | `lineitem/l_linestatus.bin` | uint8_t | CHAR | dictionary | Binary codes (0, 1) | N/A |

#### Dictionary-Encoded Columns

**l_returnflag** dictionary: `lineitem/l_returnflag_dict.txt`
```
0=A
1=N
2=R
```

**l_linestatus** dictionary: `lineitem/l_linestatus_dict.txt`
```
0=F
1=O
```

---

## Available Indexes

### Zone Maps (Block Min/Max Statistics)

These are **extremely lightweight** index structures that store min/max per block for fast range pruning.

#### l_shipdate_zone_map
- **File:** `indexes/l_shipdate_zone_map.bin`
- **Index type:** zone_map
- **Cardinality:** 458 blocks (lineitem has 59,986,052 rows ÷ 131,072 rows/block ≈ 458)
- **Binary Layout:**
  - Header: `uint32_t num_blocks` (4 bytes)
  - For each block: `int32_t min_date, int32_t max_date` (8 bytes per block)
  - Total file size: 4 + (458 × 8) = 3,668 bytes
- **Entry size:** 8 bytes per block
- **Date encoding:** Epoch days (1970-01-01 = day 0)
  - Example: 1998-12-01 is approximately 10,592 days
  - Q1 predicate: `l_shipdate <= 10502` (1998-09-02, which is 90 days before 1998-12-01)

#### l_discount_zone_map
- **File:** `indexes/l_discount_zone_map.bin`
- **Index type:** zone_map
- **Cardinality:** 458 blocks
- **Binary Layout:**
  - Header: `uint32_t num_blocks`
  - For each block: `int64_t min_discount, int64_t max_discount` (16 bytes per block)
  - Total file size: 4 + (458 × 16) = 7,332 bytes
- **Discount values:** Stored as scaled integers (0–10, multiplied by scale_factor=100)
  - Example: 0.04 is stored as 4, 0.10 is stored as 10
- **Note:** Q1 does not filter on discount, so this zone map is NOT used

#### l_quantity_zone_map
- **File:** `indexes/l_quantity_zone_map.bin`
- **Index type:** zone_map
- **Cardinality:** 458 blocks
- **Binary Layout:** Similar to l_discount_zone_map
- **Note:** Q1 does not filter on quantity, so this zone map is NOT used

---

## Index Usage Recommendations

### **USE: l_shipdate_zone_map**
- **Why:** Q1 has a **high-selectivity range predicate** on `l_shipdate <= 10502` (approximately 98.7% of rows pass)
- **Benefit:** Zone maps allow **fast block-level filtering**. Blocks whose max_date < 10502 can be skipped entirely without scanning rows
- **Expected skip rate:** ~1-2% of blocks (zone map selectivity is modest but not negligible for ~7.2K year span)
- **Implementation:**
  1. Load the zone map index
  2. For each block B: if `B.max_date < 10502`, skip entire block (no row-level scan needed)
  3. For blocks that overlap the predicate, scan all rows

### **DO NOT USE: l_discount_zone_map, l_quantity_zone_map**
- Q1 does not filter on discount or quantity, so these zones contribute no benefit

### **DO NOT USE: Hash indexes**
- Hash indexes are optimized for point lookups or join keys
- Q1 does not perform joins, so lineitem hash indexes are not needed

---

## Storage Properties

### Lineitem Column Layout
The lineitem table is stored as **binary columnar files**, one file per column, with no row delimiters.

**File example: lineitem/l_shipdate.bin**
```
[date_1] [date_2] [date_3] ... [date_59986052]  (each int32_t = 4 bytes)
```

Total size: 59,986,052 × 4 bytes = ~240 MB

### Dictionary Encoding
`l_returnflag` and `l_linestatus` are dictionary-encoded to reduce storage:
- Each value is stored as a `uint8_t` code (0, 1, or 2)
- To decode: load the dictionary mapping (e.g., 0 → 'A'), then look up the code
- On decoding: `actual_char = dict[encoded_code]`

Example: To check if `l_returnflag == 'A'`, decode the code first:
```cpp
uint8_t code = l_returnflag_buffer[row_i];
char actual = dict[code];
if (actual == 'A') { /* aggregate */ }
```

---

## Query Execution Summary

1. **Pre-filter:** Load l_shipdate_zone_map; identify blocks where max_date >= 10502
2. **Main scan:**
   - For each non-skipped block: load l_shipdate, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus
   - Apply row-level filter: `l_shipdate <= 10502`
   - Decode l_returnflag and l_linestatus using dictionaries
3. **Aggregation:** Perfect hash (6 groups: {A,F}, {A,O}, {N,F}, {N,O}, {R,F}, {R,O})
   - Update per-group counts and sums
4. **Output:** Sort by returnflag, linestatus (or use pre-sorted output)

---

## Key Numbers

- **Lineitem rows:** 59,986,052
- **Blocks (131K rows/block):** 458
- **Blocks skipped (conservative estimate):** ~1–5 blocks (low selectivity on dates)
- **Zone map I/O:** ~4 KB to read header + 458 blocks × 8 bytes = ~3.7 KB total
- **Estimated l_shipdate I/O:** ~240 MB (full column scan, likely not avoidable due to 98.7% selectivity)
- **Dictionary files:** ~12 bytes each (minimal overhead)

---

## Post-Execution Verification

After Q1 execution, verify:
1. Date values are in range [8037, 10562] days since epoch ✓
2. Decimal values are non-zero (except where explicitly 0.00) ✓
3. Group count matches expected 6 groups (2 returnflags × 2 linestatuses + edge cases) ✓
4. Sums are positive and reasonable ✓
