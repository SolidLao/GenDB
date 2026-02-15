# Q1: Pricing Summary Report - Code Generation Summary

## Status: ✅ VALIDATION PASSED (Attempt 2)

## Query
```sql
SELECT l_returnflag, l_linestatus, SUM(l_quantity) AS sum_qty,
       SUM(l_extendedprice) AS sum_base_price,
       SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
       SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
       AVG(l_quantity) AS avg_qty, AVG(l_extendedprice) AS avg_price,
       AVG(l_discount) AS avg_disc, COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - INTERVAL '90' DAY
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
```

## Code Generation Details

### Implementation
- **Language**: C++17
- **File**: `q1.cpp` (304 lines)
- **Compilation**: `g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE`

### Key Techniques

1. **Storage Access**:
   - Memory-mapped binary columnar files via `mmap()`
   - One file per column: `lineitem/{l_shipdate,l_quantity,l_extendedprice,l_discount,l_tax,l_returnflag,l_linestatus}.bin`
   - Zero-copy direct array access

2. **Dictionary Decoding**:
   - `l_returnflag`: codes {0=N, 1=R, 2=A} → decoded before grouping
   - `l_linestatus`: codes {0=O, 1=F} → decoded before grouping
   - Loaded from `*_dict.txt` files at startup

3. **Data Type Handling**:
   - `l_shipdate`: `int32_t` days since epoch (1970-01-01)
   - `l_quantity`, `l_extendedprice`, `l_discount`, `l_tax`: `int64_t` scaled by 100
     - Example: 0.05 discount stored as 5 (scaled integer)
     - Decoded to `long double` for calculation precision
   - `l_returnflag`, `l_linestatus`: `uint8_t` (dictionary codes)

4. **Filtering**:
   - Single range predicate: `l_shipdate <= 10472` (days since epoch)
   - 10472 = 1998-09-02 (1998-12-01 minus 90 days)
   - ~98.7% selectivity (almost all rows pass)
   - Zone map loaded but no blocks could be skipped due to uniform date distribution

5. **Aggregation**:
   - Group-by columns: `(l_returnflag, l_linestatus)` → 6 possible groups
   - Used `std::map<std::pair<char, char>, AggregateGroup>` for O(log 6) lookups
   - Tracking: sum_qty (int64), sum_base_price (int64), sum_disc_price (long double), sum_charge (long double), sum_discount (int64), count_rows
   - Long double used for monetary columns to preserve precision across 60M row summation

6. **Precision**:
   - Calculations use `long double` to maintain ~18-19 significant digits
   - Critical for sums exceeding 1 trillion where `double` (~15 digits) loses precision
   - Output with appropriate formatting: 2 decimals for qty/price, 4 for disc_price, 6 for charge

### Performance

| Phase | Time (ms) |
|-------|-----------|
| Load data (mmap) | 0.17 |
| Load zone map | 0.07 |
| **Scan + Filter + Aggregate** | **1192.63** |
| Output writing | 0.20 |
| **Total** | **1193.13** |

**Single-threaded scan over 59.99M rows with date filter and 6-way aggregation.**

### Correctness Issues & Fixes

#### Attempt 1
- **Issue**: SHIPDATE_CUTOFF was calculated as 10471 instead of 10472
  - Caused N,O group to have 18,751 fewer rows than expected
  - Off by exactly one day in boundary condition
- **Root Cause**: SQL `<=` predicate uses inclusive upper bound; day 10472 should be included
- **Fix**: Changed constant from 10471 to 10472

#### Issue Discovered
- Floating-point arithmetic with `double` lost precision on large sums (>1e12)
- Example: sum_charge calculation accumulated rounding errors
- **Fix**: Changed `sum_disc_price` and `sum_charge` from `double` to `long double` (80-bit precision on x86)

### Validation Results

**Ground Truth Comparison**:
```
✅ Row count: 4/4 matches
✅ A,F group: All values match (14,804,077 rows)
✅ N,F group: All values match (385,998 rows)
✅ N,O group: All values match (29,144,351 rows)
✅ R,F group: All values match (14,808,183 rows)
```

**Numeric Accuracy**:
- Max deviation from ground truth: <0.000001 (due to long double precision ceiling)
- All values correct to ≥6 decimal places

## Output Contract Compliance

✅ Single self-contained `.cpp` file
✅ All required [TIMING] instrumentation with `#ifdef GENDB_PROFILE` guards
✅ CSV output format: comma-delimited, header row, YYYY-MM-DD dates (if applicable)
✅ Proper decimal precision (2-6 places as appropriate)
✅ Memory-mapped binary column access (no text parsing)
✅ Dictionary decoding for categorical columns
✅ Proper date encoding (days since epoch)
✅ Scaled decimal handling (int64 with scale_factor=100)

## Files

- **Generated Code**: `q1.cpp` (304 lines)
- **Results**: `results/Q1.csv` (4 data rows + header)
- **Validation**: PASSED ✅

## Knowledge Base References

- `storage/encoding-handling.md`: Dictionary and decimal encoding handling
- `indexing/zone-maps.md`: Zone map pruning (loaded but not applied due to distribution)
- `parallelism/thread-parallelism.md`: (Not applied - single-threaded scan on single table is adequate)

## Notes

- The uniform distribution of dates across blocks meant zone map provided minimal benefit (<1% skipping)
- Parallel processing of 60M row full scan would benefit from morsel-driven parallelization, but gen DB iteration 0 prioritizes correctness
- Long double precision was essential for monetary columns to match ground truth
- SQL's inclusive `<=` boundary condition required careful day calculation
