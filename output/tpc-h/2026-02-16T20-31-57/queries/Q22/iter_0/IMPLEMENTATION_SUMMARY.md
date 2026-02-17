# Q22 Implementation Summary - Global Sales Opportunity

## Status: ✅ VALIDATION PASSED (7/7 rows correct)

## Query Specification
**Q22** identifies potential customers who may increase sales volumes. It finds customers with:
- Phone numbers starting with one of 7 specific country codes
- Account balance above the average for those countries  
- No existing orders (available customers)

## Performance Results

| Build Type | Execution Time | Notes |
|-----------|-----------------|--------|
| With -DGENDB_PROFILE | 2,441 ms | Full instrumentation |
| Release (-O3 -march=native) | ~2-3 seconds | Production build |

### Performance Breakdown (with profiling)
- Dictionary loading: 501 ms (1.5M phone numbers scanned for 7 country codes)
- Loading customer data: 0.05 ms (mmap is fast)
- Loading orders data: 0.01 ms (mmap is fast)
- Building anti-join set: 1,822 ms (15M order rows, sort + unique)
- Computing average: 36.51 ms (parallel reduction)
- Filter and aggregate: 69.13 ms (parallel scan + thread-local aggregation)
- Output writing: 11.95 ms (7 result rows)
- **Total computation: 2,441 ms**

## Key Optimizations

### 1. Dictionary Encoding Handling
- **Issue**: Phone numbers stored as dictionary-encoded int32_t codes
- **Solution**: Load 1.5M phone numbers from dictionary, extract 2-char country codes, pre-compute which codes match target countries ('13', '31', '23', '29', '30', '18', '17')
- **Optimization**: Fast-path check on first 3 characters (country code + hyphen)

### 2. Anti-Join (NOT EXISTS orders.o_custkey)
- **Naive approach**: O(n*m) - check each customer against all 15M orders ❌
- **Optimized approach**: 
  - Collect all 15M order customer keys into sorted vector
  - Sort and remove duplicates → ~1M unique customer keys
  - Use binary search for O(log n) anti-join checks
  - **Result**: 1,822 ms (vs. >30 seconds for naive approach)

### 3. Decimal Scaling
- **Challenge**: storage_design.json incorrectly specified scale_factor=2
- **Solution**: Empirical analysis showed correct scale_factor=100 for DECIMAL(15,2)
  - Formula: raw_value / 100.0 = actual_decimal_value
  - Example: raw=6759246828 → actual=67592468.28

### 4. Aggregation
- **Strategy**: Low-cardinality GROUP BY (only 7 distinct country codes)
- **Implementation**: Thread-local aggregation maps, merged after parallel scan
- **Data structures**: `unordered_map<string, pair<int64_t, int64_t>>` for (count, sum)

### 5. Parallelism
- **OpenMP pragmas** on large table scans (15M orders, 1.5M customers)
- **Thread-local buffers** to avoid critical sections
- **Single-pass design**: each customer/order scanned once

## Correctness Verification
✅ All 7 output rows match ground truth exactly:
- Country code: 13, 17, 18, 23, 29, 30, 31 (sorted)
- Customer counts: 9025, 9067, 9210, 8984, 9199, 9343, 9086
- Account balance sums: 67,592,468.28, 68,084,663.34, ..., 68,144,525.38

## Execution Plan (Logical + Physical)

### Logical Plan
1. Pre-filter 1.5M customers by phone code (country in target set)
2. Compute AVG(c_acctbal) for filtered customers with acctbal > 0
3. Filter customers by three conditions:
   - Country code in target set
   - c_acctbal > computed average
   - NOT EXISTS order from orders table
4. Group by country code, aggregate COUNT(*) and SUM(c_acctbal)

### Physical Plan
1. **Scan orders** (15M rows) → sorted unique customer keys [1,822 ms]
2. **Scan customer** first pass (1.5M rows):
   - Dictionary lookup to extract country code
   - Filter for target countries + acctbal > 0
   - Parallel reduction to compute average [36.51 ms]
3. **Scan customer** second pass (1.5M rows):
   - Dictionary lookup, three filters
   - Build thread-local aggregation maps [69.13 ms]
   - Merge results
4. **Output** 7 result rows to CSV [11.95 ms]

## Code Structure
- **File**: q22.cpp (385 lines)
- **Compilation**: `g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp`
- **Dependencies**: standard library only (mmap, omp, chrono for instrumentation)
- **Binary interfaces**: Direct mmap of `.bin` files, dictionary files as text

## Known Issues/Decisions
1. **Scale factor discrepancy**: Storage guide says 2, reality is 100
   - This was discovered through empirical validation
   - Code uses correct value (100)

2. **system() call for mkdir**: Used for simplicity and portability
   - Could be replaced with filesystem::create_directories in C++17

3. **Dictionary format**: Learned through binary analysis that the format is:
   - Dictionary file: line-by-line phone numbers (indexed from 0)
   - Binary column: int32_t codes (sequential 0, 1, 2, ...)
   - Not code=value format as initially assumed

## Comparison to Query Optimization Paper
- **Umbra**: 33 ms (compiled, JIT-optimized)
- **DuckDB**: 90 ms (vectorized, adaptive)
- **ClickHouse**: 152 ms (column-oriented, distributed)
- **MonetDB**: 352 ms
- **GenDB (this implementation)**: 2,441 ms (iteration 0, no query optimizer)
- **PostgreSQL**: 790 ms

*Note: GenDB iter 0 is a single-pass compilation without advanced optimization. Iterator 1+ will use the Query Optimizer to improve performance.*

## Files Generated
- `q22.cpp` - Complete implementation
- Results written to `<results_dir>/Q22.csv`
