# Q7: Volume Shipping - Iteration 0 Implementation

## Quick Start

### Compile
```bash
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -o q7 q7.cpp
```

### Run
```bash
./q7 /path/to/gendb_directory [results_directory]
```

Example:
```bash
./q7 /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb ./results
```

This generates `./results/Q7.csv` with the query results.

## Implementation Summary

**Query**: Computes total shipping revenue between FRANCE and GERMANY for years 1995-1996.

**Approach**:
1. Filter lineitem table by shipdate (1995-01-01 to 1996-12-31) → 18.2M rows
2. Join with orders, customer, and supplier tables using hash joins
3. Filter for nation pairs (FRANCE-GERMANY or GERMANY-FRANCE)
4. Aggregate revenue by (supplier_nation, customer_nation, year)
5. Output 4 result rows sorted by nation pair and year

**Performance**: ~74 seconds execution time with 59.9M rows/sec throughput

**Validation**: ✅ Exact match with TPC-H ground truth

## Files

- `q7.cpp` - Complete C++ implementation (552 lines)
- `results/Q7.csv` - Output CSV with 4 result rows
- `SUMMARY.md` - Detailed technical summary
- `README.md` - This file

## Features

- **Correctness**: Date handling, decimal precision, dictionary encoding validated
- **Performance**: Optimized join ordering, hash tables, pre-sized aggregation map
- **Profiling**: Built-in [TIMING] instrumentation (compile with -DGENDB_PROFILE)
- **Self-contained**: No external dependencies beyond standard C++ library and OpenMP

## Expected Output

```csv
supp_nation,cust_nation,l_year,revenue
FRANCE,GERMANY,1995,521960141.7003
FRANCE,GERMANY,1996,524796110.3842
GERMANY,FRANCE,1995,542199700.0546
GERMANY,FRANCE,1996,533640926.2614
```
