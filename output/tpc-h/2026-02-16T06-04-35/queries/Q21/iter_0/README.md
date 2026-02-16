# Q21 - Suppliers Who Kept Orders Waiting (Iteration 0)

## Quick Start

Compile:
```bash
cd /home/jl4492/GenDB/output/tpc-h/2026-02-16T06-04-35/queries/Q21/iter_0
g++ -O3 -march=native -std=c++17 -Wall -lpthread -fopenmp -DGENDB_PROFILE -o q21 q21.cpp
```

Run:
```bash
./q21 /home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb results
```

Results written to: `results/Q21.csv`

## Files

- **q21.cpp** - Implementation (423 lines)
- **q21** - Compiled binary (68 KB)
- **results/Q21.csv** - Output (100 suppliers + header)
- **IMPLEMENTATION_SUMMARY.md** - Detailed design and correctness analysis

## Key Statistics

| Metric | Value |
|--------|-------|
| Input rows | 59,986,052 (lineitem) |
| Output rows | 100 (LIMIT 100) |
| Execution time | ~2.3 seconds |
| Total wall time | ~15 seconds |
| Data encodings | DATE (int32_t), Dictionary (int8_t), Varstring |
| Hash tables | 4 (l_orderkey, l_suppkey, orders, supplier) |

## Correctness Features

✓ DATE comparison as epoch days (int32_t)
✓ Dictionary encoding for o_orderstatus ('F' → code 1)
✓ Variable-length string parsing for s_name, n_name
✓ EXISTS/NOT EXISTS subquery decoration
✓ Proper GROUP BY aggregation
✓ Result sorting (COUNT DESC, name ASC)
✓ LIMIT 100 enforcement

## [TIMING] Instrumentation

With `-DGENDB_PROFILE`, the binary outputs:
```
[TIMING] load:           0.21 ms
[TIMING] build_indexes:  0.00 ms
[TIMING] execute:     2298.00 ms
[TIMING] sort:           0.91 ms
[TIMING] output:         0.16 ms
[TIMING] total:      15164.00 ms
```

## Output Format

CSV file with headers:
```
s_name,numwait
Supplier#000062538,24
Supplier#000032858,22
Supplier#000063723,21
...
```

## Known Limitations

Iteration 0 (correctness focus) — no:
- Parallelization
- SIMD optimizations
- Pre-built index loads
- Zone map pruning
- Query reordering

These are targeted for iteration 1+.
