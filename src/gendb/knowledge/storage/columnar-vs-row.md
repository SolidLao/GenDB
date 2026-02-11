# Columnar vs Row Storage

## What It Is
Columnar storage stores each column contiguously in memory, while row storage stores complete records together. Columnar layouts excel at analytical queries that scan few columns but many rows.

## When To Use
- **Use Columnar**: Analytical queries, column subsetting, high compression ratios, SIMD-friendly operations
- **Use Row**: OLTP workloads, frequent full-row access, updates to individual records, key-value lookups
- **Hybrid**: Column groups (store related columns together), adaptive row groups for hot data

## Key Implementation Ideas
- **Basic columnar layout**: Store typed arrays per column with a null bitmap; table is a collection of column chunks
- **Projection pushdown**: Only materialize the columns needed by the query, skipping irrelevant columns entirely
- **Late materialization**: Filter on predicate columns first to produce qualifying row IDs, then fetch remaining columns only for those rows
- **Column groups (hybrid)**: Group frequently co-accessed columns together for better cache locality on multi-column access patterns
- **Vectorized column scanning**: Use SIMD instructions to process multiple values per column in a single CPU operation (e.g., 8 int32s at once with AVX2)
- **Null bitmap encoding**: Use 1 bit per value to track NULLs, keeping the data array dense and branch-free
- **Chunk-based organization**: Divide columns into fixed-size chunks (e.g., 2048-100K rows) for cache-friendly processing and independent compression

## Performance Characteristics
- **Memory bandwidth**: 5-10x better for selective column access (read 10% of columns vs 100%)
- **Compression**: 3-10x better compression ratios due to homogeneous data per column
- **Write penalty**: Inserts require touching multiple memory locations (one per column)

## Pitfalls
- **Small tables**: Column management overhead not worth it for <10K rows
- **Wide row access**: If query needs all columns, row format avoids tuple reconstruction cost
- **Updates**: Columnar updates are expensive (modify N arrays vs 1 row)
