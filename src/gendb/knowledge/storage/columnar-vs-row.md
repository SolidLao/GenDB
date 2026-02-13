# Columnar vs Row Storage

## What It Is
Columnar storage stores each column contiguously in memory, while row storage stores complete records together. Columnar layouts excel at analytical queries that scan few columns but many rows.

## Key Implementation Ideas
- **Basic columnar layout**: Store typed arrays per column with a null bitmap; table is a collection of column chunks
- **Projection pushdown**: Only materialize the columns needed by the query, skipping irrelevant columns entirely
- **Late materialization**: Filter on predicate columns first to produce qualifying row IDs, then fetch remaining columns only for those rows
- **Column groups (hybrid)**: Group frequently co-accessed columns together for better cache locality on multi-column access patterns
- **Vectorized column scanning**: Use SIMD instructions to process multiple values per column in a single CPU operation (e.g., 8 int32s at once with AVX2)
- **Null bitmap encoding**: Use 1 bit per value to track NULLs, keeping the data array dense and branch-free
- **Chunk-based organization**: Divide columns into fixed-size chunks (e.g., 2048-100K rows) for cache-friendly processing and independent compression
