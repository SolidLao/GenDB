# Columnar vs Row Storage

## What It Is
Columnar storage stores each column contiguously in memory, while row storage stores complete records together. Columnar layouts excel at analytical queries that scan few columns but many rows.

## When To Use
- **Use Columnar**: Analytical queries (SELECT avg(price) WHERE category='X'), column subsetting (reading 3 of 50 columns), high compression ratios, SIMD-friendly operations
- **Use Row**: OLTP workloads, frequent full-row access, updates to individual records, key-value lookups
- **Hybrid**: Column groups (store related columns together), adaptive row groups for hot data

## Key Implementation Ideas

### Basic Columnar Layout
```cpp
struct ColumnChunk {
    void* data;           // Typed array: int32_t[], double[], etc.
    uint8_t* null_bitmap; // 1 bit per value
    size_t count;
    DataType type;
};

struct Table {
    std::vector<ColumnChunk> columns;
    size_t row_count;
};
```

### Projection Pushdown
```cpp
// Only materialize needed columns
std::vector<int> project_columns = {2, 5, 7}; // col_id's
for (auto col_id : project_columns) {
    scan_column(table.columns[col_id]);
}
```

### Late Materialization
```cpp
// 1. Filter on indexed column first (produces row_ids)
std::vector<uint64_t> qualifying_rows = filter_column(price_col, predicate);

// 2. Only then fetch other columns for qualifying rows
for (auto row_id : qualifying_rows) {
    auto name = name_col.data[row_id];
    auto quantity = quantity_col.data[row_id];
}
```

### Column Groups (Hybrid Approach)
```cpp
// Group frequently co-accessed columns
struct ColumnGroup {
    std::vector<ColumnChunk> columns; // e.g., {city, state, zip}
    // Store together for better cache locality
};
```

### Vectorized Column Scanning
```cpp
void scan_column_simd(int32_t* col, int32_t threshold, size_t count) {
    __m256i thresh_vec = _mm256_set1_epi32(threshold);
    for (size_t i = 0; i < count; i += 8) {
        __m256i vals = _mm256_loadu_si256((__m256i*)&col[i]);
        __m256i mask = _mm256_cmpgt_epi32(vals, thresh_vec);
        // Process 8 values at once
    }
}
```

## Performance Characteristics
- **Memory Bandwidth**: 5-10x better for selective column access (read 10% of columns vs 100%)
- **Cache Efficiency**: Sequential access patterns, prefetcher-friendly
- **Compression**: 3-10x better compression (homogeneous data per column)
- **SIMD**: Natural fit for vectorized operations (8 consecutive ints vs scattered)
- **Write Penalty**: Inserts require touching multiple memory locations (one per column)

## Real-World Examples

### DuckDB
- Uses columnar storage with 2048-row chunks (Vector size)
- Column chunks compressed independently
- Late materialization for filters

### ClickHouse
- Columnar MergeTree storage
- Each column stored in separate `.bin` file
- Granules of 8192 rows for compression/indexing

### PostgreSQL (cstore_fdw)
- Extension for columnar storage
- Stripes of 150,000 rows
- Column-level compression (different algorithms per column type)

### Apache Parquet
- Columnar file format
- Row groups (128MB) → Column chunks → Pages (1MB)
- Dictionary encoding + bit packing per page

## Pitfalls
- **Small Tables**: Overhead of managing multiple column arrays isn't worth it (<10K rows)
- **Wide Row Access**: If query needs all columns, row format is faster (avoid tuple reconstruction cost)
- **Updates**: Columnar updates are expensive (modify N arrays vs 1 row)
- **Cache Line Splits**: Ensure column chunks align to 64-byte boundaries
- **Over-Chunking**: Too many small chunks hurt metadata overhead; sweet spot is 10K-100K rows per chunk
