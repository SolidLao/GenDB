# Persistent Binary Columnar Storage

## Overview
Store table data as binary column files on disk. Each column is written as a typed array (e.g., `int32_t[]`, `double[]`) directly to file. On read, use `mmap()` for zero-copy access — the OS maps file pages into memory on demand, avoiding explicit parsing. This eliminates the text-parsing bottleneck: a 60M-row lineitem table that takes ~10 minutes to parse from `.tbl` text takes <1 second to mmap from binary.

## File Layout
```
<table>.gendb/
├── metadata.json          # Schema, row count, column types, block info, zone maps
├── <column>.col           # Raw binary column data (typed array)
├── <column>.col.dict      # Dictionary file (if dictionary-encoded)
├── <column>.col.idx       # Index file (sorted/hash, if present)
└── ...
```

Each `.col` file is simply `N * sizeof(type)` bytes — e.g., an `int32_t` column with 60M rows = 240MB. No headers, no delimiters, no encoding overhead (unless compression is applied).

## Binary Column Files

**Writing (ingestion):**
```cpp
// Write a column of int32_t values
void write_column(const std::string& path, const int32_t* data, size_t count) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    write(fd, data, count * sizeof(int32_t));
    close(fd);
}
```

**Reading (query execution):**
```cpp
// mmap a column for zero-copy read access
template<typename T>
const T* mmap_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    count = st.st_size / sizeof(T);
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);  // fd can be closed after mmap
    return static_cast<const T*>(ptr);
}
```

## mmap Strategies

- **`MADV_SEQUENTIAL`**: For full-column scans (filters, aggregations). Tells the OS to read-ahead aggressively.
- **`MADV_RANDOM`**: For index-based point lookups. Disables read-ahead to avoid wasted I/O.
- **`MADV_WILLNEED`**: Prefetch columns you know you'll need. Call before query execution starts.
- **`MAP_POPULATE`**: Pre-fault all pages at mmap time. Useful for hot columns that will be fully scanned.

```cpp
madvise(ptr, size, MADV_SEQUENTIAL);  // scan workload
madvise(ptr, size, MADV_RANDOM);      // index lookup workload
```

## Column Pruning

Only mmap/read columns that the query actually uses. For Q6 (lineitem scan with 4 filter columns + 2 computation columns), read 6 of 16 columns — saving 62% of I/O. The storage design's `io_strategies` section specifies which columns each query needs.

## Block Organization and Zone Maps

Split each column into fixed-size blocks (e.g., 64K-256K rows per block). Store min/max metadata per block in the metadata file. During query execution, skip entire blocks whose min/max range doesn't overlap the predicate.

```
column.col:  [block_0: 64K rows][block_1: 64K rows][block_2: 64K rows]...
metadata:    block_0: {min: 1994-01-01, max: 1994-06-15}
             block_1: {min: 1994-06-16, max: 1994-12-31}
             ...
```

**Effectiveness depends on data ordering.** If lineitem is sorted by `l_shipdate`, zone maps on shipdate enable near-perfect block skipping for date range predicates. Without sorting, blocks contain mixed values and few can be skipped.

## Data Ordering (Sort Key)

Sort the table's rows by a frequently-filtered column before writing blocks. This clusters related values together, making zone maps effective and enabling range-based block skipping.

**Choose sort key based on workload:**
- If most queries filter on `l_shipdate` (Q1, Q6), sort lineitem by `l_shipdate`
- If join performance matters more, sort by the join key (e.g., `l_orderkey`)
- Composite sort keys (e.g., `l_shipdate, l_orderkey`) help multiple query patterns

Trade-off: sorting benefits range predicates on the sort column but may hurt queries that filter on other columns.

## Compression

Apply per-column compression based on data characteristics. Decompress on read (can be done lazily per block).

| Technique | Best For | Ratio | Decompression Cost |
|-----------|----------|-------|--------------------|
| **Dictionary encoding** | Low-cardinality strings (l_returnflag: 3 values, l_linestatus: 2 values) | 10-100x | Very low (integer lookup) |
| **Delta encoding** | Sorted or near-sorted integers (sorted l_shipdate, sequential keys) | 2-4x | Low (prefix sum) |
| **Run-length encoding (RLE)** | Consecutive repeated values (sorted low-cardinality columns) | Variable | Very low |
| **Bit-packing** | Small integers (flags, status codes, small quantities) | 2-8x | Low (bit shifts) |

**Rule of thumb**: Dictionary-encode all string columns. Delta-encode sorted integer columns. Bit-pack columns with known small range. Leave high-entropy numeric columns (prices, quantities) uncompressed — compression overhead outweighs I/O savings for in-memory binary data.

## Index Persistence

Write indexes to disk alongside data. On load, mmap the index file instead of rebuilding.

- **Sorted index**: Write sorted (value, row_id) pairs as binary. Use binary search for range queries.
- **Hash index**: Write bucket array + entries as binary. Rebuild only if data changes.
- **Composite index**: Sorted on (col1, col2, ...) for multi-column predicates.
- **Zone maps**: Store min/max per block in metadata.json. No separate file needed.

Pre-building indexes during ingestion amortizes the cost — building a sorted index on 60M rows takes ~10s once, but saves repeated index-build time across many query executions.

## Parallel Ingestion

- **Multi-table parallelism**: Parse and write each table in a separate thread. Tables are independent.
- **Multi-column parallelism**: Within a table, write each column file in parallel (columnar format enables this naturally).
- **Partitioned parsing**: For very large tables (lineitem), partition the `.tbl` file into chunks and parse in parallel.

```cpp
// Parse lineitem.tbl into columnar arrays in parallel
std::vector<std::thread> threads;
for (const auto& table : tables) {
    threads.emplace_back([&]() { ingest_table(table, data_dir, gendb_dir); });
}
for (auto& t : threads) t.join();
```

## Design Patterns from Production Systems

- **DuckDB**: Column chunks (vectors of 2048 values), buffer manager for larger-than-memory, zone maps per row group.
- **ClickHouse**: Granules (8192 rows), sparse primary index (one entry per granule), MergeTree storage with sorted data.
- **Apache Parquet**: Row groups (128MB default) containing column chunks with page-level statistics. Dictionary + RLE + bit-packing encoding.

**Key takeaway**: All high-performance analytical systems use (1) columnar binary storage, (2) block/chunk organization with metadata, (3) sort-based clustering for predicate pushdown. The specific block sizes and encoding choices depend on the workload.

## When to Use

- **Always for repeated query execution** — if data is loaded more than once, persistent binary storage pays for itself immediately
- **Always for large datasets** (>1M rows) — text parsing becomes the dominant cost
- **Especially when**: queries use range predicates on sortable columns (zone maps + sorted data = block skipping), queries touch few columns (column pruning), or multiple queries share the same dataset (amortized ingestion)
