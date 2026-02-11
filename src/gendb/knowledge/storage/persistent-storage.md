# Persistent Binary Columnar Storage

## What It Is
Store table data as binary column files on disk. Each column is written as a typed array directly to file. On read, use mmap() for zero-copy access. This eliminates text-parsing bottlenecks: a 60M-row table that takes ~10 minutes to parse from text takes <1 second to mmap from binary.

## When To Use
- **Always for repeated query execution** -- persistent binary storage pays for itself immediately if data is loaded more than once
- **Always for large datasets** (>1M rows) -- text parsing becomes the dominant cost
- **Range predicates on sortable columns** -- zone maps + sorted data enable block skipping
- **Queries touching few columns** -- column pruning saves I/O proportional to unused columns
- **Multiple queries on same dataset** -- amortized ingestion cost across executions

## Key Implementation Ideas
- **Binary column files**: Write each column as N * sizeof(type) bytes with no headers or delimiters; simple and fast to read/write
- **mmap-based zero-copy reads**: Map column files into memory; the OS pages in data on demand without explicit parsing
- **madvise hints**: Use MADV_SEQUENTIAL for full scans, MADV_RANDOM for index lookups, MADV_WILLNEED for prefetching, MAP_POPULATE for hot columns
- **Column pruning**: Only mmap/read columns the query actually uses; e.g., reading 6 of 16 columns saves 62% I/O
- **Block organization**: Split columns into fixed-size blocks (64K-256K rows) for granular access and per-block metadata
- **Zone maps (min/max per block)**: Store min/max per block in metadata; skip entire blocks whose range doesn't overlap the predicate
- **Sort key selection**: Sort rows by a frequently-filtered column before writing to make zone maps effective; choose based on workload
- **Composite sort keys**: Sort by multiple columns (e.g., shipdate, orderkey) to benefit multiple query patterns
- **Per-column compression**: Dictionary-encode strings, delta-encode sorted integers, bit-pack small-range columns; leave high-entropy numerics uncompressed
- **Index persistence**: Write sorted/hash indexes to disk alongside data; mmap on load instead of rebuilding each time
- **Parallel ingestion**: Parse and write multiple tables in parallel; within a table, write each column file concurrently
- **Metadata file (metadata.json)**: Store schema, row count, column types, block info, and zone maps in a single JSON sidecar

## Performance Characteristics
- **mmap vs text parsing**: <1 second binary load vs ~10 minutes text parsing for 60M rows
- **Column pruning**: I/O savings proportional to fraction of unused columns (e.g., 62% for 6/16 columns)
- **Zone maps on sorted data**: Near-perfect block skipping for range predicates on the sort column

## High-Performance Ingestion Techniques

Ingestion (parsing .tbl text → binary column files) can be the slowest step. Use these techniques:

- **mmap input files**: `mmap()` the entire .tbl file instead of using `ifstream`. Access the file as a `const char*` pointer. This avoids buffered I/O syscall overhead and lets the OS manage page caching.
  ```cpp
  int fd = open(path, O_RDONLY);
  size_t sz = lseek(fd, 0, SEEK_END);
  const char* data = (const char*)mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
  madvise((void*)data, sz, MADV_SEQUENTIAL);
  ```

- **Parallel table ingestion**: Launch `std::thread` per table. Independent tables (nation, region, supplier, part, customer) can all be parsed concurrently.

- **Pre-allocate column vectors**: Estimate row count from file size: `estimated_rows = file_size / avg_bytes_per_line`. Then `vec.reserve(estimated_rows)` for every column vector. This avoids O(n) reallocations.

- **Chunk-based parallel parsing**: For large tables (lineitem: 60M rows), split the mmap'd buffer into N chunks (N = nproc). Find the nearest newline after each chunk boundary. Parse each chunk in a separate thread into local vectors, then concatenate.

- **In-place numeric parsing**: Avoid creating `std::string` for numeric fields. Use `strtol(ptr, &end, 10)` or `strtod(ptr, &end)` directly on the mmap'd buffer. Advance the pointer past the delimiter.

- **Buffered binary writes**: Accumulate all parsed data in memory (column vectors), then write each column in one large `fwrite()` call. Use `setvbuf(f, buf, _IOFBF, 1<<20)` for 1MB write buffer.

- **Batch writes**: Write entire column at once: `fwrite(vec.data(), sizeof(T), vec.size(), f)`. One syscall per column instead of one per row.

- **Progress reporting**: Print per-table timing: `printf("lineitem: %zu rows in %.1fs\n", count, elapsed)`.

Target ingestion performance: SF1 (6M rows lineitem) in <5 seconds, SF10 (60M rows) in <30 seconds.

## Pitfalls
- **Unsorted data defeats zone maps**: Without sorting, blocks contain mixed values and few can be skipped
- **Sort key trade-off**: Sorting benefits one column's predicates but may hurt queries filtering on other columns
- **Pre-building indexes costs time**: Building a sorted index on 60M rows takes ~10s, but amortizes across many queries
