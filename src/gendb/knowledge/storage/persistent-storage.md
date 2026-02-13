# Persistent Binary Columnar Storage

## What It Is
Store table data as binary column files on disk. Each column is written as a typed array directly to file. On read, use mmap() for zero-copy access. This eliminates text-parsing bottlenecks: a 60M-row table that takes ~10 minutes to parse from text takes <1 second to mmap from binary.

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
