# Data Loading Optimization

## What
Techniques to minimize cold-start I/O cost when OS page cache is empty. Two strategies:
(A) Selective loading — read only the bytes needed, in the right order.
(B) Compression — reduce the number of bytes to read from disk.

## When
- `data_loading` phase accounts for >30% of total execution time
- Performance degrades significantly when OS page cache is cleared (`echo 3 > /proc/sys/vm/drop_caches`)
- Large fact tables (>1M rows) with selective filters that touch <50% of blocks

## Key Ideas

### A. Cold-Start I/O Techniques

**1. Explicit `data_loading` phase**
Wrap all I/O-intensive operations in `GENDB_PHASE("data_loading")` so the optimizer can distinguish I/O cost from computation cost. Place this as Phase 0, before dimension filtering.

```cpp
// Phase 0: Data loading (I/O-bound — visible to optimizer)
{
    GENDB_PHASE("data_loading");
    // mmap + madvise for all columns used in this query
}
```

**2. Zone-map-guided selective loading**
Instead of `madvise(MADV_WILLNEED)` on the entire column file, use zone maps to identify qualifying blocks, then prefetch only those byte ranges:

```cpp
// Load zone map first (small — always fits in a single page)
auto* zm = mmap_file<ZoneMapEntry>(zone_map_path, num_blocks);

// Selectively prefetch only qualifying blocks
for (uint32_t b = 0; b < num_blocks; b++) {
    if (zm[b].max >= lower_bound && zm[b].min <= upper_bound) {
        size_t byte_start = (size_t)zm[b].row_offset * sizeof(T);
        size_t byte_len = (size_t)zm[b].block_size * sizeof(T);
        madvise(col_ptr + byte_start, byte_len, MADV_WILLNEED);
    }
}
```

**3. Parallel column prefetch**
Issue `madvise(MADV_WILLNEED)` for multiple columns concurrently using threads. The kernel can dispatch parallel I/O requests to the SSD:

```cpp
#pragma omp parallel for
for (int c = 0; c < num_columns; c++) {
    madvise(column_ptrs[c], column_sizes[c], MADV_WILLNEED);
}
```

**4. Column-ordered loading**
Load columns in priority order to minimize time-to-first-result:
1. Zone maps (tiny, enables selective loading of everything else)
2. Filter columns (enables early row elimination)
3. Join key columns (enables hash table construction)
4. Payload/aggregation columns (only for qualifying rows)

**5. Late materialization for I/O**
Defer loading payload columns (strings, wide columns) until after filtering. Only read bytes for rows that pass all predicates. Combine with zone-map-guided loading for maximum I/O reduction.

### B. Compression for I/O Reduction

Decompression throughput (5-20 GB/s in-memory) far exceeds SSD read bandwidth (500 MB/s), so lightweight compression always wins on cold start.

**1. Byte-packing low-cardinality numeric columns**
If a column has <256 distinct values, store as `uint8_t` with a lookup table in a sidecar file:

```cpp
// Sidecar: lookup_table.bin — array of double[num_distinct]
// Main: column.bin — array of uint8_t[num_rows]
auto* lookup = mmap_file<double>(lookup_path, num_distinct);
auto* codes = mmap_file<uint8_t>(column_path, num_rows);
double val = lookup[codes[row]];  // decode
```
**I/O reduction**: 4-8x (8 bytes → 1 byte per value).

**2. Frame-of-Reference (FOR)**
For narrow-range numeric columns (max - min < 256), store as `base + uint8_t offset * step`:

```cpp
// Header: base (double), step (double)
// Data: uint8_t offsets[num_rows]
double val = base + offsets[row] * step;  // decode
```
**I/O reduction**: 4-8x.

**3. Delta encoding for sorted columns**
Store deltas between consecutive values as a smaller integer type:

```cpp
// First value stored as full type, rest as int8_t/int16_t deltas
int32_t val = prev_val + deltas[row];  // cumulative decode
```
**I/O reduction**: 2-4x.

**4. Choosing compression scheme**
Use column statistics from `storage_design.json`:
- `distinct_count < 256` → byte-pack with lookup table
- `max - min < 256` (numeric) → Frame-of-Reference
- Column is sorted → delta encoding
- Otherwise → no compression (mmap raw)

The storage designer stores compressed data + metadata (scheme, lookup table path) in `storage_design.json`. The code generator decodes inline — no runtime decompression library needed.

## Cold vs Hot Tradeoffs
GenDB runs each query 3 times per iteration: 1 cold (cache cleared) + 2 hot (cached). The combined
metric (cold + avg hot) is the optimization target. Key tradeoffs:
- **Cold = disk I/O bound**: dominated by page faults, SSD read latency. Optimize via selective loading, compression.
- **Hot = CPU bound**: dominated by hash probes, scans, aggregation. Optimize via better data structures, parallelism.
- **Compression**: wins on cold (fewer bytes from disk), neutral on hot (decompression is CPU-cheap vs page cache).
- **Selective loading**: helps both cold (fewer page faults) and hot (fewer TLB misses on smaller working set).
- **Parallel prefetch**: helps cold only (parallel I/O requests to SSD). No effect on hot (data in cache).
- **Huge pages**: help both cold (fewer page faults) and hot (fewer TLB misses).
- **NEVER regress hot for cold**. Both must improve or stay neutral.

## Diagnostic
- When cold >> hot (>3x): data_loading is I/O-bound. Focus on reducing bytes read from disk.
- When cold ≈ hot: compute-bound. Standard optimization (data structures, parallelism).
- When both high: suspect mmap fault overhead. Consider madvise(MADV_HUGEPAGE) or explicit read() with huge page buffers.

## Impact
- Zone-map-guided selective loading: 2-5x I/O reduction on selective queries (<50% blocks qualify)
- Byte-packing / FOR compression: 4-8x I/O reduction per compressed column
- Combined: cold-start performance approaches warm-cache levels (within 1.5-2x)
- No impact on warm-cache performance (data already in page cache, decompression is CPU-cheap)
