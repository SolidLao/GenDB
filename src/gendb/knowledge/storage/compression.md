# Lightweight Compression for Analytical Workloads

## What It Is
Fast compression schemes that reduce memory footprint and increase effective memory bandwidth without heavy CPU cost. Unlike general-purpose compression (gzip), these allow direct querying on compressed data.

## Key Implementation Ideas
- **Dictionary encoding**: Map unique values to integer codes; queries compare small integers instead of full strings
- **Run-length encoding (RLE)**: Store (value, count) pairs for consecutive repeated values; aggregations operate on runs directly without expanding
- **Bit packing**: Store integers using the minimum number of bits needed (e.g., 3-bit values pack 8 per 3 bytes, saving 62.5%)
- **Frame-of-Reference (FOR) encoding**: Store a base (minimum) value plus small per-value offsets, reducing bytes per value
- **Delta encoding**: Store differences between consecutive values; ideal for timestamps and sequential IDs
- **Cascading compression**: Apply multiple schemes in sequence (e.g., dictionary encode, then bit-pack the codes, then RLE if sorted)
- **Adaptive compression selection**: Automatically choose scheme based on column statistics (cardinality, sortedness, value range)
- **Per-column-chunk compression**: Compress each chunk independently so different chunks can use different schemes
- **Operating directly on compressed data**: Perform filters and aggregations without decompressing (e.g., RLE sum = value * count)
