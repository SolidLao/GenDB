# Lightweight Compression for Analytical Workloads

## What It Is
Fast compression schemes that reduce memory footprint and increase effective memory bandwidth without heavy CPU cost. Unlike general-purpose compression (gzip), these allow direct querying on compressed data.

## When To Use
- **High Cardinality Strings**: Dictionary encoding
- **Sorted/Sequential Data**: Delta encoding, RLE
- **Small Integer Ranges**: Bit packing, Frame-of-Reference
- **Memory-Bound Queries**: Compression speeds up queries by reducing data movement
- **Cold Storage**: Heavier compression (LZ4, Zstd) for archival data

## Key Implementation Ideas

### Dictionary Encoding
```cpp
// Best for low-cardinality strings (countries, categories, status codes)
struct DictionaryColumn {
    std::vector<std::string> dictionary; // Unique values
    std::vector<uint32_t> codes;         // Indices into dictionary
};

// Query: WHERE country = 'USA'
uint32_t usa_code = dictionary.find("USA");
for (auto code : codes) {
    if (code == usa_code) { /* match */ }
}
// Compare 4-byte int instead of string
```

### Run-Length Encoding (RLE)
```cpp
// Best for sorted columns or repeated values
struct RLEColumn {
    std::vector<int32_t> values;  // [10, 20, 30]
    std::vector<size_t> lengths;  // [100, 50, 75] (100 10's, 50 20's, 75 30's)
};

// Sum query
int64_t sum = 0;
for (size_t i = 0; i < values.size(); i++) {
    sum += values[i] * lengths[i];
}
```

### Bit Packing
```cpp
// Store integers using minimum bits needed
void pack_3bit(uint8_t* dest, uint8_t* src, size_t count) {
    // 3 bits per value → 8 values per 3 bytes
    for (size_t i = 0; i < count; i += 8) {
        dest[0] = src[0] | (src[1] << 3) | (src[2] << 6);
        dest[1] = (src[2] >> 2) | (src[3] << 1) | (src[4] << 4) | (src[5] << 7);
        dest[2] = (src[5] >> 1) | (src[6] << 2) | (src[7] << 5);
        dest += 3; src += 8;
    }
}
// Saves 62.5% space for values in [0, 7]
```

### Frame-of-Reference (FOR) Encoding
```cpp
// Store offset from minimum value
struct FORColumn {
    int32_t base;          // Minimum value
    std::vector<uint8_t> offsets; // deviations from base
};

// Example: [1000, 1001, 1003, 1002]
// → base=1000, offsets=[0, 1, 3, 2] (1 byte each instead of 4)

int32_t decode(size_t i) { return base + offsets[i]; }
```

### Delta Encoding
```cpp
// Store differences between consecutive values
struct DeltaColumn {
    int64_t first_value;
    std::vector<int16_t> deltas; // Small deltas for sequential data
};

// Timestamps: [1000, 1001, 1002, 1003]
// → first=1000, deltas=[1, 1, 1] (2 bytes vs 8 bytes each)

int64_t decode(size_t i) {
    int64_t val = first_value;
    for (size_t j = 0; j < i; j++) val += deltas[j];
    return val;
}
```

### Cascading Compression
```cpp
// Apply multiple schemes
struct CompressedColumn {
    // 1. Dictionary encode
    // 2. Bit-pack dictionary codes
    // 3. RLE if sorted
};

// Example: 1M rows, 10 unique countries
// → Dictionary: 10 strings
// → Codes: 1M × 4 bits = 500KB (instead of ~10MB raw strings)
```

### Adaptive Compression
```cpp
enum CompressionScheme { NONE, DICT, RLE, BITPACK, DELTA, FOR };

CompressionScheme choose_scheme(ColumnChunk& col) {
    size_t unique_count = count_unique(col);
    if (unique_count < col.count / 100) return DICT;
    if (is_sorted(col)) return RLE;
    int bits_needed = log2(max_value - min_value + 1);
    if (bits_needed < 16) return FOR;
    return NONE;
}
```

## Performance Characteristics
- **Dictionary Encoding**: 10-100x compression for low-cardinality (100 unique values in 1M rows)
- **RLE**: 1000x+ for highly sorted data (sorted timestamps, boolean flags)
- **Bit Packing**: 2-8x for small integer ranges
- **Delta Encoding**: 2-4x for sequential data (timestamps, auto-increment IDs)
- **CPU Cost**: <10% overhead for lightweight schemes (vs 50%+ for LZ4/Zstd)
- **Query Speed**: Often faster than uncompressed (less memory to scan)

## Real-World Examples

### DuckDB
- Automatic compression per column chunk
- Dictionary + bit packing for strings
- FOR + bit packing for integers
- RLE for constants

### ClickHouse
- Supports LZ4, ZSTD, Delta, DoubleDelta, Gorilla (for floats)
- Delta for timestamps, T64 (removes leading zero bytes)
- LZ4 by default (fast decompression)

### Apache Parquet
- Dictionary encoding per page (1MB)
- RLE for definition/repetition levels
- Bit packing for dictionary indices

### Vertica
- Automatic encoding selection per column
- Block-level compression (65K rows)
- 10:1 compression ratios typical

## Pitfalls
- **Over-Compression**: LZ4/Zstd too slow for hot data (use lightweight schemes)
- **Write Amplification**: Dictionary encoding on high-cardinality columns (dictionary grows unbounded)
- **Decompression in Hot Loop**: Cache decoded values, batch decompress
- **Wrong Scheme**: Bit-packing unsorted data with large range (no benefit, added CPU cost)
- **Metadata Overhead**: Don't compress tiny chunks (<1KB); overhead exceeds benefit
- **Update Cost**: Compressed columns harder to update; consider row-level deltas
