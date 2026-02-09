# String Optimization for Analytical Databases

## What It Is
Techniques to reduce memory overhead and speed up string operations in analytical workloads. Strings are expensive: variable-length, pointer-based, poor cache locality, and slow comparisons. Optimization is critical for query performance.

## When To Use
- **Low Cardinality**: String interning, dictionary encoding (countries, categories, status codes)
- **Fixed-Length**: Inline storage for short strings (UUIDs, codes, abbreviations)
- **High Comparison Volume**: Hash-based comparisons, dictionary-coded comparisons
- **Join Keys**: Dictionary-coded strings avoid expensive string comparisons
- **String Aggregations**: GROUP BY on strings benefits from interning/dictionaries

## Key Implementation Ideas

### String Interning (Deduplication)
```cpp
// Store each unique string once, use pointers everywhere
class StringPool {
    std::unordered_map<std::string_view, const char*> pool;
    std::vector<std::unique_ptr<char[]>> storage;

public:
    const char* intern(std::string_view str) {
        auto it = pool.find(str);
        if (it != pool.end()) return it->second;

        // Allocate new string
        auto buf = std::make_unique<char[]>(str.size() + 1);
        memcpy(buf.get(), str.data(), str.size());
        buf[str.size()] = '\0';

        const char* ptr = buf.get();
        storage.push_back(std::move(buf));
        pool[std::string_view(ptr, str.size())] = ptr;
        return ptr;
    }
};

// Usage: Compare pointers instead of strings
const char* s1 = pool.intern("USA");
const char* s2 = pool.intern("USA");
if (s1 == s2) { /* Fast pointer comparison */ }
```

### Dictionary-Coded Strings
```cpp
// Map strings to integer codes
class StringDictionary {
    std::vector<std::string> values;  // Code → String
    std::unordered_map<std::string_view, uint32_t> codes; // String → Code

public:
    uint32_t encode(std::string_view str) {
        auto it = codes.find(str);
        if (it != codes.end()) return it->second;

        uint32_t code = values.size();
        values.push_back(std::string(str));
        codes[values.back()] = code;
        return code;
    }

    const std::string& decode(uint32_t code) const {
        return values[code];
    }
};

// Column storage
struct StringColumn {
    StringDictionary dict;
    std::vector<uint32_t> codes; // One int per row
};

// Query: WHERE country = 'USA'
uint32_t usa_code = dict.encode("USA");
for (auto code : codes) {
    if (code == usa_code) { /* Compare 4-byte ints */ }
}
```

### Inline Storage for Short Strings (SSO)
```cpp
// Avoid heap allocation for strings ≤15 chars
struct InlineString {
    union {
        struct {
            char* ptr;
            uint32_t length;
            uint32_t capacity;
        } heap;

        struct {
            char data[15];
            uint8_t length; // High bit = 1 for heap mode
        } stack;
    };

    bool is_heap() const { return stack.length & 0x80; }
};

// Fixed-width strings (no pointers)
struct FixedString {
    char data[16]; // Always 16 bytes, pad with '\0'
};
// Great for: UUIDs (36 chars → truncate), country codes (2-3 chars)
```

### Pointer-Free String Storage
```cpp
// Store strings in contiguous buffer, use offsets
struct StringArray {
    std::vector<char> data;         // All strings concatenated
    std::vector<uint32_t> offsets;  // Start offset of each string

    std::string_view get(size_t i) const {
        uint32_t start = offsets[i];
        uint32_t end = (i + 1 < offsets.size()) ? offsets[i + 1] : data.size();
        return std::string_view(&data[start], end - start);
    }

    void append(std::string_view str) {
        offsets.push_back(data.size());
        data.insert(data.end(), str.begin(), str.end());
    }
};

// Benefits: Cache-friendly, no pointer chasing, SIMD-friendly for scanning
```

### Hash-Based String Comparison
```cpp
// Precompute hashes for fast equality checks
struct HashedString {
    uint64_t hash;
    const char* str;
    uint32_t length;
};

bool fast_equal(const HashedString& a, const HashedString& b) {
    if (a.hash != b.hash) return false; // Fast path (8-byte comparison)
    if (a.length != b.length) return false;
    return memcmp(a.str, b.str, a.length) == 0; // Slow path (rare)
}

// For GROUP BY / JOIN
std::unordered_map<uint64_t, std::vector<HashedString>> hash_table;
```

### String Comparison Optimization
```cpp
// strcmp() is slow; optimize common cases
int fast_strcmp(const char* a, const char* b, size_t len) {
    // Compare 8 bytes at a time
    while (len >= 8) {
        uint64_t va = *reinterpret_cast<const uint64_t*>(a);
        uint64_t vb = *reinterpret_cast<const uint64_t*>(b);
        if (va != vb) {
            return memcmp(a, b, 8); // Fallback for ordering
        }
        a += 8; b += 8; len -= 8;
    }
    return memcmp(a, b, len); // Remainder
}

// For prefix matching (LIKE 'foo%')
bool prefix_match_simd(const char* str, const char* prefix, size_t prefix_len) {
    __m256i prefix_vec = _mm256_loadu_si256((__m256i*)prefix);
    __m256i str_vec = _mm256_loadu_si256((__m256i*)str);
    __m256i cmp = _mm256_cmpeq_epi8(prefix_vec, str_vec);
    uint32_t mask = _mm256_movemask_epi8(cmp);
    return (mask & ((1u << prefix_len) - 1)) == ((1u << prefix_len) - 1);
}
```

### Compact String Representation
```cpp
// Use string_view to avoid copies
void process_strings(std::vector<std::string_view> strings) {
    // No allocations, no copies
    for (auto str : strings) {
        // Work with original data
    }
}

// Length-prefixed strings (Pascal strings)
struct PascalString {
    uint16_t length;
    char data[];
};
// No null terminator needed, faster length lookup
```

### String Pooling with Arena Allocator
```cpp
class StringArena {
    static constexpr size_t ARENA_SIZE = 1 << 20; // 1MB
    std::vector<std::unique_ptr<char[]>> arenas;
    size_t current_offset = 0;

public:
    const char* allocate(std::string_view str) {
        if (current_offset + str.size() + 1 > ARENA_SIZE) {
            arenas.push_back(std::make_unique<char[]>(ARENA_SIZE));
            current_offset = 0;
        }

        char* ptr = arenas.back().get() + current_offset;
        memcpy(ptr, str.data(), str.size());
        ptr[str.size()] = '\0';
        current_offset += str.size() + 1;
        return ptr;
    }
};

// Benefits: No per-string allocation overhead, good cache locality
```

## Performance Characteristics
- **String Interning**: O(1) comparison (pointer equality), 10-100x space reduction for low-cardinality
- **Dictionary Encoding**: 10-50x compression, 100x faster comparisons (4-byte int vs string)
- **Inline Storage**: Avoids heap allocation overhead (~40 cycles), better cache locality
- **Hash-Based Comparison**: 10x faster for long strings (8-byte hash vs full strcmp)
- **Pointer-Free Storage**: 2-3x better cache hit rate, no pointer chasing
- **Memory Overhead**: std::string = 24 bytes + data; dictionary code = 4 bytes; interned = 8 bytes (pointer)

## Real-World Examples

### DuckDB
- Dictionary encoding for low-cardinality strings
- String heap per column chunk
- Inline storage for strings ≤12 bytes

### ClickHouse
- LowCardinality(String) type (dictionary encoding)
- FixedString(N) for fixed-length (UUIDs, codes)
- String interning for GROUP BY keys

### Apache Arrow
- Dictionary-encoded string arrays
- Offset-based string storage (no pointers)
- UTF-8 validation on read

### Snowflake
- Automatic dictionary encoding
- String deduplication across partitions
- Hash-based string joins

## Pitfalls
- **High-Cardinality Dictionary**: Dictionary grows unbounded, becomes larger than original data
- **Dictionary Fragmentation**: Per-chunk dictionaries waste space; consider global dictionary
- **Interning Memory Leaks**: Interned strings never freed; use arena allocators with bounded lifetime
- **Hash Collisions**: Hash-based equality must verify full string (don't skip memcmp)
- **Fixed-Width Waste**: FixedString(100) wastes space for short strings
- **UTF-8 Assumptions**: Incorrect length calculations, case-insensitive comparison breaks
- **Over-Optimization**: Don't optimize string operations on small tables (<10K rows)
