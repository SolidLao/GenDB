# Flat Structures

## What It Is
Data structures using contiguous arrays instead of pointer-based layouts (linked lists, trees). Includes Struct-of-Arrays (SOA) patterns that separate fields into parallel arrays for cache efficiency and SIMD vectorization.

## When To Use
- Columnar storage for analytical queries (scan entire columns)
- Hot loops that access subset of fields (avoid loading unused data)
- SIMD operations on homogeneous data
- Reducing cache misses and pointer-chasing overhead

## Key Implementation Ideas

### Array-of-Structs (AOS) vs Struct-of-Arrays (SOA)
```cpp
// Array-of-Structs (BAD for columnar scans)
struct RowAOS {
    int32_t id;
    int32_t age;
    float salary;
    char name[64];
};
std::vector<RowAOS> table; // 80 bytes per row, poor cache utilization

// Struct-of-Arrays (GOOD for columnar scans)
struct TableSOA {
    std::vector<int32_t> ids;
    std::vector<int32_t> ages;
    std::vector<float> salaries;
    std::vector<std::string> names;
};
// Scan only 'ages' without loading other fields
```

### Flat Vector (DuckDB-style columnar data)
```cpp
template<typename T>
struct FlatVector {
    std::vector<T> data;           // Actual values
    std::vector<bool> validity;    // NULL bitmap (1 bit per value)
    size_t count;                  // Number of valid entries

    // SIMD-friendly scan: filter age > 30
    void filter_greater_than(int32_t threshold, uint8_t* selection) {
        __m256i thresh_vec = _mm256_set1_epi32(threshold);
        for (size_t i = 0; i < count; i += 8) {
            __m256i values = _mm256_loadu_si256((__m256i*)&data[i]);
            __m256i cmp = _mm256_cmpgt_epi32(values, thresh_vec);
            selection[i/8] = _mm256_movemask_epi8(cmp);
        }
    }
};
```

### Selection Vectors (Avoid Data Copying)
```cpp
// Instead of copying filtered rows, store indices
struct SelectionVector {
    std::vector<size_t> selected_indices;
    size_t count;
};

// Filter without materializing new columns
SelectionVector filter_column(const std::vector<int32_t>& ages, int32_t min_age) {
    SelectionVector sel;
    for (size_t i = 0; i < ages.size(); ++i) {
        if (ages[i] > min_age) {
            sel.selected_indices.push_back(i);
        }
    }
    sel.count = sel.selected_indices.size();
    return sel;
}

// Apply selection vector to other columns
void gather(const std::vector<float>& salaries, const SelectionVector& sel,
            std::vector<float>& output) {
    output.resize(sel.count);
    for (size_t i = 0; i < sel.count; ++i) {
        output[i] = salaries[sel.selected_indices[i]];
    }
}
```

### Flat Hash Table (Open Addressing)
```cpp
// Avoid std::unordered_map (pointer-heavy, many allocations)
template<typename K, typename V>
struct FlatHashTable {
    std::vector<K> keys;
    std::vector<V> values;
    std::vector<bool> occupied;

    void insert(K key, V value) {
        size_t idx = hash(key) & (keys.size() - 1);
        while (occupied[idx]) {
            if (keys[idx] == key) {
                values[idx] = value;
                return;
            }
            idx = (idx + 1) & (keys.size() - 1);
        }
        keys[idx] = key;
        values[idx] = value;
        occupied[idx] = true;
    }
};
```

### Eliminating std::map/std::list
```cpp
// BAD: std::map is a red-black tree (pointer-heavy, cache-unfriendly)
std::map<int32_t, std::string> symbol_table;

// GOOD: Flat sorted array with binary search
struct FlatMap {
    std::vector<int32_t> keys;
    std::vector<std::string> values;

    std::string* find(int32_t key) {
        auto it = std::lower_bound(keys.begin(), keys.end(), key);
        if (it != keys.end() && *it == key) {
            size_t idx = it - keys.begin();
            return &values[idx];
        }
        return nullptr;
    }

    void insert(int32_t key, std::string value) {
        auto it = std::lower_bound(keys.begin(), keys.end(), key);
        size_t idx = it - keys.begin();
        keys.insert(it, key);
        values.insert(values.begin() + idx, std::move(value));
    }
};

// Or use absl::flat_hash_map for unsorted data
```

### Contiguous String Storage
```cpp
// BAD: std::vector<std::string> (many small allocations)
std::vector<std::string> names; // Each string is a separate allocation

// GOOD: String pool with offsets
struct StringPool {
    std::vector<char> data;           // All strings concatenated
    std::vector<uint32_t> offsets;    // Offset to each string
    std::vector<uint16_t> lengths;    // Length of each string

    std::string_view get(size_t idx) {
        return {&data[offsets[idx]], lengths[idx]};
    }

    void add(std::string_view str) {
        offsets.push_back(data.size());
        lengths.push_back(str.size());
        data.insert(data.end(), str.begin(), str.end());
    }
};
```

## Performance Characteristics
- **Cache Efficiency**: 3-10x speedup from reduced cache misses (columnar vs row-wise)
- **SIMD**: SOA enables vectorization; AOS does not
- **Memory Bandwidth**: Scan only needed columns (save 5-10x bandwidth)
- **Allocation Overhead**: Flat structures eliminate malloc/free in hot paths
- **Branch Prediction**: Selection vectors avoid unpredictable branches

## Real-World Examples
- **DuckDB**: Vectorized execution with flat vectors (SOA)
- **ClickHouse**: Columnar storage with parallel arrays
- **Apache Arrow**: Standardized columnar format
- **Pandas/NumPy**: Column-oriented dataframes

## Pitfalls
- **Random Access**: Gathering scattered indices is slow (use prefetching)
- **Wide Rows**: Too many columns in SOA causes maintenance burden
- **Small Data**: Overhead of separate vectors not worth it for <1000 rows
- **Updates**: Inserting/deleting in flat sorted arrays is O(n); use hash tables for mutable data
- **Alignment**: Unaligned vectors hurt SIMD performance; use `alignas(32)`
- **False Sharing**: Parallel writes to adjacent elements; pad to cache line boundaries
- **Over-Normalization**: Extreme SOA (one vector per field) complicates code; balance with usability
