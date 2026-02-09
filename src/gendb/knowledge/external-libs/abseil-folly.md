# Abseil and Folly Hash Tables

## What It Is
High-performance hash table implementations from Google (Abseil flat_hash_map/flat_hash_set) and Facebook (Folly F14FastMap/F14NodeMap). Both use "Swiss Tables" design with SIMD-accelerated lookups and open addressing.

## When To Use
- Hash joins and GROUP BY aggregations in query execution
- Symbol tables, metadata caches, deduplication
- Replacing std::unordered_map for 2-5x speedup
- When profiling shows hash table operations as bottleneck

## Key Implementation Ideas

### Abseil flat_hash_map Basics
```cpp
#include <absl/container/flat_hash_map.h>
#include <absl/container/flat_hash_set.h>

// Drop-in replacement for std::unordered_map
absl::flat_hash_map<int64_t, std::string> symbol_table;
symbol_table[42] = "answer";
symbol_table.emplace(99, "problems");

// Reserve capacity to avoid rehashing
symbol_table.reserve(1000000);

// Use custom hash function
struct CustomHash {
    size_t operator()(int64_t x) const {
        return x * 0x9e3779b97f4a7c15; // Fibonacci hashing
    }
};
absl::flat_hash_map<int64_t, int64_t, CustomHash> custom_map;
```

### Folly F14FastMap (Facebook)
```cpp
#include <folly/container/F14Map.h>
#include <folly/container/F14Set.h>

// F14FastMap: Optimized for small keys/values (<8 bytes)
folly::F14FastMap<int32_t, int32_t> fast_map;

// F14ValueMap: Inline storage for values (better cache locality)
folly::F14ValueMap<int64_t, std::string> value_map;

// F14NodeMap: Pointer-stable (like std::unordered_map)
folly::F14NodeMap<int64_t, LargeObject> node_map;
```

### Swiss Tables Internals (Control Bytes + SIMD)
```cpp
// Simplified Swiss Tables structure
template<typename K, typename V>
struct SwissTable {
    struct Group {
        alignas(16) int8_t ctrl[16]; // Control bytes
        // ctrl[i] = -128 (empty), -2 (deleted), or hash_prefix[0..127]
    };

    std::vector<Group> groups;
    std::vector<K> keys;
    std::vector<V> values;

    // SIMD lookup: check 16 control bytes at once
    std::optional<size_t> find_in_group(size_t group_idx, int8_t hash_prefix) {
        __m128i target = _mm_set1_epi8(hash_prefix);
        __m128i ctrl_vec = _mm_load_si128((__m128i*)groups[group_idx].ctrl);
        __m128i cmp = _mm_cmpeq_epi8(target, ctrl_vec);
        uint16_t mask = _mm_movemask_epi8(cmp);

        while (mask != 0) {
            int pos = __builtin_ctz(mask); // Find first set bit
            if (keys[group_idx * 16 + pos] == target_key) {
                return group_idx * 16 + pos;
            }
            mask &= (mask - 1); // Clear lowest bit
        }
        return std::nullopt;
    }
};
```

### Heterogeneous Lookup (Avoid Temporary Allocations)
```cpp
#include <absl/container/flat_hash_map.h>

// Lookup with string_view (no std::string allocation)
struct StringHash {
    using is_transparent = void; // Enable heterogeneous lookup

    size_t operator()(std::string_view sv) const {
        return absl::Hash<std::string_view>{}(sv);
    }
};

struct StringEq {
    using is_transparent = void;

    bool operator()(std::string_view a, std::string_view b) const {
        return a == b;
    }
};

absl::flat_hash_map<std::string, int, StringHash, StringEq> map;
map["hello"] = 42;

// Lookup without creating std::string
std::string_view sv = "hello";
auto it = map.find(sv); // No temporary std::string!
```

### Parallel Aggregation with Abseil
```cpp
#include <absl/container/flat_hash_map.h>

void parallel_group_by(const int64_t* keys, const int64_t* values,
                       size_t count, size_t num_threads) {
    std::vector<absl::flat_hash_map<int64_t, int64_t>> thread_local_maps(num_threads);

    // Phase 1: Thread-local aggregation
    #pragma omp parallel for
    for (size_t i = 0; i < count; ++i) {
        int tid = omp_get_thread_num();
        thread_local_maps[tid][keys[i]] += values[i];
    }

    // Phase 2: Merge into global map
    absl::flat_hash_map<int64_t, int64_t> global_result;
    for (auto& local_map : thread_local_maps) {
        for (auto& [key, sum] : local_map) {
            global_result[key] += sum;
        }
    }
}
```

### Pre-Allocating for Known Size
```cpp
// Avoid rehashing overhead
absl::flat_hash_map<int64_t, int64_t> map;
map.reserve(1000000); // Pre-allocate for 1M entries

// Insert without rehashing
for (int i = 0; i < 1000000; ++i) {
    map[i] = i * 2;
}
```

## Performance Characteristics
- **Speedup vs std::unordered_map**: 2-5x faster lookups, 1.5-3x faster inserts
- **Memory Overhead**: 10-20% vs 100-200% for std::unordered_map
- **Cache Locality**: Contiguous storage (open addressing) vs chaining
- **SIMD Benefit**: ~15-25% speedup from parallel control byte checks
- **Load Factor**: Optimal at 87.5% (7/8 full); automatic resizing

### Abseil vs Folly Comparison
| Feature | Abseil flat_hash_map | Folly F14FastMap | std::unordered_map |
|---------|---------------------|------------------|-------------------|
| Lookup Speed | Very Fast | Fastest (small keys) | Slow |
| Memory Overhead | ~15% | ~12% | ~150% |
| Iterator Stability | Invalidated on insert | Invalidated | Stable (F14NodeMap) |
| Small Key/Value | Good | Excellent | Poor |
| SIMD | Yes (SSE2) | Yes (SSE2/NEON) | No |

## Real-World Examples
- **Google**: Abseil used across all C++ projects (Search, Ads, YouTube)
- **Facebook**: Folly F14 in production databases and caches
- **DuckDB**: Investigated Abseil for hash joins (opted for custom Robin Hood)
- **ClickHouse**: Uses custom hash tables similar to Swiss Tables

## Pitfalls
- **Iterator Invalidation**: Insertions invalidate all iterators (unlike std::unordered_map)
- **Pointer Stability**: Use F14NodeMap/node_hash_map if you need stable pointers
- **Build Complexity**: Abseil/Folly are large libraries (adds build time)
- **Dependency Weight**: Abseil (200+ KB), Folly (1+ MB) binary size increase
- **Hash Function Quality**: Poor hash (e.g., identity) kills performance; use absl::Hash or XXH3
- **Small Maps**: Overhead not worth it for <100 entries; std::map may be faster
- **Rehashing Cost**: For very large maps (>10M entries), pre-reserve to avoid multi-second pauses
- **SSE2 Requirement**: Abseil requires SSE2 (not available on some embedded systems)
