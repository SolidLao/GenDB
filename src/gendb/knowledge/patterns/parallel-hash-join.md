# Parallel Hash Join with Open-Addressing

## Why Not std::unordered_map
`std::unordered_map` uses chaining (linked list per bucket):
- Pointer chasing destroys cache locality
- ~80 bytes overhead per entry
- 2-5x slower than open-addressing for join workloads

## Open-Addressing Hash Table Template

```cpp
// Compact open-addressing hash table for joins
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};
```

## Parallel Build + Probe Pattern

```cpp
// Phase 1: Build hash table on smaller relation (single-threaded or partitioned)
CompactHashTable<int32_t, RowData> ht(build_count);
for (size_t i = 0; i < build_count; i++) {
    if (passes_filter(i)) ht.insert(key[i], {col1[i], col2[i]});
}

// Phase 2: Parallel probe with morsel-driven approach
std::vector<std::thread> threads(num_threads);
std::vector<std::vector<ResultRow>> thread_results(num_threads);

for (int t = 0; t < num_threads; t++) {
    threads[t] = std::thread([&, t]() {
        size_t chunk = (probe_count + num_threads - 1) / num_threads;
        size_t start = t * chunk;
        size_t end = std::min(start + chunk, probe_count);
        for (size_t i = start; i < end; i++) {
            auto* match = ht.find(probe_key[i]);
            if (match) thread_results[t].push_back({...});
        }
    });
}
for (auto& t : threads) t.join();
```

## Multi-Match Join (1:N)
For 1:N joins (e.g., orders→lineitem FK), multiple approaches are available:

### Approach 1: Two-Array Open-Addressing (Best Performance)
Count-based approach: count occurrences → prefix-sum → scatter positions into contiguous array.
The hash table stores `(key, offset, count)` per unique key; a separate positions array holds all row indices grouped by key.

```cpp
// Step 1: Count occurrences per key
CompactHashTable<int32_t, std::pair<uint32_t, uint32_t>> ht(num_unique_keys);
// Each entry: key → {offset_into_positions, count}

// Step 2: First pass — count
std::vector<uint32_t> counts(num_unique_keys, 0);
for (uint32_t i = 0; i < build_count; i++) counts[key_to_idx[build_key[i]]]++;

// Step 3: Prefix sum → offsets
std::vector<uint32_t> offsets(num_unique_keys);
uint32_t total = 0;
for (size_t i = 0; i < num_unique_keys; i++) { offsets[i] = total; total += counts[i]; }

// Step 4: Scatter positions
std::vector<uint32_t> positions(build_count);
std::vector<uint32_t> cursors = offsets; // copy
for (uint32_t i = 0; i < build_count; i++) {
    uint32_t idx = key_to_idx[build_key[i]];
    positions[cursors[idx]++] = i;
}

// Probe: contiguous reads — very cache-friendly
auto* entry = ht.find(probe_key[i]);
if (entry) {
    for (uint32_t j = entry->offset; j < entry->offset + entry->count; j++) {
        uint32_t build_idx = positions[j];
        // Emit join result
    }
}
```

### Approach 2: Pre-Built Hash Index (Zero Build Time)
If a `hash_multi_value` index is available in the Storage & Index Guide, load it via mmap to skip hash table construction entirely. See the guide for the binary layout: `[num_unique][table_size][hash_entries...][positions_array...]`.

### Approach 3: std::unordered_map (Simplest, Slower)
Acceptable for small build sides or when correctness is the priority (e.g., iter_0):

```cpp
std::unordered_map<int32_t, std::vector<uint32_t>, IntHash> ht;
ht.reserve(estimated_groups);
for (uint32_t i = 0; i < build_count; i++) {
    ht[build_key[i]].push_back(i);
}
```
**Tradeoff**: Simpler to implement but 2-5x slower than open-addressing due to pointer chasing and poor cache locality. Each `std::vector` in the map causes a separate heap allocation.

## When to Use Which
- **Simple equi-join, unique build keys**: Open-addressing CompactHashTable (Approach 1 without grouping)
- **1:N join, performance-critical**: Two-array open-addressing (Approach 1) — best cache locality
- **1:N join, pre-built index available**: Load via mmap (Approach 2) — zero build time
- **1:N join, correctness first (iter_0)**: `unordered_map` with `reserve()` (Approach 3) — simplest
- **Multi-column key**: Composite hash struct (see cpp-safety.md)
