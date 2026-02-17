# Direct Array Lookup for Small-Domain Keys

## What It Is
Using flat arrays indexed directly by key value instead of hash tables, when the key domain is small and known.

## When to Use
- Join or lookup key has a small known domain (<256 distinct values)
- Examples: dimension keys with small domains (<50 distinct values from workload analysis), status/flag columns, category codes
- The key values must be contiguous or near-contiguous non-negative integers

## When NOT to Use
- Domain size is unknown at code generation time
- Domain size > 256 values
- Key values are sparse (e.g., customer keys 1-1.5M)

## Anti-Pattern: Hash Table for Small Domains
```cpp
// BAD: Hash table for small-domain keys — massive overhead
std::unordered_map<int32_t, DimData> dim_map;
dim_map.reserve(MAX_DOMAIN);
for (int i = 0; i < num_dim_keys; i++) {
    dim_map[dim_key[i]] = {attr_a[i], attr_b[i]};
}
// Probe: hash computation + bucket traversal for each of N rows
auto it = dim_map.find(row_dim_key);
```

## Key Implementation Ideas

### Direct Array Lookup
```cpp
// GOOD: Flat array indexed by key — O(1), zero hash overhead
// Size from workload analysis: MAX_DOMAIN = max distinct values in column
struct DimData { int32_t attr_a; int32_t attr_b; };
DimData dim_data[MAX_DOMAIN];  // indexed by dimension key 0..MAX_DOMAIN-1

// Load once
for (int i = 0; i < num_dim_keys; i++) {
    dim_data[i] = {attr_a_col[i], attr_b_col[i]};
}

// Probe: single array access per row — no hash, no comparison
int32_t a = dim_data[row_dim_key].attr_a;
```

### Direct Array for Aggregation
```cpp
// For GROUP BY with <256 groups
int64_t sum_by_group[MAX_DOMAIN] = {};
int64_t count_by_group[MAX_DOMAIN] = {};

for (int64_t i = 0; i < num_rows; i++) {
    int32_t gk = group_key_col[i];
    sum_by_group[gk] += value_col[i];
    count_by_group[gk]++;
}
```

### Boolean Filter Array
```cpp
// For semi-join with small domain: "dimension keys matching a category filter"
bool dim_matches[MAX_DOMAIN] = {};
for (int i = 0; i < num_dim_keys; i++) {
    if (category_col[i] == target_category) {
        dim_matches[i] = true;
    }
}

// Probe: single boolean check per row
if (dim_matches[row_dim_key]) { ... }
```

### Multi-Field Lookup Array
```cpp
// Store multiple fields per key
struct CategoryInfo {
    int32_t name_code;
    bool is_target;
};
CategoryInfo category_info[MAX_DOMAIN];
```

## Performance Impact
- Hash table probe: ~50-100ns (hash + compare + possible collision chain)
- Array lookup: ~1-5ns (single memory access, likely L1 cache hit for small arrays)
- Speedup: 10-50x per lookup, compounded over millions of rows
