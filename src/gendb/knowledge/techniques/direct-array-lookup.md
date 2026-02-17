# Direct Array Lookup for Small-Domain Keys

## What It Is
Using flat arrays indexed directly by key value instead of hash tables, when the key domain is small and known.

## When to Use
- Join or lookup key has a small known domain (<256 distinct values)
- Examples: nation keys (0-24), region keys (0-4), status flags, return flags, ship modes
- The key values must be contiguous or near-contiguous non-negative integers

## When NOT to Use
- Domain size is unknown at code generation time
- Domain size > 256 values
- Key values are sparse (e.g., customer keys 1-1.5M)

## Anti-Pattern: Hash Table for Small Domains
```cpp
// BAD: Hash table for 25 nation keys — massive overhead
std::unordered_map<int32_t, NationData> nation_map;
nation_map.reserve(25);
for (int i = 0; i < 25; i++) {
    nation_map[nationkey[i]] = {name[i], regionkey[i]};
}
// Probe: hash computation + bucket traversal for each of 60M rows
auto it = nation_map.find(row_nationkey);
```

## Key Implementation Ideas

### Direct Array Lookup
```cpp
// GOOD: Flat array indexed by key — O(1), zero hash overhead
struct NationData { int32_t regionkey; int32_t name_code; };
NationData nation_data[25];  // indexed by nationkey 0-24

// Load once
for (int i = 0; i < 25; i++) {
    nation_data[i] = {regionkey_col[i], name_col[i]};
}

// Probe: single array access per row — no hash, no comparison
int32_t rk = nation_data[row_nationkey].regionkey;
```

### Direct Array for Aggregation
```cpp
// For GROUP BY with <256 groups (e.g., by nationkey)
int64_t sum_by_nation[25] = {};
int64_t count_by_nation[25] = {};

for (int64_t i = 0; i < num_rows; i++) {
    int32_t nk = nationkey_col[i];
    sum_by_nation[nk] += value_col[i];
    count_by_nation[nk]++;
}
```

### Boolean Filter Array
```cpp
// For semi-join with small domain: "nations in region EUROPE"
bool nation_in_europe[25] = {};
for (int i = 0; i < 25; i++) {
    if (regionkey_col[i] == europe_regionkey) {
        nation_in_europe[i] = true;
    }
}

// Probe: single boolean check per row
if (nation_in_europe[supp_nationkey[i]]) { ... }
```

### Multi-Field Lookup Array
```cpp
// Store multiple fields per key
struct RegionInfo {
    int32_t name_code;
    bool is_target;
};
RegionInfo region_info[5];  // 5 regions
```

## Performance Impact
- Hash table probe: ~50-100ns (hash + compare + possible collision chain)
- Array lookup: ~1-5ns (single memory access, likely L1 cache hit for small arrays)
- Speedup: 10-50x per lookup, compounded over millions of rows
