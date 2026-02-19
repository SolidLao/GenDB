# Zone Map Pruning Patterns

> **Note**: The exact binary format of zone map files varies per run (field types, entry sizes, header format). The struct below is an example only. **Always rely on the per-query guide** (`query_guides/<Qi>_guide.md`) for the authoritative binary layout of zone map files in the current run.

## Zone Map Structure (Example — actual format may differ)
```cpp
struct ZoneMapEntry {
    int64_t min_val;    // 8 bytes
    int64_t max_val;    // 8 bytes
    uint32_t start_row; // 4 bytes
    uint32_t end_row;   // 4 bytes (exclusive)
};
static_assert(sizeof(ZoneMapEntry) == 24);
```

## Loading
```cpp
int fd = open(zonemap_path, O_RDONLY);
struct stat st;
fstat(fd, &st);
auto* zones = (const ZoneMapEntry*)mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
size_t num_zones = st.st_size / sizeof(ZoneMapEntry);
close(fd);
```

## Pruning Patterns

### Range Predicate: WHERE col BETWEEN low AND high
```cpp
for (size_t z = 0; z < num_zones; z++) {
    if (zones[z].max_val < low || zones[z].min_val > high) continue; // skip
    // Process rows [zones[z].start_row, zones[z].end_row)
    for (uint32_t r = zones[z].start_row; r < zones[z].end_row; r++) {
        if (col[r] >= low && col[r] <= high) { /* match */ }
    }
}
```

### Comparison Predicate: WHERE col <= value
```cpp
for (size_t z = 0; z < num_zones; z++) {
    if (zones[z].min_val > value) continue; // all rows in block > value, skip

    if (zones[z].max_val <= value) {
        // Entire block satisfies predicate - no per-row check needed
        for (uint32_t r = zones[z].start_row; r < zones[z].end_row; r++) {
            process(r); // all rows match
        }
    } else {
        // Partial block - check per row
        for (uint32_t r = zones[z].start_row; r < zones[z].end_row; r++) {
            if (col[r] <= value) process(r);
        }
    }
}
```

## Multi-Column Zone Map Pruning
When filtering on multiple columns, intersect zone map results:
```cpp
// Collect qualifying block ranges from each predicate
std::vector<bool> block_alive(num_zones, true);
// Predicate 1: col_a <= val_a
for (size_t z = 0; z < num_zones; z++)
    if (zones_a[z].min_val > val_a) block_alive[z] = false;
// Predicate 2: col_b >= val_b
for (size_t z = 0; z < num_zones; z++)
    if (zones_b[z].max_val < val_b) block_alive[z] = false;
// Only scan alive blocks
```

## Effectiveness
- Best on sorted/clustered columns (high skip rate)
- Marginal on random/unsorted data
- Zero overhead on skipped blocks (just pointer arithmetic)
- Cost: ~24 bytes per block of zone map metadata
