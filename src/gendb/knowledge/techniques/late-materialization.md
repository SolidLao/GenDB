# Late Materialization (Deferred Column Loading)

## What It Is
Defer loading of columns not needed in the current pipeline step. Load integer/filter columns first, apply predicates, then load expensive columns (strings, varchars) only for qualifying rows.

## When to Use
- Queries that filter on some columns but output different columns
- String/varchar columns that are only needed in output, not in predicates or joins
- When only a small fraction of rows qualify (high selectivity)
- Large tables where loading all columns upfront wastes bandwidth

## When NOT to Use
- All columns are needed for filtering or joining
- Most rows qualify (low selectivity — loading everything upfront is simpler)
- Columns are small fixed-width integers (loading cost is minimal)

## Anti-Pattern: Eager Full Materialization
```cpp
// BAD: Load ALL columns including strings before any filtering
auto names = load_string_column("table_a_name.col");      // N strings
auto addresses = load_string_column("table_a_addr.col");   // N strings
auto comments = load_string_column("table_a_comment.col"); // N strings
auto filter_col = load_int_column("table_a_filter.col");   // N ints

// Then filter — 99% of loaded strings are wasted
for (int i = 0; i < rows; i++) {
    if (filter_col[i] > threshold) {
        emit(names[i], addresses[i], comments[i], filter_col[i]);
    }
}
```

## Key Implementation Ideas

### Two-Phase Scan
```cpp
// GOOD: Phase 1 — Load only filter columns, identify qualifying row positions
auto filter_col = (int64_t*)mmap_column("table_a_filter.col");
auto fk_col = (int32_t*)mmap_column("table_a_fk.col");

std::vector<int64_t> qualifying_positions;
qualifying_positions.reserve(estimated_qualifying);

for (int64_t i = 0; i < num_rows; i++) {
    if (filter_col[i] > threshold && fk_col[i] == target_key) {
        qualifying_positions.push_back(i);
    }
}

// Phase 2 — Load string columns only for qualifying rows
// With mmap, just access the positions directly
auto name_codes = (int32_t*)mmap_column("table_a_name.col");
auto addr_codes = (int32_t*)mmap_column("table_a_addr.col");

for (int64_t pos : qualifying_positions) {
    int32_t name_code = name_codes[pos];
    int32_t addr_code = addr_codes[pos];
    // decode strings only for these rows
    emit(decode(name_code), decode(addr_code), filter_col[pos]);
}
```

### With Joins: Defer Non-Join Columns
```cpp
// Phase 1: Join using integer keys only
// Build hash table with only the join key and a row index
struct BuildEntry { int32_t key; int64_t row_idx; };

// Phase 2: After join produces matching pairs, load output columns
// using the row indices from the join result
for (auto& [probe_idx, build_idx] : join_results) {
    // Now load string columns at specific positions
    output(name_codes[build_idx], value_col[probe_idx]);
}
```

### Dictionary-Encoded Strings: Defer Decoding
```cpp
// For dictionary-encoded columns, keep codes as long as possible
// Only decode to strings for final CSV output

// During processing: compare codes, join on codes, group by codes
// Only at output time:
for (auto& result_row : results) {
    std::string name = dictionary[result_row.name_code];  // decode here
    fprintf(out, "%s,...\n", name.c_str());
}
```

## Performance Impact
- If 1% of rows qualify: avoid loading 99% of string data
- String columns are typically 10-100x larger than integer columns
- Typical speedup: 2-10x when filtering is selective and output includes strings
