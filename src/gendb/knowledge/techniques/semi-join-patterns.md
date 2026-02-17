# Semi-Join and Anti-Join Patterns

## What It Is
Hash-based semi-join and anti-join for evaluating EXISTS, NOT EXISTS, and IN subqueries efficiently.

## When to Use
- `EXISTS (SELECT ...)` → hash semi-join
- `NOT EXISTS (SELECT ...)` → hash anti-join
- `col IN (SELECT ...)` → hash semi-join
- `col NOT IN (SELECT ...)` → hash anti-join (with NULL handling)

## Anti-Pattern: Per-Row Subquery Evaluation
```cpp
// BAD: Re-evaluate subquery for each outer row
for (auto& outer_row : outer_table) {
    bool found = false;
    for (auto& inner_row : inner_table) {
        if (matches(outer_row, inner_row)) {
            found = true;
            break;
        }
    }
    if (found) emit(outer_row);  // semi-join
}
```
This is O(n × m) — catastrophic for large tables.

## Key Implementation Ideas

### Hash Semi-Join (EXISTS / IN)
```cpp
// Step 1: Pre-compute inner query result into hash set (single pass)
std::unordered_set<int32_t> inner_keys;
inner_keys.reserve(estimated_inner_size);
for (int64_t i = 0; i < inner_rows; i++) {
    if (inner_predicate(i)) {
        inner_keys.insert(inner_key_col[i]);
    }
}

// Step 2: Probe outer table — emit on first match
for (int64_t i = 0; i < outer_rows; i++) {
    if (inner_keys.count(outer_key_col[i])) {
        emit(i);  // row qualifies
    }
}
```

### Hash Anti-Join (NOT EXISTS / NOT IN)
```cpp
// Step 1: Same — build set from inner query
std::unordered_set<int32_t> inner_keys;
// ... populate ...

// Step 2: Emit rows with NO match
for (int64_t i = 0; i < outer_rows; i++) {
    if (!inner_keys.count(outer_key_col[i])) {
        emit(i);  // row qualifies (no match in inner)
    }
}
```

### Multi-Column Semi-Join
When the correlation involves multiple columns:
```cpp
// Use composite key in hash set
struct PairHash {
    size_t operator()(const std::pair<int32_t, int32_t>& p) const {
        return std::hash<int64_t>()(((int64_t)p.first << 32) | (uint32_t)p.second);
    }
};
std::unordered_set<std::pair<int32_t, int32_t>, PairHash> inner_keys;
```

### Optimized Pattern: Bitmap for Small Domains
When the semi-join key has a small domain (e.g., nationkey 0-24):
```cpp
// Use bitmap instead of hash set
bool nation_qualifies[25] = {};
for (int64_t i = 0; i < inner_rows; i++) {
    nation_qualifies[inner_nationkey[i]] = true;
}
// Probe: O(1) with zero hash overhead
if (nation_qualifies[outer_nationkey[i]]) { ... }
```

## Performance Impact
- Nested loop: O(n × m) — millions × millions = catastrophic
- Hash semi-join: O(n + m) — build set O(m) + probe O(n)
- Speedup: 1000x+ for large tables
