# Compact Hash Tables

**When to use**: Joins or GROUP BY with >256 groups where `std::unordered_map` is too slow (2-5x overhead from pointer chasing).
**Impact**: 2-5x speedup over `std::unordered_map` for joins and aggregation.

## Principle
- Open addressing stores all entries in a single contiguous array (cache-friendly, no pointer chasing)
- Robin Hood hashing equalizes probe lengths by displacing entries with shorter probe distances
- Power-of-2 sizing enables fast index computation via `hash & (capacity - 1)` instead of modulo
- Pre-size to 75% load factor: `capacity = next_power_of_2(expected_entries * 4 / 3)`

## Pattern
```cpp
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; uint8_t dist; bool occupied; };
    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected) {
        size_t cap = 1;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
    }

    size_t hash_key(K key) const {
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL >> 32;
    }

    void insert(K key, V value) {
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};
        while (table[pos].occupied) {
            if (table[pos].key == key) { table[pos].value = value; return; }
            if (entry.dist > table[pos].dist) std::swap(entry, table[pos]);
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
    }

    V* find(K key) {
        size_t pos = hash_key(key) & mask;
        uint8_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }
};
```

## Pitfalls
- Never use `std::hash<int32_t>` — it is often the identity function, causing severe clustering
- Forgetting to pre-size leads to excessive resizing and rehashing
- Load factor >90% causes exponential probe chain growth
