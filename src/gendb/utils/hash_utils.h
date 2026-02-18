#pragma once
#include <cstdint>
#include <cstddef>
#include <vector>
#include <utility>
#include <algorithm>
#include <functional>

namespace gendb {

// Fast integer hash — fibonacci hashing, avoids clustering from identity hash
inline uint64_t hash_int(int64_t key) {
    return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
}

inline uint64_t hash_int(int32_t key) {
    return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
}

// Combine two hashes (for composite keys)
inline uint64_t hash_combine(uint64_t h1, uint64_t h2) {
    return h1 ^ (h2 * 0x9E3779B97F4A7C15ULL + 0x517CC1B727220A95ULL + (h1 << 6) + (h1 >> 2));
}

// Open-addressing hash map with Robin Hood hashing.
// 2-5x faster than std::unordered_map for joins and aggregation.
// Pre-sized to 75% load factor with power-of-2 capacity.
template<typename K, typename V>
struct CompactHashMap {
    struct Entry { K key; V value; uint16_t dist; bool occupied; };
    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashMap() : mask(0), count(0) {}

    explicit CompactHashMap(size_t expected) : count(0) {
        size_t cap = 16;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
        for (auto& e : table) { e.occupied = false; e.dist = 0; }
    }

    void reserve(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
        for (auto& e : table) { e.occupied = false; e.dist = 0; }
        count = 0;
    }

    size_t hash_key(K key) const {
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void rehash(size_t new_cap) {
        std::vector<Entry> old_table = std::move(table);
        table.resize(new_cap);
        mask = new_cap - 1;
        count = 0;
        for (auto& e : table) { e.occupied = false; e.dist = 0; }
        for (auto& e : old_table) {
            if (e.occupied) insert(e.key, e.value);
        }
    }

    V& operator[](K key) {
        if (count >= (table.size() * 3) / 4) {
            rehash(table.size() == 0 ? 16 : table.size() * 2);
        }
        size_t pos = hash_key(key) & mask;
        uint16_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return table[pos].value;
            if (dist > table[pos].dist) {
                // Robin Hood: swap and continue with displaced entry
                Entry entry{key, V{}, dist, true};
                std::swap(entry, table[pos]);
                key = entry.key;
                dist = entry.dist;
                // Continue inserting the displaced entry
                pos = (pos + 1) & mask;
                dist++;
                while (table[pos].occupied) {
                    if (dist > table[pos].dist) std::swap(entry, table[pos]);
                    pos = (pos + 1) & mask;
                    entry.dist++;
                    dist = entry.dist;
                }
                table[pos] = entry;
                count++;
                return operator[](key); // re-find the originally inserted key
            }
            pos = (pos + 1) & mask;
            dist++;
        }
        table[pos] = {key, V{}, dist, true};
        count++;
        return table[pos].value;
    }

    void insert(K key, V value) {
        if (count >= (table.size() * 3) / 4) {
            rehash(table.size() == 0 ? 16 : table.size() * 2);
        }
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};
        while (table[pos].occupied) {
            if (table[pos].key == key) { table[pos].value = value; return; }
            if (entry.dist > table[pos].dist) std::swap(entry, table[pos]);
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
    }

    V* find(K key) {
        size_t pos = hash_key(key) & mask;
        uint16_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    const V* find(K key) const {
        size_t pos = hash_key(key) & mask;
        uint16_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    bool contains(K key) const { return find(key) != nullptr; }
    size_t size() const { return count; }

    // Iterator support for range-based for loops
    struct Iterator {
        const Entry* ptr;
        const Entry* end;
        Iterator(const Entry* p, const Entry* e) : ptr(p), end(e) { advance(); }
        void advance() { while (ptr < end && !ptr->occupied) ptr++; }
        bool operator!=(const Iterator& o) const { return ptr != o.ptr; }
        Iterator& operator++() { ptr++; advance(); return *this; }
        std::pair<K, const V&> operator*() const { return {ptr->key, ptr->value}; }
    };
    Iterator begin() const { return Iterator(table.data(), table.data() + table.size()); }
    Iterator end() const { return Iterator(table.data() + table.size(), table.data() + table.size()); }
};

// Open-addressing hash set — for semi-joins, deduplication.
template<typename K>
struct CompactHashSet {
    struct Entry { K key; uint16_t dist; bool occupied; };
    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashSet() : mask(0), count(0) {}

    explicit CompactHashSet(size_t expected) : count(0) {
        size_t cap = 16;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
        for (auto& e : table) { e.occupied = false; e.dist = 0; }
    }

    void reserve(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
        for (auto& e : table) { e.occupied = false; e.dist = 0; }
        count = 0;
    }

    size_t hash_key(K key) const {
        return (uint64_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void rehash(size_t new_cap) {
        std::vector<Entry> old_table = std::move(table);
        table.resize(new_cap);
        mask = new_cap - 1;
        count = 0;
        for (auto& e : table) { e.occupied = false; e.dist = 0; }
        for (auto& e : old_table) {
            if (e.occupied) insert(e.key);
        }
    }

    bool insert(K key) {
        if (count >= (table.size() * 3) / 4) {
            rehash(table.size() == 0 ? 16 : table.size() * 2);
        }
        size_t pos = hash_key(key) & mask;
        Entry entry{key, 0, true};
        while (table[pos].occupied) {
            if (table[pos].key == key) return false; // already exists
            if (entry.dist > table[pos].dist) std::swap(entry, table[pos]);
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
        return true;
    }

    bool contains(K key) const {
        size_t pos = hash_key(key) & mask;
        uint16_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return true;
            if (dist > table[pos].dist) return false;
            pos = (pos + 1) & mask;
            dist++;
        }
        return false;
    }

    size_t size() const { return count; }
};

// ---------------------------------------------------------------------------
// Composite key: pair of int32_t — used for (partkey, suppkey) joins in Q9.
// Avoids std::pair<> which requires custom hash for unordered_map.
// ---------------------------------------------------------------------------
struct Key32Pair {
    int32_t a, b;
    bool operator==(const Key32Pair& o) const { return a == o.a && b == o.b; }
};

inline uint64_t hash_key32pair(Key32Pair k) {
    uint64_t ha = (uint64_t)k.a * 0x9E3779B97F4A7C15ULL;
    uint64_t hb = (uint64_t)k.b * 0x9E3779B97F4A7C15ULL;
    return hash_combine(ha, hb);
}

// CompactHashMap specialised for Key32Pair keys.
// Usage: CompactHashMapPair<V> map(expected_size);
//        map.insert({partkey, suppkey}, value);
//        V* v = map.find({partkey, suppkey});
template<typename V>
struct CompactHashMapPair {
    struct Entry { Key32Pair key; V value; uint16_t dist; bool occupied; };
    std::vector<Entry> table;
    size_t mask;
    size_t count;

    CompactHashMapPair() : mask(0), count(0) {}

    explicit CompactHashMapPair(size_t expected) : count(0) {
        size_t cap = 16;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
        for (auto& e : table) { e.occupied = false; e.dist = 0; }
    }

    void reserve(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 4 / 3) cap <<= 1;
        table.resize(cap);
        mask = cap - 1;
        for (auto& e : table) { e.occupied = false; e.dist = 0; }
        count = 0;
    }

    size_t hash_key(Key32Pair k) const { return hash_key32pair(k); }

    void rehash(size_t new_cap) {
        std::vector<Entry> old_table = std::move(table);
        table.resize(new_cap);
        mask = new_cap - 1;
        count = 0;
        for (auto& e : table) { e.occupied = false; e.dist = 0; }
        for (auto& e : old_table) {
            if (e.occupied) insert(e.key, e.value);
        }
    }

    void insert(Key32Pair key, V value) {
        if (count >= (table.size() * 3) / 4) {
            rehash(table.size() == 0 ? 16 : table.size() * 2);
        }
        size_t pos = hash_key(key) & mask;
        Entry entry{key, value, 0, true};
        while (table[pos].occupied) {
            if (table[pos].key == key) { table[pos].value = value; return; }
            if (entry.dist > table[pos].dist) std::swap(entry, table[pos]);
            pos = (pos + 1) & mask;
            entry.dist++;
        }
        table[pos] = entry;
        count++;
    }

    V* find(Key32Pair key) {
        size_t pos = hash_key(key) & mask;
        uint16_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    const V* find(Key32Pair key) const {
        size_t pos = hash_key(key) & mask;
        uint16_t dist = 0;
        while (table[pos].occupied) {
            if (table[pos].key == key) return &table[pos].value;
            if (dist > table[pos].dist) return nullptr;
            pos = (pos + 1) & mask;
            dist++;
        }
        return nullptr;
    }

    bool contains(Key32Pair key) const { return find(key) != nullptr; }
    size_t size() const { return count; }

    // Iterator support
    struct Iterator {
        const Entry* ptr;
        const Entry* end;
        Iterator(const Entry* p, const Entry* e) : ptr(p), end(e) { advance(); }
        void advance() { while (ptr < end && !ptr->occupied) ptr++; }
        bool operator!=(const Iterator& o) const { return ptr != o.ptr; }
        Iterator& operator++() { ptr++; advance(); return *this; }
        std::pair<Key32Pair, const V&> operator*() const { return {ptr->key, ptr->value}; }
    };
    Iterator begin() const { return Iterator(table.data(), table.data() + table.size()); }
    Iterator end() const { return Iterator(table.data() + table.size(), table.data() + table.size()); }
};

// ---------------------------------------------------------------------------
// Top-K heap — O(n log k) partial sort for LIMIT queries.
// Much faster than full std::sort when k << n (e.g., LIMIT 10 on 7M rows).
//
// Usage (Q3 LIMIT 10, Q18 LIMIT 100):
//   auto cmp = [](const Row& a, const Row& b){ return a.revenue > b.revenue; };
//   TopKHeap<Row> heap(10, cmp);  // keeps top-10 by revenue DESC
//   for (auto& row : results) heap.push(row);
//   auto top = heap.sorted();     // returns sorted vector of ≤k elements
// ---------------------------------------------------------------------------
template<typename T, typename Cmp = std::less<T>>
struct TopKHeap {
    size_t k;
    Cmp cmp;          // cmp(a,b) = true means a is "better" (should stay in heap)
    std::vector<T> h; // max-heap by "worst" element so we can evict easily

    // inv_cmp: the heap root is the worst element among kept top-k.
    // We use a max-heap of the "worst" key so root can be compared and evicted.
    TopKHeap(size_t k_, Cmp c = Cmp{}) : k(k_), cmp(c) { h.reserve(k_ + 1); }

    void push(const T& val) {
        if (h.size() < k) {
            h.push_back(val);
            // heap property: root = worst of current top-k (min by cmp = "least good")
            std::push_heap(h.begin(), h.end(), cmp);
        } else if (!h.empty() && cmp(val, h.front())) {
            // val is worse than current worst — skip
            return;
        } else {
            // val beats the current worst root → replace root, re-heapify
            std::pop_heap(h.begin(), h.end(), cmp);
            h.back() = val;
            std::push_heap(h.begin(), h.end(), cmp);
        }
    }

    // Returns elements in sorted order (best first).
    std::vector<T> sorted() {
        std::vector<T> out = h;
        std::sort(out.begin(), out.end(), [&](const T& a, const T& b){ return cmp(b, a); });
        return out;
    }

    size_t size() const { return h.size(); }
};

} // namespace gendb
