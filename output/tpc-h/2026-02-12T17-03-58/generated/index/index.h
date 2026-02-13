#ifndef INDEX_H
#define INDEX_H

#include <cstdint>
#include <vector>
#include <unordered_map>

// Hash index for fast key lookups (used for joins)
template<typename KeyType>
class HashIndex {
private:
    std::unordered_map<KeyType, std::vector<uint32_t>> index_map;

public:
    void insert(KeyType key, uint32_t row_id) {
        index_map[key].push_back(row_id);
    }

    const std::vector<uint32_t>* lookup(KeyType key) const {
        auto it = index_map.find(key);
        return (it != index_map.end()) ? &it->second : nullptr;
    }

    bool contains(KeyType key) const {
        return index_map.find(key) != index_map.end();
    }

    size_t size() const {
        return index_map.size();
    }

    void clear() {
        index_map.clear();
    }

    // Iterator access for probing
    auto begin() const { return index_map.begin(); }
    auto end() const { return index_map.end(); }
};

// Sorted index for range queries (used for date ranges)
template<typename ValueType>
class SortedIndex {
private:
    struct Entry {
        ValueType value;
        uint32_t row_id;

        bool operator<(const Entry& other) const {
            return value < other.value;
        }
    };

    std::vector<Entry> entries;

public:
    void insert(ValueType value, uint32_t row_id) {
        entries.push_back({value, row_id});
    }

    void finalize() {
        std::sort(entries.begin(), entries.end());
    }

    // Find all row IDs with values in range [min_value, max_value)
    std::vector<uint32_t> range_query(ValueType min_value, ValueType max_value) const {
        std::vector<uint32_t> results;

        // Binary search for lower bound
        auto lower = std::lower_bound(entries.begin(), entries.end(), Entry{min_value, 0});
        auto upper = std::lower_bound(entries.begin(), entries.end(), Entry{max_value, 0});

        for (auto it = lower; it != upper; ++it) {
            results.push_back(it->row_id);
        }

        return results;
    }

    size_t size() const {
        return entries.size();
    }
};

#endif // INDEX_H
