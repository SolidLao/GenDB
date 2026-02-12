#pragma once

#include <unordered_map>
#include <cstdint>
#include <vector>

namespace gendb {

// Hash indexes for fast lookups
// Simple wrappers around std::unordered_map

using OrderkeyIndex = std::unordered_map<int32_t, std::vector<size_t>>;
using CustkeyIndex = std::unordered_map<int32_t, std::vector<size_t>>;

// Build hash index on a column
template<typename T>
std::unordered_map<T, std::vector<size_t>> build_hash_index(const T* column, size_t size) {
    std::unordered_map<T, std::vector<size_t>> index;
    index.reserve(size / 4); // heuristic: assume moderate cardinality
    for (size_t i = 0; i < size; i++) {
        index[column[i]].push_back(i);
    }
    return index;
}

} // namespace gendb
