#pragma once

#include <algorithm>
#include <vector>

namespace gendb {

// Simple sorting utilities for ORDER BY and top-k

template<typename T, typename Comparator>
void sort_inplace(std::vector<T>& data, Comparator&& comp) {
    std::sort(data.begin(), data.end(), std::forward<Comparator>(comp));
}

template<typename T>
void sort_inplace(std::vector<T>& data) {
    std::sort(data.begin(), data.end());
}

// Top-k selection (more efficient than full sort for LIMIT queries)
template<typename T, typename Comparator>
void partial_sort_top_k(std::vector<T>& data, size_t k, Comparator&& comp) {
    if (k >= data.size()) {
        std::sort(data.begin(), data.end(), std::forward<Comparator>(comp));
    } else {
        std::partial_sort(data.begin(), data.begin() + k, data.end(),
                         std::forward<Comparator>(comp));
        data.resize(k);
    }
}

} // namespace gendb
