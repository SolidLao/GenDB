#pragma once
#include <cstdint>
#include <vector>
#include <functional>

namespace gendb {

// Scan operator: filters rows based on predicate, returns matching row IDs
template<typename T, typename Pred>
std::vector<size_t> scan_filter(const T* column, size_t row_count, Pred predicate) {
    std::vector<size_t> result;
    result.reserve(row_count / 10);  // Heuristic: expect 10% selectivity

    for (size_t i = 0; i < row_count; ++i) {
        if (predicate(column[i])) {
            result.push_back(i);
        }
    }

    return result;
}

// Multi-column scan: ANDs multiple predicates
template<typename T1, typename T2, typename Pred>
std::vector<size_t> scan_filter_2(const T1* col1, const T2* col2, size_t row_count,
                                   Pred predicate) {
    std::vector<size_t> result;
    result.reserve(row_count / 10);

    for (size_t i = 0; i < row_count; ++i) {
        if (predicate(col1[i], col2[i])) {
            result.push_back(i);
        }
    }

    return result;
}

// Four-column scan (for complex filters like Q6)
template<typename T1, typename T2, typename T3, typename T4, typename Pred>
std::vector<size_t> scan_filter_4(const T1* col1, const T2* col2,
                                   const T3* col3, const T4* col4,
                                   size_t row_count, Pred predicate) {
    std::vector<size_t> result;
    result.reserve(row_count / 10);

    for (size_t i = 0; i < row_count; ++i) {
        if (predicate(col1[i], col2[i], col3[i], col4[i])) {
            result.push_back(i);
        }
    }

    return result;
}

} // namespace gendb
