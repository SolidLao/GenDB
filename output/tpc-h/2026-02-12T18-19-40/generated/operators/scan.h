#ifndef SCAN_H
#define SCAN_H

#include <vector>
#include <functional>
#include <cstddef>

// Table scan operators with predicate pushdown

namespace operators {

// Scan with single-column predicate
template<typename T>
std::vector<size_t> scan_filter(
    const T* column,
    size_t count,
    std::function<bool(T)> predicate
) {
    std::vector<size_t> result;
    result.reserve(count / 10); // Assume ~10% selectivity

    for (size_t i = 0; i < count; i++) {
        if (predicate(column[i])) {
            result.push_back(i);
        }
    }

    return result;
}

// Scan with multi-column predicate (takes row index)
template<typename PredicateFn>
std::vector<size_t> scan_filter_multi(
    size_t count,
    PredicateFn predicate
) {
    std::vector<size_t> result;
    result.reserve(count / 10);

    for (size_t i = 0; i < count; i++) {
        if (predicate(i)) {
            result.push_back(i);
        }
    }

    return result;
}

// Scan with range predicate (optimized for sorted columns)
template<typename T>
std::vector<size_t> scan_range(
    const T* sorted_column,
    size_t count,
    T min_val,
    T max_val
) {
    std::vector<size_t> result;

    // Binary search for start
    size_t start = 0;
    size_t end = count;

    // Find first element >= min_val
    size_t left = 0, right = count;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (sorted_column[mid] < min_val) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    start = left;

    // Find first element > max_val
    left = start;
    right = count;
    while (left < right) {
        size_t mid = left + (right - left) / 2;
        if (sorted_column[mid] <= max_val) {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    end = left;

    // Collect indices in range
    result.reserve(end - start);
    for (size_t i = start; i < end; i++) {
        result.push_back(i);
    }

    return result;
}

// Materialize subset of rows from column
template<typename T>
std::vector<T> gather(
    const T* column,
    const std::vector<size_t>& indices
) {
    std::vector<T> result;
    result.reserve(indices.size());

    for (size_t idx : indices) {
        result.push_back(column[idx]);
    }

    return result;
}

// Intersect two index sets (for AND predicates)
inline std::vector<size_t> intersect_indices(
    const std::vector<size_t>& a,
    const std::vector<size_t>& b
) {
    std::vector<size_t> result;
    result.reserve(std::min(a.size(), b.size()));

    size_t i = 0, j = 0;
    while (i < a.size() && j < b.size()) {
        if (a[i] == b[j]) {
            result.push_back(a[i]);
            i++;
            j++;
        } else if (a[i] < b[j]) {
            i++;
        } else {
            j++;
        }
    }

    return result;
}

} // namespace operators

#endif // SCAN_H
