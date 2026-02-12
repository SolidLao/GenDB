#pragma once

#include <vector>
#include <algorithm>
#include <queue>
#include <functional>

namespace gendb {
namespace operators {

/**
 * Generic sort operator with custom comparator.
 *
 * Wraps std::sort with a clean interface for sorting query results.
 *
 * Template Parameters:
 *   T: Type of elements to sort
 *   Compare: Comparison function type
 *
 * Usage:
 *   std::vector<Result> results = ...;
 *   sort_results(results, [](const Result& a, const Result& b) {
 *       return a.key < b.key;
 *   });
 */
template<typename T, typename Compare>
void sort_results(std::vector<T>& data, Compare comp) {
    std::sort(data.begin(), data.end(), comp);
}

/**
 * Top-K selection using min-heap (for ORDER BY ... LIMIT K queries).
 *
 * More efficient than full sort when K << N. Maintains a heap of size K
 * and only keeps the top K elements, avoiding O(N log N) full sort.
 *
 * Complexity: O(N log K) vs O(N log N) for full sort.
 *
 * Template Parameters:
 *   T: Type of elements
 *   Compare: Comparison function (for min-heap property)
 *
 * Usage:
 *   // Get top 10 results by revenue (descending)
 *   auto top10 = top_k_selection(results, 10, [](const Result& a, const Result& b) {
 *       return a.revenue < b.revenue;  // Min-heap keeps largest at top
 *   });
 *
 * Note: The returned vector is NOT sorted. Call sort_results() if needed.
 */
template<typename T, typename Compare>
std::vector<T> top_k_selection(const std::vector<T>& data, size_t k, Compare comp) {
    if (k >= data.size()) {
        // K is too large, just return sorted copy
        std::vector<T> result = data;
        std::sort(result.begin(), result.end(), comp);
        return result;
    }

    // Use min-heap to maintain top K elements (smallest of the K at top)
    // To get top-K largest: use min-heap, comp(a,b) = a < b means a is worse than b
    // priority_queue needs comp(a,b) = a > b to create min-heap
    // So we need to INVERT the comparator!
    auto inv_comp = [&comp](const T& a, const T& b) { return !comp(a, b) && !comp(b, a) ? false : comp(b, a); };
    std::priority_queue<T, std::vector<T>, decltype(inv_comp)> heap(inv_comp);

    for (const auto& item : data) {
        if (heap.size() < k) {
            heap.push(item);
        } else if (!comp(item, heap.top())) {
            // item is NOT worse than the worst in heap, so it's better or equal
            heap.pop();
            heap.push(item);
        }
    }

    // Extract heap into vector
    std::vector<T> result;
    result.reserve(k);
    while (!heap.empty()) {
        result.push_back(heap.top());
        heap.pop();
    }

    return result;
}

/**
 * Top-K selection with online heap maintenance (streaming version).
 *
 * Maintains a heap while processing data, useful when data is generated
 * on-the-fly (e.g., during aggregation phase).
 */
template<typename T, typename Compare = std::less<T>>
class TopKHeap {
public:
    explicit TopKHeap(size_t k, Compare comp = Compare())
        : k_(k), comp_(comp), heap_(comp) {}

    void insert(const T& item) {
        if (heap_.size() < k_) {
            heap_.push(item);
        } else if (!comp_(item, heap_.top())) {
            // item is better than worst in heap
            heap_.pop();
            heap_.push(item);
        }
    }

    void insert(T&& item) {
        if (heap_.size() < k_) {
            heap_.push(std::move(item));
        } else if (!comp_(item, heap_.top())) {
            heap_.pop();
            heap_.push(std::move(item));
        }
    }

    std::vector<T> extract() {
        std::vector<T> result;
        result.reserve(heap_.size());
        while (!heap_.empty()) {
            result.push_back(heap_.top());
            heap_.pop();
        }
        return result;
    }

    size_t size() const { return heap_.size(); }

private:
    size_t k_;
    Compare comp_;
    std::priority_queue<T, std::vector<T>, Compare> heap_;
};

} // namespace operators
} // namespace gendb
