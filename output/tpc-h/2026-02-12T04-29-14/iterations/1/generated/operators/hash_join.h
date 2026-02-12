#pragma once

#include <unordered_map>
#include <vector>
#include <functional>

namespace gendb {
namespace operators {

/**
 * Generic hash join implementation
 * Builds hash table on smaller relation (build side), probes with larger (probe side)
 *
 * Template parameters:
 *   BuildKey:   Type of the build-side join key
 *   BuildValue: Type of the build-side payload
 *   ProbeKey:   Type of the probe-side join key
 *   ProbeFunc:  Function to process matching probe rows
 *               Signature: void(size_t probe_idx, const BuildValue& build_value)
 *
 * Usage:
 *   1. Create HashJoin instance
 *   2. Call build() to populate hash table from build side
 *   3. Call probe() to process matching probe-side rows
 */
template<typename BuildKey, typename BuildValue>
class HashJoin {
public:
    /**
     * Build the hash table from the build side
     *
     * @param build_count Number of rows on build side
     * @param get_key Function to extract key from build row: BuildKey(size_t row_idx)
     * @param get_value Function to extract value from build row: BuildValue(size_t row_idx)
     * @param load_factor Target load factor (default 0.75)
     */
    template<typename GetKeyFunc, typename GetValueFunc>
    void build(
        size_t build_count,
        GetKeyFunc get_key,
        GetValueFunc get_value,
        double load_factor = 0.75
    ) {
        // Pre-size hash table to avoid rehashing
        size_t estimated_size = static_cast<size_t>(build_count / load_factor);
        hash_table_.reserve(estimated_size);

        for (size_t i = 0; i < build_count; i++) {
            BuildKey key = get_key(i);
            BuildValue value = get_value(i);
            hash_table_[key] = value;
        }
    }

    /**
     * Build hash table with multi-value support (for one-to-many joins)
     *
     * Stores multiple values per key using a vector
     */
    template<typename GetKeyFunc, typename GetValueFunc>
    void build_multi(
        size_t build_count,
        GetKeyFunc get_key,
        GetValueFunc get_value,
        double load_factor = 0.75
    ) {
        // Pre-size hash table
        size_t estimated_size = static_cast<size_t>(build_count / load_factor);
        multi_hash_table_.reserve(estimated_size);

        for (size_t i = 0; i < build_count; i++) {
            BuildKey key = get_key(i);
            BuildValue value = get_value(i);
            multi_hash_table_[key].push_back(value);
        }
    }

    /**
     * Probe the hash table with probe-side rows
     *
     * @param probe_count Number of rows on probe side
     * @param get_key Function to extract key from probe row: BuildKey(size_t row_idx)
     * @param process_match Function called for each match: void(size_t probe_idx, const BuildValue&)
     */
    template<typename GetKeyFunc, typename ProcessFunc>
    void probe(
        size_t probe_count,
        GetKeyFunc get_key,
        ProcessFunc process_match
    ) {
        for (size_t i = 0; i < probe_count; i++) {
            BuildKey key = get_key(i);
            auto it = hash_table_.find(key);
            if (it != hash_table_.end()) {
                process_match(i, it->second);
            }
        }
    }

    /**
     * Probe with multi-value support (processes all matching build values)
     */
    template<typename GetKeyFunc, typename ProcessFunc>
    void probe_multi(
        size_t probe_count,
        GetKeyFunc get_key,
        ProcessFunc process_match
    ) {
        for (size_t i = 0; i < probe_count; i++) {
            BuildKey key = get_key(i);
            auto it = multi_hash_table_.find(key);
            if (it != multi_hash_table_.end()) {
                for (const auto& build_value : it->second) {
                    process_match(i, build_value);
                }
            }
        }
    }

    /**
     * Check if a key exists in the hash table (for semi-join)
     */
    template<typename GetKeyFunc>
    bool contains(GetKeyFunc get_key, size_t row_idx) const {
        BuildKey key = get_key(row_idx);
        return hash_table_.find(key) != hash_table_.end();
    }

    /**
     * Get hash table size (number of distinct keys)
     */
    size_t size() const {
        return hash_table_.empty() ? multi_hash_table_.size() : hash_table_.size();
    }

    /**
     * Clear the hash table
     */
    void clear() {
        hash_table_.clear();
        multi_hash_table_.clear();
    }

private:
    // Single-value hash table (for unique keys)
    std::unordered_map<BuildKey, BuildValue> hash_table_;

    // Multi-value hash table (for non-unique keys)
    std::unordered_map<BuildKey, std::vector<BuildValue>> multi_hash_table_;
};

} // namespace operators
} // namespace gendb
