#pragma once

#include <unordered_map>
#include <vector>
#include <cstdint>
#include <functional>

namespace gendb {

// Hash join operator: build hash table on smaller side, probe with larger
// Supports both 1:N joins (build side can have duplicates) and 1:1 joins

template<typename KeyType, typename BuildValue, typename ProbeValue>
struct HashJoinResult {
    std::vector<KeyType> keys;
    std::vector<BuildValue> build_values;
    std::vector<ProbeValue> probe_values;

    void reserve(size_t n) {
        keys.reserve(n);
        build_values.reserve(n);
        probe_values.reserve(n);
    }

    size_t size() const { return keys.size(); }
};

// Hash join operator class for cleaner interface
template<typename KeyType, typename BuildValue, typename ProbeValue, typename KeyHash = std::hash<KeyType>>
class HashJoin {
public:
    using BuildTable = std::unordered_map<KeyType, std::vector<BuildValue>, KeyHash>;
    using Result = HashJoinResult<KeyType, BuildValue, ProbeValue>;

    HashJoin() = default;

    // Build phase: create hash table from build-side keys/values
    // Supports multiple values per key (1:N join)
    void build(const std::vector<KeyType>& keys, const std::vector<BuildValue>& values) {
        build_table_.clear();
        build_table_.reserve(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            build_table_[keys[i]].push_back(values[i]);
        }
    }

    // Build phase with predicate filter (filter build side before inserting)
    template<typename FilterFunc>
    void build_filtered(const std::vector<KeyType>& keys,
                       const std::vector<BuildValue>& values,
                       FilterFunc&& filter) {
        build_table_.clear();
        build_table_.reserve(keys.size() / 2); // Estimate after filtering

        for (size_t i = 0; i < keys.size(); ++i) {
            if (filter(i)) {
                build_table_[keys[i]].push_back(values[i]);
            }
        }
    }

    // Probe phase: probe hash table with probe-side keys
    Result probe(const std::vector<KeyType>& probe_keys,
                 const std::vector<ProbeValue>& probe_values) const {
        Result result;
        result.reserve(probe_keys.size()); // Initial estimate

        for (size_t i = 0; i < probe_keys.size(); ++i) {
            auto it = build_table_.find(probe_keys[i]);
            if (it != build_table_.end()) {
                // Found matching key(s) in build table
                for (const auto& build_value : it->second) {
                    result.keys.push_back(probe_keys[i]);
                    result.build_values.push_back(build_value);
                    result.probe_values.push_back(probe_values[i]);
                }
            }
        }

        return result;
    }

    // Probe with predicate filter (filter probe side before probing)
    template<typename FilterFunc>
    Result probe_filtered(const std::vector<KeyType>& probe_keys,
                         const std::vector<ProbeValue>& probe_values,
                         FilterFunc&& filter) const {
        Result result;
        result.reserve(probe_keys.size() / 2); // Estimate after filtering

        for (size_t i = 0; i < probe_keys.size(); ++i) {
            if (!filter(i)) continue;

            auto it = build_table_.find(probe_keys[i]);
            if (it != build_table_.end()) {
                for (const auto& build_value : it->second) {
                    result.keys.push_back(probe_keys[i]);
                    result.build_values.push_back(build_value);
                    result.probe_values.push_back(probe_values[i]);
                }
            }
        }

        return result;
    }

    // Get build table size (useful for statistics)
    size_t build_size() const { return build_table_.size(); }

private:
    BuildTable build_table_;
};

// Specialized hash join for unique build keys (1:1 join, PK-FK)
// More efficient than general case - stores single value per key
template<typename KeyType, typename BuildValue, typename ProbeValue, typename KeyHash = std::hash<KeyType>>
class UniqueHashJoin {
public:
    using BuildTable = std::unordered_map<KeyType, BuildValue, KeyHash>;
    using Result = HashJoinResult<KeyType, BuildValue, ProbeValue>;

    UniqueHashJoin() = default;

    // Build phase for unique keys
    void build(const std::vector<KeyType>& keys, const std::vector<BuildValue>& values) {
        build_table_.clear();
        build_table_.reserve(keys.size());

        for (size_t i = 0; i < keys.size(); ++i) {
            build_table_[keys[i]] = values[i];
        }
    }

    // Build with filter
    template<typename FilterFunc>
    void build_filtered(const std::vector<KeyType>& keys,
                       const std::vector<BuildValue>& values,
                       FilterFunc&& filter) {
        build_table_.clear();
        build_table_.reserve(keys.size() / 2);

        for (size_t i = 0; i < keys.size(); ++i) {
            if (filter(i)) {
                build_table_[keys[i]] = values[i];
            }
        }
    }

    // Probe phase
    Result probe(const std::vector<KeyType>& probe_keys,
                 const std::vector<ProbeValue>& probe_values) const {
        Result result;
        result.reserve(probe_keys.size());

        for (size_t i = 0; i < probe_keys.size(); ++i) {
            auto it = build_table_.find(probe_keys[i]);
            if (it != build_table_.end()) {
                result.keys.push_back(probe_keys[i]);
                result.build_values.push_back(it->second);
                result.probe_values.push_back(probe_values[i]);
            }
        }

        return result;
    }

    // Probe with filter
    template<typename FilterFunc>
    Result probe_filtered(const std::vector<KeyType>& probe_keys,
                         const std::vector<ProbeValue>& probe_values,
                         FilterFunc&& filter) const {
        Result result;
        result.reserve(probe_keys.size() / 2);

        for (size_t i = 0; i < probe_keys.size(); ++i) {
            if (!filter(i)) continue;

            auto it = build_table_.find(probe_keys[i]);
            if (it != build_table_.end()) {
                result.keys.push_back(probe_keys[i]);
                result.build_values.push_back(it->second);
                result.probe_values.push_back(probe_values[i]);
            }
        }

        return result;
    }

    size_t build_size() const { return build_table_.size(); }

private:
    BuildTable build_table_;
};

// Legacy functional interface (for backward compatibility)
template<typename KeyType, typename ValueType>
std::unordered_map<KeyType, std::vector<ValueType>> hash_join_build(
    const std::vector<KeyType>& keys,
    const std::vector<ValueType>& values) {

    std::unordered_map<KeyType, std::vector<ValueType>> hash_table;
    hash_table.reserve(keys.size());

    for (size_t i = 0; i < keys.size(); ++i) {
        hash_table[keys[i]].push_back(values[i]);
    }

    return hash_table;
}

template<typename KeyType, typename BuildValue, typename ProbeValue>
HashJoinResult<KeyType, BuildValue, ProbeValue> hash_join_probe(
    const std::unordered_map<KeyType, std::vector<BuildValue>>& hash_table,
    const std::vector<KeyType>& probe_keys,
    const std::vector<ProbeValue>& probe_values) {

    HashJoinResult<KeyType, BuildValue, ProbeValue> result;
    result.reserve(probe_keys.size());

    for (size_t i = 0; i < probe_keys.size(); ++i) {
        auto it = hash_table.find(probe_keys[i]);
        if (it != hash_table.end()) {
            for (const auto& build_value : it->second) {
                result.keys.push_back(probe_keys[i]);
                result.build_values.push_back(build_value);
                result.probe_values.push_back(probe_values[i]);
            }
        }
    }

    return result;
}

} // namespace gendb
