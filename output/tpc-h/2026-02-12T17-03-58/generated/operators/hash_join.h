#ifndef HASH_JOIN_H
#define HASH_JOIN_H

#include <cstdint>
#include <vector>
#include <functional>
#include "../index/index.h"

// Hash join: Build hash table on smaller side, probe with larger side
// KeyType: type of join key (e.g., int32_t for customer_key)
// BuildFn: function to get key from build-side row
// ProbeFn: function to get key from probe-side row
// OutputFn: function called for each matching pair (build_row_id, probe_row_id)

template<typename KeyType, typename BuildFn, typename ProbeFn, typename OutputFn>
void hash_join(uint64_t build_row_count, BuildFn build_key_fn,
               uint64_t probe_row_count, ProbeFn probe_key_fn,
               OutputFn output_fn) {
    // Build phase: create hash table from smaller side
    HashIndex<KeyType> hash_table;
    for (uint64_t row = 0; row < build_row_count; ++row) {
        KeyType key = build_key_fn(row);
        hash_table.insert(key, static_cast<uint32_t>(row));
    }

    // Probe phase: lookup each probe-side row in hash table
    for (uint64_t row = 0; row < probe_row_count; ++row) {
        KeyType key = probe_key_fn(row);
        const auto* matches = hash_table.lookup(key);
        if (matches) {
            for (uint32_t build_row : *matches) {
                output_fn(build_row, row);
            }
        }
    }
}

// Semi-join variant: only returns probe-side rows that have a match
// (Used for filtering before expensive operations)
template<typename KeyType, typename BuildFn, typename ProbeFn, typename OutputFn>
void semi_join(uint64_t build_row_count, BuildFn build_key_fn,
               uint64_t probe_row_count, ProbeFn probe_key_fn,
               OutputFn output_fn) {
    // Build phase
    HashIndex<KeyType> hash_table;
    for (uint64_t row = 0; row < build_row_count; ++row) {
        KeyType key = build_key_fn(row);
        hash_table.insert(key, 0); // We only care about existence
    }

    // Probe phase: only output probe rows with a match
    for (uint64_t row = 0; row < probe_row_count; ++row) {
        KeyType key = probe_key_fn(row);
        if (hash_table.contains(key)) {
            output_fn(row);
        }
    }
}

#endif // HASH_JOIN_H
