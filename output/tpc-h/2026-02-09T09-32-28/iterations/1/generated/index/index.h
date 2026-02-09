#pragma once

#include <unordered_map>
#include <cstdint>
#include <string>
#include <functional>

// Hash index type alias for single-key lookups
using HashIndex = std::unordered_map<int32_t, size_t>;

// Composite key for Q1 grouping: (l_returnflag, l_linestatus)
// Using integer encoding to eliminate string hashing overhead
// returnflag: 'A'->0, 'N'->1, 'R'->2
// linestatus: 'F'->0, 'O'->1
// Combined into uint16_t: (returnflag << 8) | linestatus
struct Q1GroupKey {
    uint16_t encoded_key;

    // Helper constructor from strings (for compatibility)
    Q1GroupKey(const std::string& returnflag, const std::string& linestatus) {
        // Fast inline encoding using first character only
        uint8_t rf = (returnflag[0] == 'A') ? 0 : (returnflag[0] == 'N') ? 1 : 2;
        uint8_t ls = (linestatus[0] == 'F') ? 0 : 1;
        encoded_key = (static_cast<uint16_t>(rf) << 8) | ls;
    }

    // Constructor from encoded value
    Q1GroupKey(uint16_t key) : encoded_key(key) {}

    // Decode for output
    std::string get_returnflag() const {
        uint8_t rf = encoded_key >> 8;
        return (rf == 0) ? "A" : (rf == 1) ? "N" : "R";
    }

    std::string get_linestatus() const {
        uint8_t ls = encoded_key & 0xFF;
        return (ls == 0) ? "F" : "O";
    }

    bool operator==(const Q1GroupKey& other) const {
        return encoded_key == other.encoded_key;
    }

    bool operator<(const Q1GroupKey& other) const {
        return encoded_key < other.encoded_key;
    }
};

// Hash functor for Q1GroupKey
struct Q1GroupKeyHash {
    size_t operator()(const Q1GroupKey& key) const {
        return std::hash<uint16_t>()(key.encoded_key);
    }
};

// Composite key for Q3 grouping: (l_orderkey, o_orderdate, o_shippriority)
struct Q3GroupKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const Q3GroupKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

// Hash functor for Q3GroupKey
struct Q3GroupKeyHash {
    size_t operator()(const Q3GroupKey& key) const {
        return std::hash<int32_t>()(key.l_orderkey) ^
               (std::hash<int32_t>()(key.o_orderdate) << 1) ^
               (std::hash<int32_t>()(key.o_shippriority) << 2);
    }
};
