#pragma once

#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <cstdint>

// Dictionary encoding for low-cardinality string columns
// Maps strings to integer codes for faster comparison and lower memory usage
class StringDictionary {
private:
    std::vector<std::string> values;  // Code → String mapping
    std::unordered_map<std::string, uint32_t> codes; // String → Code mapping

public:
    // Encode a string to its code (creates new code if not seen before)
    uint32_t encode(const std::string& str) {
        auto it = codes.find(str);
        if (it != codes.end()) {
            return it->second;
        }

        uint32_t code = static_cast<uint32_t>(values.size());
        values.push_back(str);
        codes[str] = code;
        return code;
    }

    // Decode a code back to its string
    const std::string& decode(uint32_t code) const {
        return values[code];
    }

    // Get number of unique values
    size_t size() const {
        return values.size();
    }

    // Reserve space for expected number of unique values
    void reserve(size_t n) {
        values.reserve(n);
        codes.reserve(n);
    }
};
