#pragma once
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <string>
#include "date_utils.h"

namespace gendb {

// CLI parameter parsing utilities for parameterized query execution.
// Pattern: scan argv for "--name", parse next arg, return default if missing.

inline const char* find_arg(int argc, char* argv[], const char* name) {
    for (int i = 1; i < argc - 1; i++) {
        if (std::strcmp(argv[i], name) == 0) return argv[i + 1];
    }
    return nullptr;
}

// Parse a date argument as epoch days. Returns default_val if --name not found.
// Expects format "YYYY-MM-DD".
inline int32_t parse_date_arg(int argc, char* argv[], const char* name, int32_t default_val) {
    const char* val = find_arg(argc, argv, name);
    if (!val) return default_val;
    return date_str_to_epoch_days(val);
}

// Parse an integer argument. Returns default_val if --name not found.
inline int64_t parse_int_arg(int argc, char* argv[], const char* name, int64_t default_val) {
    const char* val = find_arg(argc, argv, name);
    if (!val) return default_val;
    return std::strtoll(val, nullptr, 10);
}

// Parse a double argument. Returns default_val if --name not found.
inline double parse_double_arg(int argc, char* argv[], const char* name, double default_val) {
    const char* val = find_arg(argc, argv, name);
    if (!val) return default_val;
    return std::strtod(val, nullptr);
}

// Parse a string argument. Returns default_val if --name not found.
inline std::string parse_string_arg(int argc, char* argv[], const char* name, const std::string& default_val) {
    const char* val = find_arg(argc, argv, name);
    if (!val) return default_val;
    return std::string(val);
}

} // namespace gendb
