#pragma once

#include <cstdint>
#include <string>
#include <ctime>

// Date utilities for TPC-H queries
// Dates are stored as int32_t representing days since Unix epoch (1970-01-01)

namespace date_utils {

// Convert YYYY-MM-DD string to days since epoch
inline int32_t parse_date(const std::string& date_str) {
    int year, month, day;
    if (sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day) != 3) {
        return 0; // Invalid date
    }

    struct tm t = {};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    t.tm_hour = 0;
    t.tm_min = 0;
    t.tm_sec = 0;
    t.tm_isdst = -1;

    time_t epoch_seconds = mktime(&t);
    return static_cast<int32_t>(epoch_seconds / 86400);
}

// Convert days since epoch to YYYY-MM-DD string
inline std::string days_to_date_str(int32_t days) {
    time_t epoch_seconds = static_cast<time_t>(days) * 86400;
    struct tm* t = gmtime(&epoch_seconds);

    char buffer[11];
    snprintf(buffer, sizeof(buffer), "%04d-%02d-%02d",
             t->tm_year + 1900, t->tm_mon + 1, t->tm_mday);
    return std::string(buffer);
}

// Helper: Add days to a date
inline int32_t add_days(int32_t base_days, int32_t offset) {
    return base_days + offset;
}

} // namespace date_utils
