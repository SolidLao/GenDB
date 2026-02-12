#pragma once

#include <cstdint>
#include <string>
#include <ctime>

namespace gendb {
namespace date_utils {

// Convert date string "YYYY-MM-DD" to days since epoch (1970-01-01)
inline int32_t parse_date(const std::string& date_str) {
    if (date_str.length() < 10) return 0;

    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Calculate days since epoch using standard formula
    struct tm t = {};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    t.tm_hour = 0;
    t.tm_min = 0;
    t.tm_sec = 0;
    t.tm_isdst = -1;

    time_t epoch = timegm(&t);
    return static_cast<int32_t>(epoch / 86400);
}

// Fast parse from const char* (for ingestion)
inline int32_t parse_date_fast(const char* ptr) {
    // Parse YYYY-MM-DD format
    int year = (ptr[0] - '0') * 1000 + (ptr[1] - '0') * 100 +
               (ptr[2] - '0') * 10 + (ptr[3] - '0');
    int month = (ptr[5] - '0') * 10 + (ptr[6] - '0');
    int day = (ptr[8] - '0') * 10 + (ptr[9] - '0');

    struct tm t = {};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    time_t epoch = timegm(&t);
    return static_cast<int32_t>(epoch / 86400);
}

// Convert days since epoch to date string "YYYY-MM-DD"
inline std::string days_to_date_str(int32_t days) {
    time_t epoch = static_cast<time_t>(days) * 86400;
    struct tm t;
    gmtime_r(&epoch, &t);

    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
             t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    return std::string(buf);
}

// Convert days to int representation for comparisons
inline int32_t date_to_days(int year, int month, int day) {
    struct tm t = {};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    time_t epoch = timegm(&t);
    return static_cast<int32_t>(epoch / 86400);
}

} // namespace date_utils
} // namespace gendb
