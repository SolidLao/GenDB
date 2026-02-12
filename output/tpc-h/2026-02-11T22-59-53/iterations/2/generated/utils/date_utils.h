#pragma once

#include <cstdint>
#include <string>
#include <ctime>

// Date utilities for TPC-H queries
// Dates stored as int32_t: days since Unix epoch (1970-01-01)

namespace date_utils {

// Convert date string "YYYY-MM-DD" to days since epoch
inline int32_t parse_date(const std::string& date_str) {
    int year, month, day;
    sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day);

    struct tm t = {};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    t.tm_isdst = -1;

    time_t epoch_time = mktime(&t);
    return static_cast<int32_t>(epoch_time / 86400);
}

// Convert days since epoch to date string "YYYY-MM-DD"
inline std::string days_to_date_str(int32_t days) {
    time_t epoch_time = static_cast<time_t>(days) * 86400;
    struct tm* t = gmtime(&epoch_time);
    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
             t->tm_year + 1900, t->tm_mon + 1, t->tm_mday);
    return std::string(buf);
}

// Convert date components to days since epoch
inline int32_t date_to_days(int year, int month, int day) {
    struct tm t = {};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    t.tm_isdst = -1;

    time_t epoch_time = mktime(&t);
    return static_cast<int32_t>(epoch_time / 86400);
}

} // namespace date_utils
