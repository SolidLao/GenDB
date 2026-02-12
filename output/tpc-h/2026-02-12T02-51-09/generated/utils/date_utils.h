#pragma once

#include <cstdint>
#include <ctime>
#include <string>
#include <cstdio>

namespace gendb {

// Convert date to days since epoch (1970-01-01)
inline int32_t date_to_days(int year, int month, int day) {
    // Days since 1970-01-01
    // Use standard mktime approach
    struct tm t = {0};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    t.tm_hour = 12; // Use noon to avoid DST issues

    time_t epoch_time = mktime(&t);
    return epoch_time / 86400; // Convert seconds to days
}

// Parse date string "YYYY-MM-DD" to days since epoch
inline int32_t parse_date(const char* str) {
    int year, month, day;
    sscanf(str, "%d-%d-%d", &year, &month, &day);
    return date_to_days(year, month, day);
}

// Convert days since epoch back to date string "YYYY-MM-DD"
inline std::string days_to_date_str(int32_t days) {
    // Convert days since epoch to calendar date
    time_t epoch_time = (time_t)days * 86400;
    struct tm* t = gmtime(&epoch_time);

    char buf[12];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
             t->tm_year + 1900, t->tm_mon + 1, t->tm_mday);
    return std::string(buf);
}

} // namespace gendb
