#pragma once
#include <cstdint>
#include <string>
#include <ctime>

namespace gendb {

// Convert DATE string ('YYYY-MM-DD') to days since epoch (1970-01-01)
inline int32_t date_to_days(const std::string& date_str) {
    int year, month, day;
    sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day);

    struct tm t = {};
    t.tm_year = year - 1900;
    t.tm_mon = month - 1;
    t.tm_mday = day;
    t.tm_hour = 0;
    t.tm_min = 0;
    t.tm_sec = 0;

    time_t epoch_time = timegm(&t);
    return static_cast<int32_t>(epoch_time / 86400);
}

// Convert days since epoch back to 'YYYY-MM-DD' string
inline std::string days_to_date(int32_t days) {
    time_t epoch_time = static_cast<time_t>(days) * 86400;
    struct tm t;
    gmtime_r(&epoch_time, &t);

    char buf[11];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
             t.tm_year + 1900, t.tm_mon + 1, t.tm_mday);
    return std::string(buf);
}

// Date literals as constants (days since epoch)
constexpr int32_t DATE_1994_01_01 = 8766;  // days since 1970-01-01
constexpr int32_t DATE_1995_01_01 = 9131;
constexpr int32_t DATE_1995_03_15 = 9204;
constexpr int32_t DATE_1998_09_02 = 10471;

} // namespace gendb
