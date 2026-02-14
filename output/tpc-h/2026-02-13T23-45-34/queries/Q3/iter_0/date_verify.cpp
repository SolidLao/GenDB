#include <iostream>
#include <ctime>

int main() {
    // 1995-03-15 as days since 1970-01-01
    struct tm tm1995 = {0};
    tm1995.tm_year = 1995 - 1900;
    tm1995.tm_mon = 3 - 1;   // 0-indexed
    tm1995.tm_mday = 15;
    time_t t1995 = mktime(&tm1995);
    
    // 1970-01-01
    struct tm tm1970 = {0};
    tm1970.tm_year = 1970 - 1900;
    tm1970.tm_mon = 0;
    tm1970.tm_mday = 1;
    time_t t1970 = mktime(&tm1970);
    
    long days_1995_03_15 = (t1995 - t1970) / (24*3600);
    
    std::cout << "1995-03-15 as days since 1970-01-01: " << days_1995_03_15 << "\n";
    std::cout << "Expected approx: 9167-9168 days\n";
    
    return 0;
}
