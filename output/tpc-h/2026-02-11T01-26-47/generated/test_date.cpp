#include "utils/date_utils.h"
#include <iostream>

int main() {
    int32_t cutoff = gendb::date_to_days(1995, 3, 15);
    int32_t test1 = gendb::date_to_days(1992, 11, 18);
    int32_t test2 = gendb::date_to_days(1995, 2, 23);
    
    std::cout << "Cutoff 1995-03-15: " << cutoff << std::endl;
    std::cout << "Test 1992-11-18: " << test1 << " (< cutoff? " << (test1 < cutoff) << ")" << std::endl;
    std::cout << "Test 1995-02-23: " << test2 << " (< cutoff? " << (test2 < cutoff) << ")" << std::endl;
    
    return 0;
}
