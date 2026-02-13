#include <iostream>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstdint>
#include <set>
#include <iomanip>

inline int32_t date_to_days(int year, int month, int day) {
    int a = (14 - month) / 12;
    int y = year - a;
    int m = month + 12 * a - 3;
    return day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 719469;
}

int main() {
    std::string dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb/lineitem";
    size_t row_count = 59985990;
    
    int fd_ship = open((dir + "/l_shipdate.bin").c_str(), O_RDONLY);
    int fd_disc = open((dir + "/l_discount.bin").c_str(), O_RDONLY);
    int fd_qty = open((dir + "/l_quantity.bin").c_str(), O_RDONLY);
    int fd_price = open((dir + "/l_extendedprice.bin").c_str(), O_RDONLY);
    
    int32_t* shipdate = (int32_t*)mmap(nullptr, row_count * 4, PROT_READ, MAP_PRIVATE, fd_ship, 0);
    int64_t* discount = (int64_t*)mmap(nullptr, row_count * 8, PROT_READ, MAP_PRIVATE, fd_disc, 0);
    int64_t* quantity = (int64_t*)mmap(nullptr, row_count * 8, PROT_READ, MAP_PRIVATE, fd_qty, 0);
    int64_t* price = (int64_t*)mmap(nullptr, row_count * 8, PROT_READ, MAP_PRIVATE, fd_price, 0);
    
    int32_t date_start = date_to_days(1994, 1, 1);
    int32_t date_end = date_to_days(1995, 1, 1);
    
    std::set<int64_t> all_discounts;
    
    __int128 revenue = 0;
    int count = 0;
    
    for (size_t i = 0; i < row_count; ++i) {
        int32_t sd = shipdate[i];
        int64_t disc = discount[i];
        int64_t qty = quantity[i];
        
        all_discounts.insert(disc);
        
        if (sd >= date_start && sd < date_end && disc >= 5 && disc <= 7 && qty < 2400) {
            revenue += (__int128)price[i] * (__int128)disc;
            count++;
        }
    }
    
    std::cout << "Using __int128 for revenue: " << std::fixed << std::setprecision(2) 
              << (double)revenue / 10000.0 << std::endl;
    std::cout << "Match count: " << count << std::endl;
    
    std::cout << "\nAll discount values in table: ";
    for (auto d : all_discounts) {
        std::cout << d << " ";
    }
    std::cout << std::endl;
    
    return 0;
}
