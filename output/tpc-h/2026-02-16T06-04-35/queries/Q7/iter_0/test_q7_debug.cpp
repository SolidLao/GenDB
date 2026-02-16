#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

template<typename T>
std::vector<T> mmap_load(const std::string& filepath, size_t count) {
    std::vector<T> data(count);
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << filepath << std::endl;
        return data;
    }
    size_t bytes = count * sizeof(T);
    void* ptr = mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for: " << filepath << std::endl;
        close(fd);
        return data;
    }
    std::memcpy(data.data(), ptr, bytes);
    munmap(ptr, bytes);
    close(fd);
    return data;
}

int32_t compute_epoch_days(int year, int month, int day) {
    int days = 0;
    for (int y = 1970; y < year; ++y) {
        int is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += is_leap ? 366 : 365;
    }
    int month_days[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (is_leap) month_days[1] = 29;
    for (int m = 1; m < month; ++m) {
        days += month_days[m - 1];
    }
    days += (day - 1);
    return days;
}

int main() {
    const std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    const size_t LINEITEM_ROWS = 59986052;
    const size_t NATION_ROWS = 25;
    const size_t SUPPLIER_ROWS = 100000;
    const size_t CUSTOMER_ROWS = 1500000;

    std::cout << "Loading data..." << std::endl;
    
    // Load lineitem dates
    std::vector<int32_t> l_shipdate = mmap_load<int32_t>(
        gendb_dir + "/lineitem/l_shipdate.bin", LINEITEM_ROWS);
    
    int32_t date_1995_01_01 = compute_epoch_days(1995, 1, 1);
    int32_t date_1996_12_31 = compute_epoch_days(1996, 12, 31);
    
    std::cout << "Date range: " << date_1995_01_01 << " - " << date_1996_12_31 << std::endl;
    
    // Count rows in date range
    size_t in_range = 0;
    for (size_t i = 0; i < std::min((size_t)1000000, LINEITEM_ROWS); ++i) {
        if (l_shipdate[i] >= date_1995_01_01 && l_shipdate[i] <= date_1996_12_31) {
            in_range++;
        }
    }
    std::cout << "In range (first 1M): " << in_range << std::endl;
    
    // Load nation names
    std::vector<int32_t> n_nationkey = mmap_load<int32_t>(
        gendb_dir + "/nation/n_nationkey.bin", NATION_ROWS);
    
    std::unordered_map<int32_t, std::string> nation_names;
    std::ifstream nation_file(gendb_dir + "/nation/n_name.bin");
    for (size_t i = 0; i < NATION_ROWS; ++i) {
        char name[26] = {0};
        nation_file.read(name, 25);
        std::string n(name);
        n.erase(n.find_last_not_of(' ') + 1);
        nation_names[n_nationkey[i]] = n;
    }
    nation_file.close();
    
    std::cout << "Nations:" << std::endl;
    for (auto& [nkey, nname] : nation_names) {
        std::cout << "  " << nkey << " -> " << nname << std::endl;
    }
    
    // Load supplier nation keys
    std::vector<int32_t> s_nationkey = mmap_load<int32_t>(
        gendb_dir + "/supplier/s_nationkey.bin", SUPPLIER_ROWS);
    
    std::cout << "Supplier nation distribution:" << std::endl;
    std::unordered_map<int32_t, int> supp_nation_count;
    for (int nk : s_nationkey) {
        supp_nation_count[nk]++;
    }
    for (auto& [nk, cnt] : supp_nation_count) {
        std::cout << "  nation " << nk << " (" << nation_names[nk] << "): " << cnt << std::endl;
    }
    
    // Load customer nation keys
    std::vector<int32_t> c_nationkey = mmap_load<int32_t>(
        gendb_dir + "/customer/c_nationkey.bin", CUSTOMER_ROWS);
    
    std::cout << "Customer nation distribution:" << std::endl;
    std::unordered_map<int32_t, int> cust_nation_count;
    for (int nk : c_nationkey) {
        cust_nation_count[nk]++;
    }
    for (auto& [nk, cnt] : cust_nation_count) {
        std::cout << "  nation " << nk << " (" << nation_names[nk] << "): " << cnt << std::endl;
    }
    
    return 0;
}
