#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_set>
#include <map>
#include <algorithm>
#include <cstring>
#include <cstdint>
#include <chrono>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cmath>

int main(int argc, char* argv[]) {
    const std::string gendb_dir = "/home/jl4492/GenDB/benchmarks/tpc-h/gendb/tpch_sf10.gendb";
    const std::string customer_dir = gendb_dir + "/customer";
    const std::string orders_dir = gendb_dir + "/orders";

    std::unordered_set<std::string> valid_codes = {"13", "31", "23", "29", "30", "18", "17"};

    // Load customer data
    int fd_custkey = open((customer_dir + "/c_custkey.bin").c_str(), O_RDONLY);
    struct stat sb;
    fstat(fd_custkey, &sb);
    size_t custkey_size = sb.st_size;
    void* custkey_data = mmap(nullptr, custkey_size, PROT_READ, MAP_SHARED, fd_custkey, 0);
    const int32_t* c_custkey = static_cast<int32_t*>(custkey_data);
    int32_t num_customers = custkey_size / sizeof(int32_t);

    int fd_phone = open((customer_dir + "/c_phone.bin").c_str(), O_RDONLY);
    fstat(fd_phone, &sb);
    size_t phone_size = sb.st_size;
    void* phone_data = mmap(nullptr, phone_size, PROT_READ, MAP_SHARED, fd_phone, 0);
    const char* c_phone_raw = static_cast<char*>(phone_data);

    int fd_acctbal = open((customer_dir + "/c_acctbal.bin").c_str(), O_RDONLY);
    fstat(fd_acctbal, &sb);
    size_t acctbal_size = sb.st_size;
    void* acctbal_data = mmap(nullptr, acctbal_size, PROT_READ, MAP_SHARED, fd_acctbal, 0);
    const int64_t* c_acctbal = static_cast<int64_t*>(acctbal_data);

    // Parse c_phone
    std::vector<std::string> c_phone_strs(num_customers);
    size_t offset = 0;
    for (int32_t i = 0; i < num_customers && offset < phone_size; i++) {
        const char* start = c_phone_raw + offset;
        size_t len = std::strlen(start);
        c_phone_strs[i] = std::string(start, len);
        offset += len + 1;
    }

    // Load orders
    int fd_o_custkey = open((orders_dir + "/o_custkey.bin").c_str(), O_RDONLY);
    fstat(fd_o_custkey, &sb);
    size_t o_custkey_size = sb.st_size;
    void* o_custkey_data = mmap(nullptr, o_custkey_size, PROT_READ, MAP_SHARED, fd_o_custkey, 0);
    const int32_t* o_custkey = static_cast<int32_t*>(o_custkey_data);
    int32_t num_orders = o_custkey_size / sizeof(int32_t);

    // Build set of customer keys that have orders
    std::unordered_set<int32_t> customers_with_orders;
    for (int32_t i = 0; i < num_orders; i++) {
        customers_with_orders.insert(o_custkey[i]);
    }

    printf("Total customers: %d\n", num_customers);
    printf("Customers with orders: %zu\n", customers_with_orders.size());
    printf("Customers WITHOUT orders: %d\n", num_customers - (int)customers_with_orders.size());

    // Step 1: Calculate average ONLY from customers WITHOUT orders
    int64_t sum_acctbal = 0;
    int32_t count_positive = 0;

    for (int32_t i = 0; i < num_customers; i++) {
        int32_t custkey = c_custkey[i];
        int64_t acctbal = c_acctbal[i];

        // Only include customers WITHOUT orders
        if (customers_with_orders.count(custkey) > 0) continue;

        if (acctbal > 0 && c_phone_strs[i].length() >= 2) {
            std::string code = c_phone_strs[i].substr(0, 2);
            if (valid_codes.count(code) > 0) {
                sum_acctbal += acctbal;
                count_positive++;
            }
        }
    }

    int64_t avg_threshold = (count_positive > 0) ? (sum_acctbal / count_positive) : 0;

    printf("Sum: %ld, Count: %d, Avg (scaled): %ld, Avg (actual): %.2f\n", 
           sum_acctbal, count_positive, avg_threshold, avg_threshold / 100.0);

    // Now count filtered
    int32_t pass_code = 0, pass_avg = 0, pass_no_orders = 0;

    for (int32_t i = 0; i < num_customers; i++) {
        int64_t acctbal = c_acctbal[i];
        int32_t custkey = c_custkey[i];

        if (c_phone_strs[i].length() < 2) continue;

        std::string code = c_phone_strs[i].substr(0, 2);

        // Filter 1: country code must be in list
        if (valid_codes.count(code) == 0) continue;
        pass_code++;

        // Filter 2: c_acctbal > average
        if (acctbal <= avg_threshold) continue;
        pass_avg++;

        // Filter 3: NOT EXISTS
        if (customers_with_orders.count(custkey) > 0) continue;
        pass_no_orders++;
    }

    printf("Pass country code filter: %d\n", pass_code);
    printf("Pass average filter: %d\n", pass_avg);
    printf("Pass no orders filter: %d\n", pass_no_orders);

    return 0;
}
