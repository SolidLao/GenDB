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

// Fixed width for string field (4-byte length prefix + variable data)
static constexpr size_t STRING_FIELD_WIDTH = 19;
static constexpr size_t MAX_CUSTKEY = 1500001;  // Customer keys are 1-based, max 1500000

struct Result {
    std::string cntrycode;
    int64_t numcust;
    int64_t totacctbal;  // stored as int64_t with scale factor 100
};

// Fast phone code extraction without allocating strings
std::string get_phone_code(const char* data_raw, int32_t idx) {
    const unsigned char* ptr = reinterpret_cast<const unsigned char*>(data_raw) + (idx * STRING_FIELD_WIDTH);

    // Read 4-byte little-endian length
    uint32_t len = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);

    // Extract first 2 characters of the string
    if (len >= 2) {
        const char* str = reinterpret_cast<const char*>(ptr + 4);
        return std::string(str, 2);
    }
    return "";
}

void run_q22(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    const std::string customer_dir = gendb_dir + "/customer";
    const std::string orders_dir = gendb_dir + "/orders";

    // Country codes to filter
    std::unordered_set<std::string> valid_codes = {"13", "31", "23", "29", "30", "18", "17"};

    // Load customer data
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

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

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_customer_data: %.2f ms\n", load_ms);
    #endif

    // Load orders o_custkey using bitset for fast lookup
    #ifdef GENDB_PROFILE
    auto t_orders_load_start = std::chrono::high_resolution_clock::now();
    #endif

    int fd_o_custkey = open((orders_dir + "/o_custkey.bin").c_str(), O_RDONLY);
    fstat(fd_o_custkey, &sb);
    size_t o_custkey_size = sb.st_size;
    void* o_custkey_data = mmap(nullptr, o_custkey_size, PROT_READ, MAP_SHARED, fd_o_custkey, 0);
    const int32_t* o_custkey = static_cast<int32_t*>(o_custkey_data);
    int32_t num_orders = o_custkey_size / sizeof(int32_t);

    // Use a bitset to track which customers have orders
    // This is much faster than unordered_set for lookup
    std::vector<bool> has_orders(MAX_CUSTKEY, false);

    for (int32_t i = 0; i < num_orders; i++) {
        int32_t custkey = o_custkey[i];
        if (custkey > 0 && custkey < (int32_t)MAX_CUSTKEY) {
            has_orders[custkey] = true;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_load_end = std::chrono::high_resolution_clock::now();
    double orders_load_ms = std::chrono::duration<double, std::milli>(t_orders_load_end - t_orders_load_start).count();
    printf("[TIMING] load_orders_data: %.2f ms\n", orders_load_ms);
    #endif

    // STEP 1: Compute average c_acctbal for filtering
    // WHERE c_acctbal > 0 AND SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
    #ifdef GENDB_PROFILE
    auto t_avg_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t sum_acctbal = 0;
    int32_t count_positive = 0;

    for (int32_t i = 0; i < num_customers; i++) {
        int64_t acctbal = c_acctbal[i];

        if (acctbal > 0) {
            std::string code = get_phone_code(c_phone_raw, i);
            if (code.length() >= 2 && valid_codes.count(code) > 0) {
                sum_acctbal += acctbal;
                count_positive++;
            }
        }
    }

    int64_t avg_threshold = (count_positive > 0) ? (sum_acctbal / count_positive) : 0;

    #ifdef GENDB_PROFILE
    auto t_avg_end = std::chrono::high_resolution_clock::now();
    double avg_ms = std::chrono::duration<double, std::milli>(t_avg_end - t_avg_start).count();
    printf("[TIMING] compute_average: %.2f ms\n", avg_ms);
    printf("[DEBUG] Average threshold (scaled): %ld (actual: %.2f)\n", avg_threshold, avg_threshold / 100.0);
    printf("[DEBUG] Count for average: %d\n", count_positive);
    #endif

    // STEP 2: Filter customers and group by country code
    #ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    std::map<std::string, Result> grouped;
    int32_t total_filtered = 0;

    for (int32_t i = 0; i < num_customers; i++) {
        int64_t acctbal = c_acctbal[i];
        int32_t custkey = c_custkey[i];

        std::string code = get_phone_code(c_phone_raw, i);
        if (code.length() < 2) continue;

        // Filter 1: country code must be in list
        if (valid_codes.count(code) == 0) continue;

        // Filter 2: c_acctbal > average
        if (acctbal <= avg_threshold) continue;

        // Filter 3: NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)
        if (custkey < (int32_t)MAX_CUSTKEY && has_orders[custkey]) continue;

        total_filtered++;

        // Passed all filters - add to result
        if (grouped.count(code) == 0) {
            grouped[code] = {code, 0, 0};
        }
        grouped[code].numcust++;
        grouped[code].totacctbal += acctbal;
    }

    #ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] filter_and_aggregate: %.2f ms\n", filter_ms);
    printf("[DEBUG] Total filtered rows: %d\n", total_filtered);
    #endif

    // STEP 3: Sort by country code (already sorted due to std::map)
    std::vector<Result> results;
    for (auto& entry : grouped) {
        results.push_back(entry.second);
    }

    // STEP 4: Write results to CSV
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_file = results_dir + "/Q22.csv";
    std::ofstream out(output_file);

    out << "cntrycode,numcust,totacctbal\n";
    for (const auto& res : results) {
        // totacctbal is stored as int64_t with scale factor 100
        // Convert to decimal: divide by 100
        double total_decimal = res.totacctbal / 100.0;
        out << res.cntrycode << "," << res.numcust << "," << std::fixed;
        out.precision(2);
        out << total_decimal << "\n";
    }
    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    // Cleanup
    munmap(custkey_data, custkey_size);
    munmap(phone_data, phone_size);
    munmap(acctbal_data, acctbal_size);
    munmap(o_custkey_data, o_custkey_size);
    close(fd_custkey);
    close(fd_phone);
    close(fd_acctbal);
    close(fd_o_custkey);

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    #endif

    std::cout << "Q22 complete. Results written to " << output_file << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q22(gendb_dir, results_dir);
    return 0;
}
#endif
