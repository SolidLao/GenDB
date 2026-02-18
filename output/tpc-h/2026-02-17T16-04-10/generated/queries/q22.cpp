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
#include <thread>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cmath>
#include <omp.h>
#include <atomic>

namespace {

// Fixed width for string field (4-byte length prefix + variable data)
static constexpr size_t STRING_FIELD_WIDTH = 19;
static constexpr size_t MAX_CUSTKEY = 1500001;  // Customer keys are 1-based, max 1500000

struct Result {
    std::string cntrycode;
    int64_t numcust;
    int64_t totacctbal;  // stored as int64_t with scale factor 100
};

// Fast lookup: map 2-char phone code to array index
// Valid codes: '13'=0, '17'=1, '18'=2, '23'=3, '29'=4, '30'=5, '31'=6
inline int code_to_idx(char c0, char c1) {
    if (c0 == '1') {
        if (c1 == '3') return 0;  // '13'
        if (c1 == '7') return 1;  // '17'
        if (c1 == '8') return 2;  // '18'
    } else if (c0 == '2') {
        if (c1 == '3') return 3;  // '23'
        if (c1 == '9') return 4;  // '29'
    } else if (c0 == '3') {
        if (c1 == '0') return 5;  // '30'
        if (c1 == '1') return 6;  // '31'
    }
    return -1;  // invalid
}

// Extract phone code as 2-character array (no string allocation)
// Returns true if code is in valid set, avoiding string construction entirely
inline bool is_valid_phone_code(const char* data_raw, int32_t idx) {
    const unsigned char* ptr = reinterpret_cast<const unsigned char*>(data_raw) + (idx * STRING_FIELD_WIDTH);

    // Read 4-byte little-endian length
    uint32_t len = ptr[0] | (ptr[1] << 8) | (ptr[2] << 16) | (ptr[3] << 24);

    if (len < 2) return false;

    const char* str = reinterpret_cast<const char*>(ptr + 4);
    char c0 = str[0];
    char c1 = str[1];

    // Inline check for valid codes: '13', '31', '23', '29', '30', '18', '17'
    return (c0 == '1' && (c1 == '3' || c1 == '8' || c1 == '7')) ||
           (c0 == '3' && (c1 == '1' || c1 == '0')) ||
           (c0 == '2' && (c1 == '3' || c1 == '9'));
}

// Extract phone code for grouping (only called on filtered rows)
inline std::string get_phone_code_from_ptr(const char* str) {
    return std::string(str, 2);
}

// Get phone string pointer for later use
inline const char* get_phone_ptr(const char* data_raw, int32_t idx) {
    const unsigned char* ptr = reinterpret_cast<const unsigned char*>(data_raw) + (idx * STRING_FIELD_WIDTH);
    return reinterpret_cast<const char*>(ptr + 4);
}

// Bitmap lookup: check if custkey has orders
inline bool has_orders_bitmap(const std::vector<uint8_t>& bitmap, int32_t custkey) {
    if (custkey <= 0 || custkey >= (int32_t)MAX_CUSTKEY) return false;
    size_t byte_idx = custkey / 8;
    uint8_t bit_idx = custkey % 8;
    return (bitmap[byte_idx] & (1 << bit_idx)) != 0;
}

} // end anonymous namespace

void run_q22(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    const std::string customer_dir = gendb_dir + "/customer";
    const std::string orders_dir = gendb_dir + "/orders";

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

    // Load orders o_custkey using efficient byte-aligned bitmap (not vector<bool>)
    #ifdef GENDB_PROFILE
    auto t_orders_load_start = std::chrono::high_resolution_clock::now();
    #endif

    int fd_o_custkey = open((orders_dir + "/o_custkey.bin").c_str(), O_RDONLY);
    fstat(fd_o_custkey, &sb);
    size_t o_custkey_size = sb.st_size;
    void* o_custkey_data = mmap(nullptr, o_custkey_size, PROT_READ, MAP_SHARED, fd_o_custkey, 0);
    const int32_t* o_custkey = static_cast<int32_t*>(o_custkey_data);
    int32_t num_orders = o_custkey_size / sizeof(int32_t);

    // Use byte-aligned bitmap instead of vector<bool> for better cache locality
    // Allocate (MAX_CUSTKEY + 7) / 8 bytes
    size_t bitmap_bytes = (MAX_CUSTKEY + 7) / 8;
    std::vector<uint8_t> has_orders(bitmap_bytes, 0);

    // Parallel bitmap building - direct writes with atomic OR
    int num_threads = std::min(64, (int)std::thread::hardware_concurrency());

    #pragma omp parallel num_threads(num_threads)
    {
        #pragma omp for schedule(static, 10000)
        for (int32_t i = 0; i < num_orders; i++) {
            int32_t custkey = o_custkey[i];
            if (custkey > 0 && custkey < (int32_t)MAX_CUSTKEY) {
                size_t byte_idx = custkey / 8;
                uint8_t bit_idx = custkey % 8;
                // Use atomic OR to avoid contention: single byte per cache line
                uint8_t mask = (1 << bit_idx);
                // Load-check-update pattern: most reads hit without CAS
                if ((has_orders[byte_idx] & mask) == 0) {
                    __sync_fetch_and_or(&has_orders[byte_idx], mask);
                }
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_load_end = std::chrono::high_resolution_clock::now();
    double orders_load_ms = std::chrono::duration<double, std::milli>(t_orders_load_end - t_orders_load_start).count();
    printf("[TIMING] load_orders_data: %.2f ms\n", orders_load_ms);
    #endif

    // STEP 1: Parallel compute average threshold with thread-local reduction
    #ifdef GENDB_PROFILE
    auto t_combined_start = std::chrono::high_resolution_clock::now();
    #endif

    // Thread-local sums for parallel reduction
    std::vector<int64_t> thread_sums(num_threads, 0);
    std::vector<int32_t> thread_counts(num_threads, 0);

    #pragma omp parallel num_threads(num_threads)
    {
        int tid = omp_get_thread_num();
        int64_t local_sum = 0;
        int32_t local_count = 0;

        #pragma omp for schedule(static, 10000) nowait
        for (int32_t i = 0; i < num_customers; i++) {
            int64_t acctbal = c_acctbal[i];
            if (acctbal > 0 && is_valid_phone_code(c_phone_raw, i)) {
                local_sum += acctbal;
                local_count++;
            }
        }

        thread_sums[tid] = local_sum;
        thread_counts[tid] = local_count;
    }

    // Reduce thread-local sums
    int64_t sum_acctbal = 0;
    int32_t count_positive = 0;
    for (int tid = 0; tid < num_threads; tid++) {
        sum_acctbal += thread_sums[tid];
        count_positive += thread_counts[tid];
    }

    int64_t avg_threshold = (count_positive > 0) ? (sum_acctbal / count_positive) : 0;

    #ifdef GENDB_PROFILE
    auto t_avg_end = std::chrono::high_resolution_clock::now();
    double avg_ms = std::chrono::duration<double, std::milli>(t_avg_end - t_combined_start).count();
    printf("[TIMING] compute_average: %.2f ms\n", avg_ms);
    printf("[DEBUG] Average threshold (scaled): %ld (actual: %.2f)\n", avg_threshold, avg_threshold / 100.0);
    printf("[DEBUG] Count for average: %d\n", count_positive);
    #endif

    // STEP 2: Parallel filter and aggregate using thread-local flat arrays (7 codes only)
    #ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    // Thread-local aggregation: flat arrays for 7 country codes
    struct CountryAgg {
        int64_t numcust = 0;
        int64_t totacctbal = 0;
    };

    std::vector<std::vector<CountryAgg>> thread_groups(num_threads, std::vector<CountryAgg>(7));
    std::vector<int32_t> thread_filtered(num_threads, 0);

    #pragma omp parallel num_threads(num_threads)
    {
        int tid = omp_get_thread_num();
        std::vector<CountryAgg>& local_grouped = thread_groups[tid];
        int32_t local_filtered = 0;

        #pragma omp for schedule(static, 10000) nowait
        for (int32_t i = 0; i < num_customers; i++) {
            int64_t acctbal = c_acctbal[i];
            int32_t custkey = c_custkey[i];

            // Filter 1: country code must be in list (without string construction)
            if (!is_valid_phone_code(c_phone_raw, i)) continue;

            // Filter 2: c_acctbal > average
            if (acctbal <= avg_threshold) continue;

            // Filter 3: NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)
            if (has_orders_bitmap(has_orders, custkey)) continue;

            local_filtered++;

            // Extract phone code and get index for flat array
            const char* phone_ptr = get_phone_ptr(c_phone_raw, i);
            int idx = code_to_idx(phone_ptr[0], phone_ptr[1]);
            if (idx >= 0) {
                local_grouped[idx].numcust++;
                local_grouped[idx].totacctbal += acctbal;
            }
        }

        thread_filtered[tid] = local_filtered;
    }

    // Merge thread-local aggregations (simple sum for flat arrays)
    std::vector<CountryAgg> global_grouped(7);
    int32_t total_filtered = 0;
    for (int tid = 0; tid < num_threads; tid++) {
        total_filtered += thread_filtered[tid];
        for (int idx = 0; idx < 7; idx++) {
            global_grouped[idx].numcust += thread_groups[tid][idx].numcust;
            global_grouped[idx].totacctbal += thread_groups[tid][idx].totacctbal;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] filter_and_aggregate: %.2f ms\n", filter_ms);
    printf("[DEBUG] Total filtered rows: %d\n", total_filtered);
    #endif

    // STEP 3: Convert to result vector in sorted order of codes
    // Country codes sorted: '13', '17', '18', '23', '29', '30', '31'
    const char* codes[] = {"13", "17", "18", "23", "29", "30", "31"};
    std::vector<Result> results;
    for (int idx = 0; idx < 7; idx++) {
        if (global_grouped[idx].numcust > 0) {
            results.push_back({
                codes[idx],
                global_grouped[idx].numcust,
                global_grouped[idx].totacctbal
            });
        }
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
