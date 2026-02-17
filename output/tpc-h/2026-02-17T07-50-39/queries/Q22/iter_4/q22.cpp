/*
 * TPC-H Q22: Global Sales Opportunity
 *
 * LOGICAL PLAN:
 * 1. Pre-compute scalar subquery: AVG(c_acctbal) from customer
 *    WHERE c_acctbal > 0 AND SUBSTRING(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
 *    → single double value
 * 2. Build anti-join set: scan orders, insert all o_custkey into hash set (~15M entries)
 * 3. Main customer scan with filters:
 *    - Phone prefix IN ('13','31','23','29','30','18','17')
 *    - c_acctbal > avg_threshold
 *    - c_custkey NOT IN orders_set (anti-join)
 * 4. Aggregate: GROUP BY country_code (7 groups), COUNT(*), SUM(c_acctbal)
 * 5. Sort by country_code
 *
 * PHYSICAL PLAN:
 * - Scalar avg: single-threaded scan (1.5M rows, fast)
 * - Anti-join: use pre-built hash index if available, else build hash set from orders
 * - Main scan: parallel with thread-local aggregation (7 groups → flat array)
 * - Country code extraction: first 2 chars of phone string
 * - Aggregation: flat array indexed by country code enum (7 elements)
 * - Sort: std::sort on 7 elements
 *
 * DATA NOTES:
 * - c_acctbal is int64_t scaled by 100
 * - c_phone is std::string
 * - Country codes: "13","17","18","23","29","30","31"
 */

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <unordered_set>
#include <vector>
#include <omp.h>

// Helper: mmap a binary column file
template<typename T>
T* mmap_column(const std::string& path, size_t expected_count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat sb;
    if (fstat(fd, &sb) < 0) { perror("fstat"); exit(1); }
    size_t expected_size = expected_count * sizeof(T);
    if ((size_t)sb.st_size != expected_size) {
        fprintf(stderr, "Size mismatch: %s expected %zu got %ld\n", path.c_str(), expected_size, sb.st_size);
        exit(1);
    }
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (addr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return static_cast<T*>(addr);
}

// Country code mapping
enum CountryCode : int {
    CODE_13 = 0,
    CODE_17 = 1,
    CODE_18 = 2,
    CODE_23 = 3,
    CODE_29 = 4,
    CODE_30 = 5,
    CODE_31 = 6,
    CODE_INVALID = -1
};

const char* country_code_strings[] = {"13", "17", "18", "23", "29", "30", "31"};

// Parse country code from phone prefix (2 bytes: char1 << 8 | char2)
CountryCode parse_country_code(uint16_t prefix) {
    // Encoding: '1' = 0x31, '3' = 0x33, '7' = 0x37, '8' = 0x38, '2' = 0x32, '9' = 0x39, '0' = 0x30
    if (prefix == 0x3133) return CODE_13; // "13"
    if (prefix == 0x3137) return CODE_17; // "17"
    if (prefix == 0x3138) return CODE_18; // "18"
    if (prefix == 0x3233) return CODE_23; // "23"
    if (prefix == 0x3239) return CODE_29; // "29"
    if (prefix == 0x3330) return CODE_30; // "30"
    if (prefix == 0x3331) return CODE_31; // "31"
    return CODE_INVALID;
}

bool is_valid_country_code(CountryCode code) {
    return code >= CODE_13 && code <= CODE_31;
}

void run_q22(const std::string& gendb_dir, const std::string& results_dir) {
#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    const size_t customer_rows = 1500000;
    const size_t orders_rows = 15000000;

    // Load customer columns
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif
    auto* c_custkey = mmap_column<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_rows);
    auto* c_acctbal = mmap_column<int64_t>(gendb_dir + "/customer/c_acctbal.bin", customer_rows);
    const int phone_width = 19;
    auto* c_phone = mmap_column<char>(gendb_dir + "/customer/c_phone.bin", customer_rows * phone_width);

    // Load orders column
    auto* o_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_rows);
#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] load: %.2f ms\n", std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count());
#endif

    // Parse phone strings (c_phone is stored as length-prefixed strings: 4 bytes length + string data)
    // We only need the first 2 characters for country code extraction
    std::vector<uint16_t> phone_prefix(customer_rows);
#ifdef GENDB_PROFILE
    auto t_parse_start = std::chrono::high_resolution_clock::now();
#endif
    for (size_t i = 0; i < customer_rows; i++) {
        const char* entry_ptr = c_phone + i * phone_width;
        uint32_t len = *(uint32_t*)entry_ptr;
        const char* phone_ptr = entry_ptr + 4;
        // Extract first 2 characters and encode as uint16_t (char1 << 8 | char2)
        if (len >= 2) {
            phone_prefix[i] = (static_cast<uint16_t>(phone_ptr[0]) << 8) | static_cast<uint16_t>(phone_ptr[1]);
        } else {
            phone_prefix[i] = 0; // Invalid
        }
    }
#ifdef GENDB_PROFILE
    auto t_parse_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] parse_phone: %.2f ms\n", std::chrono::duration<double, std::milli>(t_parse_end - t_parse_start).count());
#endif

    // Step 1: Compute scalar subquery (AVG of c_acctbal for qualifying customers)
#ifdef GENDB_PROFILE
    auto t_avg_start = std::chrono::high_resolution_clock::now();
#endif
    int64_t sum_acctbal = 0;
    int64_t count_acctbal = 0;
    for (size_t i = 0; i < customer_rows; i++) {
        if (c_acctbal[i] <= 0) continue;
        CountryCode code = parse_country_code(phone_prefix[i]);
        if (!is_valid_country_code(code)) continue;
        sum_acctbal += c_acctbal[i];
        count_acctbal++;
    }
#ifdef GENDB_PROFILE
    auto t_avg_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] scalar_avg: %.2f ms (count=%ld, sum=%ld)\n",
           std::chrono::duration<double, std::milli>(t_avg_end - t_avg_start).count(),
           count_acctbal, sum_acctbal);
#endif

    // Step 2: Build anti-join bitmap from orders (all o_custkey)
    // c_custkey ranges from 1 to 1,500,000, so use bitmap for O(1) lookup
#ifdef GENDB_PROFILE
    auto t_anti_start = std::chrono::high_resolution_clock::now();
#endif
    const size_t max_custkey = 1500000;
    std::vector<bool> has_order(max_custkey + 1, false);
    for (size_t i = 0; i < orders_rows; i++) {
        has_order[o_custkey[i]] = true;
    }
#ifdef GENDB_PROFILE
    auto t_anti_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] build_antijoin: %.2f ms\n",
           std::chrono::duration<double, std::milli>(t_anti_end - t_anti_start).count());
#endif

    // Step 3: Main scan + aggregation
    // Use thread-local aggregation (7 groups)
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    const int num_threads = omp_get_max_threads();
    std::vector<int64_t> local_count[num_threads];
    std::vector<int64_t> local_sum[num_threads];
    for (int t = 0; t < num_threads; t++) {
        local_count[t].resize(7, 0);
        local_sum[t].resize(7, 0);
    }

#pragma omp parallel for schedule(static)
    for (size_t i = 0; i < customer_rows; i++) {
        // Filter 1: valid country code
        CountryCode code = parse_country_code(phone_prefix[i]);
        if (!is_valid_country_code(code)) continue;

        // Filter 2: c_acctbal > AVG(c_acctbal)
        // Use integer comparison: c_acctbal[i] * count > sum
        // This avoids any floating-point precision issues
        if (c_acctbal[i] * count_acctbal <= sum_acctbal) continue;

        // Filter 3: anti-join (NOT EXISTS in orders)
        if (has_order[c_custkey[i]]) continue;

        // Aggregate
        int tid = omp_get_thread_num();
        local_count[tid][code]++;
        local_sum[tid][code] += c_acctbal[i];
    }

    // Merge thread-local results
    int64_t final_count[7] = {0};
    int64_t final_sum[7] = {0};
    for (int t = 0; t < num_threads; t++) {
        for (int c = 0; c < 7; c++) {
            final_count[c] += local_count[t][c];
            final_sum[c] += local_sum[t][c];
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] scan_aggregate: %.2f ms\n", std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count());
#endif

    // Step 4: Build result and sort
    struct Result {
        const char* cntrycode;
        int64_t numcust;
        int64_t totacctbal;
    };
    std::vector<Result> results;
    for (int c = 0; c < 7; c++) {
        if (final_count[c] > 0) {
            results.push_back({country_code_strings[c], final_count[c], final_sum[c]});
        }
    }
    std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return strcmp(a.cntrycode, b.cntrycode) < 0;
    });

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] total: %.2f ms\n", std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count());
#endif

    // Step 5: Write output
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif
    std::string output_path = results_dir + "/Q22.csv";
    FILE* fp = fopen(output_path.c_str(), "w");
    if (!fp) { perror("fopen output"); exit(1); }
    fprintf(fp, "cntrycode,numcust,totacctbal\n");
    for (const auto& row : results) {
        fprintf(fp, "%s,%ld,%.2f\n", row.cntrycode, row.numcust, row.totacctbal / 100.0);
    }
    fclose(fp);
#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    printf("[TIMING] output: %.2f ms\n", std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count());
#endif
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
