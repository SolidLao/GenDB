#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <cmath>
#include <algorithm>
#include <omp.h>

// ============================================================================
// Date helper
// ============================================================================

int32_t date_to_epoch_days(int year, int month, int day) {
    const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    int32_t days = 0;

    // Add days for complete years since 1970
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Add days for complete months in this year
    bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    for (int m = 1; m < month; m++) {
        days += days_in_month[m-1];
        if (m == 2 && is_leap) days += 1;
    }

    // Add remaining days (day is 1-indexed, epoch days are 0-indexed)
    days += (day - 1);

    return days;
}

void epoch_days_to_date(int32_t days, int& year, int& month, int& day) {
    const int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    year = 1970;
    while (true) {
        bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
        int days_in_year = is_leap ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }

    month = 1;
    bool is_leap = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0));
    for (int m = 1; m <= 12; m++) {
        int dim = days_in_month[m-1];
        if (m == 2 && is_leap) dim = 29;
        if (days < dim) break;
        days -= dim;
        month++;
    }

    day = days + 1;
}

// ============================================================================
// Flat array lookup for direct indexing (no hashing needed)
// ============================================================================

template<typename T>
struct FlatLookup {
    std::vector<T> data;

    FlatLookup(size_t max_key) {
        // Allocate array with space for [0, max_key]
        // Index 0 is unused; [1, max_key] store values
        data.resize(max_key + 1);
    }

    void insert(size_t key, T value) {
        if (key < data.size()) {
            data[key] = value;
        }
    }

    T* find(size_t key) {
        if (key > 0 && key < data.size()) {
            return &data[key];
        }
        return nullptr;
    }
};

// ============================================================================
// Utility: mmap file
// ============================================================================

template<typename T>
const T* mmap_file(const std::string& path, size_t& num_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }

    struct stat st;
    fstat(fd, &st);
    size_t file_size = st.st_size;
    num_elements = file_size / sizeof(T);

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }

    return (const T*)ptr;
}

// ============================================================================
// Load dictionary for p_type
// ============================================================================

std::unordered_map<int16_t, std::string> load_dictionary(const std::string& dict_path) {
    std::unordered_map<int16_t, std::string> dict;
    std::ifstream f(dict_path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        int16_t code = std::stoi(line.substr(0, eq));
        std::string value = line.substr(eq + 1);
        dict[code] = value;
    }
    return dict;
}

// ============================================================================
// Check if string starts with prefix (dictionary decoded)
// ============================================================================

bool starts_with(const std::string& str, const std::string& prefix) {
    return str.size() >= prefix.size() && str.substr(0, prefix.size()) == prefix;
}


// ============================================================================
// Main query execution
// ============================================================================

void run_Q14(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Date constants for 1995-09-01 to 1995-10-01
    const int32_t date_start = 9374;   // 1995-09-01
    const int32_t date_end = 9404;     // 1995-10-01

    // ========================================================================
    // Load lineitem data
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t lineitem_rows = 0;
    const int32_t* l_partkey_ptr = mmap_file<int32_t>(
        gendb_dir + "/lineitem/l_partkey.bin", lineitem_rows);
    const int64_t* l_extendedprice_ptr = mmap_file<int64_t>(
        gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_rows);
    const int64_t* l_discount_ptr = mmap_file<int64_t>(
        gendb_dir + "/lineitem/l_discount.bin", lineitem_rows);
    const int32_t* l_shipdate_ptr = mmap_file<int32_t>(
        gendb_dir + "/lineitem/l_shipdate.bin", lineitem_rows);

    if (!l_partkey_ptr || !l_extendedprice_ptr || !l_discount_ptr || !l_shipdate_ptr) {
        std::cerr << "Failed to load lineitem data" << std::endl;
        return;
    }

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // Load part data
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_part_load_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t part_rows = 0;
    const int32_t* p_partkey_ptr = mmap_file<int32_t>(
        gendb_dir + "/part/p_partkey.bin", part_rows);
    const int16_t* p_type_codes_ptr = mmap_file<int16_t>(
        gendb_dir + "/part/p_type.bin", part_rows);

    if (!p_partkey_ptr || !p_type_codes_ptr) {
        std::cerr << "Failed to load part data" << std::endl;
        return;
    }

    // Load dictionary for p_type
    auto p_type_dict = load_dictionary(gendb_dir + "/part/p_type_dict.txt");

    #ifdef GENDB_PROFILE
    auto t_part_load_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_part_load_end - t_part_load_start).count();
    printf("[TIMING] load_part: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // Pre-compute PROMO type codes (avoid repeated dictionary lookups)
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_part_hash_start = std::chrono::high_resolution_clock::now();
    #endif

    // Pre-compute which type codes start with "PROMO" - this is a fast array lookup
    // instead of hash map + string comparison in the hot loop
    std::vector<bool> is_promo_type(65536, false);  // int16_t range [0, 65535]
    for (auto& entry : p_type_dict) {
        if (starts_with(entry.second, "PROMO")) {
            is_promo_type[entry.first & 0xFFFF] = true;
        }
    }

    // ========================================================================
    // Build flat array lookup for part (on p_partkey)
    // Partkey ranges [1, 2000000], so flat array is faster than hash table
    // ========================================================================

    FlatLookup<int16_t> part_type_map(2000001);  // max_key = 2000000

    // Sequential insertion into flat array - O(1) per insert, no hash computation
    for (size_t i = 0; i < part_rows; i++) {
        part_type_map.insert(p_partkey_ptr[i], p_type_codes_ptr[i]);
    }

    #ifdef GENDB_PROFILE
    auto t_part_hash_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_part_hash_end - t_part_hash_start).count();
    printf("[TIMING] build_part_hash: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // Scan and aggregate lineitem
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    // Thread-local accumulators
    int num_threads = omp_get_max_threads();
    std::vector<int64_t> thread_promo_revenue(num_threads, 0);
    std::vector<int64_t> thread_total_revenue(num_threads, 0);

    // Parallel scan with optimized inner loop
    #pragma omp parallel for schedule(dynamic, 100000)
    for (size_t i = 0; i < lineitem_rows; i++) {
        // Filter by date (early exit, good branch prediction)
        int32_t shipdate = l_shipdate_ptr[i];
        if (shipdate < date_start || shipdate >= date_end) {
            continue;
        }

        // Get thread ID once
        int tid = omp_get_thread_num();

        // Get revenue for this line: extendedprice * (1 - discount)
        // Both are scaled by 100, so product is scaled by 10000
        // We need to scale back by dividing by 100 to get scale factor 100
        int64_t extended = l_extendedprice_ptr[i];
        int64_t discount = l_discount_ptr[i];  // scaled by 100

        // revenue = extended * (1 - discount/100)
        //         = extended * (100 - discount) / 100
        // Keep scaled by 100 for integer arithmetic
        int64_t revenue = extended * (100 - discount) / 100;

        // Add to total revenue
        thread_total_revenue[tid] += revenue;

        // Check if this is a PROMO part
        int32_t partkey = l_partkey_ptr[i];
        int16_t* type_code_ptr = part_type_map.find(partkey);

        // Branchless promo check using pre-computed lookup table
        if (__builtin_expect(type_code_ptr != nullptr, 1)) {
            int16_t type_code = *type_code_ptr;
            // Use pre-computed PROMO lookup (O(1) array access, no dictionary lookup)
            if (__builtin_expect(is_promo_type[type_code & 0xFFFF], 0)) {
                thread_promo_revenue[tid] += revenue;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_join: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // Merge thread-local results
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t total_promo_revenue = 0;
    int64_t total_revenue = 0;

    for (int i = 0; i < num_threads; i++) {
        total_promo_revenue += thread_promo_revenue[i];
        total_revenue += thread_total_revenue[i];
    }

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] merge: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // Compute final result
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_compute_start = std::chrono::high_resolution_clock::now();
    #endif

    double promo_percentage = 0.0;
    if (total_revenue > 0) {
        // Both are scaled by 100, so ratio is correct
        // Multiply by 100 to get percentage, then divide by scale
        promo_percentage = (100.0 * total_promo_revenue) / total_revenue;
    }

    #ifdef GENDB_PROFILE
    auto t_compute_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_compute_end - t_compute_start).count();
    printf("[TIMING] compute: %.2f ms\n", ms);
    #endif

    // ========================================================================
    // Write output
    // ========================================================================

    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q14.csv";
    std::ofstream out(output_path);

    // Write header
    out << "promo_revenue\n";

    // Write result (2 decimal places for monetary value)
    char buf[64];
    snprintf(buf, sizeof(buf), "%.2f\n", promo_percentage);
    out << buf;

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms);
    #endif

    std::cout << "Q14 execution complete. Result written to " << output_path << std::endl;
}

// ============================================================================
// Main entry point
// ============================================================================

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_Q14(gendb_dir, results_dir);

    return 0;
}
#endif
