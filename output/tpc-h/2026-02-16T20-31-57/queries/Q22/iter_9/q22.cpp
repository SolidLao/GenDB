/*
================================================================================
Q22 Implementation - Global Sales Opportunity (ITERATION 8: PRE-BUILT INDEX)
================================================================================

LOGICAL PLAN:
1. Load dictionaries for c_phone (to extract country codes)
2. Load pre-built hash index for orders_custkey instead of building from scratch
   - mmap orders_custkey_hash.bin directly
   - Parse binary layout: [num_unique][table_size][hash_entries][positions]
   - O(1) lookup instead of O(1) build + O(1) probe
3. Compute average acctbal for customers matching the subquery conditions
   - Filter: c_acctbal > 0 AND SUBSTRING(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
4. Filter customers and apply anti-join with orders:
   - SUBSTRING(c_phone, 1, 2) IN target set
   - c_acctbal > computed average
   - NOT IN orders (checked via pre-built index lookup)
5. Extract country code (first 2 chars of phone)
6. Group by country code and aggregate COUNT(*) and SUM(c_acctbal)
7. Order by country code
8. Output with 2 decimal places for monetary values

PHYSICAL PLAN (OPTIMIZED):
1. Load customer data via mmap
2. Load pre-built orders_custkey hash index via mmap (~50ms instead of 271ms)
   - Binary format: [uint32_t num_unique][uint32_t table_size][entries][positions]
   - Each entry: [int32_t key][uint32_t offset][uint32_t count]
3. Parallel scan for average computation:
   - Filter: acctbal > 0, country in target set
   - Compute sum and count
4. Single parallel scan of customer with all filters:
   a. Country code in target set (via dictionary lookup)
   b. acctbal > computed average (predicate pushdown)
   c. Has NO orders: O(1) lookup in pre-built hash index
5. Aggregate to hash map (7 distinct values)
6. Sort and output

KEY OPTIMIZATIONS:
- Load pre-built orders_custkey_hash index instead of building (271ms → ~50ms)
- Reduced build phase time from 45% to <10%
- Keep efficient open-addressing anti-join structure for fast contains() checks
- Better overall balance between phases

Target: ~370ms (down from 602ms baseline)

================================================================================
*/

#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <chrono>
#include <omp.h>
#include <iomanip>
#include <sstream>
#include <sys/types.h>

// Helper: Build map from phone code to country code by reading dictionary file
// OPTIMIZATION: Avoid loading entire dictionary into std::vector<std::string>
// Instead, stream through the file once and store only country codes for target prefixes
std::unordered_map<int32_t, std::string> build_phone_code_to_country(const std::string& dict_file,
                                                                      const std::unordered_set<std::string>& target_countries) {
    std::unordered_map<int32_t, std::string> phone_code_to_country;
    std::ifstream f(dict_file);
    std::string line;
    int32_t code = 0;

    while (std::getline(f, line)) {
        // Fast path: check first 2 chars directly (country code is always at position 0-1)
        if (line.length() >= 2 && line[2] == '-') {
            std::string country = line.substr(0, 2);
            if (target_countries.count(country)) {
                phone_code_to_country[code] = country;
            }
        }
        code++;
    }
    return phone_code_to_country;
}

// Helper: Extract country code (first 2 chars before hyphen) from phone string
// (Kept for reference, but we now use the pre-built map instead)
std::string extract_country_code(const std::string& phone) {
    size_t hyphen_pos = phone.find('-');
    if (hyphen_pos != std::string::npos && hyphen_pos >= 2) {
        return phone.substr(0, hyphen_pos);
    }
    return "";
}

// Mmap helper
template <typename T>
T* mmap_file(const std::string& file_path, int32_t& count) {
    int fd = open(file_path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Cannot open " << file_path << std::endl;
        return nullptr;
    }
    off_t file_size = lseek(fd, 0, SEEK_END);
    count = static_cast<int32_t>(file_size / sizeof(T));

    T* data = static_cast<T*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << file_path << std::endl;
        return nullptr;
    }
    return data;
}

// Hash index entry for multi-value hash table (from pre-built index)
// Binary layout: [int32_t key][uint32_t offset][uint32_t count]
struct HashIndexEntry {
    int32_t key;
    uint32_t offset;  // Offset into positions array
    uint32_t count;   // Number of matching positions (0 = empty slot)
};

// Pre-built multi-value hash index loaded from file
struct PrebuiltHashIndex {
    uint8_t* data;       // Raw mmap'ed data
    size_t file_size;

    uint32_t num_unique;
    uint32_t table_size;
    HashIndexEntry* hash_table;
    uint32_t* positions;

    PrebuiltHashIndex() : data(nullptr), file_size(0),
                          num_unique(0), table_size(0),
                          hash_table(nullptr), positions(nullptr) {}

    bool load(const std::string& file_path) {
        int fd = open(file_path.c_str(), O_RDONLY);
        if (fd == -1) {
            std::cerr << "Cannot open " << file_path << std::endl;
            return false;
        }

        file_size = lseek(fd, 0, SEEK_END);
        data = static_cast<uint8_t*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
        close(fd);

        if (data == MAP_FAILED) {
            std::cerr << "mmap failed for " << file_path << std::endl;
            return false;
        }

        // Parse binary layout:
        // [uint32_t num_unique][uint32_t table_size][12*table_size bytes of entries][positions...]
        size_t offset = 0;

        num_unique = *reinterpret_cast<uint32_t*>(data + offset);
        offset += sizeof(uint32_t);

        table_size = *reinterpret_cast<uint32_t*>(data + offset);
        offset += sizeof(uint32_t);

        hash_table = reinterpret_cast<HashIndexEntry*>(data + offset);
        offset += table_size * sizeof(HashIndexEntry);

        positions = reinterpret_cast<uint32_t*>(data + offset);

        return true;
    }

    // Quick hash function matching the index builder's logic
    inline uint32_t hash_fn(int32_t key) const {
        uint64_t h = static_cast<uint32_t>(key);
        h *= 0x9E3779B97F4A7C15ULL;  // Fibonacci hash constant
        return (h >> 32) % table_size;
    }

    // Linear probe through hash table to find the key
    bool contains(int32_t key) const {
        if (!hash_table) return false;

        uint32_t idx = hash_fn(key);
        for (uint32_t probe = 0; probe < table_size; probe++) {
            if (hash_table[idx].count == 0) {
                // Empty slot → key not found
                return false;
            }
            if (hash_table[idx].key == key) {
                // Key found
                return true;
            }
            idx = (idx + 1) % table_size;
        }
        return false;
    }

    ~PrebuiltHashIndex() {
        if (data != nullptr && data != reinterpret_cast<uint8_t*>(MAP_FAILED)) {
            munmap(data, file_size);
        }
    }
};

void run_q22(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    // Target country codes (7 specific codes)
    std::unordered_set<std::string> target_countries = {"13", "31", "23", "29", "30", "18", "17"};

    // Build map from phone code to country code (streaming dictionary file, not loading all strings)
    std::string dict_path = gendb_dir + "/customer/c_phone_dict.txt";
    auto phone_code_to_country = build_phone_code_to_country(dict_path, target_countries);

    // Extract the set of phone codes that match target countries
    std::unordered_set<int32_t> target_phone_codes;
    for (const auto& kv : phone_code_to_country) {
        target_phone_codes.insert(kv.first);
    }

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] dict_load: %.2f ms\n", ms);
#endif

    // Load customer data
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t cust_count = 0;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", cust_count);
    int32_t* c_phone_codes = mmap_file<int32_t>(gendb_dir + "/customer/c_phone.bin", cust_count);
    int64_t* c_acctbal = mmap_file<int64_t>(gendb_dir + "/customer/c_acctbal.bin", cust_count);

    if (!c_custkey || !c_phone_codes || !c_acctbal) {
        std::cerr << "Failed to load customer data" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", ms);
#endif

    // Step 1: Load pre-built anti-join index from orders_custkey_hash.bin
    // Optimization: Instead of building the hash set from scratch, load pre-built index via mmap
    // This eliminates the 271ms build phase
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    PrebuiltHashIndex orders_index;
    if (!orders_index.load(gendb_dir + "/indexes/orders_custkey_hash.bin")) {
        std::cerr << "Failed to load pre-built orders_custkey hash index" << std::endl;
        return;
    }

    auto has_order = [&orders_index](int32_t custkey) {
        return orders_index.contains(custkey);
    };

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_index: %.2f ms\n", ms);
#endif

    // Step 2: Compute average acctbal for target country codes with acctbal > 0 (predicate pushdown)
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int64_t sum_for_avg = 0;
    int64_t count_for_avg = 0;

    #pragma omp parallel for reduction(+:sum_for_avg,count_for_avg)
    for (int32_t i = 0; i < cust_count; i++) {
        if (target_phone_codes.count(c_phone_codes[i]) && c_acctbal[i] > 0) {
            sum_for_avg += c_acctbal[i];
            count_for_avg++;
        }
    }

    // scale_factor is 2, so average is computed in scaled domain
    int64_t avg_acctbal = (count_for_avg > 0) ? (sum_for_avg / count_for_avg) : 0;

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] compute_avg: %.2f ms\n", ms);
    printf("[DEBUG] avg_acctbal (scaled): %ld, count: %ld\n", avg_acctbal, count_for_avg);
#endif

    // Step 3: Filter customers and aggregate by country code
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // We already have the phone_code_to_country map from the dictionary load phase
    // No need to re-extract country codes - use the pre-built map directly

    // Map from country code string to (count, sum)
    std::unordered_map<std::string, std::pair<int64_t, int64_t>> aggregates;

    // Use thread-local aggregation with flat arrays (only 7 countries, so small)
    // Array indexed by country string (created lazily in each thread)
    int max_threads = omp_get_max_threads();
    std::vector<std::unordered_map<std::string, std::pair<int64_t, int64_t>>> thread_aggs(max_threads);

    #pragma omp parallel for
    for (int32_t i = 0; i < cust_count; i++) {
        int32_t phone_code = c_phone_codes[i];

        // Check all three conditions:
        // 1. Country code in target set
        // 2. acctbal > average
        // 3. Customer has NO orders (anti-join)
        if (target_phone_codes.count(phone_code) &&
            c_acctbal[i] > avg_acctbal &&
            !has_order(c_custkey[i])) {

            // Look up country code from pre-built map (O(1))
            const std::string& country = phone_code_to_country.at(phone_code);

            int tid = omp_get_thread_num();
            if (thread_aggs[tid].find(country) == thread_aggs[tid].end()) {
                thread_aggs[tid][country] = {0, 0};
            }
            thread_aggs[tid][country].first++;  // count
            thread_aggs[tid][country].second += c_acctbal[i];  // sum
        }
    }

    // Merge thread-local aggregates
    for (int t = 0; t < max_threads; t++) {
        for (const auto& kv : thread_aggs[t]) {
            if (aggregates.find(kv.first) == aggregates.end()) {
                aggregates[kv.first] = {0, 0};
            }
            aggregates[kv.first].first += kv.second.first;
            aggregates[kv.first].second += kv.second.second;
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] filter_aggregate: %.2f ms\n", ms);
#endif

    // Step 4: Output results
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Prepare output (aggregates already have country codes as keys)
    std::vector<std::tuple<std::string, int64_t, int64_t>> results;
    for (const auto& kv : aggregates) {
        results.push_back(std::make_tuple(kv.first, kv.second.first, kv.second.second));
    }

    // Sort by country code
    std::sort(results.begin(), results.end());

    // Write CSV (create directory if it doesn't exist)
    std::string output_file = results_dir + "/Q22.csv";

    // Create results directory if needed
    std::string mkdir_cmd = std::string("mkdir -p ") + results_dir;
    int ret = system(mkdir_cmd.c_str());
    (void)ret;  // Suppress warning

    std::ofstream out(output_file);
    out << "cntrycode,numcust,totacctbal\n";

    for (const auto& row : results) {
        std::string country = std::get<0>(row);
        int64_t numcust = std::get<1>(row);
        int64_t totacctbal_scaled = std::get<2>(row);

        // Convert scaled decimal to actual value (scale_factor = 100 for DECIMAL(15,2))
        double totacctbal = static_cast<double>(totacctbal_scaled) / 100.0;

        out << country << ","
            << numcust << ","
            << std::fixed << std::setprecision(2) << totacctbal << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
#else
    (void)t_total_start;  // Suppress warning if GENDB_PROFILE not defined
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
