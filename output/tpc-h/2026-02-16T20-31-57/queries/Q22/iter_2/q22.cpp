/*
================================================================================
Q22 Implementation - Global Sales Opportunity
================================================================================

LOGICAL PLAN:
1. Load dictionaries for c_phone (to extract country codes)
2. Compute average acctbal for customers matching the subquery conditions
   - Filter: c_acctbal > 0 AND SUBSTRING(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
3. Filter customers:
   - SUBSTRING(c_phone, 1, 2) IN target set
   - c_acctbal > computed average
4. Apply anti-join with orders (NOT EXISTS o_custkey = c_custkey)
   - Pre-compute order customer keys into hash set
   - Exclude customers with orders
5. Extract country code (first 2 chars of phone)
6. Group by country code and aggregate COUNT(*) and SUM(c_acctbal)
7. Order by country code
8. Output with 2 decimal places for monetary values

PHYSICAL PLAN:
1. Parallel scan of orders to build anti-join set (orders_cust_set) - large table
2. Parallel scan of customer with two filters:
   a. Dictionary lookup to extract country code from phone, compare against {'13','31','23','29','30','18','17'}
   b. Pre-filtered customers for average computation (acctbal > 0, country in target set)
3. Compute average acctbal from pre-filtered set
4. Second parallel scan of customer with three filters:
   a. Country code in target set (via dictionary lookup)
   b. acctbal > computed average
   c. NOT in orders_cust_set (anti-join via hash set)
5. Aggregate with flat array indexed by country code (7 distinct values) - very low cardinality
6. Sort output by country code and write CSV

OPTIMIZATION NOTES:
- Dictionary encoding for c_phone: Extract 2-char country codes via dictionary lookup
- Decimal scaling: c_acctbal is int64_t with scale_factor=2, thresholds computed as scaled integers
- Pre-built index (orders_custkey_hash) available but using hash set is simpler and sufficient
- Low-cardinality aggregation: Only 7 target country codes, use direct array [country_code] = {count, sum}
- Parallelism: OpenMP for large table scans, thread-local buffers for average computation
- Early exit: Dictionary codes loaded once at startup, avoid per-row string operations

================================================================================
*/

#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
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

// Helper: Load dictionary file (indexed from 0, each line is a phone number)
std::vector<std::string> load_dictionary(const std::string& dict_file) {
    std::vector<std::string> dict;
    std::ifstream f(dict_file);
    std::string line;
    while (std::getline(f, line)) {
        dict.push_back(line);
    }
    return dict;
}

// Helper: Extract country code (first 2 chars before hyphen) from phone string
std::string extract_country_code(const std::string& phone) {
    size_t hyphen_pos = phone.find('-');
    if (hyphen_pos != std::string::npos && hyphen_pos >= 2) {
        return phone.substr(0, hyphen_pos);
    }
    return "";
}

// Helper: Build set of dictionary codes that match target country codes
std::unordered_set<int32_t> build_target_codes(const std::vector<std::string>& dict,
                                                const std::unordered_set<std::string>& target_countries) {
    std::unordered_set<int32_t> target_codes;
    target_codes.reserve(dict.size() / 4);  // Rough estimate of matching codes

    for (size_t i = 0; i < dict.size(); i++) {
        // Fast path: check first 2 chars directly before string extraction
        if (dict[i].length() >= 2) {
            char c1 = dict[i][0];
            char c2 = dict[i][1];
            if (dict[i][2] == '-') {  // Ensure hyphen at position 2
                std::string country(2, ' ');
                country[0] = c1;
                country[1] = c2;
                if (target_countries.count(country)) {
                    target_codes.insert(static_cast<int32_t>(i));
                }
            }
        }
    }
    return target_codes;
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

void run_q22(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    // Load dictionaries
    std::string dict_path = gendb_dir + "/customer/c_phone_dict.txt";
    auto phone_dict = load_dictionary(dict_path);

    // Target country codes
    std::unordered_set<std::string> target_countries = {"13", "31", "23", "29", "30", "18", "17"};

    // Build set of dictionary codes for target countries
    std::unordered_set<int32_t> target_phone_codes = build_target_codes(phone_dict, target_countries);

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] dict_load: %.2f ms\n", ms);
#endif

    // Get thread count for later use
    int num_threads = omp_get_max_threads();

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

    // Load orders data
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t orders_count = 0;
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);

    if (!o_custkey) {
        std::cerr << "Failed to load orders data" << std::endl;
        return;
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms);
#endif

    // Step 1: Load pre-built multi-value hash index for orders (customer keys that have orders)
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    // Load pre-built hash index: indexes/orders_custkey_hash.bin
    // Layout: [uint32_t num_unique][uint32_t table_size][hash_entries...][positions_array...]
    // Each hash entry: [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot)
    std::string orders_index_path = gendb_dir + "/indexes/orders_custkey_hash.bin";
    int orders_index_fd = open(orders_index_path.c_str(), O_RDONLY);

    // Fallback: if pre-built index not available, build hash set from scratch
    std::unordered_set<int32_t> orders_keys_set;
    bool using_prebuilt_index = false;

    if (orders_index_fd != -1) {
        // Pre-built index available - load it
        off_t index_file_size = lseek(orders_index_fd, 0, SEEK_END);
        uint8_t* index_data = static_cast<uint8_t*>(mmap(nullptr, index_file_size, PROT_READ, MAP_PRIVATE, orders_index_fd, 0));
        close(orders_index_fd);

        if (index_data != MAP_FAILED) {
            using_prebuilt_index = true;
            // Read header
            uint32_t* header = reinterpret_cast<uint32_t*>(index_data);
            uint32_t num_unique = header[0];
            uint32_t table_size = header[1];

            // Hash table entries are at offset 8 bytes
            struct HashEntry {
                int32_t key;
                uint32_t offset;
                uint32_t count;
            };

            HashEntry* hash_table = reinterpret_cast<HashEntry*>(index_data + 8);

            // Build set from hash table keys (all unique customer keys that have orders)
            orders_keys_set.reserve(num_unique);
            for (uint32_t i = 0; i < table_size; i++) {
                if (hash_table[i].count > 0) {  // Valid entry
                    orders_keys_set.insert(hash_table[i].key);
                }
            }

            munmap(index_data, index_file_size);
        }
    }

    // If pre-built index not available, build from orders table
    if (!using_prebuilt_index) {
        orders_keys_set.reserve(orders_count / 10);  // Most customers have multiple orders
        for (int32_t i = 0; i < orders_count; i++) {
            orders_keys_set.insert(o_custkey[i]);
        }
    }

    // Lambda for O(1) hash set lookup
    auto has_order = [&orders_keys_set](int32_t custkey) {
        return orders_keys_set.count(custkey) > 0;
    };

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_antijoin: %.2f ms\n", ms);
#endif

    // Step 2: Compute average acctbal for target country codes with acctbal > 0
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

    // Build mapping from phone code to country string (only 7 target codes)
    // Also create a compact map: phone_code -> array index [0..6]
    std::vector<std::string> country_names;
    std::unordered_map<int32_t, int32_t> phone_code_to_array_idx;
    int32_t array_idx = 0;

    for (int32_t code : target_phone_codes) {
        std::string country = extract_country_code(phone_dict[code]);
        country_names.push_back(country);
        phone_code_to_array_idx[code] = array_idx++;
    }

    // Thread-local aggregation using flat arrays (7 countries × 2 int64_t = 112 bytes per thread)
    // Much faster than hash table lookups
    std::vector<std::vector<std::pair<int64_t, int64_t>>> thread_aggs(num_threads);
    for (int t = 0; t < num_threads; t++) {
        thread_aggs[t].resize(country_names.size(), {0, 0});
    }

    #pragma omp parallel for
    for (int32_t i = 0; i < cust_count; i++) {
        int32_t phone_code = c_phone_codes[i];

        // Fast path: check if phone code is in target set
        auto it = phone_code_to_array_idx.find(phone_code);
        if (it != phone_code_to_array_idx.end()) {
            // Check remaining conditions:
            // 1. acctbal > average
            // 2. Customer has NO orders (anti-join)
            if (c_acctbal[i] > avg_acctbal && !has_order(c_custkey[i])) {
                int32_t agg_idx = it->second;
                int tid = omp_get_thread_num();
                thread_aggs[tid][agg_idx].first++;  // count
                thread_aggs[tid][agg_idx].second += c_acctbal[i];  // sum
            }
        }
    }

    // Merge thread-local aggregates into final map
    std::unordered_map<std::string, std::pair<int64_t, int64_t>> aggregates;
    for (int32_t idx = 0; idx < static_cast<int32_t>(country_names.size()); idx++) {
        aggregates[country_names[idx]] = {0, 0};
        for (int t = 0; t < num_threads; t++) {
            aggregates[country_names[idx]].first += thread_aggs[t][idx].first;
            aggregates[country_names[idx]].second += thread_aggs[t][idx].second;
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
