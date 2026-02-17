/*
================================================================================
Q22 Implementation - Global Sales Opportunity (ITERATION 6: LOAD PRE-BUILT INDEX)
================================================================================

LOGICAL PLAN:
1. Load dictionaries for c_phone (to extract country codes)
2. Load pre-built multi-value hash index for orders (o_custkey) via mmap → O(1) existence checks
3. Compute average acctbal for customers matching the subquery conditions
   - Filter: c_acctbal > 0 AND SUBSTRING(c_phone, 1, 2) IN ('13','31','23','29','30','18','17')
4. Filter customers and apply anti-join with orders:
   - SUBSTRING(c_phone, 1, 2) IN target set
   - c_acctbal > computed average
   - NOT IN orders (checked via pre-built hash index)
5. Extract country code (first 2 chars of phone)
6. Group by country code and aggregate COUNT(*) and SUM(c_acctbal)
7. Order by country code
8. Output with 2 decimal places for monetary values

PHYSICAL PLAN (OPTIMIZED):
1. Load pre-built orders_custkey_hash index via mmap (eliminates 391ms build time!)
   - Index structure: [num_unique][table_size][hash_table...][positions_array...]
   - Multi-value design: one entry per unique key, with offset + count into positions array
2. Parallel scan of customer for average computation:
   - Filter: acctbal > 0, country in target set
   - Compute sum and count
3. Single parallel scan of customer with all filters:
   a. Country code in target set (via dictionary lookup)
   b. acctbal > computed average (predicate pushdown)
   c. Has NO orders: O(1) lookup in pre-built hash index
4. Aggregate to flat array indexed by country code (7 distinct values)
5. Sort and output

KEY OPTIMIZATION:
- Pre-built hash index eliminates 391ms (49% of total time) by loading via mmap instead of building
- Index binary format: [num_unique][table_size][12B entries][positions...]
- Load factor ~0.5 (1M unique / 2M slots) enables fast lookups
- Single scan through customer table (1.5M rows) for aggregation
- Predicate pushdown: filter by country/balance before checking orders

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
#include <sys/types.h>

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

// Helper: Fast country code lookup table (only 7 target countries)
// Avoid unordered_set overhead for just 7 values
struct FastTargetLookup {
    bool valid[256];  // One byte per country code pair (00-99)

    FastTargetLookup(const std::unordered_set<std::string>& target_countries) {
        std::memset(valid, false, sizeof(valid));
        for (const auto& country : target_countries) {
            if (country.length() == 2) {
                // Convert country code string to index: "13" -> 13, "31" -> 31, etc.
                int idx = (country[0] - '0') * 10 + (country[1] - '0');
                if (idx >= 0 && idx < 256) {
                    valid[idx] = true;
                }
            }
        }
    }

    inline bool is_target(int32_t dict_code, const std::vector<std::string>& dict) const {
        if (dict_code < 0 || dict_code >= static_cast<int32_t>(dict.size())) {
            return false;
        }
        const std::string& phone = dict[dict_code];
        if (phone.length() < 3 || phone[2] != '-') {
            return false;
        }
        int idx = (phone[0] - '0') * 10 + (phone[1] - '0');
        return idx >= 0 && idx < 256 && valid[idx];
    }
};

// Helper: Build set of dictionary codes that match target country codes
std::unordered_set<int32_t> build_target_codes(const std::vector<std::string>& dict,
                                                const std::unordered_set<std::string>& target_countries) {
    std::unordered_set<int32_t> target_codes;
    target_codes.reserve(dict.size() / 4);  // Rough estimate of matching codes

    for (size_t i = 0; i < dict.size(); i++) {
        // Fast path: check first 2 chars directly before string extraction
        if (dict[i].length() >= 3 && dict[i][2] == '-') {
            char c1 = dict[i][0];
            char c2 = dict[i][1];
            std::string country(2, ' ');
            country[0] = c1;
            country[1] = c2;
            if (target_countries.count(country)) {
                target_codes.insert(static_cast<int32_t>(i));
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

// Pre-built multi-value hash index structure (loaded from binary file)
struct PreBuiltOrdersIndex {
    struct HashEntry {
        int32_t key;
        uint32_t offset;
        uint32_t count;
    };

    HashEntry* hash_table;
    uint32_t* positions;
    uint32_t table_size;
    uint32_t positions_count;
    size_t mask;
    int fd;
    void* mmap_ptr;
    size_t mmap_size;

    PreBuiltOrdersIndex() : hash_table(nullptr), positions(nullptr), table_size(0),
                            positions_count(0), mask(0), fd(-1), mmap_ptr(nullptr), mmap_size(0) {}

    // Load pre-built index from binary file
    bool load(const std::string& index_file) {
        fd = open(index_file.c_str(), O_RDONLY);
        if (fd == -1) {
            std::cerr << "Cannot open index file: " << index_file << std::endl;
            return false;
        }

        off_t file_size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);
        mmap_size = static_cast<size_t>(file_size);

        mmap_ptr = mmap(nullptr, mmap_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mmap_ptr == MAP_FAILED) {
            std::cerr << "mmap failed for index file" << std::endl;
            close(fd);
            return false;
        }

        // Parse header
        uint32_t* header = static_cast<uint32_t*>(mmap_ptr);
        table_size = header[1];
        mask = table_size - 1;

        // Hash table starts at offset 8 bytes
        hash_table = reinterpret_cast<HashEntry*>(static_cast<char*>(mmap_ptr) + 8);

        // Positions array starts after hash table
        uint32_t* pos_array_start = reinterpret_cast<uint32_t*>(
            static_cast<char*>(mmap_ptr) + 8 + table_size * sizeof(HashEntry)
        );
        positions_count = pos_array_start[0];
        positions = pos_array_start + 1;

        return true;
    }

    // Lookup: O(1) average case, returns count of matching order positions
    inline uint32_t get_count(int32_t custkey) const {
        // Hash the customer key
        uint64_t h = custkey;
        h ^= h >> 16;
        h *= 0x7feb352d;
        h ^= h >> 15;
        size_t idx = h & mask;

        // Linear probe to find entry
        while (hash_table[idx].key != -1) {
            if (hash_table[idx].key == custkey) {
                return hash_table[idx].count;
            }
            idx = (idx + 1) & mask;
        }
        return 0;  // Not found
    }

    // Check if customer has any orders
    inline bool has_orders(int32_t custkey) const {
        return get_count(custkey) > 0;
    }

    ~PreBuiltOrdersIndex() {
        if (mmap_ptr != nullptr && mmap_ptr != MAP_FAILED) {
            munmap(mmap_ptr, mmap_size);
        }
        if (fd != -1) {
            close(fd);
        }
    }
};

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

    // Get thread count for later use (kept for reference)
    // int num_threads = omp_get_max_threads();

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

    // Step 1: Load pre-built multi-value hash index for orders
    // OPTIMIZATION: Instead of building from scratch (391ms), load via mmap (1ms)
    // Index has O(1) lookups, multi-value design for duplicate keys
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    PreBuiltOrdersIndex orders_index;
    std::string index_path = gendb_dir + "/indexes/orders_custkey_hash.bin";
    if (!orders_index.load(index_path)) {
        std::cerr << "Failed to load pre-built orders index from: " << index_path << std::endl;
        // Fallback to building from scratch if index not found
        std::cerr << "WARNING: Pre-built index not found, would need fallback" << std::endl;
        return;
    }

    auto has_order = [&orders_index](int32_t custkey) {
        return orders_index.has_orders(custkey);
    };

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] build_antijoin: %.2f ms\n", ms);
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

    // Build mapping from dictionary code to country string (only 7 codes)
    // Use a compact array indexed by dictionary code for O(1) lookup
    std::unordered_map<int32_t, std::string> target_code_to_country;
    for (int32_t code : target_phone_codes) {
        std::string country = extract_country_code(phone_dict[code]);
        target_code_to_country[code] = country;
    }

    // Map from country code string to (count, sum)
    std::unordered_map<std::string, std::pair<int64_t, int64_t>> aggregates;

    // Use thread-local aggregation with small maps (only 7 countries)
    int actual_num_threads = omp_get_max_threads();
    std::vector<std::unordered_map<std::string, std::pair<int64_t, int64_t>>> thread_aggs(actual_num_threads);

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

            // Look up country code from pre-extracted map (O(1))
            const std::string& country = target_code_to_country.at(phone_code);

            int tid = omp_get_thread_num();
            if (thread_aggs[tid].find(country) == thread_aggs[tid].end()) {
                thread_aggs[tid][country] = {0, 0};
            }
            thread_aggs[tid][country].first++;  // count
            thread_aggs[tid][country].second += c_acctbal[i];  // sum
        }
    }

    // Merge thread-local aggregates
    for (int t = 0; t < actual_num_threads; t++) {
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
