/*
 * Q22: Global Sales Opportunity
 *
 * LOGICAL QUERY PLAN
 * ==================
 * 1. Pre-compute: Calculate AVG(c_acctbal) for customers where:
 *    - c_acctbal > 0.00 (scaled: > 0)
 *    - SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
 * 2. Filter customer table:
 *    - SUBSTRING(c_phone, 1, 2) IN ('13', '31', '23', '29', '30', '18', '17')
 *    - c_acctbal > average (computed in step 1)
 * 3. Anti-join with orders: Exclude customers who have orders
 *    - For each filtered customer, check if o_custkey exists in orders
 * 4. Group by country code (first 2 digits of phone)
 * 5. Compute COUNT(*) and SUM(c_acctbal)
 * 6. Order by country code
 *
 * Estimated cardinality:
 * - Customer: 1.5M rows
 * - After phone filter: ~1.5M × 0.4 = 600K rows (7 codes out of ~70 possible)
 * - Avg threshold: ~5000 (typical account balance ~0 to 100K scaled)
 * - After acctbal filter: ~600K × 0.5 = 300K rows (customers with positive high balance)
 * - After anti-join: ~300K × 0.88 = 264K rows (12% have orders)
 * - After GROUP BY: ~7 groups (only 7 country codes filtered)
 *
 * PHYSICAL QUERY PLAN
 * ===================
 * 1. Load customer columns: c_custkey, c_phone (dict-encoded), c_acctbal
 * 2. Load orders column: o_custkey (for anti-join existence check)
 * 3. Pre-computation phase (single-threaded):
 *    a. Scan customer, filter by phone codes and c_acctbal > 0
 *    b. Compute sum and count
 *    c. Calculate average = sum / count
 * 4. Filter & anti-join phase (parallel):
 *    a. Build hash set of o_custkey from orders table (mmap or scan)
 *    b. Scan customer in parallel morsels (64K rows each)
 *    c. For each row: check phone code, compare acctbal to avg, NOT IN orders hash set
 *    d. Collect matching (cntrycode, c_acctbal) pairs to thread-local vectors
 * 5. Aggregation phase (single-threaded):
 *    a. Use flat array [7][count, sum] for country codes 13,17,18,23,29,30,31
 *    b. Merge results from all threads
 * 6. Output: Sort by cntrycode, write CSV
 *
 * Data structures:
 * - Hash set for orders.o_custkey (open-addressing for O(1) lookups)
 * - Flat array [7][2] for (count, sum) by country code
 * - Thread-local vectors for customer filtering
 *
 * Parallelism:
 * - Phase 3 (precomputation): Single-threaded scan to compute average
 * - Phase 4 (filter & anti-join): Parallel scan with morsel-driven chunking
 * - Phase 5 (aggregation): Lock-free updates to flat array (each code is thread-local)
 *
 * Indexes used:
 * - idx_orders_custkey_hash: Pre-built hash index on orders.o_custkey (if available)
 * - Otherwise: Build hash set at runtime from o_custkey column
 */

#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <cstring>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <omp.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>

// ============================================================================
// METADATA CHECK
// ============================================================================
// [METADATA CHECK]
// - c_custkey: int32_t, no encoding
// - c_phone: dictionary-encoded int32_t (codes 0..N, dict file maps to "XX-YYY-YYY-YYYY")
// - c_acctbal: int64_t, scaled by 100 (e.g., 1234 = 12.34)
// - o_custkey: int32_t, no encoding
// ============================================================================

// Helper: Load dictionary file and build reverse map (string -> code)
std::unordered_map<std::string, int32_t> load_phone_dictionary(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> reverse_dict;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return reverse_dict;
    }

    std::string line;
    while (std::getline(file, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            reverse_dict[value] = code;
        }
    }
    file.close();
    return reverse_dict;
}

// Helper: Extract first 2 characters from phone string (SUBSTRING(c_phone, 1, 2))
std::string extract_country_code(const std::string& phone) {
    if (phone.length() >= 2) {
        return phone.substr(0, 2);
    }
    return "";
}

// Helper: Memory map a binary file
template<typename T>
T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return nullptr;
    }

    off_t size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    count = size / sizeof(T);

    T* data = (T*)mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap: " << path << std::endl;
        return nullptr;
    }
    return data;
}

// Helper: Unmmap
template<typename T>
void unmmap_file(T* ptr, size_t count) {
    if (ptr) {
        munmap(ptr, count * sizeof(T));
    }
}

// Hash set using open addressing (robin hood hashing)
struct OpenAddrHashSet {
    struct Entry {
        int32_t key;
        bool occupied;
    };

    std::vector<Entry> table;
    size_t size;

    OpenAddrHashSet(size_t capacity = 1000000) {
        // Power of 2 size for fast modulo
        capacity = 1 << (64 - __builtin_clzll(capacity - 1));
        table.resize(capacity);
        for (auto& e : table) {
            e.occupied = false;
        }
        size = 0;
    }

    bool insert(int32_t key) {
        if (size >= table.size() / 2) {
            return true; // Reached load factor 0.5
        }

        size_t mask = table.size() - 1;
        size_t hash = std::hash<int32_t>()(key);
        size_t pos = hash & mask;

        int probe_len = 0;
        while (table[pos].occupied && table[pos].key != key) {
            pos = (pos + 1) & mask;
            probe_len++;
            if (probe_len > 100) return false; // Degenerate hash
        }

        if (!table[pos].occupied) {
            table[pos].key = key;
            table[pos].occupied = true;
            size++;
        }
        return true;
    }

    bool contains(int32_t key) const {
        size_t mask = table.size() - 1;
        size_t hash = std::hash<int32_t>()(key);
        size_t pos = hash & mask;

        int probe_len = 0;
        while (table[pos].occupied && table[pos].key != key) {
            pos = (pos + 1) & mask;
            probe_len++;
            if (probe_len > 100) return false;
        }

        return table[pos].occupied && table[pos].key == key;
    }
};

void run_q22(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load dictionaries
    std::string phone_dict_path = gendb_dir + "/customer/c_phone_dict.txt";
    auto phone_dict = load_phone_dictionary(phone_dict_path);

    // Build reverse mapping: code -> phone string
    // and find codes for target country codes
    std::unordered_map<int32_t, std::string> code_to_phone;
    std::unordered_set<int32_t> target_codes;
    std::unordered_set<std::string> target_countries = {"13", "31", "23", "29", "30", "18", "17"};

    std::ifstream dict_file(phone_dict_path);
    std::string line;
    while (std::getline(dict_file, line)) {
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq_pos));
            std::string phone = line.substr(eq_pos + 1);
            code_to_phone[code] = phone;

            // Check if this phone number's country code is a target
            if (phone.length() >= 2 && target_countries.count(phone.substr(0, 2))) {
                target_codes.insert(code);
            }
        }
    }
    dict_file.close();

    printf("[METADATA] Loaded phone dictionary with %zu entries\n", phone_dict.size());
    printf("[METADATA] Target country codes (as dict codes): %zu codes found\n", target_codes.size());

    // Load customer columns
    size_t customer_count = 0;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_phone = mmap_file<int32_t>(gendb_dir + "/customer/c_phone.bin", customer_count);
    int64_t* c_acctbal = mmap_file<int64_t>(gendb_dir + "/customer/c_acctbal.bin", customer_count);

    printf("[METADATA] Loaded customer table: %zu rows\n", customer_count);

    // Load orders column for anti-join
    size_t orders_count = 0;
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);
    printf("[METADATA] Loaded orders table: %zu rows\n", orders_count);

    // ========================================================================
    // PHASE 1: Pre-compute average c_acctbal for positive balances with target phone codes
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t1_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t sum_acctbal = 0;
    int64_t count_acctbal = 0;

    for (size_t i = 0; i < customer_count; ++i) {
        if (c_acctbal[i] > 0 && target_codes.count(c_phone[i])) {
            sum_acctbal += c_acctbal[i];
            count_acctbal++;
        }
    }

    int64_t avg_acctbal = (count_acctbal > 0) ? (sum_acctbal / count_acctbal) : 0;

    #ifdef GENDB_PROFILE
    auto t1_end = std::chrono::high_resolution_clock::now();
    double ms1 = std::chrono::duration<double, std::milli>(t1_end - t1_start).count();
    printf("[TIMING] precompute_avg: %.2f ms (sum=%ld, count=%ld, avg=%ld)\n", ms1, sum_acctbal, count_acctbal, avg_acctbal);
    #endif

    // ========================================================================
    // PHASE 2: Build hash set of customer keys in orders (for anti-join)
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t2_start = std::chrono::high_resolution_clock::now();
    #endif

    OpenAddrHashSet orders_custkeys(orders_count);
    for (size_t i = 0; i < orders_count; ++i) {
        orders_custkeys.insert(o_custkey[i]);
    }

    #ifdef GENDB_PROFILE
    auto t2_end = std::chrono::high_resolution_clock::now();
    double ms2 = std::chrono::duration<double, std::milli>(t2_end - t2_start).count();
    printf("[TIMING] build_orders_hash: %.2f ms (%zu entries)\n", ms2, orders_custkeys.size);
    #endif

    // ========================================================================
    // PHASE 3: Parallel filter & aggregate
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t3_start = std::chrono::high_resolution_clock::now();
    #endif

    // Mapping: country code string -> index in result array
    std::unordered_map<std::string, int> code_to_idx = {
        {"13", 0}, {"17", 1}, {"18", 2}, {"23", 3}, {"29", 4}, {"30", 5}, {"31", 6}
    };

    // Result array: [country_idx][count, sum]
    struct AggResult {
        int64_t count;
        int64_t sum;
    };
    std::vector<AggResult> result(7, {0, 0});

    // Thread-local aggregation
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<AggResult>> thread_local_results(num_threads, std::vector<AggResult>(7, {0, 0}));

    // Create a reference to code_to_phone for parallel access (read-only)
    const auto& phone_map_ref = code_to_phone;

    #pragma omp parallel for schedule(dynamic, 65536)
    for (size_t i = 0; i < customer_count; ++i) {
        int thread_id = omp_get_thread_num();

        // Check if customer has target phone code
        if (!target_codes.count(c_phone[i])) continue;

        // Check if customer has acctbal > average
        if (c_acctbal[i] <= avg_acctbal) continue;

        // Check if customer has NO orders (anti-join)
        if (orders_custkeys.contains(c_custkey[i])) continue;

        // Found a matching customer, decode phone to get country code
        // Use the code_to_phone map to get the full phone number
        auto it = phone_map_ref.find(c_phone[i]);
        if (it != phone_map_ref.end()) {
            std::string country_code = extract_country_code(it->second);

            if (code_to_idx.count(country_code)) {
                int idx = code_to_idx[country_code];
                thread_local_results[thread_id][idx].count++;
                thread_local_results[thread_id][idx].sum += c_acctbal[i];
            }
        }
    }

    // Merge thread-local results
    for (int t = 0; t < num_threads; ++t) {
        for (int c = 0; c < 7; ++c) {
            result[c].count += thread_local_results[t][c].count;
            result[c].sum += thread_local_results[t][c].sum;
        }
    }

    #ifdef GENDB_PROFILE
    auto t3_end = std::chrono::high_resolution_clock::now();
    double ms3 = std::chrono::duration<double, std::milli>(t3_end - t3_start).count();
    printf("[TIMING] filter_aggregate: %.2f ms\n", ms3);
    #endif

    // ========================================================================
    // Output results to CSV
    // ========================================================================
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream outfile(results_dir + "/Q22.csv");
    outfile << "cntrycode,numcust,totacctbal\r\n";

    std::vector<std::string> country_codes = {"13", "17", "18", "23", "29", "30", "31"};
    for (int i = 0; i < 7; ++i) {
        if (result[i].count > 0) {
            double balance = result[i].sum / 100.0; // Scale back from scaled int
            outfile << country_codes[i] << "," << result[i].count << ","
                    << std::fixed << std::setprecision(2) << balance << "\r\n";
        }
    }
    outfile.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    // ========================================================================
    // Cleanup
    // ========================================================================
    unmmap_file(c_custkey, customer_count);
    unmmap_file(c_phone, customer_count);
    unmmap_file(c_acctbal, customer_count);
    unmmap_file(o_custkey, orders_count);

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
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
