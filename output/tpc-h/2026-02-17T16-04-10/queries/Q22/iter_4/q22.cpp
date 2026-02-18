#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_set>
#include <unordered_map>
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
#include <omp.h>
#include <memory>

// Fixed width for string field (4-byte length prefix + variable data)
static constexpr size_t STRING_FIELD_WIDTH = 19;
static constexpr size_t MAX_CUSTKEY = 1500001;  // Customer keys are 1-based, max 1500000

// Hash index structure (multi-value hash for o_custkey)
// Format: [hash_table_size:uint32_t][entry_count:uint32_t][entries...]
// Each entry: [custkey:int32_t][next_offset:uint32_t] (linked list chain in hash table)
struct HashIndexEntry {
    int32_t custkey;
    uint32_t next_offset;  // offset to next entry in chain, or 0 for end
};

// Simple hash index reader (multi-value, collision chains)
class HashIndexReader {
private:
    const char* data;
    size_t data_size;
    uint32_t table_size;
    uint32_t entry_count;
    const uint32_t* hash_table;  // array of offsets into entry pool
    const HashIndexEntry* entries;  // pool of entries

public:
    HashIndexReader(const char* mmap_data, size_t size)
        : data(mmap_data), data_size(size) {
        if (size < 8) {
            throw std::runtime_error("Hash index too small");
        }
        // Read header
        memcpy(&table_size, data, sizeof(uint32_t));
        memcpy(&entry_count, data + 4, sizeof(uint32_t));

        // Validate
        if (table_size == 0 || entry_count == 0) {
            throw std::runtime_error("Invalid hash index header");
        }

        // Layout: [uint32_t table_size][uint32_t entry_count][hash_table][entries]
        hash_table = reinterpret_cast<const uint32_t*>(data + 8);
        entries = reinterpret_cast<const HashIndexEntry*>(
            data + 8 + (table_size * sizeof(uint32_t))
        );
    }

    // Returns true if custkey exists in the index (for NOT EXISTS check)
    bool contains(int32_t custkey) const {
        if (custkey <= 0 || custkey >= (int32_t)MAX_CUSTKEY) {
            return false;
        }

        uint32_t hash_val = (uint32_t)custkey % table_size;
        uint32_t offset = hash_table[hash_val];

        if (offset == 0) {
            return false;  // Empty slot
        }

        // Follow collision chain
        while (offset != 0) {
            const HashIndexEntry* entry = &entries[offset - 1];  // offsets are 1-based
            if (entry->custkey == custkey) {
                return true;
            }
            offset = entry->next_offset;
        }

        return false;
    }
};


struct Result {
    std::string cntrycode;
    int64_t numcust;
    int64_t totacctbal;  // stored as int64_t with scale factor 100
};

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

void run_q22(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    const std::string customer_dir = gendb_dir + "/customer";
    const std::string orders_dir = gendb_dir + "/orders";
    const std::string indexes_dir = gendb_dir + "/indexes";

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

    // Load pre-built orders hash index or fallback to sequential scan
    #ifdef GENDB_PROFILE
    auto t_orders_load_start = std::chrono::high_resolution_clock::now();
    #endif

    // Variables for has_orders (fallback path)
    std::vector<bool> has_orders;
    std::unique_ptr<HashIndexReader> hash_index_ptr;
    void* o_custkey_data = nullptr;
    void* hash_index_data = nullptr;
    int fd_o_custkey = -1;
    int fd_hash_index = -1;

    // Try to load pre-built hash index
    fd_hash_index = open((indexes_dir + "/orders_o_custkey_hash.bin").c_str(), O_RDONLY);
    if (fd_hash_index < 0) {
        // Fallback: use sequential scan to build bitset
        fd_o_custkey = open((orders_dir + "/o_custkey.bin").c_str(), O_RDONLY);
        fstat(fd_o_custkey, &sb);
        size_t o_custkey_size = sb.st_size;
        o_custkey_data = mmap(nullptr, o_custkey_size, PROT_READ, MAP_SHARED, fd_o_custkey, 0);
        const int32_t* o_custkey = static_cast<int32_t*>(o_custkey_data);
        int32_t num_orders = o_custkey_size / sizeof(int32_t);

        has_orders.resize(MAX_CUSTKEY, false);
        for (int32_t i = 0; i < num_orders; i++) {
            int32_t custkey = o_custkey[i];
            if (custkey > 0 && custkey < (int32_t)MAX_CUSTKEY) {
                has_orders[custkey] = true;
            }
        }
    } else {
        // Load hash index via mmap
        fstat(fd_hash_index, &sb);
        size_t hash_index_size = sb.st_size;
        hash_index_data = mmap(nullptr, hash_index_size, PROT_READ, MAP_SHARED, fd_hash_index, 0);
        const char* hash_index_raw = static_cast<const char*>(hash_index_data);
        try {
            hash_index_ptr = std::make_unique<HashIndexReader>(hash_index_raw, hash_index_size);
        } catch (...) {
            // Fallback to bitset if index is corrupted
            fd_o_custkey = open((orders_dir + "/o_custkey.bin").c_str(), O_RDONLY);
            fstat(fd_o_custkey, &sb);
            size_t o_custkey_size = sb.st_size;
            o_custkey_data = mmap(nullptr, o_custkey_size, PROT_READ, MAP_SHARED, fd_o_custkey, 0);
            const int32_t* o_custkey = static_cast<int32_t*>(o_custkey_data);
            int32_t num_orders = o_custkey_size / sizeof(int32_t);
            has_orders.resize(MAX_CUSTKEY, false);
            for (int32_t i = 0; i < num_orders; i++) {
                int32_t custkey = o_custkey[i];
                if (custkey > 0 && custkey < (int32_t)MAX_CUSTKEY) {
                    has_orders[custkey] = true;
                }
            }
            hash_index_ptr = nullptr;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_load_end = std::chrono::high_resolution_clock::now();
    double orders_load_ms = std::chrono::duration<double, std::milli>(t_orders_load_end - t_orders_load_start).count();
    printf("[TIMING] load_orders_data: %.2f ms\n", orders_load_ms);
    #endif

    // STEP 1: Compute average threshold (with parallelization)
    #ifdef GENDB_PROFILE
    auto t_combined_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t sum_acctbal = 0;
    int32_t count_positive = 0;
    #pragma omp parallel for reduction(+:sum_acctbal,count_positive) schedule(static, 16384)
    for (int32_t i = 0; i < num_customers; i++) {
        int64_t acctbal = c_acctbal[i];
        if (acctbal > 0 && is_valid_phone_code(c_phone_raw, i)) {
            sum_acctbal += acctbal;
            count_positive++;
        }
    }

    int64_t avg_threshold = (count_positive > 0) ? (sum_acctbal / count_positive) : 0;

    #ifdef GENDB_PROFILE
    auto t_avg_end = std::chrono::high_resolution_clock::now();
    double avg_ms = std::chrono::duration<double, std::milli>(t_avg_end - t_combined_start).count();
    printf("[TIMING] compute_average: %.2f ms\n", avg_ms);
    printf("[DEBUG] Average threshold (scaled): %ld (actual: %.2f)\n", avg_threshold, avg_threshold / 100.0);
    printf("[DEBUG] Count for average: %d\n", count_positive);
    #endif

    // STEP 2: Parallel filter and aggregate with thread-local results
    #ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    int num_threads = omp_get_max_threads();
    std::vector<std::map<std::string, Result>> thread_local_results(num_threads);
    int32_t total_filtered = 0;

    #pragma omp parallel reduction(+:total_filtered)
    {
        int thread_id = omp_get_thread_num();
        auto& local_grouped = thread_local_results[thread_id];

        #pragma omp for schedule(static, 16384)
        for (int32_t i = 0; i < num_customers; i++) {
            int64_t acctbal = c_acctbal[i];
            int32_t custkey = c_custkey[i];

            // Filter 1: country code must be in list
            if (!is_valid_phone_code(c_phone_raw, i)) continue;

            // Filter 2: c_acctbal > average
            if (acctbal <= avg_threshold) continue;

            // Filter 3: NOT EXISTS (SELECT * FROM orders WHERE o_custkey = c_custkey)
            bool has_order = false;
            if (hash_index_ptr) {
                has_order = (custkey > 0 && custkey < (int32_t)MAX_CUSTKEY) ? hash_index_ptr->contains(custkey) : false;
            } else {
                has_order = (custkey > 0 && custkey < (int32_t)MAX_CUSTKEY) ? has_orders[custkey] : false;
            }
            if (has_order) continue;

            total_filtered++;

            // Extract phone code only for qualifying rows
            const char* phone_ptr = get_phone_ptr(c_phone_raw, i);
            std::string code = get_phone_code_from_ptr(phone_ptr);

            // Add to thread-local result
            if (local_grouped.count(code) == 0) {
                local_grouped[code] = {code, 0, 0};
            }
            local_grouped[code].numcust++;
            local_grouped[code].totacctbal += acctbal;
        }
    }

    // Merge thread-local results
    std::map<std::string, Result> grouped;
    for (int tid = 0; tid < num_threads; tid++) {
        for (auto& entry : thread_local_results[tid]) {
            if (grouped.count(entry.first) == 0) {
                grouped[entry.first] = {entry.first, 0, 0};
            }
            grouped[entry.first].numcust += entry.second.numcust;
            grouped[entry.first].totacctbal += entry.second.totacctbal;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] filter_and_aggregate: %.2f ms\n", filter_ms);
    printf("[DEBUG] Total filtered rows: %d\n", total_filtered);
    #endif

    // STEP 3: Convert to vector for output (already sorted due to std::map)
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
    if (o_custkey_data) munmap(o_custkey_data, sb.st_size);
    if (hash_index_data) munmap(hash_index_data, sb.st_size);
    close(fd_custkey);
    close(fd_phone);
    close(fd_acctbal);
    if (fd_o_custkey >= 0) close(fd_o_custkey);
    if (fd_hash_index >= 0) close(fd_hash_index);

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
