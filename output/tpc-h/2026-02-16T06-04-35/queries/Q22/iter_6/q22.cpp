#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <string>
#include <cstring>
#include <algorithm>
#include <cmath>
#include <chrono>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// [METADATA CHECK] Q22 Query
// - c_custkey: int32_t, no encoding
// - c_acctbal: int64_t, scale_factor=100 (DECIMAL)
// - c_phone: CHAR(15), stored as fixed-width strings
// - o_custkey: int32_t, no encoding
// - orders_o_custkey_hash: hash_multi_value index on orders(o_custkey)

struct MmapFile {
    int fd;
    void* ptr;
    size_t size;

    MmapFile(const std::string& path) : fd(-1), ptr(nullptr), size(0) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            throw std::runtime_error("Cannot open file: " + path);
        }

        off_t file_size = lseek(fd, 0, SEEK_END);
        if (file_size < 0) {
            close(fd);
            throw std::runtime_error("Cannot determine file size: " + path);
        }
        size = file_size;

        ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) {
            close(fd);
            throw std::runtime_error("Cannot mmap file: " + path);
        }
    }

    ~MmapFile() {
        if (ptr != nullptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }
};

// Hash index structure: [uint32_t num_unique] [uint32_t table_size]
// then [key:int32_t, offset:uint32_t, count:uint32_t] per slot,
// then [uint32_t pos_count] [positions...]
struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

class OrdersHashIndex {
public:
    uint32_t num_unique;
    uint32_t table_size;
    const HashIndexEntry* slots_ptr;  // Direct pointer to mmapped data
    size_t slots_size;
    MmapFile* mmap_file;  // Keep mmap alive

    OrdersHashIndex(const std::string& path) {
        mmap_file = new MmapFile(path);
        const char* data = (const char*)mmap_file->ptr;
        size_t offset = 0;

        // Read header
        std::memcpy(&num_unique, data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);
        std::memcpy(&table_size, data + offset, sizeof(uint32_t));
        offset += sizeof(uint32_t);

        // Direct pointer to slots (no copy)
        slots_ptr = (const HashIndexEntry*)(data + offset);
        slots_size = table_size;
        offset += table_size * sizeof(HashIndexEntry);

        // We skip reading positions since we only need slots for this query
    }

    ~OrdersHashIndex() {
        delete mmap_file;
    }

    const HashIndexEntry& getSlot(uint32_t i) const {
        return slots_ptr[i];
    }
};

// Helper to extract first 2 characters from phone number
// c_phone is variable-length encoded: [num_strings (uint32_t)] [offsets...] [strings...]
class PhoneDecoder {
public:
    const uint32_t* offsets;
    const char* strings;
    uint32_t num_strings;

    PhoneDecoder(const void* data, size_t size) {
        const char* char_data = (const char*)data;
        const uint32_t* uint_data = (const uint32_t*)data;

        num_strings = uint_data[0];
        offsets = uint_data + 1;  // offsets start right after num_strings

        // String pool starts after all offsets
        strings = char_data + (1 + num_strings) * sizeof(uint32_t);
    }

    std::string getCountryCode(uint32_t idx) const {
        if (idx >= num_strings) return "";
        uint32_t offset = offsets[idx];
        const char* phone_str = strings + offset;
        // Phone format is "XX-...", first 2 chars are country code
        return std::string(phone_str, 2);
    }
};

void run_q22(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    const int NUM_CUSTOMERS = 1500000;
    const int64_t SCALE_FACTOR = 100;

    // Target country codes
    const std::string target_codes[] = {"13", "31", "23", "29", "30", "18", "17"};
    const int NUM_TARGET_CODES = 7;

    // Load customer data
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    MmapFile custkey_file(gendb_dir + "/customer/c_custkey.bin");
    MmapFile acctbal_file(gendb_dir + "/customer/c_acctbal.bin");
    MmapFile phone_file(gendb_dir + "/customer/c_phone.bin");

    const int32_t* c_custkey = (const int32_t*)custkey_file.ptr;
    const int64_t* c_acctbal = (const int64_t*)acctbal_file.ptr;

    // Decode phone strings
    PhoneDecoder phone_decoder(phone_file.ptr, phone_file.size);

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double t_load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", t_load_ms);
    #endif

    // Load orders hash index
    #ifdef GENDB_PROFILE
    auto t_index_start = std::chrono::high_resolution_clock::now();
    #endif

    OrdersHashIndex orders_idx(gendb_dir + "/indexes/orders_o_custkey_hash.bin");

    #ifdef GENDB_PROFILE
    auto t_index_end = std::chrono::high_resolution_clock::now();
    double t_index_ms = std::chrono::duration<double, std::milli>(t_index_end - t_index_start).count();
    printf("[TIMING] load_index: %.2f ms\n", t_index_ms);
    #endif

    // Pre-build a fast lookup table for target country codes (256 byte pairs)
    bool is_target_code[256][256];
    std::memset(is_target_code, false, sizeof(is_target_code));
    for (int j = 0; j < NUM_TARGET_CODES; ++j) {
        unsigned char c0 = (unsigned char)target_codes[j][0];
        unsigned char c1 = (unsigned char)target_codes[j][1];
        is_target_code[c0][c1] = true;
    }

    // Step 1: Compute average account balance for target country codes where acctbal > 0
    #ifdef GENDB_PROFILE
    auto t_avg_start = std::chrono::high_resolution_clock::now();
    #endif

    int64_t sum_acctbal_for_avg = 0;
    int64_t count_for_avg = 0;

    // Compute average: SUM(c_acctbal) WHERE c_acctbal > 0 AND code IN (...)
    // Use direct char lookups, parallel reduction, and loop unrolling
    #pragma omp parallel for reduction(+:sum_acctbal_for_avg,count_for_avg) schedule(static, 16384)
    for (int i = 0; i < NUM_CUSTOMERS; ++i) {
        uint32_t offset = phone_decoder.offsets[i];
        const char* phone_str = phone_decoder.strings + offset;
        unsigned char c0 = (unsigned char)phone_str[0];
        unsigned char c1 = (unsigned char)phone_str[1];

        // Fast lookup: is_target_code[c0][c1]
        if (is_target_code[c0][c1]) {
            int64_t bal = c_acctbal[i];
            if (bal > 0) {
                sum_acctbal_for_avg += bal;
                count_for_avg++;
            }
        }
    }

    // Average in scaled units
    int64_t avg_acctbal = (count_for_avg > 0) ? (sum_acctbal_for_avg / count_for_avg) : 0;


    #ifdef GENDB_PROFILE
    auto t_avg_end = std::chrono::high_resolution_clock::now();
    double t_avg_ms = std::chrono::duration<double, std::milli>(t_avg_end - t_avg_start).count();
    printf("[TIMING] compute_avg: %.2f ms\n", t_avg_ms);
    #endif

    // Step 2: Pre-build a dense lookup array for orders presence (O(1) lookup)
    // Only iterate through hash index to mark which customer keys have orders.
    // Use a much smaller compact lookup structure.
    #ifdef GENDB_PROFILE
    auto t_antijoin_start = std::chrono::high_resolution_clock::now();
    #endif

    // Create a bitset-like lookup: since c_custkey is in range [1, 1500000],
    // we can use a vector<uint8_t> as a dense lookup (1 byte per customer, much faster than hash set).
    std::vector<uint8_t> has_orders(NUM_CUSTOMERS + 1, 0);

    // Mark all customer keys that have orders by scanning hash index slots
    for (uint32_t i = 0; i < orders_idx.table_size; ++i) {
        const HashIndexEntry& slot = orders_idx.getSlot(i);
        if (slot.count > 0) {
            int32_t key = slot.key;
            if (key >= 0 && key <= NUM_CUSTOMERS) {
                has_orders[key] = 1;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_antijoin_end = std::chrono::high_resolution_clock::now();
    double t_antijoin_ms = std::chrono::duration<double, std::milli>(t_antijoin_end - t_antijoin_start).count();
    printf("[TIMING] build_antijoin: %.2f ms\n", t_antijoin_ms);
    #endif

    // Step 3: Filter customers and aggregate
    // WHERE: country code in list, acctbal > avg, NO orders
    #ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    // Use map for GROUP BY country code (low cardinality: 7 groups)
    std::map<std::string, std::pair<int64_t, int64_t>> results;  // cntrycode -> (count, sum_acctbal)

    // Thread-local aggregation to avoid contention
    int num_threads = omp_get_max_threads();
    std::vector<std::map<std::string, std::pair<int64_t, int64_t>>> thread_results(num_threads);

    #pragma omp parallel for schedule(static, 16384)
    for (int i = 0; i < NUM_CUSTOMERS; ++i) {
        // Get country code directly without allocating string
        uint32_t offset = phone_decoder.offsets[i];
        const char* phone_str = phone_decoder.strings + offset;
        unsigned char c0 = (unsigned char)phone_str[0];
        unsigned char c1 = (unsigned char)phone_str[1];

        // Fast lookup: is_target_code[c0][c1]
        if (!is_target_code[c0][c1]) continue;

        // Check acctbal > avg (early filter before string construction)
        int64_t bal = c_acctbal[i];
        if (bal <= avg_acctbal) continue;

        // Check NOT EXISTS (orders where o_custkey = c_custkey) (early filter)
        // Use dense lookup array instead of hash set
        int32_t custkey = c_custkey[i];
        if (custkey > 0 && custkey <= NUM_CUSTOMERS && has_orders[custkey]) continue;

        // Only construct string code for rows that pass all filters
        char code_buf[3];
        code_buf[0] = phone_str[0];
        code_buf[1] = phone_str[1];
        code_buf[2] = '\0';
        std::string code(code_buf);

        // Aggregate in thread-local map
        int tid = omp_get_thread_num();
        thread_results[tid][code].first++;  // count
        thread_results[tid][code].second += bal;  // sum
    }

    // Merge thread-local results
    for (int t = 0; t < num_threads; ++t) {
        for (const auto& [code, aggs] : thread_results[t]) {
            results[code].first += aggs.first;
            results[code].second += aggs.second;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double t_filter_ms = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter_group: %.2f ms\n", t_filter_ms);
    #endif

    // Step 3: Write results to CSV
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_file = results_dir + "/Q22.csv";
    std::ofstream out(output_file);

    // Header
    out << "cntrycode,numcust,totacctbal\n";

    // Results (already sorted by key due to std::map)
    for (const auto& [code, aggs] : results) {
        int64_t count = aggs.first;
        int64_t sum_scaled = aggs.second;

        // Convert scaled sum to decimal: divide by SCALE_FACTOR
        double sum_decimal = (double)sum_scaled / SCALE_FACTOR;

        out << code << "," << count << ",";
        out << std::fixed << std::setprecision(2) << sum_decimal << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double t_output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", t_output_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double t_total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", t_total_ms);
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

    try {
        run_q22(gendb_dir, results_dir);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
#endif
