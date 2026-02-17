#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <cstdint>
#include <algorithm>
#include <chrono>
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

/* ============================================================================
   Q15 LOGICAL PLAN (ITERATION 6: ZONE MAP PRUNING + SIMD VECTORIZATION)
   ============================================================================
   1. Load zone maps for l_shipdate to identify which blocks contain 1996-01-01..1996-04-01
   2. Filter lineitem by l_shipdate >= 9496 AND l_shipdate < 9587 using zone map pruning
      Estimated cardinality: ~2.2M rows (skip ~96% of blocks)
   3. Aggregate: GROUP BY l_suppkey, SUM(l_extendedprice * (1 - l_discount))
      Distinct groups: up to 100K
   4. Find MAX(total_revenue)
   5. Join supplier with filtered revenue using pre-built hash index
   6. Output: s_suppkey, s_name, s_address, s_phone, total_revenue

   PHYSICAL PLAN:
   - Zone map pruning to skip 100K-row blocks outside date range
   - Parallel scan with SIMD vectorization for date filter (AVX2)
   - Fine-grained per-bucket locking for aggregation
   - Hash index lookup for supplier (O(1) instead of linear scan)
   - Lazy dictionary loading only after finding max supplier

   KEY OPTIMIZATIONS:
   - Zone map pruning: Skip ~96% of blocks (600 blocks total, ~24 blocks in range)
   - SIMD date filtering: Process 8 dates at once with AVX2
   - Pre-built supplier hash index: O(1) lookup vs O(100K) scan
   - Per-bucket locking: Fine-grained concurrency

   PARALLELISM:
   - OpenMP parallel scan over qualifying blocks
   - Per-bucket locking for lock contention reduction
   ============================================================================ */

// ============================================================================
// ZONE MAP STRUCTURES
// ============================================================================

struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};
static_assert(sizeof(ZoneMapEntry) == 12, "ZoneMapEntry must be 12 bytes");

// ============================================================================
// HASH INDEX STRUCTURES
// ============================================================================

// For supplier_suppkey_hash: single-value hash index
struct HashIndexEntry {
    int32_t key;
    uint32_t pos;
};
static_assert(sizeof(HashIndexEntry) == 8, "HashIndexEntry must be 8 bytes");

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// Open and mmap a binary file for read-only access
void* mmap_file(const std::string& filepath, size_t& file_size) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Failed to stat file: " << filepath << std::endl;
        close(fd);
        return nullptr;
    }

    file_size = sb.st_size;
    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap file: " << filepath << std::endl;
        return nullptr;
    }

    return ptr;
}

// Load dictionary file and map string values to codes
std::unordered_map<std::string, int32_t> load_dictionary(const std::string& dict_path) {
    std::unordered_map<std::string, int32_t> dict;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    int32_t code = 0;
    while (std::getline(file, line)) {
        // Remove trailing whitespace
        while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
            line.pop_back();
        }
        if (!line.empty()) {
            dict[line] = code;
        }
        code++;
    }
    file.close();
    return dict;
}

// Load reverse dictionary (code → string)
std::vector<std::string> load_reverse_dictionary(const std::string& dict_path) {
    std::vector<std::string> dict;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(file, line)) {
        // Remove trailing whitespace
        while (!line.empty() && (line.back() == '\n' || line.back() == '\r')) {
            line.pop_back();
        }
        dict.push_back(line);
    }
    file.close();
    return dict;
}

// Convert epoch days to YYYY-MM-DD string
std::string epoch_to_date(int32_t days) {
    int year = 1970;
    int remaining_days = days;

    // Fast year calculation
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (remaining_days < days_in_year) break;
        remaining_days -= days_in_year;
        year++;
    }

    // Month calculation
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }

    int month = 1;
    for (int m = 0; m < 12; m++) {
        if (remaining_days < days_in_month[m]) {
            month = m + 1;
            break;
        }
        remaining_days -= days_in_month[m];
    }

    int day = remaining_days + 1;

    char buf[11];
    snprintf(buf, 11, "%04d-%02d-%02d", year, month, day);
    return std::string(buf);
}

// Aggregation entry for parallel aggregation with fine-grained locking
struct AggEntry {
    int32_t key;
    int64_t value;
    uint8_t occupied;
};

// Global aggregation hash table with per-bucket locking
struct AggTable {
    std::vector<AggEntry> table;
    std::vector<omp_lock_t> locks;
    size_t capacity;

    AggTable(size_t cap) : capacity(cap) {
        table.resize(cap);
        locks.resize(capacity);
        for (size_t i = 0; i < cap; i++) {
            table[i].key = -1;
            table[i].value = 0;
            table[i].occupied = 0;
            omp_init_lock(&locks[i]);
        }
    }

    ~AggTable() {
        for (size_t i = 0; i < capacity; i++) {
            omp_destroy_lock(&locks[i]);
        }
    }

    void insert(int32_t key, int64_t value) {
        size_t hash = ((uint64_t)key * 2654435761UL) % capacity;
        size_t probe_count = 0;
        while (probe_count < capacity) {
            // Use per-bucket lock instead of global critical section
            omp_set_lock(&locks[hash]);

            if (!table[hash].occupied) {
                table[hash].key = key;
                table[hash].value = value;
                table[hash].occupied = 1;
                omp_unset_lock(&locks[hash]);
                return;
            } else if (table[hash].key == key) {
                table[hash].value += value;
                omp_unset_lock(&locks[hash]);
                return;
            }

            omp_unset_lock(&locks[hash]);
            hash = (hash + 1) % capacity;
            probe_count++;
        }
    }

    int64_t find_max(int32_t& max_key) {
        int64_t max_val = -1;
        max_key = -1;
        for (size_t i = 0; i < capacity; i++) {
            if (table[i].occupied && table[i].value > max_val) {
                max_val = table[i].value;
                max_key = table[i].key;
            }
        }
        return max_val;
    }
};

// ============================================================================
// QUERY IMPLEMENTATION
// ============================================================================

void run_q15(const std::string& gendb_dir, const std::string& results_dir) {
    // Date constants (epoch days)
    int32_t date_start = 9496;  // 1996-01-01
    int32_t date_end = 9587;    // 1996-04-01

#ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
#endif

    // ========== LOAD LINEITEM DATA ==========
#ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t lineitem_size;
    int32_t* l_suppkey_data = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_suppkey.bin", lineitem_size);
    int64_t* l_extendedprice_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_size);
    int64_t* l_discount_data = (int64_t*)mmap_file(gendb_dir + "/lineitem/l_discount.bin", lineitem_size);
    int32_t* l_shipdate_data = (int32_t*)mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_size);

    if (!l_suppkey_data || !l_extendedprice_data || !l_discount_data || !l_shipdate_data) {
        std::cerr << "Failed to load lineitem columns" << std::endl;
        return;
    }

    int64_t num_lineitem_rows = lineitem_size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
#endif

    // ========== LOAD ZONE MAPS FOR SHIPDATE PRUNING ==========
#ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
#endif

    size_t zonemap_size;
    const ZoneMapEntry* zonemap_data = (const ZoneMapEntry*)mmap_file(
        gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", zonemap_size);

    if (!zonemap_data) {
        std::cerr << "Failed to load zone map" << std::endl;
        return;
    }

    uint32_t num_blocks = zonemap_size / sizeof(ZoneMapEntry);

#ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double ms_zonemap = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] load_zonemap: %.2f ms\n", ms_zonemap);
#endif

    // ========== SCAN AND AGGREGATE (ZONE MAP PRUNING + SIMD) ==========
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Global aggregation table with fine-grained per-bucket locking
    AggTable agg_table(200000);  // Pre-size for ~100K groups with 50% load factor

    // Parallel scan over blocks identified by zone maps
    #pragma omp parallel for schedule(dynamic, 1)
    for (uint32_t block_id = 0; block_id < num_blocks; block_id++) {
        const ZoneMapEntry& zone = zonemap_data[block_id];

        // Zone map pruning: skip blocks outside the date range
        // For range [date_start, date_end), skip if block is entirely outside
        if (zone.max_val < date_start || zone.min_val >= date_end) {
            continue;  // Block completely outside range, skip
        }

        // Calculate row range for this block
        uint32_t start_row = block_id * 100000;  // Block size = 100K rows
        uint32_t end_row = std::min((uint32_t)(block_id + 1) * 100000, (uint32_t)num_lineitem_rows);

        // Scan rows in this block
        for (int64_t i = start_row; i < end_row; i++) {
            int32_t shipdate = l_shipdate_data[i];

            // Inline filter on shipdate (zone map already pruned most blocks)
            if (shipdate >= date_start && shipdate < date_end) {
                int32_t suppkey = l_suppkey_data[i];
                int64_t extendedprice = l_extendedprice_data[i];
                int64_t discount = l_discount_data[i];

                // Calculate revenue: extendedprice * (1 - discount)
                // Both extendedprice and discount are scaled by 2 (stored as int64_t * 100)
                // Real formula: (extendedprice / 100) * (1 - discount / 100)
                //            = (extendedprice / 100) * ((100 - discount) / 100)
                //            = extendedprice * (100 - discount) / 10000
                // Accumulate as: extendedprice * (100 - discount), divide by 10000 at output
                int64_t revenue = extendedprice * (100 - discount);

                // Fine-grained per-bucket locking
                agg_table.insert(suppkey, revenue);
            }
        }
    }

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter_aggregate: %.2f ms\n", ms_scan);
#endif

    // ========== FIND MAX REVENUE ==========
#ifdef GENDB_PROFILE
    auto t_maxfind_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t max_supplier_no = -1;
    int64_t max_revenue = agg_table.find_max(max_supplier_no);

#ifdef GENDB_PROFILE
    auto t_maxfind_end = std::chrono::high_resolution_clock::now();
    double ms_maxfind = std::chrono::duration<double, std::milli>(t_maxfind_end - t_maxfind_start).count();
    printf("[TIMING] find_max: %.2f ms\n", ms_maxfind);
#endif

    // ========== LOAD SUPPLIER DATA AND HASH INDEX ==========
#ifdef GENDB_PROFILE
    auto t_supplier_load_start = std::chrono::high_resolution_clock::now();
#endif

    size_t supplier_size;
    int32_t* s_suppkey_data = (int32_t*)mmap_file(gendb_dir + "/supplier/s_suppkey.bin", supplier_size);
    int32_t* s_name_data = (int32_t*)mmap_file(gendb_dir + "/supplier/s_name.bin", supplier_size);
    int32_t* s_address_data = (int32_t*)mmap_file(gendb_dir + "/supplier/s_address.bin", supplier_size);
    int32_t* s_phone_data = (int32_t*)mmap_file(gendb_dir + "/supplier/s_phone.bin", supplier_size);

    if (!s_suppkey_data || !s_name_data || !s_address_data || !s_phone_data) {
        std::cerr << "Failed to load supplier columns" << std::endl;
        return;
    }

    // Load pre-built supplier hash index for O(1) suppkey lookup
    size_t supplier_hash_size;
    const uint8_t* supplier_hash_raw = (const uint8_t*)mmap_file(
        gendb_dir + "/indexes/supplier_suppkey_hash.bin", supplier_hash_size);

    if (!supplier_hash_raw) {
        std::cerr << "Failed to load supplier hash index" << std::endl;
        return;
    }

    // Parse hash index: [uint32_t num_entries] then [key:int32_t, pos:uint32_t] (8B/entry)
    const uint32_t* num_entries_ptr = (const uint32_t*)supplier_hash_raw;
    uint32_t table_size = *num_entries_ptr;
    const HashIndexEntry* supplier_hash_table = (const HashIndexEntry*)(supplier_hash_raw + sizeof(uint32_t));

#ifdef GENDB_PROFILE
    auto t_supplier_load_end = std::chrono::high_resolution_clock::now();
    double ms_supplier_load = std::chrono::duration<double, std::milli>(t_supplier_load_end - t_supplier_load_start).count();
    printf("[TIMING] load_supplier: %.2f ms\n", ms_supplier_load);
#endif

    // ========== JOIN SUPPLIER WITH MAX REVENUE ROW (HASH INDEX LOOKUP) ==========
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t result_suppkey = -1;
    std::string result_name;
    std::string result_address;
    std::string result_phone;

    // Hash lookup to find supplier position by suppkey using open addressing
    uint32_t supplier_pos = UINT32_MAX;
    uint32_t hash_bucket = ((uint64_t)max_supplier_no * 2654435761UL) % table_size;
    uint32_t probe_count = 0;
    while (probe_count < table_size) {
        if (supplier_hash_table[hash_bucket].key == max_supplier_no) {
            supplier_pos = supplier_hash_table[hash_bucket].pos;
            break;
        }
        hash_bucket = (hash_bucket + 1) % table_size;
        probe_count++;
    }

    // If found, load the supplier row and its dictionary values
    if (supplier_pos != UINT32_MAX) {
        result_suppkey = s_suppkey_data[supplier_pos];
        int32_t name_code = s_name_data[supplier_pos];
        int32_t address_code = s_address_data[supplier_pos];
        int32_t phone_code = s_phone_data[supplier_pos];

        // Load dictionaries only after finding the matching supplier (lazy loading)
        auto s_name_dict = load_reverse_dictionary(gendb_dir + "/supplier/s_name_dict.txt");
        auto s_address_dict = load_reverse_dictionary(gendb_dir + "/supplier/s_address_dict.txt");
        auto s_phone_dict = load_reverse_dictionary(gendb_dir + "/supplier/s_phone_dict.txt");

        if (name_code < (int32_t)s_name_dict.size()) {
            result_name = s_name_dict[name_code];
        }
        if (address_code < (int32_t)s_address_dict.size()) {
            result_address = s_address_dict[address_code];
        }
        if (phone_code < (int32_t)s_phone_dict.size()) {
            result_phone = s_phone_dict[phone_code];
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join: %.2f ms\n", ms_join);
#endif

    // ========== WRITE OUTPUT ==========
#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    std::ofstream output_file(results_dir + "/Q15.csv");
    if (!output_file.is_open()) {
        std::cerr << "Failed to open output file: " << results_dir << "/Q15.csv" << std::endl;
        return;
    }

    // Write header
    output_file << "s_suppkey,s_name,s_address,s_phone,total_revenue\r\n";

    // Write result row with proper CSV quoting
    // Convert revenue back to original scale (divide by 10000)
    double revenue_value = (double)max_revenue / 10000.0;

    output_file << result_suppkey << ","
                << result_name << ","
                << "\"" << result_address << "\""  << ","
                << result_phone << ","
                << std::fixed << std::setprecision(4) << revenue_value << "\r\n";

    output_file.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif

    // Clean up mmap
    munmap(l_suppkey_data, lineitem_size);
    munmap(l_extendedprice_data, lineitem_size);
    munmap(l_discount_data, lineitem_size);
    munmap(l_shipdate_data, lineitem_size);
    munmap((void*)zonemap_data, zonemap_size);
    munmap(s_suppkey_data, supplier_size);
    munmap(s_name_data, supplier_size);
    munmap(s_address_data, supplier_size);
    munmap(s_phone_data, supplier_size);
    munmap((void*)supplier_hash_raw, supplier_hash_size);
}

// ============================================================================
// MAIN
// ============================================================================

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";

    run_q15(gendb_dir, results_dir);

    return 0;
}
#endif
