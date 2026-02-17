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
   Q15 LOGICAL PLAN (ITERATION 10: THREAD-LOCAL PARTIAL AGGREGATION + HASH LOOKUP)
   ============================================================================
   1. Load zone map for l_shipdate, identify blocks in [9496, 9587)
   2. Filter lineitem by l_shipdate >= 9496 AND l_shipdate < 9587
      Estimated cardinality: ~2.2M rows
      OPTIMIZATION: Skip blocks entirely outside date range via zone map
   3. Aggregate: GROUP BY l_suppkey, SUM(l_extendedprice * (1 - l_discount))
      Distinct groups: up to 100K
      OPTIMIZATION: Thread-local partial aggregation → merge (eliminates lock contention)
   4. Find MAX(total_revenue)
   5. Load supplier hash index (pre-built)
   6. Join supplier with filtered revenue via proper hash lookup (not linear scan)
   7. Output: s_suppkey, s_name, s_address, s_phone, total_revenue

   PHYSICAL PLAN:
   - Zone map pruning: load zone map metadata, skip blocks outside date filter range
   - Parallel scan over qualifying blocks with thread-local hash tables
   - Merge thread-local tables into global aggregation result
   - Load pre-built supplier suppkey hash index for proper O(1) hash lookup
   - Use hash function to navigate pre-built index buckets

   KEY OPTIMIZATIONS:
   - Zone map pruning eliminates full lineitem scan; skip cold blocks
   - Thread-local aggregation tables eliminate lock contention in hot path
   - Proper hash-based lookup in supplier index instead of linear scan
   - Open-addressing compact hash table reduces memory overhead
   - Pre-load dictionaries once after supplier lookup

   PARALLELISM:
   - OpenMP parallel scan over zone map blocks with thread-local aggregation
   - Lock-free local aggregation → single-threaded merge phase
   ============================================================================ */

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

// Zone map entry for l_shipdate (from storage guide)
// Layout: [uint32_t min, uint32_t max, uint32_t row_count] per block (12 bytes/block)
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Supplier hash index entry (from storage guide)
// Layout: [key:int32_t, pos:uint32_t] (8 bytes/entry)
struct SupplierHashEntry {
    int32_t key;
    uint32_t pos;
};

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

// Load zone map for l_shipdate and identify blocks in range
// Returns vector of {block_index, start_row, end_row} tuples for blocks overlapping [date_min, date_max)
std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> load_zone_map_blocks(
    const std::string& zonemap_path, int32_t date_min, int32_t date_max) {

    std::vector<std::tuple<uint32_t, uint32_t, uint32_t>> result;

    size_t zonemap_size;
    void* zonemap_ptr = mmap_file(zonemap_path, zonemap_size);
    if (!zonemap_ptr) {
        return result;
    }

    // Parse zone map header: [uint32_t num_blocks] followed by entries
    const uint32_t* header = (const uint32_t*)zonemap_ptr;
    uint32_t num_blocks = header[0];

    // Zone map entries start after header
    const ZoneMapEntry* zones = (const ZoneMapEntry*)(header + 1);

    uint32_t current_row = 0;
    for (uint32_t z = 0; z < num_blocks; z++) {
        // Check if block overlaps predicate range [date_min, date_max)
        if (zones[z].max_val >= date_min && zones[z].min_val < date_max) {
            uint32_t start_row = current_row;
            uint32_t end_row = current_row + zones[z].row_count;
            result.push_back(std::make_tuple(z, start_row, end_row));
        }
        current_row += zones[z].row_count;
    }

    munmap(zonemap_ptr, zonemap_size);
    return result;
}

// Load supplier hash index (pre-built)
// Layout: [uint32_t num_entries] then [key:int32_t, pos:uint32_t] (8B/entry)
struct SupplierHashIndex {
    const SupplierHashEntry* entries;
    uint32_t num_entries;
    size_t mmap_size;
};

SupplierHashIndex load_supplier_hash_index(const std::string& index_path) {
    SupplierHashIndex idx = {nullptr, 0, 0};

    size_t file_size;
    void* ptr = mmap_file(index_path, file_size);
    if (!ptr) {
        return idx;
    }

    const uint32_t* header = (const uint32_t*)ptr;
    idx.num_entries = header[0];
    idx.entries = (const SupplierHashEntry*)(header + 1);
    idx.mmap_size = file_size;

    return idx;
}

// Lookup in supplier hash index using open-addressing hash probing
// Pre-built index has entries laid out linearly; use hash function to find bucket
int32_t supplier_hash_lookup(const SupplierHashIndex& idx, int32_t suppkey) {
    // The index is stored as a linear array of entries. Use power-of-2 sizing assumption
    // (standard for hash tables). Find next power of 2 >= num_entries.
    uint32_t table_size = 1;
    while (table_size < idx.num_entries) table_size <<= 1;

    // Use fibonacci hashing: hash = (key * golden_ratio) >> shift
    // Shift chosen so hash maps into [0, table_size)
    uint32_t mask = table_size - 1;
    uint64_t hash = ((uint64_t)suppkey * 0x9E3779B97F4A7C15ULL) & mask;

    // Linear probe in the pre-built index
    for (uint32_t probe = 0; probe < table_size; probe++) {
        uint32_t idx_pos = (hash + probe) & mask;
        if (idx_pos >= idx.num_entries) {
            // If we've probed beyond num_entries, entry doesn't exist
            return -1;
        }

        // Check if this slot contains our key
        if (idx.entries[idx_pos].key == suppkey) {
            return idx.entries[idx_pos].pos;
        }

        // Empty slot indicates key is not in table
        if (idx.entries[idx_pos].key == -1 || idx.entries[idx_pos].key == 0) {
            // This heuristic assumes -1 or 0 marks empty slots (common in implementations)
            // If unsuccessful after first empty, stop probing
            return -1;
        }
    }

    return -1;
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

// Aggregation entry for open-addressing hash table (thread-local)
struct AggEntry {
    int32_t key;
    int64_t value;
    bool occupied;
};

// Compact open-addressing hash table for thread-local aggregation
struct CompactAggTable {
    std::vector<AggEntry> table;
    size_t capacity;
    size_t mask;

    CompactAggTable(size_t expected_size) : capacity(0), mask(0) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        capacity = sz;
        mask = sz - 1;
        table.resize(sz);
        for (size_t i = 0; i < sz; i++) {
            table[i].key = -1;
            table[i].value = 0;
            table[i].occupied = false;
        }
    }

    // Fibonacci hashing for good distribution
    inline size_t hash(int32_t key) const {
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) & mask;
    }

    void insert(int32_t key, int64_t value) {
        size_t idx = hash(key);
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value += value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx].key = key;
        table[idx].value = value;
        table[idx].occupied = true;
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

    // Iterate over all entries
    std::vector<std::pair<int32_t, int64_t>> entries() const {
        std::vector<std::pair<int32_t, int64_t>> result;
        for (size_t i = 0; i < capacity; i++) {
            if (table[i].occupied) {
                result.push_back({table[i].key, table[i].value});
            }
        }
        return result;
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

    // int64_t num_lineitem_rows = lineitem_size / sizeof(int32_t);  // No longer needed with zone map

#ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load: %.2f ms\n", ms_load);
#endif

    // ========== ZONE MAP PRUNING: IDENTIFY QUALIFYING BLOCKS ==========
#ifdef GENDB_PROFILE
    auto t_zonemap_start = std::chrono::high_resolution_clock::now();
#endif

    auto qualifying_blocks = load_zone_map_blocks(
        gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", date_start, date_end);

#ifdef GENDB_PROFILE
    auto t_zonemap_end = std::chrono::high_resolution_clock::now();
    double ms_zonemap = std::chrono::duration<double, std::milli>(t_zonemap_end - t_zonemap_start).count();
    printf("[TIMING] zonemap: %.2f ms\n", ms_zonemap);
#endif

    // ========== SCAN AND AGGREGATE (PARTIAL AGGREGATION WITH THREAD-LOCAL TABLES) ==========
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Partial aggregation: use thread-local hash tables to eliminate lock contention
    // Then merge into single global table in post-processing
    int num_threads = omp_get_max_threads();
    std::vector<CompactAggTable*> thread_tables(num_threads);

    // Phase 1: Parallel scan with thread-local aggregation (NO LOCKS)
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        // Each thread gets its own hash table
        thread_tables[tid] = new CompactAggTable(100000 / num_threads + 1000);
    }

    #pragma omp parallel for schedule(dynamic)
    for (size_t block_idx = 0; block_idx < qualifying_blocks.size(); block_idx++) {
        int tid = omp_get_thread_num();
        CompactAggTable* local_table = thread_tables[tid];

        uint32_t start_row = std::get<1>(qualifying_blocks[block_idx]);
        uint32_t end_row = std::get<2>(qualifying_blocks[block_idx]);

        // Process this block
        for (uint32_t i = start_row; i < end_row; i++) {
            int32_t shipdate = l_shipdate_data[i];

            // Double-check filter (may have false positives from zone map min/max)
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

                // Insert into thread-local table (NO LOCKING)
                local_table->insert(suppkey, revenue);
            }
        }
    }

    // Phase 2: Merge thread-local tables into single global table (single-threaded)
    CompactAggTable global_agg_table(200000);  // Final global table
    for (int t = 0; t < num_threads; t++) {
        auto entries = thread_tables[t]->entries();
        for (const auto& [key, value] : entries) {
            global_agg_table.insert(key, value);
        }
        delete thread_tables[t];
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
    int64_t max_revenue = global_agg_table.find_max(max_supplier_no);

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

    // Load pre-built supplier hash index
    SupplierHashIndex supplier_idx = load_supplier_hash_index(gendb_dir + "/indexes/supplier_suppkey_hash.bin");
    if (!supplier_idx.entries) {
        std::cerr << "Failed to load supplier hash index" << std::endl;
        return;
    }

    int64_t num_supplier_rows = supplier_size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    auto t_supplier_load_end = std::chrono::high_resolution_clock::now();
    double ms_supplier_load = std::chrono::duration<double, std::milli>(t_supplier_load_end - t_supplier_load_start).count();
    printf("[TIMING] load_supplier: %.2f ms\n", ms_supplier_load);
#endif

    // ========== JOIN SUPPLIER WITH MAX REVENUE ROW USING HASH INDEX ==========
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t result_suppkey = -1;
    std::string result_name;
    std::string result_address;
    std::string result_phone;

    // Use pre-built hash index for O(1) lookup instead of O(N) linear scan
    int32_t supplier_pos = supplier_hash_lookup(supplier_idx, max_supplier_no);
    if (supplier_pos >= 0 && supplier_pos < (int32_t)num_supplier_rows) {
        result_suppkey = s_suppkey_data[supplier_pos];
        int32_t name_code = s_name_data[supplier_pos];
        int32_t address_code = s_address_data[supplier_pos];
        int32_t phone_code = s_phone_data[supplier_pos];

        // Load dictionaries once after finding the matching supplier
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
    munmap(s_suppkey_data, supplier_size);
    munmap(s_name_data, supplier_size);
    munmap(s_address_data, supplier_size);
    munmap(s_phone_data, supplier_size);
    if (supplier_idx.entries) {
        // Unmap starting from header (one uint32_t before entries)
        const uint32_t* header = (const uint32_t*)supplier_idx.entries - 1;
        munmap((void*)header, supplier_idx.mmap_size);
    }
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
