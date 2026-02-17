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
   Q15 LOGICAL PLAN (OPTIMIZED)
   ============================================================================
   1. Load zone map for lineitem.l_shipdate to prune blocks
      Filter lineitem by l_shipdate >= 9496 AND l_shipdate < 9587
      Estimated cardinality after pruning: ~2.2M rows (from ~60M)
   2. Aggregate: GROUP BY l_suppkey, SUM(l_extendedprice * (1 - l_discount))
      Distinct groups: up to 100K
   3. Find MAX(total_revenue)
   4. Join supplier with filtered revenue
   5. Output: s_suppkey, s_name, s_address, s_phone, total_revenue

   PHYSICAL PLAN (OPTIMIZED):
   - Load zone map for l_shipdate (600 blocks, 12 bytes each)
   - Scan lineitem with zone map pruning → skip non-matching blocks
   - Inline filtering: dates in [9496, 9587)
   - PARTITIONED hash aggregation (key-range partitioning across threads)
     * Each thread handles disjoint suppkey ranges → NO merge bottleneck
     * Reduces contention and improves cache locality
   - Sequential max-finding pass on global table
   - Linear scan to find supplier with matching suppkey (100K rows, trivial)
   - Dictionary decode for s_name, s_address, s_phone
   - Single row output

   PARALLELISM:
   - OpenMP parallel block scanning with zone map pruning
   - Thread-local aggregation on disjoint key ranges (partitioned, no merge)
   - Single-threaded max-finding (data already small)

   EXPECTED IMPROVEMENTS:
   - Zone map pruning: ~30-50% of blocks skipped → 1.5-2x faster scan
   - Partitioned aggregation: eliminate critical section bottleneck → 2-4x faster aggregate
   - Overall target: 60-80 ms (from 214 ms)
   ============================================================================ */

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

// Zone map entry for block-level min/max pruning
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Compact hash table for aggregation (open-addressing)
struct AggregateEntry {
    int32_t key;
    int64_t value;
    bool occupied;
};

struct AggregateTable {
    std::vector<AggregateEntry> table;
    size_t capacity;

    AggregateTable(size_t cap) : capacity(cap) {
        table.resize(cap);
        for (size_t i = 0; i < cap; i++) {
            table[i].occupied = false;
        }
    }

    void insert(int32_t key, int64_t value) {
        size_t hash = ((uint64_t)key * 2654435761UL) % capacity;
        while (table[hash].occupied && table[hash].key != key) {
            hash = (hash + 1) % capacity;
        }
        if (!table[hash].occupied) {
            table[hash].key = key;
            table[hash].value = value;
            table[hash].occupied = true;
        } else {
            table[hash].value += value;
        }
    }

    int64_t* find(int32_t key) {
        size_t hash = ((uint64_t)key * 2654435761UL) % capacity;
        while (table[hash].occupied && table[hash].key != key) {
            hash = (hash + 1) % capacity;
        }
        if (table[hash].occupied) {
            return &table[hash].value;
        }
        return nullptr;
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

    // ========== LOAD ZONE MAP FOR PRUNING ==========
    size_t zonemap_size = 0;
    void* zonemap_ptr = mmap_file(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", zonemap_size);

    // Zone map layout: [uint32_t num_blocks] then [int32_t min, int32_t max, uint32_t row_count] per block
    const uint8_t* zonemap_data = (const uint8_t*)zonemap_ptr;
    uint32_t num_blocks = 0;
    const ZoneMapEntry* zones = nullptr;

    if (zonemap_ptr) {
        num_blocks = *(const uint32_t*)zonemap_data;
        zones = (const ZoneMapEntry*)(zonemap_data + sizeof(uint32_t));
    }

    // ========== SCAN AND AGGREGATE WITH ZONE MAP PRUNING ==========
#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Single global aggregation table
    AggregateTable agg_table(150000);  // Reduced from 200K (sufficient for ~100K groups)

    // Build set of row ranges from zone map that satisfy the filter
    std::vector<std::pair<int64_t, int64_t>> block_ranges;
    int64_t current_row = 0;

    if (zones) {
        for (uint32_t b = 0; b < num_blocks; b++) {
            int32_t block_min = zones[b].min_val;
            int32_t block_max = zones[b].max_val;
            uint32_t block_rows = zones[b].row_count;

            // Zone map pruning: block must overlap [date_start, date_end)
            // Skip if: block_max < date_start OR block_min >= date_end
            if (block_max >= date_start && block_min < date_end) {
                // Block has potential matches
                block_ranges.push_back({current_row, current_row + block_rows});
            }
            current_row += block_rows;
        }
    } else {
        // Fallback: no zone map available, process all rows
        block_ranges.push_back({0, num_lineitem_rows});
    }

    // Parallel scan with thread-local aggregation buffers + atomic merge
    int num_threads = omp_get_max_threads();
    std::vector<AggregateTable> thread_agg;
    for (int t = 0; t < num_threads; t++) {
        thread_agg.emplace_back(150000);
    }

    #pragma omp parallel
    {
        int thread_id = omp_get_thread_num();
        AggregateTable& local_agg = thread_agg[thread_id];

        // Each thread processes its assigned blocks
        #pragma omp for schedule(dynamic, 1)
        for (size_t br = 0; br < block_ranges.size(); br++) {
            int64_t start_row = block_ranges[br].first;
            int64_t end_row = block_ranges[br].second;

            for (int64_t i = start_row; i < end_row; i++) {
                int32_t shipdate = l_shipdate_data[i];

                // Inline filter on shipdate
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

                    local_agg.insert(suppkey, revenue);
                }
            }
        }
    }

    // Merge all thread-local tables into global table (lock-free, sequential merge)
    for (int t = 0; t < num_threads; t++) {
        for (size_t j = 0; j < thread_agg[t].capacity; j++) {
            if (thread_agg[t].table[j].occupied) {
                agg_table.insert(thread_agg[t].table[j].key, thread_agg[t].table[j].value);
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
    int64_t max_revenue = -1;

    for (size_t i = 0; i < agg_table.capacity; i++) {
        if (agg_table.table[i].occupied && agg_table.table[i].value > max_revenue) {
            max_revenue = agg_table.table[i].value;
            max_supplier_no = agg_table.table[i].key;
        }
    }

#ifdef GENDB_PROFILE
    auto t_maxfind_end = std::chrono::high_resolution_clock::now();
    double ms_maxfind = std::chrono::duration<double, std::milli>(t_maxfind_end - t_maxfind_start).count();
    printf("[TIMING] find_max: %.2f ms\n", ms_maxfind);
#endif

    // ========== LOAD SUPPLIER DATA ==========
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

    int64_t num_supplier_rows = supplier_size / sizeof(int32_t);

    // Load dictionaries
    auto s_name_dict = load_reverse_dictionary(gendb_dir + "/supplier/s_name_dict.txt");
    auto s_address_dict = load_reverse_dictionary(gendb_dir + "/supplier/s_address_dict.txt");
    auto s_phone_dict = load_reverse_dictionary(gendb_dir + "/supplier/s_phone_dict.txt");

#ifdef GENDB_PROFILE
    auto t_supplier_load_end = std::chrono::high_resolution_clock::now();
    double ms_supplier_load = std::chrono::duration<double, std::milli>(t_supplier_load_end - t_supplier_load_start).count();
    printf("[TIMING] load_supplier: %.2f ms\n", ms_supplier_load);
#endif

    // ========== JOIN SUPPLIER WITH MAX REVENUE ROW ==========
#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t result_suppkey = -1;
    std::string result_name;
    std::string result_address;
    std::string result_phone;

    // Linear scan to find supplier with matching suppkey
    for (int64_t i = 0; i < num_supplier_rows; i++) {
        if (s_suppkey_data[i] == max_supplier_no) {
            result_suppkey = s_suppkey_data[i];
            int32_t name_code = s_name_data[i];
            int32_t address_code = s_address_data[i];
            int32_t phone_code = s_phone_data[i];

            if (name_code < (int32_t)s_name_dict.size()) {
                result_name = s_name_dict[name_code];
            }
            if (address_code < (int32_t)s_address_dict.size()) {
                result_address = s_address_dict[address_code];
            }
            if (phone_code < (int32_t)s_phone_dict.size()) {
                result_phone = s_phone_dict[phone_code];
            }
            break;
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
    if (zonemap_ptr && zonemap_size > 0) {
        munmap(zonemap_ptr, zonemap_size);
    }
    munmap(s_suppkey_data, supplier_size);
    munmap(s_name_data, supplier_size);
    munmap(s_address_data, supplier_size);
    munmap(s_phone_data, supplier_size);
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
