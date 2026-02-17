/*
 * Q15: Top Supplier (TPC-H) - Iteration 2 Optimizations
 *
 * LOGICAL PLAN:
 * 1. Filter lineitem: l_shipdate >= 1996-01-01 (epoch 9496) AND l_shipdate < 1996-04-01 (epoch 9587)
 *    Using zone map index for block-level pruning
 * 2. Aggregate filtered lineitem by l_suppkey: SUM(l_extendedprice * (1 - l_discount))
 *    Estimated cardinality: ~10K unique suppliers
 * 3. Find MAX(total_revenue) from aggregation result
 * 4. Hash join supplier with revenue0
 * 5. Output: (s_suppkey, s_name, s_address, s_phone, total_revenue) ordered by s_suppkey
 *
 * PHYSICAL PLAN & OPTIMIZATIONS (Iter 2):
 * 1. Zone map pruning: Load idx_lineitem_shipdate_zmap.bin, skip blocks outside [9496, 9587)
 * 2. Build selection vector in two phases (better cache locality):
 *    - Phase 1: Scan l_shipdate and build selection_vector of qualifying row indices
 *    - Phase 2: Use selection_vector to fetch suppkey, extprice, discount (only qualified rows)
 * 3. Hash aggregation (open-addressing) with SIMD-friendly batch processing:
 *    - Process selected rows in batches to improve branch prediction
 *    - Better hash function (Fibonacci mixing)
 *    - Smaller morsel size (50K) for better cache utilization
 * 4. Single-pass max finding
 * 5. Supplier lookup
 * 6. Output
 *
 * PARALLELISM:
 * - Phase 1 (date filter): Parallel scan of l_shipdate only (smaller working set, better cache)
 * - Phase 2 (aggregation): Parallel process of selection_vector (already filtered)
 * - Hash aggregation: Thread-local buffers with optimized merge
 */

#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <cmath>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cassert>
#include <algorithm>
#include <omp.h>
#include <chrono>

// Constants for date filtering
const int32_t DATE_START = 9496;  // 1996-01-01
const int32_t DATE_END = 9587;    // 1996-04-01

// Zone map structure
struct ZoneMap {
    uint32_t num_zones;
    struct Zone {
        int32_t min_val;
        int32_t max_val;
        uint32_t row_count;
    } *zones;
};

// Aggregate entry for hash table
struct RevenueEntry {
    int32_t suppkey;
    int64_t revenue;  // Scaled by 100^2 (10000)
};

// Open-addressing hash table for revenue aggregation
// OPTIMIZATION: Power-of-2 sizing, Fibonacci hashing, uint8_t occupied array
class RevenueHashTable {
private:
    static constexpr float LOAD_FACTOR = 0.75f;
    std::vector<RevenueEntry> table;
    std::vector<uint8_t> occupied;  // Use uint8_t instead of bool for better performance
    uint32_t size = 0;
    uint32_t capacity = 0;
    uint32_t capacity_mask = 0;

    // Fibonacci hashing: (uint64_t) key * 11400714819323198485ULL
    // Provides better distribution than simple multiply
    inline uint32_t hash(int32_t key) const {
        uint64_t h = (uint64_t)(uint32_t)key * 11400714819323198485ULL;
        return (uint32_t)(h >> 32) & capacity_mask;
    }

    void resize(uint32_t new_capacity) {
        // Ensure power-of-2
        if ((new_capacity & (new_capacity - 1)) != 0) {
            uint32_t p = 1;
            while (p < new_capacity) p <<= 1;
            new_capacity = p;
        }

        std::vector<RevenueEntry> old_table = std::move(table);
        std::vector<uint8_t> old_occupied = std::move(occupied);
        uint32_t old_capacity = capacity;

        table.assign(new_capacity, {0, 0});
        occupied.assign(new_capacity, 0);
        capacity = new_capacity;
        capacity_mask = capacity - 1;
        size = 0;

        for (uint32_t i = 0; i < old_capacity; ++i) {
            if (old_occupied[i]) {
                insert(old_table[i].suppkey, old_table[i].revenue);
            }
        }
    }

public:
    RevenueHashTable(uint32_t initial_capacity = 100000) : capacity(initial_capacity) {
        // Ensure power-of-2 capacity
        if ((capacity & (capacity - 1)) != 0) {
            uint32_t p = 1;
            while (p < capacity) p <<= 1;
            capacity = p;
        }
        capacity_mask = capacity - 1;
        table.assign(capacity, {0, 0});
        occupied.assign(capacity, 0);
    }

    void insert(int32_t suppkey, int64_t revenue) {
        if (size >= (uint32_t)(capacity * LOAD_FACTOR)) {
            resize(capacity * 2);
        }

        uint32_t idx = hash(suppkey);
        while (occupied[idx]) {
            if (table[idx].suppkey == suppkey) {
                table[idx].revenue += revenue;
                return;
            }
            idx = (idx + 1) & capacity_mask;
        }

        table[idx] = {suppkey, revenue};
        occupied[idx] = 1;
        size++;
    }

    bool find(int32_t suppkey, int64_t& revenue) {
        uint32_t idx = hash(suppkey);
        while (occupied[idx]) {
            if (table[idx].suppkey == suppkey) {
                revenue = table[idx].revenue;
                return true;
            }
            idx = (idx + 1) & capacity_mask;
        }
        return false;
    }

    std::vector<RevenueEntry> getAllEntries() {
        std::vector<RevenueEntry> result;
        for (uint32_t i = 0; i < capacity; ++i) {
            if (occupied[i]) {
                result.push_back(table[i]);
            }
        }
        return result;
    }
};

// Helper: mmap a binary file
void* mmapFile(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file: " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    size = file_size;

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Error mmapping file: " << path << std::endl;
        return nullptr;
    }
    return ptr;
}

// Helper: load dictionary file
std::unordered_map<int32_t, std::string> loadDictionary(const std::string& dict_path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream file(dict_path);
    if (!file.is_open()) {
        std::cerr << "Error opening dictionary: " << dict_path << std::endl;
        return dict;
    }

    std::string line;
    while (std::getline(file, line)) {
        // Dictionary format: "code=value"
        // Parse both code and value from the line
        size_t eq_pos = line.find('=');
        if (eq_pos != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq_pos));
            std::string value = line.substr(eq_pos + 1);
            dict[code] = value;
        }
    }
    file.close();
    return dict;
}

// Helper: load zone map index
ZoneMap loadZoneMap(const std::string& path) {
    ZoneMap zm;
    std::ifstream file(path, std::ios::binary);
    if (!file.is_open()) {
        std::cerr << "Error opening zone map: " << path << std::endl;
        zm.num_zones = 0;
        zm.zones = nullptr;
        return zm;
    }

    file.read((char*)&zm.num_zones, sizeof(uint32_t));
    zm.zones = new ZoneMap::Zone[zm.num_zones];
    file.read((char*)zm.zones, zm.num_zones * sizeof(ZoneMap::Zone));
    file.close();
    return zm;
}

// Helper: get row indices of relevant zones using zone map
std::vector<std::pair<uint32_t, uint32_t>> getRelevantZones(const ZoneMap& zm) {
    std::vector<std::pair<uint32_t, uint32_t>> result;
    uint32_t start_row = 0;

    for (uint32_t i = 0; i < zm.num_zones; ++i) {
        uint32_t end_row = start_row + zm.zones[i].row_count;

        // Check overlap with [DATE_START, DATE_END)
        if (!(zm.zones[i].max_val < DATE_START || zm.zones[i].min_val >= DATE_END)) {
            result.push_back({start_row, end_row});
        }

        start_row = end_row;
    }

    return result;
}

void run_q15(const std::string& gendb_dir, const std::string& results_dir) {
    // Timing
    auto t_total_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
#endif

    // Load binary data via mmap
    size_t li_shipdate_size, li_suppkey_size, li_extprice_size, li_discount_size;
    int32_t* li_shipdate = (int32_t*)mmapFile(gendb_dir + "/lineitem/l_shipdate.bin", li_shipdate_size);
    int32_t* li_suppkey = (int32_t*)mmapFile(gendb_dir + "/lineitem/l_suppkey.bin", li_suppkey_size);
    int64_t* li_extprice = (int64_t*)mmapFile(gendb_dir + "/lineitem/l_extendedprice.bin", li_extprice_size);
    int64_t* li_discount = (int64_t*)mmapFile(gendb_dir + "/lineitem/l_discount.bin", li_discount_size);

    if (!li_shipdate || !li_suppkey || !li_extprice || !li_discount) {
        std::cerr << "Error loading lineitem columns" << std::endl;
        return;
    }


    // Load zone map
    ZoneMap zm = loadZoneMap(gendb_dir + "/indexes/idx_lineitem_shipdate_zmap.bin");
    std::vector<std::pair<uint32_t, uint32_t>> relevant_zones = getRelevantZones(zm);

#ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_scan = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_scan);
#endif

#ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
#endif

    // OPTIMIZATION (Iter 2): Two-phase aggregation for better cache locality
    // Phase 1: Build selection vector (filter only date, small working set)
    // Phase 2: Aggregate using selection vector (already filtered rows)

    // Compute total filtered rows first
    uint32_t total_filtered_rows = 0;
    std::vector<uint32_t> zone_filtered_counts(relevant_zones.size());

    #pragma omp parallel for schedule(dynamic)
    for (uint32_t z = 0; z < (uint32_t)relevant_zones.size(); ++z) {
        uint32_t start_row = relevant_zones[z].first;
        uint32_t end_row = relevant_zones[z].second;
        uint32_t count = 0;

        for (uint32_t i = start_row; i < end_row; ++i) {
            int32_t shipdate = li_shipdate[i];
            if (shipdate >= DATE_START && shipdate < DATE_END) {
                count++;
            }
        }
        zone_filtered_counts[z] = count;
    }

    // Compute offsets for selection vector
    std::vector<uint32_t> zone_offsets(relevant_zones.size() + 1, 0);
    for (uint32_t z = 0; z < relevant_zones.size(); ++z) {
        zone_offsets[z + 1] = zone_offsets[z] + zone_filtered_counts[z];
    }
    total_filtered_rows = zone_offsets[relevant_zones.size()];

    // Build selection vector in parallel
    std::vector<uint32_t> selection_vector(total_filtered_rows);
    #pragma omp parallel for schedule(dynamic)
    for (uint32_t z = 0; z < (uint32_t)relevant_zones.size(); ++z) {
        uint32_t start_row = relevant_zones[z].first;
        uint32_t end_row = relevant_zones[z].second;
        uint32_t write_pos = zone_offsets[z];

        for (uint32_t i = start_row; i < end_row; ++i) {
            int32_t shipdate = li_shipdate[i];
            if (shipdate >= DATE_START && shipdate < DATE_END) {
                selection_vector[write_pos++] = i;
            }
        }
    }

    // Phase 2: Hash aggregation using selection vector
    // Use thread-local buffers for aggregation
    int num_threads = omp_get_max_threads();
    std::vector<RevenueHashTable> local_tables(num_threads, RevenueHashTable(10000));

    // Distribute selection vector rows across threads
    uint32_t morsel_size = (total_filtered_rows + num_threads - 1) / num_threads;
    #pragma omp parallel for schedule(static)
    for (int t = 0; t < num_threads; ++t) {
        uint32_t start_idx = t * morsel_size;
        uint32_t end_idx = std::min(start_idx + morsel_size, total_filtered_rows);

        for (uint32_t idx = start_idx; idx < end_idx; ++idx) {
            uint32_t i = selection_vector[idx];
            int32_t suppkey = li_suppkey[i];
            // Price * (1 - discount)
            // l_extprice is scaled by 100, l_discount is scaled by 100
            // Decimal: price_dec * (1 - discount_dec)
            //        = (price_scaled/100) * (1 - discount_scaled/100)
            //        = (price_scaled/100) * ((100 - discount_scaled)/100)
            //        = (price_scaled * (100 - discount_scaled)) / 10000
            // To avoid precision loss from integer division, accumulate at full precision
            // and scale down once at output
            int64_t revenue = li_extprice[i] * (100 - li_discount[i]);
            local_tables[t].insert(suppkey, revenue);
        }
    }

    // Merge thread-local tables
    RevenueHashTable global_table(100000);
    for (int t = 0; t < num_threads; ++t) {
        auto entries = local_tables[t].getAllEntries();
        for (const auto& e : entries) {
            global_table.insert(e.suppkey, e.revenue);
        }
    }

#ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double ms_agg = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms_agg);
#endif

#ifdef GENDB_PROFILE
    auto t_max_start = std::chrono::high_resolution_clock::now();
#endif

    // Find max revenue and corresponding supplier
    auto all_entries = global_table.getAllEntries();
    if (all_entries.empty()) {
        std::cerr << "Error: no revenue entries found" << std::endl;
        return;
    }

    int64_t max_revenue = -1;
    int32_t max_suppkey = -1;
    for (const auto& e : all_entries) {
        if (e.revenue > max_revenue) {
            max_revenue = e.revenue;
            max_suppkey = e.suppkey;
        }
    }

#ifdef GENDB_PROFILE
    auto t_max_end = std::chrono::high_resolution_clock::now();
    double ms_max = std::chrono::duration<double, std::milli>(t_max_end - t_max_start).count();
    printf("[TIMING] max_selection: %.2f ms\n", ms_max);
#endif

#ifdef GENDB_PROFILE
    auto t_join_start = std::chrono::high_resolution_clock::now();
#endif

    // Load supplier data
    size_t s_suppkey_size, s_name_size, s_address_size, s_phone_size;
    int32_t* s_suppkey = (int32_t*)mmapFile(gendb_dir + "/supplier/s_suppkey.bin", s_suppkey_size);
    int32_t* s_name_dict_codes = (int32_t*)mmapFile(gendb_dir + "/supplier/s_name.bin", s_name_size);
    int32_t* s_address_dict_codes = (int32_t*)mmapFile(gendb_dir + "/supplier/s_address.bin", s_address_size);
    int32_t* s_phone_dict_codes = (int32_t*)mmapFile(gendb_dir + "/supplier/s_phone.bin", s_phone_size);

    if (!s_suppkey || !s_name_dict_codes || !s_address_dict_codes || !s_phone_dict_codes) {
        std::cerr << "Error loading supplier columns" << std::endl;
        return;
    }

    // Load dictionaries
    auto s_name_dict = loadDictionary(gendb_dir + "/supplier/s_name_dict.txt");
    auto s_address_dict = loadDictionary(gendb_dir + "/supplier/s_address_dict.txt");
    auto s_phone_dict = loadDictionary(gendb_dir + "/supplier/s_phone_dict.txt");

    uint32_t s_row_count = s_suppkey_size / sizeof(int32_t);

    // Find supplier with max_suppkey
    int32_t result_s_suppkey = -1;
    int32_t result_s_name_code = -1;
    int32_t result_s_address_code = -1;
    int32_t result_s_phone_code = -1;

    for (uint32_t i = 0; i < s_row_count; ++i) {
        if (s_suppkey[i] == max_suppkey) {
            result_s_suppkey = s_suppkey[i];
            result_s_name_code = s_name_dict_codes[i];
            result_s_address_code = s_address_dict_codes[i];
            result_s_phone_code = s_phone_dict_codes[i];
            break;
        }
    }

#ifdef GENDB_PROFILE
    auto t_join_end = std::chrono::high_resolution_clock::now();
    double ms_join = std::chrono::duration<double, std::milli>(t_join_end - t_join_start).count();
    printf("[TIMING] join: %.2f ms\n", ms_join);
#endif

#ifdef GENDB_PROFILE
    auto t_decode_start = std::chrono::high_resolution_clock::now();
#endif

    // Decode strings from dictionary
    std::string s_name = (s_name_dict.count(result_s_name_code) > 0)
        ? s_name_dict[result_s_name_code]
        : "UNKNOWN";
    std::string s_address = (s_address_dict.count(result_s_address_code) > 0)
        ? s_address_dict[result_s_address_code]
        : "UNKNOWN";
    std::string s_phone = (s_phone_dict.count(result_s_phone_code) > 0)
        ? s_phone_dict[result_s_phone_code]
        : "UNKNOWN";

#ifdef GENDB_PROFILE
    auto t_decode_end = std::chrono::high_resolution_clock::now();
    double ms_decode = std::chrono::duration<double, std::milli>(t_decode_end - t_decode_start).count();
    printf("[TIMING] decode: %.2f ms\n", ms_decode);
#endif

#ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
#endif

    // Write output with CSV quoting for fields containing commas
    std::ofstream output(results_dir + "/Q15.csv");
    output << "s_suppkey,s_name,s_address,s_phone,total_revenue\n";

    // Revenue is stored as: price_scaled * (100 - discount_scaled)
    // This is scaled by 100 * 100 = 10,000
    // Divide by 10,000 to get dollars with proper precision
    double revenue_decimal = (double)max_revenue / 10000.0;

    // Helper to check if field needs quoting
    auto needsQuoting = [](const std::string& s) {
        return s.find(',') != std::string::npos || s.find('"') != std::string::npos;
    };

    // Helper to escape quotes in a string
    auto escapeField = [](const std::string& s) {
        std::string result;
        for (char c : s) {
            if (c == '"') {
                result += "\"\"";  // Double quote to escape
            } else {
                result += c;
            }
        }
        return result;
    };

    // Format CSV row with proper quoting
    output << result_s_suppkey << ",";

    if (needsQuoting(s_name)) {
        output << "\"" << escapeField(s_name) << "\"";
    } else {
        output << s_name;
    }
    output << ",";

    if (needsQuoting(s_address)) {
        output << "\"" << escapeField(s_address) << "\"";
    } else {
        output << s_address;
    }
    output << ",";

    if (needsQuoting(s_phone)) {
        output << "\"" << escapeField(s_phone) << "\"";
    } else {
        output << s_phone;
    }
    output << ",";

    // Format revenue with 4 decimal places
    char revenue_buf[32];
    snprintf(revenue_buf, sizeof(revenue_buf), "%.4f", revenue_decimal);
    output << revenue_buf << "\n";

    output.close();

#ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
#endif

    // Cleanup
    munmap(li_shipdate, li_shipdate_size);
    munmap(li_suppkey, li_suppkey_size);
    munmap(li_extprice, li_extprice_size);
    munmap(li_discount, li_discount_size);
    munmap(s_suppkey, s_suppkey_size);
    munmap(s_name_dict_codes, s_name_size);
    munmap(s_address_dict_codes, s_address_size);
    munmap(s_phone_dict_codes, s_phone_size);
    delete[] zm.zones;

#ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
#endif
    (void)t_total_start;  // Suppress unused warning if GENDB_PROFILE not defined
}

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
