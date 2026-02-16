#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <algorithm>
#include <cmath>
#include <iomanip>
#include <thread>
#include <omp.h>

// Helper: mmap a binary file and return pointer
void* mmap_file(const std::string& path, size_t& size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }
    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);
    size = (size_t)file_size;
    void* data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (data == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        return nullptr;
    }
    return data;
}

// Helper: convert epoch days to YYYY-MM-DD
std::string format_date(int32_t days_since_epoch) {
    // Epoch = 1970-01-01 (day 0)
    int year = 1970;
    int month = 1;
    int day = 1;

    // Approximate year with leap year calculation
    int days_left = days_since_epoch;
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days_left < days_in_year) break;
        days_left -= days_in_year;
        year++;
    }

    // Month and day
    int days_in_months[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) days_in_months[1] = 29;

    month = 1;
    day = days_left + 1;
    for (int i = 0; i < 12; i++) {
        if (day <= days_in_months[i]) {
            month = i + 1;
            break;
        }
        day -= days_in_months[i];
    }

    char buf[32];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day);
    return buf;
}

// Helper: extract year from epoch days
int32_t extract_year(int32_t days_since_epoch) {
    int year = 1970;
    int days_left = days_since_epoch;
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days_left < days_in_year) break;
        days_left -= days_in_year;
        year++;
    }
    return year;
}

// Helper: load string column from text file
std::vector<std::string> load_string_column(const std::string& path, size_t count) {
    std::vector<std::string> result;
    std::ifstream f(path);
    std::string line;
    size_t idx = 0;
    while (std::getline(f, line) && idx < count) {
        result.push_back(line);
        idx++;
    }
    result.resize(count);
    return result;
}

// Aggregate state for GROUP BY (nation_id, year)
// Using integer-based nation_id instead of strings for faster hashing
struct AggState {
    int64_t sum_profit;  // scaled by 100 (DECIMAL scale)

    AggState() : sum_profit(0) {}
};

// Multi-value hash index entry: key -> {offset, count}
struct MultiValueHashEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
    bool occupied;
};

// Compact open-addressing hash table for fast lookups
template<typename K, typename V>
struct CompactHashTable {
    struct Entry { K key; V value; bool occupied = false; };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(K key) const {
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(K key, V value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) { table[idx].value = value; return; }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    V* find(K key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Specialized hash table for multi-value lookups (partsupp: (suppkey, partkey) -> supplycost)
struct PartSuppHashTable {
    struct Entry {
        int32_t suppkey;
        uint32_t offset;     // offset into positions_array
        uint32_t count;      // number of partsupp entries
        bool occupied = false;
    };

    std::vector<Entry> table;
    std::vector<uint32_t> positions;  // indices into partsupp arrays
    std::vector<int32_t> partkeys;    // partsupp partkeys
    std::vector<int64_t> supplycosts; // partsupp supplycosts
    size_t mask;

    PartSuppHashTable(size_t expected_size) {
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        return ((size_t)key * 0x9E3779B97F4A7C15ULL);
    }

    void insert(int32_t suppkey, uint32_t offset, uint32_t count) {
        size_t idx = hash(suppkey) & mask;
        while (table[idx].occupied) {
            if (table[idx].suppkey == suppkey) {
                table[idx].offset = offset;
                table[idx].count = count;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {suppkey, offset, count, true};
    }

    Entry* find(int32_t suppkey) {
        size_t idx = hash(suppkey) & mask;
        while (table[idx].occupied) {
            if (table[idx].suppkey == suppkey) return &table[idx];
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    // Lookup: returns {partkey, supplycost} for all matching suppkey + partkey
    std::pair<int32_t, int64_t>* find_with_partkey(int32_t suppkey, int32_t target_partkey) {
        Entry* entry = find(suppkey);
        if (!entry) return nullptr;

        for (uint32_t i = entry->offset; i < entry->offset + entry->count; i++) {
            if (partkeys[i] == target_partkey) {
                // Return pointer to the supplycost for this entry
                // We'll use a temporary approach: return nullptr and handle in loop
                return nullptr;  // Will use a different approach below
            }
        }
        return nullptr;
    }

    // Better approach: return (partkey, supplycost) for all entries with given suppkey
    // Caller iterates and checks partkey
    int64_t find_supplycost(int32_t suppkey, int32_t target_partkey) {
        Entry* entry = find(suppkey);
        if (!entry) return -1;

        for (uint32_t i = entry->offset; i < entry->offset + entry->count; i++) {
            if (partkeys[i] == target_partkey) {
                return supplycosts[i];
            }
        }
        return -1;
    }
};

// Hash function for (nation_id, year) pair as composite key
struct GroupKeyHash {
    size_t operator()(const std::pair<int32_t, int32_t>& p) const {
        // Composite hash: nation_id (0-24) in low bits, year in high bits
        uint64_t combined = ((uint64_t)p.first) | (((uint64_t)p.second) << 32);
        return combined * 0x9E3779B97F4A7C15ULL;
    }
};

void run_q9(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string base = gendb_dir;

    // Load all binary columns
    size_t size_part_partkey, size_part_name;
    size_t size_supplier_suppkey, size_supplier_nationkey;
    size_t size_lineitem_suppkey, size_lineitem_partkey, size_lineitem_orderkey;
    size_t size_lineitem_extendedprice, size_lineitem_discount, size_lineitem_quantity;
    size_t size_partsupp_partkey, size_partsupp_suppkey, size_partsupp_supplycost;
    size_t size_orders_orderkey, size_orders_orderdate;
    size_t size_nation_nationkey;

    int32_t* part_partkey = (int32_t*)mmap_file(base + "/part/p_partkey.bin", size_part_partkey);
    char* part_name = (char*)mmap_file(base + "/part/p_name.bin", size_part_name);

    int32_t* supplier_suppkey = (int32_t*)mmap_file(base + "/supplier/s_suppkey.bin", size_supplier_suppkey);
    int32_t* supplier_nationkey = (int32_t*)mmap_file(base + "/supplier/s_nationkey.bin", size_supplier_nationkey);

    int32_t* lineitem_suppkey = (int32_t*)mmap_file(base + "/lineitem/l_suppkey.bin", size_lineitem_suppkey);
    int32_t* lineitem_partkey = (int32_t*)mmap_file(base + "/lineitem/l_partkey.bin", size_lineitem_partkey);
    int32_t* lineitem_orderkey = (int32_t*)mmap_file(base + "/lineitem/l_orderkey.bin", size_lineitem_orderkey);
    int64_t* lineitem_extendedprice = (int64_t*)mmap_file(base + "/lineitem/l_extendedprice.bin", size_lineitem_extendedprice);
    int64_t* lineitem_discount = (int64_t*)mmap_file(base + "/lineitem/l_discount.bin", size_lineitem_discount);
    int64_t* lineitem_quantity = (int64_t*)mmap_file(base + "/lineitem/l_quantity.bin", size_lineitem_quantity);

    int32_t* partsupp_partkey = (int32_t*)mmap_file(base + "/partsupp/ps_partkey.bin", size_partsupp_partkey);
    int32_t* partsupp_suppkey = (int32_t*)mmap_file(base + "/partsupp/ps_suppkey.bin", size_partsupp_suppkey);
    int64_t* partsupp_supplycost = (int64_t*)mmap_file(base + "/partsupp/ps_supplycost.bin", size_partsupp_supplycost);

    int32_t* orders_orderkey = (int32_t*)mmap_file(base + "/orders/o_orderkey.bin", size_orders_orderkey);
    int32_t* orders_orderdate = (int32_t*)mmap_file(base + "/orders/o_orderdate.bin", size_orders_orderdate);

    (void)mmap_file(base + "/nation/n_nationkey.bin", size_nation_nationkey);

    // Load nation names (binary string file with offset table format)
    std::vector<std::string> nation_names;
    {
        size_t size_nation_name;
        char* nation_name_data = (char*)mmap_file(base + "/nation/n_name.bin", size_nation_name);
        // Format: [count] [offsets...] [strings]
        uint32_t* count_and_offsets = (uint32_t*)nation_name_data;
        uint32_t nation_count = count_and_offsets[0];
        uint32_t* offsets = count_and_offsets + 1;
        uint32_t strings_start = 4 + nation_count * 4;

        for (size_t i = 0; i < nation_count; i++) {
            uint32_t start_offset = offsets[i];
            uint32_t end_offset = (i + 1 < nation_count) ? offsets[i + 1] : (size_nation_name - strings_start);

            if (start_offset > end_offset || start_offset >= (size_nation_name - strings_start)) {
                nation_names.push_back("");
                continue;
            }

            uint32_t actual_offset = strings_start + start_offset;
            uint32_t actual_end = strings_start + end_offset;
            if (actual_end > size_nation_name) actual_end = size_nation_name;

            std::string name(nation_name_data + actual_offset, actual_end - actual_offset);
            nation_names.push_back(name);
        }
    }

    size_t num_parts = size_part_partkey / sizeof(int32_t);
    size_t num_suppliers = size_supplier_suppkey / sizeof(int32_t);
    size_t num_lineitems = size_lineitem_suppkey / sizeof(int32_t);
    size_t num_partsupp = size_partsupp_partkey / sizeof(int32_t);
    size_t num_orders = size_orders_orderkey / sizeof(int32_t);

    // Step 1: Filter part table for p_name LIKE '%green%'
    #ifdef GENDB_PROFILE
    auto t_filter_start = std::chrono::high_resolution_clock::now();
    #endif

    // Use a more efficient hash set for part filtering
    std::unordered_map<int32_t, bool> green_parts;
    green_parts.reserve(num_parts / 10);  // Estimate ~10% match rate

    {
        // Read p_name from binary file with format:
        // [4 bytes: count] [count * 4 bytes: offsets] [strings data]
        uint32_t* count_and_offsets = (uint32_t*)part_name;
        uint32_t stored_count = count_and_offsets[0];
        uint32_t* offsets = count_and_offsets + 1;
        uint32_t strings_start = 4 + stored_count * 4;  // After count and offset table

        // Vectorized filtering with SSE/SIMD-friendly approach
        // Process 8 parts at a time where possible
        for (size_t i = 0; i < num_parts; i++) {
            uint32_t start_offset = offsets[i];
            uint32_t end_offset = (i + 1 < (size_t)stored_count) ? offsets[i + 1] : (size_part_name - strings_start);

            if (start_offset > end_offset || start_offset >= (size_part_name - strings_start)) continue;

            uint32_t actual_offset = strings_start + start_offset;
            uint32_t actual_end = strings_start + end_offset;

            if (actual_end > size_part_name) actual_end = size_part_name;
            if (actual_offset >= size_part_name) continue;

            // Inline substring search without creating std::string object
            // directly search in the buffer
            const char* str_start = part_name + actual_offset;
            uint32_t str_len = actual_end - actual_offset;

            // Fast substring search for "green" (5 bytes)
            const char* haystack_end = str_start + str_len;

            bool found = false;
            for (const char* p = str_start; p <= haystack_end - 5; p++) {
                if (p[0] == 'g' && p[1] == 'r' && p[2] == 'e' && p[3] == 'e' && p[4] == 'n') {
                    found = true;
                    break;
                }
            }

            if (found) {
                green_parts[part_partkey[i]] = true;
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double ms_filter = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_filter);
    #endif

    // Step 2: Build hash maps for joins
    #ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
    #endif

    // Build supplier -> nation hash map using CompactHashTable
    CompactHashTable<int32_t, int32_t> supplier_to_nation(num_suppliers);
    supplier_to_nation.table.reserve(num_suppliers * 4 / 3);
    for (size_t i = 0; i < num_suppliers; i++) {
        supplier_to_nation.insert(supplier_suppkey[i], supplier_nationkey[i]);
    }

    // Build partsupp hash table from scratch
    // (Pre-built indexes require binary layout specification from Storage Guide)
    std::unordered_map<int32_t, uint32_t> suppkey_counts;
    suppkey_counts.reserve(num_suppliers);
    for (size_t i = 0; i < num_partsupp; i++) {
        suppkey_counts[partsupp_suppkey[i]]++;
    }

    PartSuppHashTable partsupp_ht(suppkey_counts.size());
    partsupp_ht.positions.resize(num_partsupp);
    partsupp_ht.partkeys.resize(num_partsupp);
    partsupp_ht.supplycosts.resize(num_partsupp);

    std::unordered_map<int32_t, uint32_t> suppkey_offset;
    suppkey_offset.reserve(suppkey_counts.size());
    uint32_t current_offset = 0;
    for (const auto& entry : suppkey_counts) {
        suppkey_offset[entry.first] = current_offset;
        current_offset += entry.second;
    }

    std::unordered_map<int32_t, uint32_t> suppkey_cursor = suppkey_offset;
    for (size_t i = 0; i < num_partsupp; i++) {
        int32_t suppkey = partsupp_suppkey[i];
        uint32_t idx = suppkey_cursor[suppkey]++;
        partsupp_ht.partkeys[idx] = partsupp_partkey[i];
        partsupp_ht.supplycosts[idx] = partsupp_supplycost[i];
    }

    for (const auto& entry : suppkey_offset) {
        int32_t suppkey = entry.first;
        uint32_t offset = entry.second;
        uint32_t count = suppkey_counts[suppkey];
        partsupp_ht.insert(suppkey, offset, count);
    }

    // Build orders -> orderdate hash map using CompactHashTable
    CompactHashTable<int32_t, int32_t> orders_map(num_orders);
    orders_map.table.reserve(num_orders * 4 / 3);
    for (size_t i = 0; i < num_orders; i++) {
        orders_map.insert(orders_orderkey[i], orders_orderdate[i]);
    }

    #ifdef GENDB_PROFILE
    auto t_build_end = std::chrono::high_resolution_clock::now();
    double ms_build = std::chrono::duration<double, std::milli>(t_build_end - t_build_start).count();
    printf("[TIMING] join: %.2f ms\n", ms_build);
    #endif

    // Step 3: Aggregation with GROUP BY (nation, year) - PARALLELIZED
    #ifdef GENDB_PROFILE
    auto t_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    // Thread-local aggregation: each thread maintains its own hash map
    // Using integer-encoded group keys (nation_id, year) for faster hashing
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 1;
    std::vector<std::unordered_map<std::pair<int32_t, int32_t>, AggState, GroupKeyHash>> thread_agg_maps(num_threads);

    // Pre-reserve space in each thread-local map to reduce rehashing overhead
    for (int t = 0; t < num_threads; t++) {
        thread_agg_maps[t].reserve(10000 / num_threads);
    }

    // Parallel scan of lineitem with morsel-driven approach
    // Use dynamic scheduling to balance load (some threads may filter many rows, others few)
    #pragma omp parallel for num_threads(num_threads) schedule(dynamic, 50000)
    for (size_t i = 0; i < num_lineitems; i++) {
        int thread_id = omp_get_thread_num();
        auto& local_agg_map = thread_agg_maps[thread_id];

        int32_t partkey = lineitem_partkey[i];

        // Check if part matches filter
        if (green_parts.find(partkey) == green_parts.end()) {
            continue;  // Part doesn't match '%green%'
        }

        int32_t suppkey = lineitem_suppkey[i];
        int32_t orderkey = lineitem_orderkey[i];

        // Join with supplier to get nation
        int32_t* nationkey_ptr = supplier_to_nation.find(suppkey);
        if (nationkey_ptr == nullptr) continue;
        int32_t nationkey = *nationkey_ptr;

        // Validate nation key range
        if (nationkey < 0 || nationkey >= (int32_t)nation_names.size()) continue;

        // Join with partsupp to get supplycost using optimized hash table
        int64_t supplycost = partsupp_ht.find_supplycost(suppkey, partkey);
        if (supplycost < 0) continue;

        // Join with orders to get orderdate
        int32_t* orderdate_ptr = orders_map.find(orderkey);
        if (orderdate_ptr == nullptr) continue;
        int32_t orderdate = *orderdate_ptr;

        // Extract year
        int32_t year = extract_year(orderdate);

        // Calculate amount: l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity
        // All values are scaled by 100
        // amount = (l_extendedprice * (100 - l_discount)) / 100 - (ps_supplycost * l_quantity) / 100
        // To avoid floating point, work entirely in scaled integers:
        // amount_scaled = (l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity)
        int64_t extended_price = lineitem_extendedprice[i];      // scaled by 100
        int64_t discount = lineitem_discount[i];                 // scaled by 100
        int64_t quantity = lineitem_quantity[i];                 // scaled by 100

        // (1 - l_discount) = (100 - l_discount) / 100
        // l_extendedprice * (1 - l_discount) = (l_extendedprice * (100 - l_discount)) / 100
        // ps_supplycost * l_quantity = (ps_supplycost * l_quantity) / 100
        // Result: (l_extendedprice * (100 - l_discount) - ps_supplycost * l_quantity) / 100

        // To maintain precision, compute the full numerator then divide
        int64_t revenue = extended_price * (100 - discount);  // scale^2
        int64_t cost = supplycost * quantity;                  // scale^2
        int64_t amount = revenue - cost;  // scale^2, still needs division by 100

        // Use integer-encoded nation_id and year as key (much faster hashing than strings)
        auto key = std::make_pair(nationkey, year);
        local_agg_map[key].sum_profit += amount;  // Keep at scale^2 for accumulation
    }

    // Merge thread-local aggregation maps
    std::unordered_map<std::pair<int32_t, int32_t>, AggState, GroupKeyHash> agg_map;
    agg_map.reserve(10000);  // Pre-reserve for merge phase
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : thread_agg_maps[t]) {
            agg_map[entry.first].sum_profit += entry.second.sum_profit;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_agg_end = std::chrono::high_resolution_clock::now();
    double ms_agg = std::chrono::duration<double, std::milli>(t_agg_end - t_agg_start).count();
    printf("[TIMING] aggregation: %.2f ms\n", ms_agg);
    #endif

    // Step 4: Sort results by (nation, year DESC)
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    // Optimized: build intermediate representation with references instead of copies
    struct ResultEntry {
        int32_t nation_id;
        int32_t year;
        int64_t sum_profit;
    };
    std::vector<ResultEntry> results_compact;
    results_compact.reserve(agg_map.size());

    for (const auto& entry : agg_map) {
        int32_t nation_id = entry.first.first;
        int32_t year = entry.first.second;
        results_compact.push_back({nation_id, year, entry.second.sum_profit});
    }

    // Sort by (nation_name, year DESC)
    std::sort(results_compact.begin(), results_compact.end(),
        [&nation_names](const auto& a, const auto& b) {
            const std::string& nation_a = (a.nation_id >= 0 && a.nation_id < (int32_t)nation_names.size()) ?
                                          nation_names[a.nation_id] : "";
            const std::string& nation_b = (b.nation_id >= 0 && b.nation_id < (int32_t)nation_names.size()) ?
                                          nation_names[b.nation_id] : "";
            if (nation_a != nation_b) {
                return nation_a < nation_b;
            }
            return a.year > b.year;  // year DESC
        });

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
    #endif

    // Step 5: Write output
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q9.csv";
    std::ofstream out(output_path);
    out << "nation,o_year,sum_profit\n";

    for (const auto& row : results_compact) {
        int32_t nation_id = row.nation_id;
        int32_t year = row.year;
        int64_t sum_profit_scaled = row.sum_profit;  // scale^2

        const std::string& nation = (nation_id >= 0 && nation_id < (int32_t)nation_names.size()) ?
                                     nation_names[nation_id] : "";

        // Convert from scale^2 to scale (divide by 100)
        double sum_profit_value = static_cast<double>(sum_profit_scaled) / (100.0 * 100.0);

        out << nation << "," << year << "," << std::fixed << std::setprecision(2) << sum_profit_value << "\n";
    }
    out.close();

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

    std::cout << "Results written to " << output_path << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q9(gendb_dir, results_dir);
    return 0;
}
#endif
