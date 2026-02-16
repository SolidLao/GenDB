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

// Pre-built hash index metadata and data structures
struct PreBuiltHashIndex {
    uint32_t num_unique;
    uint32_t table_size;
    MultiValueHashEntry* table;  // Points into mmap'd region
    uint32_t* positions;         // Points into mmap'd region

    PreBuiltHashIndex() : num_unique(0), table_size(0), table(nullptr), positions(nullptr) {}

    // Load from mmap'd file
    static PreBuiltHashIndex* load_from_mmap(void* data, size_t size) {
        if (size < 8) return nullptr;
        uint32_t* header = (uint32_t*)data;
        uint32_t num_unique = header[0];
        uint32_t table_size = header[1];

        // Validate sizes
        size_t min_size = 8 + (size_t)table_size * sizeof(MultiValueHashEntry) + (size_t)num_unique * sizeof(uint32_t);
        if (size < min_size) return nullptr;

        PreBuiltHashIndex* idx = new PreBuiltHashIndex();
        idx->num_unique = num_unique;
        idx->table_size = table_size;
        idx->table = (MultiValueHashEntry*)((char*)data + 8);
        idx->positions = (uint32_t*)((char*)data + 8 + (size_t)table_size * sizeof(MultiValueHashEntry));
        return idx;
    }

    // Find entry by key using hash table
    MultiValueHashEntry* find(int32_t key) const {
        if (!table) return nullptr;
        size_t mask = table_size - 1;
        size_t hash_val = ((size_t)key * 0x9E3779B97F4A7C15ULL);
        size_t idx = hash_val & mask;

        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx];
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }

    // Get position array slice for a key
    uint32_t* get_positions(int32_t key, uint32_t& count) const {
        MultiValueHashEntry* entry = find(key);
        if (!entry) {
            count = 0;
            return nullptr;
        }
        count = entry->count;
        return positions + entry->offset;
    }
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

    std::unordered_map<int32_t, bool> green_parts;
    // p_name is variable-length strings stored in binary format
    // We need to scan through the file to find matching parts
    // For now, we'll use a simple approach: load all p_name strings
    // Note: p_name.bin contains variable-length strings, we need to process carefully
    // Since p_name is VARCHAR, it's stored with length prefix

    {
        // Read p_name from binary file with format:
        // [4 bytes: count] [count * 4 bytes: offsets] [strings data]
        uint32_t* count_and_offsets = (uint32_t*)part_name;
        uint32_t stored_count = count_and_offsets[0];
        uint32_t* offsets = count_and_offsets + 1;
        uint32_t strings_start = 4 + stored_count * 4;  // After count and offset table

        for (size_t i = 0; i < num_parts; i++) {
            uint32_t start_offset = offsets[i];
            uint32_t end_offset = (i + 1 < (size_t)stored_count) ? offsets[i + 1] : (size_part_name - strings_start);

            if (start_offset > end_offset || start_offset >= (size_part_name - strings_start)) continue;

            uint32_t actual_offset = strings_start + start_offset;
            uint32_t actual_end = strings_start + end_offset;

            if (actual_end > size_part_name) actual_end = size_part_name;
            if (actual_offset >= size_part_name) continue;

            std::string name(part_name + actual_offset, actual_end - actual_offset);

            // Check for '%green%' substring
            if (name.find("green") != std::string::npos) {
                green_parts[part_partkey[i]] = true;
            }
        }
    }


    #ifdef GENDB_PROFILE
    auto t_filter_end = std::chrono::high_resolution_clock::now();
    double ms_filter = std::chrono::duration<double, std::milli>(t_filter_end - t_filter_start).count();
    printf("[TIMING] scan_filter: %.2f ms\n", ms_filter);
    #endif

    // Precompute year cache before join phase to reuse in aggregation
    // This reduces 59M extract_year() calls to ~2400 lookups
    std::unordered_map<int32_t, int32_t> year_cache;
    for (size_t i = 0; i < num_orders; i++) {
        int32_t od = orders_orderdate[i];
        if (year_cache.find(od) == year_cache.end()) {
            year_cache[od] = extract_year(od);
        }
    }

    // Step 2: Build hash maps for joins
    #ifdef GENDB_PROFILE
    auto t_build_start = std::chrono::high_resolution_clock::now();
    #endif

    // Build supplier -> nation hash map using CompactHashTable
    CompactHashTable<int32_t, int32_t> supplier_to_nation(num_suppliers);
    for (size_t i = 0; i < num_suppliers; i++) {
        supplier_to_nation.insert(supplier_suppkey[i], supplier_nationkey[i]);
    }

    // Load pre-built partsupp_ps_suppkey_hash index via mmap
    // This avoids the expensive ~800ms manual build of the PartSuppHashTable
    size_t size_partsupp_hash;
    void* partsupp_hash_data = mmap_file(base + "/indexes/partsupp_ps_suppkey_hash.bin", size_partsupp_hash);
    PreBuiltHashIndex* partsupp_hash_idx = nullptr;
    if (partsupp_hash_data) {
        partsupp_hash_idx = PreBuiltHashIndex::load_from_mmap(partsupp_hash_data, size_partsupp_hash);
    }

    // If index loading failed, fall back to manual build
    PartSuppHashTable partsupp_ht(8000);  // Fallback only
    if (!partsupp_hash_idx) {
        // Step 1: Count unique suppkeys to pre-size hash table
        std::unordered_map<int32_t, uint32_t> suppkey_counts;
        for (size_t i = 0; i < num_partsupp; i++) {
            suppkey_counts[partsupp_suppkey[i]]++;
        }

        // Step 2: Create hash table and pre-allocate arrays
        partsupp_ht = PartSuppHashTable(suppkey_counts.size());
        partsupp_ht.positions.resize(num_partsupp);
        partsupp_ht.partkeys.resize(num_partsupp);
        partsupp_ht.supplycosts.resize(num_partsupp);

        // Step 3: Compute offsets for each suppkey
        std::unordered_map<int32_t, uint32_t> suppkey_offset;
        uint32_t current_offset = 0;
        for (const auto& entry : suppkey_counts) {
            suppkey_offset[entry.first] = current_offset;
            current_offset += entry.second;
        }

        // Step 4: Scatter data into arrays using cursors
        std::unordered_map<int32_t, uint32_t> suppkey_cursor = suppkey_offset;
        for (size_t i = 0; i < num_partsupp; i++) {
            int32_t suppkey = partsupp_suppkey[i];
            uint32_t idx = suppkey_cursor[suppkey]++;
            partsupp_ht.partkeys[idx] = partsupp_partkey[i];
            partsupp_ht.supplycosts[idx] = partsupp_supplycost[i];
        }

        // Step 5: Build hash table entries pointing to grouped data
        for (const auto& entry : suppkey_offset) {
            int32_t suppkey = entry.first;
            uint32_t offset = entry.second;
            uint32_t count = suppkey_counts[suppkey];
            partsupp_ht.insert(suppkey, offset, count);
        }
    }

    // Build orders -> orderdate hash map using CompactHashTable (parallel build)
    // Pre-size the hash table once
    CompactHashTable<int32_t, int32_t> orders_map(num_orders);

    // Parallel insertion into the hash table
    // Note: CompactHashTable is not thread-safe for concurrent insertions
    // Use a serial build for correctness, but could be optimized with partitioned tables per thread
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

    // Parallel scan of lineitem with morsel-driven approach
    #pragma omp parallel for num_threads(num_threads)
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

        // Join with partsupp to get supplycost using pre-built index or fallback
        int64_t supplycost = -1;
        if (partsupp_hash_idx) {
            // Use pre-built index: get position indices for this suppkey
            uint32_t count = 0;
            uint32_t* positions = partsupp_hash_idx->get_positions(suppkey, count);
            if (positions && count > 0) {
                // Linear search through positions to find matching partkey
                for (uint32_t pi = 0; pi < count; pi++) {
                    uint32_t pos = positions[pi];
                    if (pos < num_partsupp && partsupp_partkey[pos] == partkey) {
                        supplycost = partsupp_supplycost[pos];
                        break;
                    }
                }
            }
        } else {
            // Fallback: use manually built hash table
            supplycost = partsupp_ht.find_supplycost(suppkey, partkey);
        }
        if (supplycost < 0) continue;

        // Join with orders to get orderdate
        int32_t* orderdate_ptr = orders_map.find(orderkey);
        if (orderdate_ptr == nullptr) continue;
        int32_t orderdate = *orderdate_ptr;

        // Extract year from cache (much faster than calling extract_year 59M times)
        int32_t year = year_cache[orderdate];

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

    // Convert from integer-encoded keys (nation_id, year) to (nation_name, year) for output
    std::vector<std::pair<std::pair<std::string, int32_t>, AggState>> results;
    for (const auto& entry : agg_map) {
        int32_t nation_id = entry.first.first;
        int32_t year = entry.first.second;
        std::string nation_name = (nation_id >= 0 && nation_id < (int32_t)nation_names.size()) ?
                                   nation_names[nation_id] : "";
        results.push_back({{nation_name, year}, entry.second});
    }

    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            if (a.first.first != b.first.first) {
                return a.first.first < b.first.first;
            }
            return a.first.second > b.first.second;  // year DESC
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

    for (const auto& row : results) {
        std::string nation = row.first.first;
        int32_t year = row.first.second;
        int64_t sum_profit_scaled = row.second.sum_profit;  // scale^2

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
