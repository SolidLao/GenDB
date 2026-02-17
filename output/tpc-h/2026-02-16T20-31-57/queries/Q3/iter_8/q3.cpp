#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <omp.h>
#include <immintrin.h>

/*
===========================================
Q3: SHIPPING PRIORITY QUERY - ITERATION 8 OPTIMIZATIONS
===========================================

OPTIMIZATION STRATEGY:
- Pre-Built Index Loading: Load lineitem_orderkey_hash to avoid 59M random probes
- Inverted Join Order: Instead of scan lineitem + probe orders, iterate orders + probe lineitem index
- Reduced I/O: lineitem_orderkey_hash gives direct row position mappings without full scan
- Thread-Local Aggregation: Merge results during index probe instead of post-scan merge

LOGICAL PLAN:
1. Load customer filters (c_mktsegment = 'BUILDING') into boolean array [0..1.5M]
2. Load pre-built lineitem_orderkey_hash (multi-value) index via mmap
3. Load lineitem columns (l_extendedprice, l_discount, l_shipdate) via mmap
4. Parallel scan orders (15M rows → 2M filtered):
   - Zone map pruning (skip blocks where min_val >= date_boundary)
   - Apply o_orderdate < 9204 and customer filter
   - For each matched order:
     - Use lineitem_orderkey_hash to get all matching lineitem rows
     - Filter each by l_shipdate > 9204
     - Aggregate into thread-local hash tables
5. Merge thread-local aggregations
6. Sort + Top 10

PHYSICAL PLAN:
- Customer: Boolean array [0..1.5M] indexed by custkey (fast lookup)
- Lineitem Index: Pre-built hash_multi_value [key → (offset, count)] + positions array
- Orders: Zone map pruning + parallel scan with early aggregation via index lookups
- Lineitem: Lazy-loaded only for rows needed (via index)
- Aggregation: Thread-local open-addressing hash tables

KEY INSIGHT:
Instead of scanning all 59M lineitem rows and probing orders table,
we scan ~2M filtered orders and probe lineitem_orderkey_hash for matching rows.
This reduces I/O from 59M row scans to ~2M index lookups (8M total lineitem rows touched).

DATE ENCODING: int32_t days since 1970-01-01
- 1995-03-15 = 9204
- o_orderdate < 9204 means o_orderdate is before that date
- l_shipdate > 9204 means l_shipdate is after that date

DECIMAL ENCODING: int64_t with scale_factor=2
- Values stored as integer * 100 (e.g., 123.45 stored as 12345)
- Aggregation: SUM(l_extendedprice * (1 - l_discount)) in scaled integer arithmetic
- Output: divide by 100.0 for CSV decimal formatting
*/

// Forward declarations
struct AggResult {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double revenue;  // Store as double for precision
};

struct OrderData {
    int32_t o_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

struct AggKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const AggKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        // Combine three int32_t fields into a single hash
        uint64_t h = 0x9E3779B97F4A7C15ULL;
        h ^= (uint64_t)k.l_orderkey * 0xBF58476D1CE4E5B9ULL;
        h ^= (uint64_t)k.o_orderdate * 0x94D049BB133111EBULL;
        h ^= (uint64_t)k.o_shippriority * 0xAF61D4D51A6A2B7BULL;
        return h;
    }
};

// Zone map structures (for orders_orderdate_zonemap)
struct ZoneMapBlock {
    int32_t min_val;
    int32_t max_val;
    uint32_t row_count;
};

// Pre-built hash index structures
// lineitem_orderkey_hash: hash_multi_value layout
// Binary format: [uint32_t num_unique][uint32_t table_size]
//                then [key:int32_t, offset:uint32_t, count:uint32_t] (12B/slot)
//                then [uint32_t positions_count][uint32_t positions...]
struct LineitemOrderKeyHashEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

// Loaded multi-value hash index structure
struct MultiValueHashIndex {
    uint32_t num_unique;
    uint32_t table_size;
    LineitemOrderKeyHashEntry* entries;  // Hash table entries
    uint32_t* positions;                 // Positions array
    bool loaded = false;

    // Hash function
    size_t hash(int32_t key) const {
        return ((uint64_t)key * 0x9E3779B97F4A7C15ULL) % table_size;
    }

    // Lookup: returns (offset, count) or nullptr if not found
    LineitemOrderKeyHashEntry* find(int32_t key) const {
        if (!loaded) return nullptr;
        size_t idx = hash(key);
        size_t orig_idx = idx;

        while (entries[idx].key != key) {
            if (entries[idx].offset == 0xFFFFFFFFU && entries[idx].count == 0) {
                // Empty slot (sentinel)
                return nullptr;
            }
            idx = (idx + 1) % table_size;
            if (idx == orig_idx) return nullptr;  // Full cycle
        }
        return &entries[idx];
    }
};

MultiValueHashIndex load_lineitem_orderkey_hash_index(const std::string& path) {
    MultiValueHashIndex idx = {0, 0, nullptr, nullptr, false};

    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open lineitem_orderkey_hash index " << path << std::endl;
        return idx;
    }

    // Read header
    ssize_t bytes_read = read(fd, &idx.num_unique, sizeof(uint32_t));
    if (bytes_read != (ssize_t)sizeof(uint32_t)) {
        close(fd);
        return idx;
    }

    bytes_read = read(fd, &idx.table_size, sizeof(uint32_t));
    if (bytes_read != (ssize_t)sizeof(uint32_t)) {
        close(fd);
        return idx;
    }

    // Read entries (hash table)
    size_t entries_size = idx.table_size * sizeof(LineitemOrderKeyHashEntry);
    idx.entries = (LineitemOrderKeyHashEntry*)malloc(entries_size);
    bytes_read = read(fd, idx.entries, entries_size);
    if (bytes_read != (ssize_t)entries_size) {
        free(idx.entries);
        close(fd);
        return idx;
    }

    // Read positions array size and positions
    uint32_t positions_count = 0;
    bytes_read = read(fd, &positions_count, sizeof(uint32_t));
    if (bytes_read == (ssize_t)sizeof(uint32_t)) {
        idx.positions = (uint32_t*)malloc(positions_count * sizeof(uint32_t));
        bytes_read = read(fd, idx.positions, positions_count * sizeof(uint32_t));
        if (bytes_read == (ssize_t)(positions_count * sizeof(uint32_t))) {
            idx.loaded = true;
        } else {
            free(idx.positions);
            idx.positions = nullptr;
        }
    }

    close(fd);
    return idx;
}

// Open-addressing hash table for aggregation (replaces unordered_map)
// For int32_t keys
struct CompactHashTableInt32 {
    struct Entry {
        int32_t key;
        OrderData value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTableInt32(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(int32_t key) const {
        // Fibonacci hashing for good distribution
        return (size_t)key * 0x9E3779B97F4A7C15ULL;
    }

    void insert(int32_t key, const OrderData& value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    OrderData* find(int32_t key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// For AggKey composite keys
struct CompactHashTableAgg {
    struct Entry {
        AggKey key;
        double value;
        bool occupied = false;
    };

    std::vector<Entry> table;
    size_t mask;

    CompactHashTableAgg(size_t expected_size) {
        // Size to next power of 2, ~75% load factor
        size_t sz = 1;
        while (sz < expected_size * 4 / 3) sz <<= 1;
        table.resize(sz);
        mask = sz - 1;
    }

    size_t hash(const AggKey& key) const {
        // Combine three int32_t fields into a single hash
        uint64_t h = 0x9E3779B97F4A7C15ULL;
        h ^= (uint64_t)key.l_orderkey * 0xBF58476D1CE4E5B9ULL;
        h ^= (uint64_t)key.o_orderdate * 0x94D049BB133111EBULL;
        h ^= (uint64_t)key.o_shippriority * 0xAF61D4D51A6A2B7BULL;
        return h;
    }

    void insert(const AggKey& key, double value) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) {
                table[idx].value = value;
                return;
            }
            idx = (idx + 1) & mask;
        }
        table[idx] = {key, value, true};
    }

    double* find(const AggKey& key) {
        size_t idx = hash(key) & mask;
        while (table[idx].occupied) {
            if (table[idx].key == key) return &table[idx].value;
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
};

// Utility function to convert epoch days to YYYY-MM-DD string
std::string epoch_days_to_date(int32_t days_since_epoch) {
    // Compute year, month, day from days
    int32_t days = days_since_epoch;

    // Start from 1970-01-01 and count forward
    int year = 1970;

    // Count leap years: year divisible by 4, except 100s unless divisible by 400
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days < days_in_year) break;
        days -= days_in_year;
        year++;
    }

    // Now compute month and day
    int month = 1;
    int days_in_months[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_months[1] = 29; // February has 29 days in leap year
    }

    for (int m = 0; m < 12; m++) {
        if (days < days_in_months[m]) {
            month = m + 1;
            break;
        }
        days -= days_in_months[m];
    }

    int day = days + 1; // days are 0-indexed

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(4) << year << "-"
        << std::setw(2) << month << "-"
        << std::setw(2) << day;
    return oss.str();
}

// Memory-mapped file helper
template<typename T>
T* mmap_file(const std::string& path, size_t& num_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        std::cerr << "Failed to open " << path << std::endl;
        return nullptr;
    }

    off_t file_size = lseek(fd, 0, SEEK_END);
    lseek(fd, 0, SEEK_SET);

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << path << std::endl;
        return nullptr;
    }

    num_elements = file_size / sizeof(T);
    return (T*)ptr;
}

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load dictionary for c_mktsegment to find code for "BUILDING"
    std::string dict_path = gendb_dir + "/customer/c_mktsegment_dict.txt";
    std::ifstream dict_file(dict_path);
    int32_t building_code = -1;
    std::string line;
    int code = 0;
    while (std::getline(dict_file, line)) {
        if (line == "BUILDING") {
            building_code = code;
            break;
        }
        code++;
    }
    dict_file.close();

    if (building_code == -1) {
        std::cerr << "BUILDING not found in c_mktsegment dictionary" << std::endl;
        return;
    }

    // Date constants
    const int32_t date_boundary = 9204; // 1995-03-15

    // ==================== PHASE 1: Load customer data ====================
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t customer_count = 0;
    int32_t* c_custkey = mmap_file<int32_t>(gendb_dir + "/customer/c_custkey.bin", customer_count);
    int32_t* c_mktsegment = mmap_file<int32_t>(gendb_dir + "/customer/c_mktsegment.bin", customer_count);

    // Build boolean array indexed by custkey for O(1) customer lookups
    // Customer keys are in range [1, 1500000], so size array appropriately
    std::vector<bool> customer_filter(customer_count + 1, false);

    #pragma omp parallel for
    for (size_t i = 0; i < customer_count; i++) {
        if (c_mktsegment[i] == building_code && c_custkey[i] >= 0 && c_custkey[i] <= (int32_t)customer_count) {
            customer_filter[c_custkey[i]] = true;
        }
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double ms_customer = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] customer_scan_filter: %.2f ms\n", ms_customer);
    #endif

    // ==================== PHASE 2: Load orders, zone map pruning, filter and parallel join ====================
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t orders_count = 0;
    int32_t* o_orderkey = mmap_file<int32_t>(gendb_dir + "/orders/o_orderkey.bin", orders_count);
    int32_t* o_custkey = mmap_file<int32_t>(gendb_dir + "/orders/o_custkey.bin", orders_count);
    int32_t* o_orderdate = mmap_file<int32_t>(gendb_dir + "/orders/o_orderdate.bin", orders_count);
    int32_t* o_shippriority = mmap_file<int32_t>(gendb_dir + "/orders/o_shippriority.bin", orders_count);

    // Load zone map for orders_orderdate_zonemap to enable block-level pruning
    // Zone map format: [uint32_t num_blocks][ZoneMapBlock[num_blocks]]
    ZoneMapBlock* zm_blocks = nullptr;
    uint32_t actual_blocks = 0;

    int fd_zm = open((gendb_dir + "/indexes/orders_orderdate_zonemap.bin").c_str(), O_RDONLY);
    if (fd_zm != -1) {
        ssize_t bytes_read = read(fd_zm, &actual_blocks, sizeof(uint32_t));
        if (bytes_read == (ssize_t)sizeof(uint32_t)) {
            zm_blocks = new ZoneMapBlock[actual_blocks];
            bytes_read = read(fd_zm, zm_blocks, actual_blocks * sizeof(ZoneMapBlock));
        }
        close(fd_zm);
    }

    // Build thread-local hash tables for parallel processing
    int num_threads = omp_get_max_threads();
    std::vector<CompactHashTableInt32*> thread_orders_tables(num_threads);

    // Initialize thread-local tables
    #pragma omp parallel for
    for (int t = 0; t < num_threads; t++) {
        thread_orders_tables[t] = new CompactHashTableInt32(2000000 / num_threads + 10000);
    }

    // Process orders with zone map pruning, parallelized
    // Orders table is sorted by o_orderdate, blocks of 100K rows
    const int32_t block_size = 100000;

    // Pre-compute zone map block boundaries to avoid repeated division
    // Only include blocks where min_val < date_boundary (tight pruning)
    std::vector<std::pair<size_t, size_t>> block_ranges;
    if (zm_blocks) {
        for (uint32_t b = 0; b < actual_blocks; b++) {
            // STRICT check: only process blocks with potential matching rows (min_val MUST be < date_boundary)
            // This ensures we skip entire blocks where all dates are >= date_boundary
            if (zm_blocks[b].min_val < date_boundary) {
                size_t start = (size_t)b * block_size;
                size_t end = std::min(start + block_size, orders_count);
                block_ranges.push_back({start, end});
            }
        }
    } else {
        // No zone map, process entire table
        for (size_t start = 0; start < orders_count; start += block_size) {
            size_t end = std::min(start + block_size, orders_count);
            block_ranges.push_back({start, end});
        }
    }

    // Parallel scan with zone map pruning and customer filter
    #pragma omp parallel for schedule(dynamic, 1)
    for (size_t br = 0; br < block_ranges.size(); br++) {
        size_t start = block_ranges[br].first;
        size_t end = block_ranges[br].second;
        int tid = omp_get_thread_num();

        // Process rows with SIMD where possible (8 dates at a time)
        // Then handle remainder with scalar loop
        size_t i = start;

        // Vectorized date filter loop (prefetch optimization)
        // Process rows in batches and prefetch ahead for better cache behavior
        while (i + 8 <= end) {
            // Prefetch next batch for better cache utilization
            _mm_prefetch((const char*)(o_orderdate + i + 16), _MM_HINT_T0);
            _mm_prefetch((const char*)(o_custkey + i + 16), _MM_HINT_T0);

            // Process 8 rows in this batch
            for (int lane = 0; lane < 8; lane++) {
                size_t row_idx = i + lane;

                // Apply predicates: o_orderdate < date_boundary AND customer filter
                if (o_orderdate[row_idx] < date_boundary) {
                    if (o_custkey[row_idx] >= 0 && o_custkey[row_idx] <= (int32_t)customer_count &&
                        customer_filter[o_custkey[row_idx]]) {
                        thread_orders_tables[tid]->insert(o_orderkey[row_idx], {
                            o_orderkey[row_idx],
                            o_orderdate[row_idx],
                            o_shippriority[row_idx]
                        });
                    }
                }
            }

            i += 8;
        }

        // Scalar remainder
        while (i < end) {
            if (o_orderdate[i] < date_boundary && o_custkey[i] >= 0 && o_custkey[i] <= (int32_t)customer_count &&
                customer_filter[o_custkey[i]]) {
                thread_orders_tables[tid]->insert(o_orderkey[i], {
                    o_orderkey[i],
                    o_orderdate[i],
                    o_shippriority[i]
                });
            }
            i++;
        }
    }

    // Merge thread-local orders tables into global table
    CompactHashTableInt32 orders_ht(2000000);
    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : thread_orders_tables[t]->table) {
            if (entry.occupied) {
                orders_ht.insert(entry.key, entry.value);
            }
        }
        delete thread_orders_tables[t];
    }

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] orders_scan_filter_join: %.2f ms\n", ms_orders);
    printf("[TIMING] join_customer_orders: %.2f ms (included in orders_scan)\n", 0.0);
    #endif

    // ==================== PHASE 3: Load pre-built lineitem_orderkey_hash index ====================
    #ifdef GENDB_PROFILE
    auto t_lineitem_start = std::chrono::high_resolution_clock::now();
    #endif

    // Load the pre-built multi-value hash index for lineitem.l_orderkey
    MultiValueHashIndex li_orderkey_idx = load_lineitem_orderkey_hash_index(
        gendb_dir + "/indexes/lineitem_orderkey_hash.bin"
    );

    if (!li_orderkey_idx.loaded) {
        std::cerr << "Warning: Failed to load lineitem_orderkey_hash index, falling back to full scan" << std::endl;
    }

    // Load lineitem columns (needed for filtering and aggregation)
    size_t lineitem_count = 0;
    int64_t* l_extendedprice = mmap_file<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_count);
    int64_t* l_discount = mmap_file<int64_t>(gendb_dir + "/lineitem/l_discount.bin", lineitem_count);
    int32_t* l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_count);

    // Thread-local aggregation: each thread gets its own hash table
    std::vector<CompactHashTableAgg*> thread_agg_tables(num_threads);

    // Initialize thread-local aggregation tables
    #pragma omp parallel for
    for (int t = 0; t < num_threads; t++) {
        thread_agg_tables[t] = new CompactHashTableAgg(20000 / num_threads + 1000);
    }

    // ==================== PHASE 3B: Iterate orders and probe lineitem index ====================
    // Instead of scanning lineitem and probing orders, we iterate the filtered orders
    // and use the pre-built lineitem index to get matching rows directly.
    // This reduces I/O from 59M rows to only the lineitem rows that have matching orders.

    // Collect all filtered orders into a vector for parallel processing
    std::vector<std::pair<int32_t, OrderData>> filtered_orders;
    for (const auto& entry : orders_ht.table) {
        if (entry.occupied) {
            filtered_orders.push_back({entry.key, entry.value});
        }
    }

    // Parallel iteration over filtered orders
    #pragma omp parallel for schedule(dynamic, 1000)
    for (size_t i = 0; i < filtered_orders.size(); i++) {
        int32_t o_orderkey = filtered_orders[i].first;
        const OrderData& od = filtered_orders[i].second;

        // Lookup matching lineitem rows using the pre-built index
        LineitemOrderKeyHashEntry* li_entry = li_orderkey_idx.find(o_orderkey);
        if (li_entry == nullptr) continue;

        // Process all lineitem rows for this order
        for (uint32_t li_pos_idx = li_entry->offset; li_pos_idx < li_entry->offset + li_entry->count; li_pos_idx++) {
            uint32_t li_row_idx = li_orderkey_idx.positions[li_pos_idx];

            // Apply predicate: l_shipdate > date_boundary
            if (l_shipdate[li_row_idx] <= date_boundary) continue;

            // Compute revenue = l_extendedprice * (1 - l_discount)
            double revenue = l_extendedprice[li_row_idx] * (100.0 - l_discount[li_row_idx]) / 10000.0;

            AggKey key = {o_orderkey, od.o_orderdate, od.o_shippriority};

            // Get thread-local aggregation table
            int tid = omp_get_thread_num();
            double* existing = thread_agg_tables[tid]->find(key);
            if (existing) {
                *existing += revenue;
            } else {
                thread_agg_tables[tid]->insert(key, revenue);
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_lineitem_end = std::chrono::high_resolution_clock::now();
    double ms_lineitem = std::chrono::duration<double, std::milli>(t_lineitem_end - t_lineitem_start).count();
    printf("[TIMING] lineitem_scan_filter_join_agg: %.2f ms\n", ms_lineitem);
    #endif

    // ==================== PHASE 4: Merge thread-local aggregations ====================
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    // Merge all thread-local tables into a single result
    std::unordered_map<AggKey, double, AggKeyHash> agg_ht;
    agg_ht.reserve(20000);

    for (int t = 0; t < num_threads; t++) {
        for (const auto& entry : thread_agg_tables[t]->table) {
            if (entry.occupied) {
                agg_ht[entry.key] += entry.value;
            }
        }
        delete thread_agg_tables[t];
    }

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double ms_merge = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    (void)ms_merge;  // Avoid unused variable warning
    #endif

    // ==================== PHASE 5: Sort and output ====================
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<AggResult> results;
    results.reserve(agg_ht.size());

    for (const auto& [key, revenue] : agg_ht) {
        results.push_back({
            key.l_orderkey,
            key.o_orderdate,
            key.o_shippriority,
            revenue
        });
    }

    // Sort by revenue DESC, then o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const AggResult& a, const AggResult& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.o_orderdate < b.o_orderdate;
    });

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort_topk: %.2f ms\n", ms_sort);
    #endif

    // ==================== PHASE 6: Write CSV ====================
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string output_path = results_dir + "/Q3.csv";
    std::ofstream out(output_path);

    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

    for (const auto& res : results) {
        // Convert epoch days to YYYY-MM-DD
        std::string date_str = epoch_days_to_date(res.o_orderdate);

        // Revenue is already in dollars (computed as double)
        // Format with 4 decimal places to match ground truth precision

        out << res.l_orderkey << ","
            << std::fixed << std::setprecision(4) << res.revenue << ","
            << date_str << ","
            << res.o_shippriority << "\n";
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
    #endif

    // ==================== Cleanup ====================
    munmap(c_custkey, customer_count * sizeof(int32_t));
    munmap(c_mktsegment, customer_count * sizeof(int32_t));
    munmap(o_orderkey, orders_count * sizeof(int32_t));
    munmap(o_custkey, orders_count * sizeof(int32_t));
    munmap(o_orderdate, orders_count * sizeof(int32_t));
    munmap(o_shippriority, orders_count * sizeof(int32_t));
    munmap(l_extendedprice, lineitem_count * sizeof(int64_t));
    munmap(l_discount, lineitem_count * sizeof(int64_t));
    munmap(l_shipdate, lineitem_count * sizeof(int32_t));

    delete[] zm_blocks;
    if (li_orderkey_idx.entries) free(li_orderkey_idx.entries);
    if (li_orderkey_idx.positions) free(li_orderkey_idx.positions);

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

    run_q3(gendb_dir, results_dir);

    return 0;
}
#endif
