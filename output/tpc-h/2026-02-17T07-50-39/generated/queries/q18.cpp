// Q18: Large Volume Customer
//
// CORRECTNESS FIXES (Iteration 9):
// 1. Fixed hash index positions array offset: skip pos_count header field
// 2. Fixed empty slot sentinel: use -1 instead of 0 (orderkey can be 0)
//
// LOGICAL PLAN:
// 1. Subquery: Iterate lineitem_orderkey_hash index to aggregate by orderkey
//    - For each unique orderkey in index, sum quantities from multi-value positions
//    - Build hash set of qualifying orderkeys (SUM(l_quantity) > 300)
//    - Avoids 60M row scan; processes ~15M unique orderkeys via index
// 2. Filter orders by qualifying orderkeys (semi-join reduction)
//    - Expected cardinality: ~624 orders (very selective)
// 3. Join customer + lineitem to filtered orders:
//    - For each qualifying order:
//      a) Lookup customer via customer_custkey_hash index
//      b) Lookup lineitem rows via lineitem_orderkey_hash index
//      c) Aggregate SUM(l_quantity) for this order
// 4. Sort by o_totalprice DESC, o_orderdate, LIMIT 100
//
// PHYSICAL PLAN:
// - Subquery: Parallel iteration over lineitem_orderkey_hash slots, thread-local qualifying sets
// - Qualifying orderkey set: unordered_set (~624 entries)
// - Orders hash table: unordered_map on o_orderkey (~624 entries)
// - Customer lookup: direct index probe (hash_single)
// - Lineitem lookup: multi-value index probe (hash_multi_value)
// - Final aggregation: Hash table on composite key (c_custkey, o_orderkey)
// - Sort: Min-heap for top-100 selection
// - Parallelism: Parallel subquery aggregation over hash table slots

#include <iostream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <cstdio>
#include <queue>
#include <omp.h>

namespace {

// Helper: mmap a binary column file
template<typename T>
T* mmap_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << path << std::endl;
        exit(1);
    }
    struct stat sb;
    fstat(fd, &sb);
    count = sb.st_size / sizeof(T);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (addr == MAP_FAILED) {
        std::cerr << "mmap failed for " << path << std::endl;
        exit(1);
    }
    return static_cast<T*>(addr);
}

// Hash function for composite key (avoid std::hash identity on int32_t)
inline uint64_t hash_combine(uint64_t a, uint64_t b) {
    return a * 0x9E3779B97F4A7C15ULL + b;
}

struct CompositeKey {
    int32_t c_custkey;
    int32_t o_orderkey;

    bool operator==(const CompositeKey& other) const {
        return c_custkey == other.c_custkey && o_orderkey == other.o_orderkey;
    }
};

struct CompositeKeyHash {
    size_t operator()(const CompositeKey& k) const {
        return hash_combine(k.c_custkey, k.o_orderkey);
    }
};

struct AggState {
    std::string c_name;
    int32_t o_orderdate;
    int64_t o_totalprice;
    int64_t sum_qty;
};

struct ResultRow {
    std::string c_name;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    int64_t o_totalprice;
    int64_t sum_qty;

    bool operator<(const ResultRow& other) const {
        // For min-heap (keep worst of top-100 at top, so we can discard it)
        // We want: larger totalprice is BETTER, earlier orderdate is BETTER
        // So: smaller totalprice is WORSE (should be at top of min-heap)
        if (o_totalprice != other.o_totalprice)
            return o_totalprice > other.o_totalprice; // larger totalprice is "less" (better), so smaller at top
        return o_orderdate < other.o_orderdate; // earlier orderdate is "less" (better), so later at top
    }
};

// Convert epoch days to YYYY-MM-DD
std::string epoch_to_date(int32_t epoch_day) {
    int year = 1970;
    int days_remaining = epoch_day;

    // Find year
    while (true) {
        int days_in_year = (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) ? 366 : 365;
        if (days_remaining < days_in_year) break;
        days_remaining -= days_in_year;
        year++;
    }

    // Find month and day
    int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[2] = 29;
    }

    int month = 1;
    for (month = 1; month <= 12; month++) {
        if (days_remaining < days_in_month[month]) {
            break;
        }
        days_remaining -= days_in_month[month];
    }

    int day_of_month = days_remaining + 1;  // 1-indexed

    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", year, month, day_of_month);
    return std::string(buf);
}

} // end anonymous namespace

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    #ifdef GENDB_PROFILE
    auto t_total_start = std::chrono::high_resolution_clock::now();
    #endif

    // Step 1: Load lineitem columns
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t li_qty_count;
    int64_t* li_quantity = mmap_column<int64_t>(gendb_dir + "/lineitem/l_quantity.bin", li_qty_count);

    // Load lineitem_orderkey_hash index
    std::string idx_path = gendb_dir + "/indexes/lineitem_orderkey_hash.bin";
    int fd_idx = open(idx_path.c_str(), O_RDONLY);
    if (fd_idx < 0) {
        std::cerr << "Failed to open lineitem_orderkey_hash index" << std::endl;
        exit(1);
    }
    struct stat sb_idx;
    fstat(fd_idx, &sb_idx);
    uint8_t* idx_data = (uint8_t*)mmap(nullptr, sb_idx.st_size, PROT_READ, MAP_PRIVATE, fd_idx, 0);
    close(fd_idx);

    struct HashEntry { int32_t key; uint32_t offset; uint32_t count; };

    uint32_t* index_header = (uint32_t*)idx_data;
    uint32_t li_idx_num_unique = index_header[0];
    uint32_t li_idx_table_size = index_header[1];
    HashEntry* li_idx_entries = (HashEntry*)(index_header + 2);
    // Skip the pos_count field (1 uint32_t) before positions array
    uint32_t* pos_array_header = (uint32_t*)(li_idx_entries + li_idx_table_size);
    uint32_t li_idx_pos_count = pos_array_header[0];
    uint32_t* li_idx_positions = pos_array_header + 1;

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double ms_load = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_data: %.2f ms\n", ms_load);
    #endif

    // Step 2: Subquery - aggregate lineitem by orderkey using index, find qualifying orderkeys
    #ifdef GENDB_PROFILE
    auto t_subq_start = std::chrono::high_resolution_clock::now();
    #endif

    // Use lineitem_orderkey_hash index to iterate over unique orderkeys and aggregate
    // This avoids scanning 60M rows; instead we process ~15M unique orderkeys
    std::unordered_set<int32_t> qualifying_orderkeys;
    qualifying_orderkeys.reserve(10000);

    // Parallel iteration over hash table slots
    #pragma omp parallel
    {
        std::unordered_set<int32_t> local_qualifying;
        local_qualifying.reserve(1000);

        #pragma omp for schedule(dynamic, 10000)
        for (uint32_t slot = 0; slot < li_idx_table_size; slot++) {
            const HashEntry& entry = li_idx_entries[slot];
            if (entry.key == -1) continue; // empty slot (sentinel value)

            // Aggregate quantity for this orderkey
            int64_t sum_qty = 0;
            uint32_t pos_offset = entry.offset;
            uint32_t count = entry.count;
            for (uint32_t j = 0; j < count; j++) {
                uint32_t pos = li_idx_positions[pos_offset + j];
                sum_qty += li_quantity[pos];
            }

            // Check if qualifies (sum_qty > 300, scaled: 30000)
            if (sum_qty > 30000) {
                local_qualifying.insert(entry.key);
            }
        }

        // Merge into global set
        #pragma omp critical
        {
            for (int32_t key : local_qualifying) {
                qualifying_orderkeys.insert(key);
            }
        }
    }

    #ifdef GENDB_PROFILE
    auto t_subq_end = std::chrono::high_resolution_clock::now();
    double ms_subq = std::chrono::duration<double, std::milli>(t_subq_end - t_subq_start).count();
    printf("[TIMING] subquery_aggregation: %.2f ms (found %zu qualifying orderkeys)\n",
           ms_subq, qualifying_orderkeys.size());
    #endif

    // Step 3: Load orders and filter by qualifying orderkeys
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    size_t ord_count;
    int32_t* ord_orderkey = mmap_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin", ord_count);
    int32_t* ord_custkey = mmap_column<int32_t>(gendb_dir + "/orders/o_custkey.bin", ord_count);
    int32_t* ord_orderdate = mmap_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin", ord_count);
    int64_t* ord_totalprice = mmap_column<int64_t>(gendb_dir + "/orders/o_totalprice.bin", ord_count);

    // Build hash map of qualifying orders
    struct OrderInfo {
        int32_t o_custkey;
        int32_t o_orderdate;
        int64_t o_totalprice;
    };
    std::unordered_map<int32_t, OrderInfo> orders_map;
    orders_map.reserve(qualifying_orderkeys.size());

    for (size_t i = 0; i < ord_count; i++) {
        int32_t ok = ord_orderkey[i];
        if (qualifying_orderkeys.count(ok)) {
            orders_map[ok] = {ord_custkey[i], ord_orderdate[i], ord_totalprice[i]};
        }
    }

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double ms_orders = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] filter_orders: %.2f ms (found %zu qualifying orders)\n",
           ms_orders, orders_map.size());
    #endif

    // Step 4: Load customer index
    #ifdef GENDB_PROFILE
    auto t_cust_start = std::chrono::high_resolution_clock::now();
    #endif

    std::string cust_idx_path = gendb_dir + "/indexes/customer_custkey_hash.bin";
    int fd_cust_idx = open(cust_idx_path.c_str(), O_RDONLY);
    if (fd_cust_idx < 0) {
        std::cerr << "Failed to open customer_custkey_hash index" << std::endl;
        exit(1);
    }
    struct stat sb_cust_idx;
    fstat(fd_cust_idx, &sb_cust_idx);
    uint8_t* cust_idx_data = (uint8_t*)mmap(nullptr, sb_cust_idx.st_size, PROT_READ, MAP_PRIVATE, fd_cust_idx, 0);
    close(fd_cust_idx);

    struct CustomerHashEntry { int32_t key; uint32_t position; };

    uint32_t* cust_index_header = (uint32_t*)cust_idx_data;
    uint32_t cust_idx_num_entries = cust_index_header[0];
    uint32_t cust_idx_table_size = cust_index_header[1];
    CustomerHashEntry* cust_idx_entries = (CustomerHashEntry*)(cust_index_header + 2);

    // Load customer names (length-prefixed strings)
    std::string cust_name_path = gendb_dir + "/customer/c_name.bin";
    int fd_cust_name = open(cust_name_path.c_str(), O_RDONLY);
    if (fd_cust_name < 0) {
        std::cerr << "Failed to open customer c_name.bin" << std::endl;
        exit(1);
    }
    struct stat sb_cust_name;
    fstat(fd_cust_name, &sb_cust_name);
    uint8_t* cust_name_data = (uint8_t*)mmap(nullptr, sb_cust_name.st_size, PROT_READ, MAP_PRIVATE, fd_cust_name, 0);
    close(fd_cust_name);

    // Parse length-prefixed strings
    std::vector<std::string> cust_names;
    cust_names.reserve(1500000);
    uint8_t* ptr = cust_name_data;
    uint8_t* end = cust_name_data + sb_cust_name.st_size;
    while (ptr < end) {
        uint32_t len = *(uint32_t*)ptr;
        ptr += 4;
        if (ptr + len > end) break;
        cust_names.emplace_back((char*)ptr, len);
        ptr += len;
    }

    #ifdef GENDB_PROFILE
    auto t_cust_end = std::chrono::high_resolution_clock::now();
    double ms_cust = std::chrono::duration<double, std::milli>(t_cust_end - t_cust_start).count();
    printf("[TIMING] load_customer: %.2f ms\n", ms_cust);
    #endif

    // Step 5: Join lineitem to qualifying orders and aggregate
    #ifdef GENDB_PROFILE
    auto t_join_agg_start = std::chrono::high_resolution_clock::now();
    #endif

    // Final aggregation: hash table on (c_custkey, o_orderkey)
    std::unordered_map<CompositeKey, AggState, CompositeKeyHash> agg_map;
    agg_map.reserve(orders_map.size());

    // For each qualifying order, use lineitem_orderkey_hash index to look up lineitems
    // and aggregate quantity, while also joining with customer
    for (const auto& ord_kv : orders_map) {
        int32_t o_orderkey = ord_kv.first;
        const OrderInfo& oi = ord_kv.second;

        // Lookup customer using index
        int32_t c_custkey = oi.o_custkey;
        uint64_t cust_hash = c_custkey * 0x9E3779B97F4A7C15ULL;
        uint32_t cust_slot = (cust_hash >> 32) % cust_idx_table_size;

        std::string c_name;
        while (true) {
            const CustomerHashEntry& entry = cust_idx_entries[cust_slot];
            if (entry.key == -1) break; // empty slot, not found (shouldn't happen)
            if (entry.key == c_custkey) {
                c_name = cust_names[entry.position];
                break;
            }
            cust_slot = (cust_slot + 1) % cust_idx_table_size;
        }

        // Lookup lineitem rows using index
        uint64_t li_hash = o_orderkey * 0x9E3779B97F4A7C15ULL;
        uint32_t li_slot = (li_hash >> 32) % li_idx_table_size;

        int64_t sum_qty = 0;
        while (true) {
            const HashEntry& entry = li_idx_entries[li_slot];
            if (entry.key == -1) break; // empty slot, not found
            if (entry.key == o_orderkey) {
                // Found matching orderkey, aggregate quantities
                uint32_t pos_offset = entry.offset;
                uint32_t count = entry.count;
                for (uint32_t j = 0; j < count; j++) {
                    uint32_t pos = li_idx_positions[pos_offset + j];
                    sum_qty += li_quantity[pos];
                }
                break;
            }
            li_slot = (li_slot + 1) % li_idx_table_size;
        }

        // Store result
        CompositeKey key = {c_custkey, o_orderkey};
        agg_map[key] = {c_name, oi.o_orderdate, oi.o_totalprice, sum_qty};
    }

    #ifdef GENDB_PROFILE
    auto t_join_agg_end = std::chrono::high_resolution_clock::now();
    double ms_join_agg = std::chrono::duration<double, std::milli>(t_join_agg_end - t_join_agg_start).count();
    printf("[TIMING] join_aggregation: %.2f ms (aggregated %zu groups)\n",
           ms_join_agg, agg_map.size());
    #endif

    // Step 6: Sort and limit to top 100
    #ifdef GENDB_PROFILE
    auto t_sort_start = std::chrono::high_resolution_clock::now();
    #endif

    // Use min-heap to track top 100
    std::priority_queue<ResultRow> heap;

    for (const auto& kv : agg_map) {
        ResultRow row = {
            kv.second.c_name,
            kv.first.c_custkey,
            kv.first.o_orderkey,
            kv.second.o_orderdate,
            kv.second.o_totalprice,
            kv.second.sum_qty
        };

        if (heap.size() < 100) {
            heap.push(row);
        } else if (!(row < heap.top())) {
            // row is better than the worst in heap
            heap.pop();
            heap.push(row);
        }
    }

    // Extract results and sort
    std::vector<ResultRow> results;
    results.reserve(heap.size());
    while (!heap.empty()) {
        results.push_back(heap.top());
        heap.pop();
    }

    // Proper sort: ORDER BY o_totalprice DESC, o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.o_totalprice != b.o_totalprice)
            return a.o_totalprice > b.o_totalprice; // DESC
        return a.o_orderdate < b.o_orderdate; // ASC
    });

    #ifdef GENDB_PROFILE
    auto t_sort_end = std::chrono::high_resolution_clock::now();
    double ms_sort = std::chrono::duration<double, std::milli>(t_sort_end - t_sort_start).count();
    printf("[TIMING] sort: %.2f ms\n", ms_sort);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double ms_total = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", ms_total);
    #endif

    // Step 7: Write output
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    std::ofstream out(results_dir + "/Q18.csv");
    out << std::fixed << std::setprecision(2);
    out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";

    for (const auto& row : results) {
        out << row.c_name << ","
            << row.c_custkey << ","
            << row.o_orderkey << ","
            << epoch_to_date(row.o_orderdate) << ","
            << (row.o_totalprice / 100.0) << ","
            << (row.sum_qty / 100.0) << "\n";
    }

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double ms_output = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", ms_output);
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
    run_q18(gendb_dir, results_dir);
    return 0;
}
#endif
