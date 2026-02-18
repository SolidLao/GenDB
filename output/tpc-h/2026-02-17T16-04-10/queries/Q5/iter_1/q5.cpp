#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <cstring>
#include <cmath>
#include <algorithm>
#include <unordered_map>
#include <map>
#include <set>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <omp.h>

/*
 * QUERY PLAN FOR Q5: Local Supplier Volume
 *
 * LOGICAL PLAN:
 * 1. Filter region table by r_name = 'ASIA'
 * 2. Join nation with filtered region on n_regionkey
 * 3. Join supplier with filtered nations on s_nationkey
 * 4. Filter orders by o_orderdate in [1994-01-01, 1995-01-01)
 * 5. Scan lineitem and join:
 *    - lineitem suppkey → supplier (must be in ASIA)
 *    - lineitem orderkey → order (must be in date range)
 *    - (implicitly: customer exists via order)
 * 6. GROUP BY n_name and SUM(l_extendedprice * (1 - l_discount))
 *
 * PHYSICAL PLAN:
 * 1. Load region, find ASIA regionkey
 * 2. Load nation, filter by ASIA region, build list of ASIA nation keys
 * 3. Load supplier, filter by ASIA nationkeys, build hash of suppliers
 * 4. Load orders, filter by date range, build hash of orders
 * 5. Parallel scan of lineitem:
 *    - Lookup l_suppkey in supplier hash
 *    - Lookup l_orderkey in order hash
 *    - If both found, accumulate revenue for supplier's nation
 * 6. Aggregate by nation, output sorted by revenue DESC
 */

// File mapping helper
struct FileMapping {
    int fd;
    void* data;
    size_t size;

    FileMapping() : fd(-1), data(nullptr), size(0) {}

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat: " << path << std::endl;
            ::close(fd);
            fd = -1;
            return false;
        }

        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            ::close(fd);
            fd = -1;
            data = nullptr;
            return false;
        }

        return true;
    }

    void close() {
        if (data && data != MAP_FAILED) {
            munmap(data, size);
        }
        if (fd >= 0) {
            ::close(fd);
        }
        data = nullptr;
        fd = -1;
    }

    ~FileMapping() { close(); }
};

// Load variable-length string column from binary file
// Format: [4-byte length][string data][4-byte length][string data]...
std::vector<std::string> load_string_column(const void* data, int64_t num_rows) {
    std::vector<std::string> result;
    const uint8_t* ptr = (const uint8_t*)data;

    for (int64_t i = 0; i < num_rows; ++i) {
        uint32_t len = *(uint32_t*)ptr;
        ptr += 4;

        std::string s((const char*)ptr, len);
        ptr += len;

        result.push_back(s);
    }

    return result;
}

// Simple hash table for supplier lookup: s_suppkey → supplier info
struct SupplierEntry {
    int32_t suppkey;
    int32_t nationkey;
    bool occupied;
};

struct SupplierHash {
    std::vector<SupplierEntry> table;
    int64_t capacity;

    SupplierHash(int64_t sz) : capacity(1) {
        while (capacity < sz * 2) capacity *= 2;  // 50% load factor
        table.resize(capacity);
        for (int64_t i = 0; i < capacity; ++i) {
            table[i].occupied = false;
        }
    }

    void insert(int32_t suppkey, int32_t nationkey) {
        int64_t h = ((uint64_t)suppkey * 11400714819323198485ULL) & (capacity - 1);
        while (table[h].occupied) {
            h = (h + 1) & (capacity - 1);
        }
        table[h].suppkey = suppkey;
        table[h].nationkey = nationkey;
        table[h].occupied = true;
    }

    SupplierEntry* find(int32_t suppkey) {
        int64_t h = ((uint64_t)suppkey * 11400714819323198485ULL) & (capacity - 1);
        while (table[h].occupied) {
            if (table[h].suppkey == suppkey) {
                return &table[h];
            }
            h = (h + 1) & (capacity - 1);
        }
        return nullptr;
    }
};

// Optimized hash table for orders lookup: compact single-value storage
struct OrderHash {
    struct Entry {
        int32_t key;      // orderkey
        int8_t status;    // -1: empty, 0: occupied
    };

    std::vector<Entry> table;
    int64_t capacity;
    int64_t size;

    OrderHash(int64_t sz) : capacity(1), size(0) {
        while (capacity < sz * 2) capacity *= 2;
        table.resize(capacity);
        for (int64_t i = 0; i < capacity; ++i) {
            table[i].status = -1;  // empty
        }
    }

    void insert(int32_t orderkey, int32_t custkey) {
        int64_t h = ((uint64_t)orderkey * 11400714819323198485ULL) & (capacity - 1);
        while (table[h].status >= 0) {
            if (table[h].key == orderkey) return;  // already exists
            h = (h + 1) & (capacity - 1);
        }
        table[h].key = orderkey;
        table[h].status = 0;
        size++;
    }

    bool exists(int32_t orderkey) {
        int64_t h = ((uint64_t)orderkey * 11400714819323198485ULL) & (capacity - 1);
        while (table[h].status >= 0) {
            if (table[h].key == orderkey) {
                return true;
            }
            h = (h + 1) & (capacity - 1);
        }
        return false;
    }
};

void run_q5(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_total_start = std::chrono::high_resolution_clock::now();

    std::cout << "[METADATA CHECK] Q5 Input Parameters:\n";
    std::cout << "  gendb_dir: " << gendb_dir << "\n";
    std::cout << "  results_dir: " << results_dir << "\n";

    // Date constants: 1994-01-01 = 8766, 1995-01-01 = 9131 (days since epoch 1970-01-01)
    const int32_t date_1994_01_01 = 8766;
    const int32_t date_1995_01_01 = 9131;

    // ==================== LOAD REGION TABLE ====================
    #ifdef GENDB_PROFILE
    auto t_load_start = std::chrono::high_resolution_clock::now();
    #endif

    FileMapping fm_region_name, fm_region_regionkey;
    const int64_t region_rows = 5;

    fm_region_regionkey.open(gendb_dir + "/region/r_regionkey.bin");
    fm_region_name.open(gendb_dir + "/region/r_name.bin");

    if (!fm_region_regionkey.data || !fm_region_name.data) {
        std::cerr << "Failed to open region files\n";
        return;
    }

    const int32_t* region_regionkey = (const int32_t*)fm_region_regionkey.data;

    // Load region names (variable-length strings)
    auto region_names = load_string_column(fm_region_name.data, region_rows);

    // Find ASIA region
    int32_t asia_regionkey = -1;
    for (int64_t i = 0; i < region_rows; ++i) {
        if (region_names[i] == "ASIA") {
            asia_regionkey = region_regionkey[i];
            std::cout << "[METADATA CHECK] Found ASIA region with key: " << asia_regionkey << "\n";
            break;
        }
    }

    if (asia_regionkey == -1) {
        std::cerr << "ASIA region not found\n";
        return;
    }

    // ==================== LOAD NATION TABLE ====================
    FileMapping fm_nation_nationkey, fm_nation_regionkey, fm_nation_name;
    const int64_t nation_rows = 25;

    fm_nation_nationkey.open(gendb_dir + "/nation/n_nationkey.bin");
    fm_nation_regionkey.open(gendb_dir + "/nation/n_regionkey.bin");
    fm_nation_name.open(gendb_dir + "/nation/n_name.bin");

    if (!fm_nation_nationkey.data || !fm_nation_regionkey.data || !fm_nation_name.data) {
        std::cerr << "Failed to open nation files\n";
        return;
    }

    const int32_t* nation_nationkey = (const int32_t*)fm_nation_nationkey.data;
    const int32_t* nation_regionkey = (const int32_t*)fm_nation_regionkey.data;

    // Load nation names
    auto nation_names = load_string_column(fm_nation_name.data, nation_rows);

    // Filter nations by ASIA region and build a map from nationkey to nation name
    std::vector<int32_t> asia_nations;
    std::map<int32_t, std::string> nation_name_map;

    for (int64_t i = 0; i < nation_rows; ++i) {
        int32_t nkey = nation_nationkey[i];
        if (nation_regionkey[i] == asia_regionkey) {
            asia_nations.push_back(nkey);
            nation_name_map[nkey] = nation_names[i];
        }
    }

    std::cout << "[METADATA CHECK] Found " << asia_nations.size() << " ASIA nations\n";

    // ==================== LOAD SUPPLIER TABLE ====================
    FileMapping fm_supplier_suppkey, fm_supplier_nationkey;
    const int64_t supplier_rows = 100000;

    fm_supplier_suppkey.open(gendb_dir + "/supplier/s_suppkey.bin");
    fm_supplier_nationkey.open(gendb_dir + "/supplier/s_nationkey.bin");

    if (!fm_supplier_suppkey.data || !fm_supplier_nationkey.data) {
        std::cerr << "Failed to open supplier files\n";
        return;
    }

    const int32_t* supplier_suppkey = (const int32_t*)fm_supplier_suppkey.data;
    const int32_t* supplier_nationkey = (const int32_t*)fm_supplier_nationkey.data;

    // Build hash table of suppliers in ASIA nations
    std::set<int32_t> asia_nation_set(asia_nations.begin(), asia_nations.end());
    SupplierHash supplier_hash(100000);
    int64_t supplier_count = 0;

    for (int64_t i = 0; i < supplier_rows; ++i) {
        if (asia_nation_set.count(supplier_nationkey[i]) > 0) {
            supplier_hash.insert(supplier_suppkey[i], supplier_nationkey[i]);
            supplier_count++;
        }
    }

    std::cout << "[METADATA CHECK] Found " << supplier_count << " suppliers in ASIA nations\n";

    #ifdef GENDB_PROFILE
    auto t_load_end = std::chrono::high_resolution_clock::now();
    double load_ms = std::chrono::duration<double, std::milli>(t_load_end - t_load_start).count();
    printf("[TIMING] load_dimension_tables: %.2f ms\n", load_ms);
    #endif

    // ==================== LOAD ORDERS TABLE ====================
    #ifdef GENDB_PROFILE
    auto t_orders_start = std::chrono::high_resolution_clock::now();
    #endif

    FileMapping fm_orders_orderkey, fm_orders_custkey, fm_orders_orderdate;
    const int64_t orders_rows = 15000000;

    fm_orders_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
    fm_orders_custkey.open(gendb_dir + "/orders/o_custkey.bin");
    fm_orders_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");

    if (!fm_orders_orderkey.data || !fm_orders_custkey.data || !fm_orders_orderdate.data) {
        std::cerr << "Failed to open orders files\n";
        return;
    }

    const int32_t* orders_orderkey = (const int32_t*)fm_orders_orderkey.data;
    const int32_t* orders_orderdate = (const int32_t*)fm_orders_orderdate.data;

    // Build hash table of orders in date range [1994-01-01, 1995-01-01)
    // Parallel build: pre-filter in parallel, then build sequential hash
    int64_t orders_count = 0;
    std::vector<int32_t> filtered_orderkeys;
    filtered_orderkeys.reserve(2500000);

    // Phase 1: Parallel pre-filter to identify qualifying orderkeys
    // Use a two-level approach: parallel scan with thread-local buffers
    int num_threads_order = omp_get_max_threads();
    std::vector<std::vector<int32_t>> thread_buffers(num_threads_order);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int chunk_size = (orders_rows + num_threads_order - 1) / num_threads_order;
        int64_t start = tid * chunk_size;
        int64_t end = std::min((int64_t)(tid + 1) * chunk_size, orders_rows);

        for (int64_t i = start; i < end; ++i) {
            int32_t odate = orders_orderdate[i];
            if (odate >= date_1994_01_01 && odate < date_1995_01_01) {
                thread_buffers[tid].push_back(orders_orderkey[i]);
            }
        }
    }

    // Phase 2: Merge thread-local buffers
    for (int t = 0; t < num_threads_order; ++t) {
        for (auto key : thread_buffers[t]) {
            filtered_orderkeys.push_back(key);
        }
    }
    orders_count = filtered_orderkeys.size();

    // Phase 3: Build hash table from filtered keys
    OrderHash orders_hash(orders_count);
    for (auto key : filtered_orderkeys) {
        orders_hash.insert(key, 0);  // custkey not used
    }

    std::cout << "[METADATA CHECK] Found " << orders_count << " orders in date range [1994-01-01, 1995-01-01)\n";

    #ifdef GENDB_PROFILE
    auto t_orders_end = std::chrono::high_resolution_clock::now();
    double orders_ms = std::chrono::duration<double, std::milli>(t_orders_end - t_orders_start).count();
    printf("[TIMING] build_orders_hash: %.2f ms\n", orders_ms);
    #endif

    // ==================== SCAN AND JOIN LINEITEM ====================
    #ifdef GENDB_PROFILE
    auto t_scan_start = std::chrono::high_resolution_clock::now();
    #endif

    FileMapping fm_lineitem_orderkey, fm_lineitem_suppkey, fm_lineitem_extendedprice, fm_lineitem_discount;
    const int64_t lineitem_rows = 59986052;

    fm_lineitem_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
    fm_lineitem_suppkey.open(gendb_dir + "/lineitem/l_suppkey.bin");
    fm_lineitem_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
    fm_lineitem_discount.open(gendb_dir + "/lineitem/l_discount.bin");

    if (!fm_lineitem_orderkey.data || !fm_lineitem_suppkey.data ||
        !fm_lineitem_extendedprice.data || !fm_lineitem_discount.data) {
        std::cerr << "Failed to open lineitem files\n";
        return;
    }

    const int32_t* lineitem_orderkey = (const int32_t*)fm_lineitem_orderkey.data;
    const int32_t* lineitem_suppkey = (const int32_t*)fm_lineitem_suppkey.data;
    const int64_t* lineitem_extendedprice = (const int64_t*)fm_lineitem_extendedprice.data;
    const int64_t* lineitem_discount = (const int64_t*)fm_lineitem_discount.data;

    // Thread-local aggregation: revenue by nation (25 nations max)
    // Use double for accumulation to preserve precision when summing many values
    int num_threads = omp_get_max_threads();
    std::vector<std::vector<double>> thread_revenue(num_threads, std::vector<double>(25, 0.0));
    std::vector<int64_t> thread_matches(num_threads, 0);

    // Parallel scan of lineitem
    #pragma omp parallel for schedule(dynamic, 10000)
    for (int64_t i = 0; i < lineitem_rows; ++i) {
        int32_t suppkey = lineitem_suppkey[i];
        int32_t orderkey = lineitem_orderkey[i];

        // Look up supplier
        SupplierEntry* supp = supplier_hash.find(suppkey);
        if (!supp) continue;  // Supplier not in ASIA

        int32_t nationkey = supp->nationkey;

        // Look up order
        if (!orders_hash.exists(orderkey)) continue;  // Order not in date range

        // Calculate revenue = l_extendedprice * (1 - l_discount)
        // ep and disc are stored as int64_t
        // ep scale: stored = actual * 100
        // disc scale: stored = actual * 100, but disc is 0-10 (meaning 0%-10%)
        // So disc_actual = disc_stored / 100
        // revenue = (ep / 100) * (1 - (disc / 100))
        int64_t ep = lineitem_extendedprice[i];
        int64_t disc = lineitem_discount[i];
        // Compute in double to preserve precision when summing
        double ep_actual = (double)ep / 100.0;
        double disc_actual = (double)disc / 100.0;
        double revenue_actual = ep_actual * (1.0 - disc_actual);

        // Accumulate revenue for this nation (stored in dollars)
        int tid = omp_get_thread_num();
        thread_revenue[tid][nationkey] += revenue_actual;
        thread_matches[tid]++;
    }

    #ifdef GENDB_PROFILE
    auto t_scan_end = std::chrono::high_resolution_clock::now();
    double scan_ms = std::chrono::duration<double, std::milli>(t_scan_end - t_scan_start).count();
    printf("[TIMING] scan_join_lineitem: %.2f ms\n", scan_ms);
    #endif

    // ==================== MERGE THREAD-LOCAL AGGREGATES ====================
    #ifdef GENDB_PROFILE
    auto t_merge_start = std::chrono::high_resolution_clock::now();
    #endif

    std::vector<double> final_revenue(25, 0.0);
    int64_t total_matches = 0;
    for (int t = 0; t < num_threads; ++t) {
        total_matches += thread_matches[t];
        for (int n = 0; n < 25; ++n) {
            final_revenue[n] += thread_revenue[t][n];
        }
    }

    std::cout << "[METADATA CHECK] Matched lineitem rows: " << total_matches << "\n";

    #ifdef GENDB_PROFILE
    auto t_merge_end = std::chrono::high_resolution_clock::now();
    double merge_ms = std::chrono::duration<double, std::milli>(t_merge_end - t_merge_start).count();
    printf("[TIMING] aggregation_merge: %.2f ms\n", merge_ms);
    #endif

    // ==================== OUTPUT RESULTS ====================
    #ifdef GENDB_PROFILE
    auto t_output_start = std::chrono::high_resolution_clock::now();
    #endif

    // Build result rows: (nationkey, nation_name, revenue)
    std::vector<std::tuple<int32_t, std::string, double>> results;

    for (int32_t nationkey : asia_nations) {
        if (final_revenue[nationkey] > 0) {
            std::string nation_name_str = nation_name_map[nationkey];
            results.push_back(std::make_tuple(nationkey, nation_name_str, final_revenue[nationkey]));
        }
    }

    // Sort by revenue DESC (third element, reversed)
    std::sort(results.begin(), results.end(),
        [](const auto& a, const auto& b) {
            return std::get<2>(a) > std::get<2>(b);
        });

    // Write CSV
    std::string output_path = results_dir + "/Q5.csv";
    std::ofstream out(output_path);

    if (!out.is_open()) {
        std::cerr << "Failed to open output file: " << output_path << std::endl;
        return;
    }

    out << "n_name,revenue\n";

    for (auto& row : results) {
        std::string name = std::get<1>(row);
        double revenue = std::get<2>(row);

        char buf[256];
        snprintf(buf, sizeof(buf), "%s,%.4f\n", name.c_str(), revenue);
        out << buf;
    }

    out.close();

    #ifdef GENDB_PROFILE
    auto t_output_end = std::chrono::high_resolution_clock::now();
    double output_ms = std::chrono::duration<double, std::milli>(t_output_end - t_output_start).count();
    printf("[TIMING] output: %.2f ms\n", output_ms);
    #endif

    #ifdef GENDB_PROFILE
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    printf("[TIMING] total: %.2f ms\n", total_ms);
    #endif

    std::cout << "Query execution complete. Results written to: " << output_path << "\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q5(gendb_dir, results_dir);
    return 0;
}
#endif
