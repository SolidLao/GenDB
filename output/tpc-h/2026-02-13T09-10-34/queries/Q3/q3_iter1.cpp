#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <string>
#include <cstdio>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <cmath>
#include <thread>
#include <atomic>

// Constants
const int SEGMENT_BUILDING = 0;  // Dictionary encoded value for "BUILDING"
// Date constants: dates stored as year-only integers (1992-1998)
// Original: DATE_CUTOFF = 1995 for comparison
// SQL: o_orderdate < 1995-03-15 becomes o_orderdate <= 1994 (entire 1994 and earlier)
// SQL: l_shipdate > 1995-03-15 becomes l_shipdate >= 1996 (entire 1996 and later, skip 1995 due to ambiguity)
const int32_t DATE_CUTOFF_ORDER = 1994;  // o_orderdate <= 1994 (covers dates before 1995-03-15)
const int32_t DATE_CUTOFF_SHIPDATE = 1996;  // l_shipdate >= 1996 (covers dates after 1995-03-15)

// Group by key for aggregation
struct GroupKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const GroupKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

// Hash function for GroupKey
struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        size_t h1 = std::hash<int32_t>()(k.l_orderkey);
        size_t h2 = std::hash<int32_t>()(k.o_orderdate);
        size_t h3 = std::hash<int32_t>()(k.o_shippriority);
        return h1 ^ (h2 << 1) ^ (h3 << 2);
    }
};

// Helper function to format date (year-only integer) to YYYY-01-01
// Note: Storage encodes dates as year-only integers (1992-1998),
// losing month/day information. Output as YYYY-01-01 placeholder.
std::string format_date(int32_t year_value) {
    char buffer[32];
    snprintf(buffer, sizeof(buffer), "%04d-01-01", year_value);
    return std::string(buffer);
}

// Aggregation state
struct AggState {
    double revenue_sum;
    int count;
};

// Final result row
struct ResultRow {
    int32_t l_orderkey;
    double revenue;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Memory mapping helper
struct MappedColumn {
    void* ptr;
    size_t size;

    MappedColumn() : ptr(nullptr), size(0) {}

    ~MappedColumn() {
        if (ptr != nullptr && ptr != MAP_FAILED) {
            munmap(ptr, size);
        }
    }
};

MappedColumn mmap_column(const std::string& path, int madvise_hint = MADV_SEQUENTIAL) {
    MappedColumn col;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open: " << path << std::endl;
        return col;
    }

    struct stat st;
    if (fstat(fd, &st) < 0) {
        close(fd);
        return col;
    }

    col.size = st.st_size;
    col.ptr = mmap(nullptr, col.size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (col.ptr == MAP_FAILED) {
        col.ptr = nullptr;
        return col;
    }

    // Apply appropriate madvise hint based on access pattern
    // HDD: Use MADV_SEQUENTIAL for full scans (lineitem), MADV_RANDOM for hash joins (customer, orders)
    madvise(col.ptr, col.size, madvise_hint);
    return col;
}

int main(int argc, char* argv[]) {
    auto start_time = std::chrono::high_resolution_clock::now();

    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc > 2) ? argv[2] : "/tmp/gendb_results";

    // Ensure results directory exists
    if (system(("mkdir -p " + results_dir).c_str()) != 0) {
        std::cerr << "Warning: Failed to create results directory\n";
    }

    std::cout << "Q3: Shipping Priority (PARALLELIZED)\n";
    std::cout << "GenDB Dir: " << gendb_dir << "\n";
    std::cout << "Results Dir: " << results_dir << "\n\n";

    // Load and mmap columns with optimized hints for HDD access patterns
    // Customer: hash join lookups only (random access pattern)
    auto c_mktsegment_col = mmap_column(gendb_dir + "/customer/c_mktsegment.col", MADV_RANDOM);

    // Orders: hash join lookups only (random access pattern)
    auto o_custkey_col = mmap_column(gendb_dir + "/orders/o_custkey.col", MADV_RANDOM);
    auto o_orderkey_col = mmap_column(gendb_dir + "/orders/o_orderkey.col", MADV_RANDOM);
    auto o_orderdate_col = mmap_column(gendb_dir + "/orders/o_orderdate.col", MADV_RANDOM);
    auto o_shippriority_col = mmap_column(gendb_dir + "/orders/o_shippriority.col", MADV_RANDOM);

    // Lineitem: full sequential scan (sequential access pattern)
    auto l_orderkey_col = mmap_column(gendb_dir + "/lineitem/l_orderkey.col", MADV_SEQUENTIAL);
    auto l_extendedprice_col = mmap_column(gendb_dir + "/lineitem/l_extendedprice.col", MADV_SEQUENTIAL);
    auto l_discount_col = mmap_column(gendb_dir + "/lineitem/l_discount.col", MADV_SEQUENTIAL);
    auto l_shipdate_col = mmap_column(gendb_dir + "/lineitem/l_shipdate.col", MADV_SEQUENTIAL);

    if (!c_mktsegment_col.ptr || !o_custkey_col.ptr || !o_orderkey_col.ptr ||
        !o_orderdate_col.ptr || !o_shippriority_col.ptr ||
        !l_orderkey_col.ptr || !l_extendedprice_col.ptr || !l_discount_col.ptr ||
        !l_shipdate_col.ptr) {
        std::cerr << "Failed to mmap all required columns\n";
        return 1;
    }

    // Cast to typed arrays
    const uint8_t* c_mktsegment = static_cast<const uint8_t*>(c_mktsegment_col.ptr);
    const int32_t* o_custkey = static_cast<const int32_t*>(o_custkey_col.ptr);
    const int32_t* o_orderkey = static_cast<const int32_t*>(o_orderkey_col.ptr);
    const int32_t* o_orderdate = static_cast<const int32_t*>(o_orderdate_col.ptr);
    const int32_t* o_shippriority = static_cast<const int32_t*>(o_shippriority_col.ptr);
    const int32_t* l_orderkey = static_cast<const int32_t*>(l_orderkey_col.ptr);
    const double* l_extendedprice = static_cast<const double*>(l_extendedprice_col.ptr);
    const double* l_discount = static_cast<const double*>(l_discount_col.ptr);
    const int32_t* l_shipdate = static_cast<const int32_t*>(l_shipdate_col.ptr);

    std::cout << "Loaded all columns via mmap\n";

    // Step 1: Filter customer by c_mktsegment = 'BUILDING'
    std::vector<int32_t> filtered_custkeys;
    for (size_t i = 0; i < 1500000; ++i) {
        if (c_mktsegment[i] == SEGMENT_BUILDING) {
            filtered_custkeys.push_back(static_cast<int32_t>(i + 1));
        }
    }
    std::cout << "Filtered customer: " << filtered_custkeys.size() << " rows\n";

    // Step 2: Hash join customer->orders with date filter
    std::unordered_map<int32_t, bool> custkey_set;
    for (int32_t ck : filtered_custkeys) {
        custkey_set[ck] = true;
    }

    std::vector<int32_t> joined_o_orderkey;
    std::vector<int32_t> joined_o_orderdate;
    std::vector<int32_t> joined_o_shippriority;

    for (size_t i = 0; i < 15000000; ++i) {
        if (custkey_set.count(o_custkey[i]) > 0 && o_orderdate[i] <= 1995) {
            joined_o_orderkey.push_back(o_orderkey[i]);
            joined_o_orderdate.push_back(o_orderdate[i]);
            joined_o_shippriority.push_back(o_shippriority[i]);
        }
    }
    std::cout << "After join orders: " << joined_o_orderkey.size() << " rows\n";

    // Step 3: Build order_map (shared read-only for all threads)
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> order_map;
    order_map.reserve(joined_o_orderkey.size() * 1.2);  // Pre-allocate to reduce resizing
    for (size_t i = 0; i < joined_o_orderkey.size(); ++i) {
        order_map[joined_o_orderkey[i]] = {joined_o_orderdate[i], joined_o_shippriority[i]};
    }

    // Step 4: PARALLELIZED Lineitem scan with morsel-driven execution
    // Each thread processes a morsel of rows and maintains its own hash table
    const size_t LINEITEM_ROWS = 59986052;
    const size_t MORSEL_SIZE = 8000;  // Optimal morsel size for this hardware (64 cores, 44MB L3)
    const int NUM_THREADS = std::thread::hardware_concurrency();

    std::cout << "Using " << NUM_THREADS << " threads with morsel size " << MORSEL_SIZE << "\n";

    // Thread-local aggregation tables
    std::vector<std::unordered_map<GroupKey, AggState, GroupKeyHash>> thread_agg_tables(NUM_THREADS);
    std::vector<size_t> thread_filter_counts(NUM_THREADS, 0);
    std::atomic<size_t> morsel_counter(0);

    // Pre-allocate hash tables to reduce contention during resizing
    for (int t = 0; t < NUM_THREADS; ++t) {
        thread_agg_tables[t].reserve(570000 / NUM_THREADS + 1000);
    }

    // Lambda for morsel-driven lineitem scan with I/O prefetching
    auto scan_lineitem_morsel = [&](int thread_id) {
        std::unordered_map<GroupKey, AggState, GroupKeyHash>& local_agg = thread_agg_tables[thread_id];
        size_t& local_filter_count = thread_filter_counts[thread_id];

        const size_t PREFETCH_DISTANCE = 256;  // Prefetch 256 rows ahead for HDD

        while (true) {
            // Atomically pull next morsel
            size_t morsel_start = morsel_counter.fetch_add(MORSEL_SIZE, std::memory_order_relaxed);
            if (morsel_start >= LINEITEM_ROWS) {
                break;
            }

            size_t morsel_end = std::min(morsel_start + MORSEL_SIZE, LINEITEM_ROWS);

            // Prefetch column data for this morsel (HDD optimization for sequential scan)
            // madvise WILLNEED tells the kernel to eagerly page in this range
            if (morsel_end - morsel_start >= PREFETCH_DISTANCE) {
                const size_t PREFETCH_BYTES = sizeof(int32_t) * (morsel_end - morsel_start);
                const size_t L_SHIPDATE_OFFSET = sizeof(int32_t) * morsel_start;
                const size_t L_ORDERKEY_OFFSET = sizeof(int32_t) * morsel_start;
                const size_t L_EXTENDEDPRICE_OFFSET = sizeof(double) * morsel_start;
                const size_t L_DISCOUNT_OFFSET = sizeof(double) * morsel_start;

                // Prefetch critical columns for this morsel
                madvise((char*)l_shipdate + L_SHIPDATE_OFFSET, PREFETCH_BYTES, MADV_WILLNEED);
                madvise((char*)l_orderkey + L_ORDERKEY_OFFSET, PREFETCH_BYTES, MADV_WILLNEED);
                madvise((char*)l_extendedprice + L_EXTENDEDPRICE_OFFSET, PREFETCH_BYTES * 2, MADV_WILLNEED);
                madvise((char*)l_discount + L_DISCOUNT_OFFSET, PREFETCH_BYTES * 2, MADV_WILLNEED);
            }

            // Process this morsel
            for (size_t i = morsel_start; i < morsel_end; ++i) {
                // Date filter: l_shipdate > 1995-03-15 (year-only: >= 1995 for year containment)
                if (l_shipdate[i] >= 1995) {
                    ++local_filter_count;

                    // Probe hash table
                    int32_t ok = l_orderkey[i];
                    auto it = order_map.find(ok);
                    if (it != order_map.end()) {
                        int32_t od = it->second.first;
                        int32_t sp = it->second.second;

                        double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);

                        GroupKey key = {ok, od, sp};
                        local_agg[key].revenue_sum += revenue;
                        local_agg[key].count += 1;
                    }
                }
            }
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; ++t) {
        threads.emplace_back(scan_lineitem_morsel, t);
    }

    // Wait for all threads to complete
    for (auto& th : threads) {
        th.join();
    }

    // Merge thread-local tables into global aggregation
    std::unordered_map<GroupKey, AggState, GroupKeyHash> agg_groups;
    agg_groups.reserve(570000);
    size_t total_filter_count = 0;

    for (int t = 0; t < NUM_THREADS; ++t) {
        total_filter_count += thread_filter_counts[t];
        for (const auto& [key, agg] : thread_agg_tables[t]) {
            agg_groups[key].revenue_sum += agg.revenue_sum;
            agg_groups[key].count += agg.count;
        }
    }

    std::cout << "Lineitem pass filter: " << total_filter_count << " rows\n";
    std::cout << "After aggregation: " << agg_groups.size() << " groups\n";

    // Step 5: Sort results and output
    std::vector<ResultRow> results;
    results.reserve(agg_groups.size());
    for (const auto& [key, agg] : agg_groups) {
        results.push_back({key.l_orderkey, agg.revenue_sum, key.o_orderdate, key.o_shippriority});
    }

    // Sort: revenue DESC, o_orderdate ASC
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (std::abs(a.revenue - b.revenue) > 1e-6) {
            return a.revenue > b.revenue;
        }
        return a.o_orderdate < b.o_orderdate;
    });

    // Write output
    size_t limit = std::min(size_t(10), results.size());
    std::ofstream out(results_dir + "/Q3.csv");
    out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
    out << std::fixed << std::setprecision(4);

    for (size_t i = 0; i < limit; ++i) {
        out << results[i].l_orderkey << ","
            << results[i].revenue << ","
            << format_date(results[i].o_orderdate) << ","
            << results[i].o_shippriority << "\n";
    }
    out.close();

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "\n=== Query Results ===\n";
    std::cout << "Total result groups: " << agg_groups.size() << "\n";
    std::cout << "Output rows (LIMIT 10): " << limit << "\n";
    std::cout << "Execution time: " << std::fixed << std::setprecision(2)
              << (duration.count() / 1000.0) << " seconds\n";

    return 0;
}
