#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include <iomanip>
#include <cmath>

// Constants
const int SEGMENT_BUILDING = 0;  // Dictionary encoded value for "BUILDING"
const int32_t DATE_CUTOFF = 1995;  // Year 1995

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
        if (ptr != nullptr) {
            munmap(ptr, size);
        }
    }
};

MappedColumn mmap_column(const std::string& path) {
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

    madvise(col.ptr, col.size, MADV_SEQUENTIAL);
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
    system(("mkdir -p " + results_dir).c_str());

    std::cout << "Q3: Shipping Priority\n";
    std::cout << "GenDB Dir: " << gendb_dir << "\n";
    std::cout << "Results Dir: " << results_dir << "\n\n";

    // Load and mmap columns
    auto c_mktsegment_col = mmap_column(gendb_dir + "/customer/c_mktsegment.col");
    auto o_custkey_col = mmap_column(gendb_dir + "/orders/o_custkey.col");
    auto o_orderkey_col = mmap_column(gendb_dir + "/orders/o_orderkey.col");
    auto o_orderdate_col = mmap_column(gendb_dir + "/orders/o_orderdate.col");
    auto o_shippriority_col = mmap_column(gendb_dir + "/orders/o_shippriority.col");
    auto l_orderkey_col = mmap_column(gendb_dir + "/lineitem/l_orderkey.col");
    auto l_extendedprice_col = mmap_column(gendb_dir + "/lineitem/l_extendedprice.col");
    auto l_discount_col = mmap_column(gendb_dir + "/lineitem/l_discount.col");
    auto l_shipdate_col = mmap_column(gendb_dir + "/lineitem/l_shipdate.col");

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
        if (custkey_set.count(o_custkey[i]) > 0 && o_orderdate[i] <= DATE_CUTOFF) {
            joined_o_orderkey.push_back(o_orderkey[i]);
            joined_o_orderdate.push_back(o_orderdate[i]);
            joined_o_shippriority.push_back(o_shippriority[i]);
        }
    }
    std::cout << "After join orders: " << joined_o_orderkey.size() << " rows\n";

    // Step 3: Hash aggregation: join lineitem + filter + aggregate
    std::unordered_map<GroupKey, AggState, GroupKeyHash> agg_groups;

    // Build hash map from joined orders
    std::unordered_map<int32_t, std::pair<int32_t, int32_t>> order_map;
    for (size_t i = 0; i < joined_o_orderkey.size(); ++i) {
        order_map[joined_o_orderkey[i]] = {joined_o_orderdate[i], joined_o_shippriority[i]};
    }

    // Probe: scan lineitem with filter and aggregate
    size_t li_filter_pass = 0;
    for (size_t i = 0; i < 59986052; ++i) {
        if (l_shipdate[i] >= DATE_CUTOFF) {
            ++li_filter_pass;
            auto it = order_map.find(l_orderkey[i]);
            if (it != order_map.end()) {
                int32_t ok = l_orderkey[i];
                int32_t od = it->second.first;
                int32_t sp = it->second.second;

                double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);

                GroupKey key = {ok, od, sp};
                agg_groups[key].revenue_sum += revenue;
                agg_groups[key].count += 1;
            }
        }
    }
    std::cout << "Lineitem pass filter: " << li_filter_pass << " rows\n";
    std::cout << "After aggregation: " << agg_groups.size() << " groups\n";

    // Step 4: Sort results and output
    std::vector<ResultRow> results;
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
            << results[i].o_orderdate << ","
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
