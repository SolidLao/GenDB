// Q3: Shipping Priority
// SELECT l_orderkey, SUM(l_extendedprice*(1-l_discount)) AS revenue,
//        o_orderdate, o_shippriority
// FROM customer, orders, lineitem
// WHERE c_mktsegment = 'BUILDING'
//   AND c_custkey = o_custkey
//   AND l_orderkey = o_orderkey
//   AND o_orderdate < DATE '1995-03-15'
//   AND l_shipdate  > DATE '1995-03-15'
// GROUP BY l_orderkey, o_orderdate, o_shippriority
// ORDER BY revenue DESC, o_orderdate ASC
// LIMIT 10;
//
// Strategy:
// 1. Build bool qualifying_cust[1500001] (bitset by custkey) — 1-threaded
// 2. Build compact open-addressing orders hash map (filtered by date & segment) — 1-threaded
// 3. Parallel morsel-driven lineitem scan with zone-map skipping — N threads
// 4. Merge thread-local aggregation maps
// 5. Top-10 partial sort and output

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <stdexcept>
#include <algorithm>
#include <filesystem>
#include <climits>
#include <vector>
#include <unordered_map>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"
#include "date_utils.h"

// ---------------------------------------------------------------------------
// hash32 — same function used in build_indexes.cpp
// ---------------------------------------------------------------------------
static inline uint32_t hash32(uint32_t x) {
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = ((x >> 16) ^ x) * 0x45d9f3bu;
    x = (x >> 16) ^ x;
    return x;
}

// ---------------------------------------------------------------------------
// Orders compact hash map slot
// key = INT32_MIN means empty
// ---------------------------------------------------------------------------
struct OrdSlot {
    int32_t key;          // o_orderkey  (INT32_MIN = empty)
    int32_t orderdate;    // o_orderdate
    int32_t shippriority; // o_shippriority
};

static const int32_t EMPTY_KEY = INT32_MIN;

// ---------------------------------------------------------------------------
// Aggregation entry (thread-local)
// ---------------------------------------------------------------------------
struct AggEntry {
    double  revenue;
    int32_t orderdate;
    int32_t shippriority;
};

// ---------------------------------------------------------------------------
// Read zone map: int32 num_blocks, int32 block_size, int32 min[], int32 max[]
// ---------------------------------------------------------------------------
static void read_zone_map(const std::string& path,
                          int32_t& num_blocks,
                          int32_t& block_size,
                          std::vector<int32_t>& zm_min,
                          std::vector<int32_t>& zm_max) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open zone map: " + path);

    if (::read(fd, &num_blocks, 4) != 4 || ::read(fd, &block_size, 4) != 4) {
        ::close(fd); throw std::runtime_error("Short read zone map header: " + path);
    }
    zm_min.resize(num_blocks);
    zm_max.resize(num_blocks);
    ssize_t nb4 = (ssize_t)num_blocks * 4;
    if (::read(fd, zm_min.data(), nb4) != nb4 ||
        ::read(fd, zm_max.data(), nb4) != nb4) {
        ::close(fd); throw std::runtime_error("Short read zone map data: " + path);
    }
    ::close(fd);
}

// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // -----------------------------------------------------------------------
    // Constants
    // -----------------------------------------------------------------------
    gendb::init_date_tables();
    const int32_t kThreshold = gendb::date_str_to_epoch_days("1995-03-15"); // 9204
    const int32_t kOrderDateMax = kThreshold;  // o_orderdate < kThreshold
    const int32_t kShipdateMin  = kThreshold;  // l_shipdate  > kThreshold

    // kBuilding derived from encode_mktsegment switch: case 'B' -> 1
    const int8_t kBuilding = 1;

    constexpr size_t BLOCK_SIZE = 100000;

    // -----------------------------------------------------------------------
    // Phase: data_loading — mmap all columns
    // -----------------------------------------------------------------------
    const std::string cust_dir = gendb_dir + "/customer";
    const std::string ord_dir  = gendb_dir + "/orders";
    const std::string li_dir   = gendb_dir + "/lineitem";

    gendb::MmapColumn<int8_t>  col_c_mkt   (cust_dir + "/c_mktsegment.bin");
    gendb::MmapColumn<int32_t> col_c_ckey  (cust_dir + "/c_custkey.bin");

    gendb::MmapColumn<int32_t> col_o_ckey  (ord_dir  + "/o_custkey.bin");
    gendb::MmapColumn<int32_t> col_o_odate (ord_dir  + "/o_orderdate.bin");
    gendb::MmapColumn<int32_t> col_o_okey  (ord_dir  + "/o_orderkey.bin");
    gendb::MmapColumn<int32_t> col_o_spri  (ord_dir  + "/o_shippriority.bin");

    gendb::MmapColumn<int32_t> col_l_okey  (li_dir   + "/l_orderkey.bin");
    gendb::MmapColumn<int32_t> col_l_ship  (li_dir   + "/l_shipdate.bin");
    gendb::MmapColumn<double>  col_l_ext   (li_dir   + "/l_extendedprice.bin");
    gendb::MmapColumn<double>  col_l_disc  (li_dir   + "/l_discount.bin");

    const size_t n_cust = col_c_mkt.size();
    const size_t n_ord  = col_o_ckey.size();
    const size_t n_li   = col_l_okey.size();

    // Prefetch lineitem columns for parallel scan (HDD — fire readahead early)
    mmap_prefetch_all(col_l_ship, col_l_okey, col_l_ext, col_l_disc);

    // Read zone maps
    int32_t ord_nb, ord_bs;
    std::vector<int32_t> ord_zm_min, ord_zm_max;
    read_zone_map(ord_dir + "/o_orderdate_zone_map.bin",
                  ord_nb, ord_bs, ord_zm_min, ord_zm_max);

    int32_t li_nb, li_bs;
    std::vector<int32_t> li_zm_min, li_zm_max;
    read_zone_map(li_dir + "/l_shipdate_zone_map.bin",
                  li_nb, li_bs, li_zm_min, li_zm_max);

    // -----------------------------------------------------------------------
    // Phase: dim_filter — build qualifying customer bitset
    // -----------------------------------------------------------------------
    // bool qualifying_cust[1500001], indexed directly by c_custkey value
    // TPC-H c_custkey is 1-based [1..1500000]; max value = 1500000
    static bool qualifying_cust[1500001];
    std::memset(qualifying_cust, 0, sizeof(qualifying_cust));

    {
        GENDB_PHASE("dim_filter");
        const int8_t*  mkt  = col_c_mkt.data;
        const int32_t* ckey = col_c_ckey.data;
        for (size_t i = 0; i < n_cust; ++i) {
            if (mkt[i] == kBuilding) {
                qualifying_cust[ckey[i]] = true;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase: build_joins — build qualifying orders hash map
    //   open-addressing, capacity 4194304 (power-of-2), sentinel = INT32_MIN
    // -----------------------------------------------------------------------
    const uint32_t ORD_CAP  = 4194304u;
    const uint32_t ORD_MASK = ORD_CAP - 1u;

    // Allocate on heap (~50 MB)
    std::vector<OrdSlot> ord_map(ORD_CAP, OrdSlot{EMPTY_KEY, 0, 0});

    {
        GENDB_PHASE("build_joins");

        const int32_t* o_ckey  = col_o_ckey.data;
        const int32_t* o_odate = col_o_odate.data;
        const int32_t* o_okey  = col_o_okey.data;
        const int32_t* o_spri  = col_o_spri.data;
        OrdSlot* slots = ord_map.data();

        for (int32_t b = 0; b < ord_nb; ++b) {
            // orders sorted ascending by o_orderdate:
            // if min[b] >= kOrderDateMax, all subsequent blocks also fail → break
            if (ord_zm_min[b] >= kOrderDateMax) break;

            size_t row_lo = (size_t)b * (size_t)ord_bs;
            size_t row_hi = std::min(row_lo + (size_t)ord_bs, n_ord);
            bool all_pass_date = (ord_zm_max[b] < kOrderDateMax);

            for (size_t i = row_lo; i < row_hi; ++i) {
                if (!all_pass_date && o_odate[i] >= kOrderDateMax) continue;
                int32_t ck = o_ckey[i];
                if (!qualifying_cust[ck]) continue;

                // Insert into orders hash map
                int32_t ok = o_okey[i];
                uint32_t h = hash32((uint32_t)ok) & ORD_MASK;
                while (slots[h].key != EMPTY_KEY) {
                    h = (h + 1u) & ORD_MASK;
                }
                slots[h].key          = ok;
                slots[h].orderdate    = o_odate[i];
                slots[h].shippriority = o_spri[i];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase: main_scan — parallel morsel-driven lineitem scan
    // Each thread owns a local unordered_map<int32_t, AggEntry>
    // -----------------------------------------------------------------------
    const int nthreads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, AggEntry>> thread_maps(nthreads);
    for (auto& m : thread_maps) m.reserve(65536);

    {
        GENDB_PHASE("main_scan");

        const int32_t* l_okey = col_l_okey.data;
        const int32_t* l_ship = col_l_ship.data;
        const double*  l_ext  = col_l_ext.data;
        const double*  l_disc = col_l_disc.data;
        const OrdSlot* slots  = ord_map.data();
        const int32_t  li_nb_ = li_nb;
        const size_t   li_bs_ = (size_t)li_bs;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local_map = thread_maps[tid];

            #pragma omp for schedule(dynamic, 4) nowait
            for (int32_t b = 0; b < li_nb_; ++b) {
                // Skip blocks entirely before threshold
                if (li_zm_max[b] <= kShipdateMin) continue;

                size_t row_lo = (size_t)b * li_bs_;
                size_t row_hi = std::min(row_lo + li_bs_, n_li);
                bool all_pass_ship = (li_zm_min[b] > kShipdateMin);

                for (size_t i = row_lo; i < row_hi; ++i) {
                    if (!all_pass_ship && l_ship[i] <= kShipdateMin) continue;

                    int32_t orderkey = l_okey[i];
                    uint32_t h = hash32((uint32_t)orderkey) & ORD_MASK;
                    const OrdSlot* s = slots + h;

                    // Probe orders hash map
                    while (s->key != EMPTY_KEY) {
                        if (s->key == orderkey) {
                            double rev = l_ext[i] * (1.0 - l_disc[i]);
                            auto it = local_map.find(orderkey);
                            if (__builtin_expect(it != local_map.end(), 1)) {
                                it->second.revenue += rev;
                            } else {
                                local_map.emplace(orderkey,
                                    AggEntry{rev, s->orderdate, s->shippriority});
                            }
                            break;
                        }
                        h = (h + 1u) & ORD_MASK;
                        s = slots + h;
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase: merge thread-local maps into a single aggregation map
    // -----------------------------------------------------------------------
    std::unordered_map<int32_t, AggEntry> final_map;
    final_map.reserve(3500000);

    {
        for (int t = 0; t < nthreads; ++t) {
            for (auto& [key, val] : thread_maps[t]) {
                auto it = final_map.find(key);
                if (it != final_map.end()) {
                    it->second.revenue += val.revenue;
                } else {
                    final_map.emplace(key, val);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase: output — top-10 by (revenue DESC, o_orderdate ASC)
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        // Build result vector
        struct ResultRow {
            int32_t l_orderkey;
            double  revenue;
            int32_t o_orderdate;
            int32_t o_shippriority;
        };

        std::vector<ResultRow> results;
        results.reserve(final_map.size());
        for (auto& [key, val] : final_map) {
            results.push_back({key, val.revenue, val.orderdate, val.shippriority});
        }

        // Partial sort: top-10 by revenue DESC, o_orderdate ASC
        size_t k = std::min((size_t)10, results.size());
        std::partial_sort(results.begin(), results.begin() + (ptrdiff_t)k, results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.revenue != b.revenue) return a.revenue > b.revenue;
                return a.o_orderdate < b.o_orderdate;
            });

        std::filesystem::create_directories(results_dir);
        const std::string out_path = results_dir + "/Q3.csv";
        FILE* f = std::fopen(out_path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "Cannot open output: %s\n", out_path.c_str());
            return 1;
        }

        std::fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[11];
        for (size_t i = 0; i < k; ++i) {
            gendb::epoch_days_to_date_str(results[i].o_orderdate, date_buf);
            std::fprintf(f, "%d,%.4f,%s,%d\n",
                results[i].l_orderkey,
                results[i].revenue,
                date_buf,
                results[i].o_shippriority);
        }
        std::fclose(f);

        // Print to stdout for debugging
        for (size_t i = 0; i < k; ++i) {
            gendb::epoch_days_to_date_str(results[i].o_orderdate, date_buf);
            std::printf("%d,%.4f,%s,%d\n",
                results[i].l_orderkey,
                results[i].revenue,
                date_buf,
                results[i].o_shippriority);
        }
    }

    return 0;
}
