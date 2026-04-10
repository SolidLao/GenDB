// Q3 Tail: Shipping Priority
// Shared inputs: scan_lineitem_full, scan_orders_full, hash_customer_by_custkey
// Pipeline: bitset(customer BUILDING) → filter+semi-join(orders) → hash build(orders) →
//           filter+probe+aggregate(lineitem) → top-10 → output CSV

#include "mqo_profile.hpp"
#include "shared/scan_lineitem_full.hpp"
#include "shared/scan_orders_full.hpp"
#include "shared/hash_customer_by_custkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <cmath>

namespace mqo { namespace tails {

// Date constant: 1995-03-15 as days since epoch
static constexpr int32_t DATE_19950315 = 9204;

// Compact orders hash entry: keyed by o_orderkey, payload = (o_orderdate, o_shippriority)
struct OrdersEntry {
    int32_t o_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
};

// Aggregation entry
struct AggEntry {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double  revenue;
};

// Open-addressing hash map with linear probing
struct OrdersHashMap {
    static constexpr int32_t EMPTY = 0;  // o_orderkey is always > 0
    size_t capacity;
    size_t mask;
    int32_t* keys;       // o_orderkey (0 = empty)
    int32_t* dates;      // o_orderdate
    int32_t* priorities; // o_shippriority

    void init(size_t cap) {
        capacity = cap;
        mask = cap - 1;
        keys = new int32_t[cap]();
        dates = new int32_t[cap];
        priorities = new int32_t[cap];
    }

    void insert(int32_t key, int32_t date, int32_t prio) {
        size_t pos = static_cast<uint32_t>(key) & mask;
        while (keys[pos] != EMPTY) {
            pos = (pos + 1) & mask;
        }
        keys[pos] = key;
        dates[pos] = date;
        priorities[pos] = prio;
    }

    // Returns index or -1 if not found
    int64_t probe(int32_t key) const {
        size_t pos = static_cast<uint32_t>(key) & mask;
        while (true) {
            int32_t k = keys[pos];
            if (k == key) return static_cast<int64_t>(pos);
            if (k == EMPTY) return -1;
            pos = (pos + 1) & mask;
        }
    }

    void destroy() {
        delete[] keys;
        delete[] dates;
        delete[] priorities;
    }
};

// Thread-local aggregation hash map
struct AggHashMap {
    static constexpr int32_t EMPTY = 0;
    size_t capacity;
    size_t mask;
    int32_t* keys;
    double*  revenues;
    int32_t* dates;
    int32_t* priorities;
    size_t   count;

    void init(size_t cap) {
        capacity = cap;
        mask = cap - 1;
        count = 0;
        keys = new int32_t[cap]();
        revenues = new double[cap]();
        dates = new int32_t[cap];
        priorities = new int32_t[cap];
    }

    void insert_or_add(int32_t key, double rev, int32_t date, int32_t prio) {
        size_t pos = static_cast<uint32_t>(key) & mask;
        while (true) {
            int32_t k = keys[pos];
            if (k == key) {
                revenues[pos] += rev;
                return;
            }
            if (k == EMPTY) {
                keys[pos] = key;
                revenues[pos] = rev;
                dates[pos] = date;
                priorities[pos] = prio;
                ++count;
                return;
            }
            pos = (pos + 1) & mask;
        }
    }

    void destroy() {
        delete[] keys;
        delete[] revenues;
        delete[] dates;
        delete[] priorities;
    }
};

void run_Q3(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q3_tail");

    // Fetch shared data
    const auto& li = mqo::shared::scan_lineitem_full::get();
    const auto& ord = mqo::shared::scan_orders_full::get();
    const auto& cust = mqo::shared::hash_customer_by_custkey::get();

    // Step 1: Build bitset of custkeys where c_mktsegment = 'BUILDING'
    // Resolve dict code for 'BUILDING'
    uint8_t building_code = 255;
    {
        MQO_TIME_PHASE("Q3_resolve_dict");
        mqo::io::Dictionary dict;
        dict.load(ctx.gendb_dir + "/customer/c_mktsegment_dict.bin");
        for (uint32_t i = 0; i < dict.count; ++i) {
            if (dict.entries[i] == "BUILDING") {
                building_code = static_cast<uint8_t>(i);
                break;
            }
        }
    }

    // Bitset for qualifying custkeys
    static constexpr size_t BITSET_SIZE = 1500001;
    std::vector<uint64_t> cust_bitset((BITSET_SIZE + 63) / 64, 0);

    {
        MQO_TIME_PHASE("Q3_customer_bitset");
        const size_t n_cust = cust.n_rows;
        const uint8_t* mktseg = cust.c_mktsegment;
        const int32_t* custkeys = cust.c_custkey;
        for (size_t i = 0; i < n_cust; ++i) {
            if (mktseg[i] == building_code) {
                int32_t ck = custkeys[i];
                cust_bitset[static_cast<uint32_t>(ck) >> 6] |=
                    (1ULL << (static_cast<uint32_t>(ck) & 63));
            }
        }
    }

    // Step 2: Filter orders (o_orderdate < 1995-03-15 AND custkey in bitset)
    // Step 3: Build hash map on qualifying orders
    OrdersHashMap orders_ht;
    {
        MQO_TIME_PHASE("Q3_orders_filter_and_hash");
        // Pre-size to next power of 2 above estimated 1.44M
        orders_ht.init(2097152);

        const size_t n_ord = ord.n_rows;
        const int32_t* okeys = ord.o_orderkey;
        const int32_t* ocust = ord.o_custkey;
        const int32_t* odates = ord.o_orderdate;
        const int32_t* oprio = ord.o_shippriority;

        for (size_t i = 0; i < n_ord; ++i) {
            if (odates[i] < DATE_19950315) {
                int32_t ck = ocust[i];
                if (ck >= 0 && static_cast<uint32_t>(ck) < BITSET_SIZE) {
                    if (cust_bitset[static_cast<uint32_t>(ck) >> 6] &
                        (1ULL << (static_cast<uint32_t>(ck) & 63))) {
                        orders_ht.insert(okeys[i], odates[i], oprio[i]);
                    }
                }
            }
        }
    }

    // Step 4: Scan lineitem, filter l_shipdate > 1995-03-15, probe orders hash,
    //         fused aggregation with thread-local hash maps
    // Step 5: Merge thread-local maps

    // Use a global aggregation map (single-threaded for simplicity given tail concurrency)
    AggHashMap global_agg;
    {
        MQO_TIME_PHASE("Q3_lineitem_probe_agg");
        // Pre-size: ~1.44M groups, use 4M capacity for low load factor
        global_agg.init(4194304);

        const size_t n_li = li.n_rows;
        const int32_t* lkeys = li.l_orderkey;
        const double* lprices = li.l_extendedprice;
        const double* ldiscs = li.l_discount;
        const int32_t* lship = li.l_shipdate;

        for (size_t i = 0; i < n_li; ++i) {
            if (lship[i] > DATE_19950315) {
                int64_t pos = orders_ht.probe(lkeys[i]);
                if (pos >= 0) {
                    double rev = lprices[i] * (1.0 - ldiscs[i]);
                    global_agg.insert_or_add(lkeys[i], rev,
                                             orders_ht.dates[pos],
                                             orders_ht.priorities[pos]);
                }
            }
        }
    }

    // Step 6: Top-10 by revenue DESC, o_orderdate ASC
    struct ResultRow {
        int32_t l_orderkey;
        double  revenue;
        int32_t o_orderdate;
        int32_t o_shippriority;
    };

    std::vector<ResultRow> results;
    {
        MQO_TIME_PHASE("Q3_topk");
        // Collect all entries from aggregation map
        results.reserve(global_agg.count);
        for (size_t i = 0; i < global_agg.capacity; ++i) {
            if (global_agg.keys[i] != AggHashMap::EMPTY) {
                results.push_back({global_agg.keys[i],
                                   global_agg.revenues[i],
                                   global_agg.dates[i],
                                   global_agg.priorities[i]});
            }
        }

        // Partial sort for top 10
        if (results.size() > 10) {
            std::partial_sort(results.begin(), results.begin() + 10, results.end(),
                [](const ResultRow& a, const ResultRow& b) {
                    if (a.revenue != b.revenue) return a.revenue > b.revenue;
                    return a.o_orderdate < b.o_orderdate;
                });
            results.resize(10);
        } else {
            std::sort(results.begin(), results.end(),
                [](const ResultRow& a, const ResultRow& b) {
                    if (a.revenue != b.revenue) return a.revenue > b.revenue;
                    return a.o_orderdate < b.o_orderdate;
                });
        }
    }

    // Step 7: Output CSV
    {
        MQO_TIME_PHASE("Q3_output");
        std::string outpath = ctx.output_dir + "/q3.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }
        std::fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");

        for (const auto& r : results) {
            // Convert days_since_epoch to date string
            // Epoch: 1970-01-01
            int32_t days = r.o_orderdate;
            // Use a simple conversion
            int y, m, d;
            {
                // Civil date from days since epoch (Euclidean algorithm)
                int z = days + 719468;
                int era = (z >= 0 ? z : z - 146096) / 146097;
                int doe = z - era * 146097;
                int yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
                y = yoe + era * 400;
                int doy = doe - (365*yoe + yoe/4 - yoe/100);
                int mp = (5*doy + 2) / 153;
                d = doy - (153*mp + 2) / 5 + 1;
                m = mp + (mp < 10 ? 3 : -9);
                if (m <= 2) ++y;
            }
            std::fprintf(fp, "%d,%.2f,%04d-%02d-%02d,%d\n",
                         r.l_orderkey, r.revenue, y, m, d, r.o_shippriority);
        }
        std::fclose(fp);
    }

    // Cleanup
    orders_ht.destroy();
    global_agg.destroy();
}

}} // namespace mqo::tails
