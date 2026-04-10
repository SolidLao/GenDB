// Q3 tail — Shipping Priority
// Shared inputs: scan_lineitem_full, hash_orders_by_orderkey, hash_customer_by_custkey
// Operators: bitset build (customer filter), residual filter (shipdate), hash probe (orders),
//            bitset semi-filter (custkey), hash aggregation, top-10, project+output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"
#include "shared/hash_orders_by_orderkey.hpp"
#include "shared/hash_customer_by_custkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <unordered_map>
#include <vector>
#include <omp.h>

namespace mqo::tails {

// Date constant: 1995-03-15 as days since epoch
static constexpr int32_t DATE_1995_03_15 = 9204;

// Dict code for 'BUILDING' in c_mktsegment
static constexpr uint8_t MKTSEG_BUILDING = 0;

// Aggregation entry: keyed by l_orderkey (which functionally determines o_orderdate, o_shippriority)
struct AggEntry {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;
    double  revenue;
};

// Comparator for top-K min-heap: we want top-10 by revenue DESC, o_orderdate ASC
// Min-heap evicts the smallest element. An element is "smaller" (lower priority) if:
//   - its revenue is lower, OR
//   - same revenue but o_orderdate is larger (later date = lower priority)
struct HeapCmp {
    bool operator()(const AggEntry& a, const AggEntry& b) const {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;  // higher revenue = higher priority
        return a.o_orderdate < b.o_orderdate;  // earlier date = higher priority
    }
};

void run_Q3(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q3_tail");

    // --- Step 0: Build BUILDING custkey bitset ---
    // Customer keys are dense 1..1500000, so a bitset is ~188KB (fits in L2 cache)
    static constexpr int32_t CUST_MAX = 1500001;
    static constexpr size_t BITSET_WORDS = (CUST_MAX + 63) / 64;
    std::vector<uint64_t> building_bitset(BITSET_WORDS, 0);

    {
        MQO_TIME_PHASE("Q3_build_bitset");
        const auto& cust = mqo::shared::hash_customer_by_custkey::get();
        // Scan all customer entries in the direct array
        for (int32_t k = 1; k <= mqo::shared::hash_customer_by_custkey::MAX_KEY; ++k) {
            const auto& e = cust.entries[k];
            if (e.c_custkey != 0 && e.c_mktsegment == MKTSEG_BUILDING) {
                building_bitset[k >> 6] |= (1ULL << (k & 63));
            }
        }
    }

    // --- Steps 1-4: Parallel scan of lineitem with filter + probe + bitset check + aggregate ---
    const auto& li = mqo::shared::scan_lineitem_full::get_columns(ctx);
    const auto& orders = mqo::shared::hash_orders_by_orderkey::get();
    const size_t n_rows = li.n_rows;

    const int n_threads = omp_get_max_threads();

    // Thread-local hash maps: key = l_orderkey -> AggEntry
    // Using a simple open-addressing hash map for compactness
    // ~780K groups expected, use 1M buckets per thread
    struct LocalMap {
        static constexpr size_t CAPACITY = 1 << 20;  // 1M buckets
        static constexpr size_t MASK = CAPACITY - 1;
        struct Bucket {
            int32_t key = 0;  // 0 = empty
            int32_t o_orderdate;
            int32_t o_shippriority;
            double  revenue;
        };
        std::vector<Bucket> buckets;
        size_t count = 0;

        LocalMap() : buckets(CAPACITY) {}

        void insert(int32_t orderkey, int32_t orderdate, int32_t shippri, double rev) {
            size_t h = static_cast<uint32_t>(orderkey) * 2654435761u;
            h &= MASK;
            while (true) {
                auto& b = buckets[h];
                if (b.key == 0) {
                    b.key = orderkey;
                    b.o_orderdate = orderdate;
                    b.o_shippriority = shippri;
                    b.revenue = rev;
                    ++count;
                    return;
                }
                if (b.key == orderkey) {
                    b.revenue += rev;
                    return;
                }
                h = (h + 1) & MASK;
            }
        }
    };

    std::vector<LocalMap> thread_maps(n_threads);

    {
        MQO_TIME_PHASE("Q3_main_scan");

        const int32_t* __restrict__ l_orderkey      = li.l_orderkey;
        const double*  __restrict__ l_extendedprice  = li.l_extendedprice;
        const double*  __restrict__ l_discount       = li.l_discount;
        const int32_t* __restrict__ l_shipdate       = li.l_shipdate;
        const uint64_t* __restrict__ bset            = building_bitset.data();

        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            LocalMap& lmap = thread_maps[tid];

            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_rows; ++i) {
                // Step 1: residual filter on l_shipdate
                if (l_shipdate[i] <= DATE_1995_03_15) continue;

                // Step 2: probe orders hash by l_orderkey, check o_orderdate < 9204
                const int32_t okey = l_orderkey[i];
                const auto* oe = orders.probe(okey);
                if (!oe) continue;
                if (oe->o_orderdate >= DATE_1995_03_15) continue;

                // Step 3: bitset semi-filter on o_custkey
                const int32_t ck = oe->o_custkey;
                if (!(bset[ck >> 6] & (1ULL << (ck & 63)))) continue;

                // Step 4: aggregate
                double rev = l_extendedprice[i] * (1.0 - l_discount[i]);
                lmap.insert(okey, oe->o_orderdate, oe->o_shippriority, rev);
            }
        }
    }

    // --- Step 5-6: Merge thread-local maps with top-10 heap ---
    std::vector<AggEntry> top10;
    top10.reserve(11);
    HeapCmp cmp;

    {
        MQO_TIME_PHASE("Q3_merge_topk");

        // Merge all thread maps into a single map, then extract top-10
        // Use thread 0's map as the merge target
        auto& merged = thread_maps[0];
        for (int t = 1; t < n_threads; ++t) {
            auto& src = thread_maps[t];
            for (size_t b = 0; b < LocalMap::CAPACITY; ++b) {
                if (src.buckets[b].key != 0) {
                    merged.insert(src.buckets[b].key, src.buckets[b].o_orderdate,
                                  src.buckets[b].o_shippriority, src.buckets[b].revenue);
                }
            }
            // Free source map memory
            src.buckets.clear();
            src.buckets.shrink_to_fit();
        }

        // Extract top-10 using a min-heap
        for (size_t b = 0; b < LocalMap::CAPACITY; ++b) {
            const auto& bkt = merged.buckets[b];
            if (bkt.key == 0) continue;

            AggEntry entry{bkt.key, bkt.o_orderdate, bkt.o_shippriority, bkt.revenue};

            if (top10.size() < 10) {
                top10.push_back(entry);
                if (top10.size() == 10) {
                    std::make_heap(top10.begin(), top10.end(), cmp);
                }
            } else {
                // Compare with heap top (minimum priority element)
                if (cmp(top10[0], entry)) {
                    // entry has higher priority than heap top → replace
                    std::pop_heap(top10.begin(), top10.end(), cmp);
                    top10.back() = entry;
                    std::push_heap(top10.begin(), top10.end(), cmp);
                }
            }
        }

        // Sort the top-10 in final order: revenue DESC, o_orderdate ASC
        std::sort(top10.begin(), top10.end(), [](const AggEntry& a, const AggEntry& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.o_orderdate < b.o_orderdate;
        });
    }

    // --- Step 7: Output ---
    {
        MQO_TIME_PHASE("Q3_output");

        std::string outpath = ctx.output_dir + "/q3.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[MQO] Cannot open output: %s\n", outpath.c_str());
            return;
        }

        std::fprintf(fp, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        for (const auto& e : top10) {
            // Convert o_orderdate from epoch days to YYYY-MM-DD
            int y, m, d;
            mqo::io::from_epoch_days(e.o_orderdate, y, m, d);
            std::fprintf(fp, "%d,%.2f,%04d-%02d-%02d,%d\n",
                         e.l_orderkey, e.revenue, y, m, d, e.o_shippriority);
        }
        std::fclose(fp);
    }
}

}  // namespace mqo::tails
