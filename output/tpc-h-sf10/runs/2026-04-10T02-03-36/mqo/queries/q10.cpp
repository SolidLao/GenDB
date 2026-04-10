// Q10 tail — Returned Item Reporting
// Shared inputs: scan_lineitem_full, scan_orders_full, hash_customer_by_custkey
// Operators: residual_filter(orders), hash_build(orders), filter+probe(lineitem→orders),
//            probe(→customer), nation_lookup, hash_aggregate(c_custkey), topk(revenue DESC, 20)

#include "mqo_profile.hpp"
#include "../shared/scan_lineitem_full.hpp"
#include "../shared/scan_orders_full.hpp"
#include "../shared/hash_customer_by_custkey.hpp"
#include "../shared/mqo_io.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <queue>

namespace mqo { namespace tails {

void run_Q10(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q10_tail");

    // ================================================================
    // Fetch shared components
    // ================================================================
    const auto& li  = mqo::shared::scan_lineitem_full::get();
    const auto& ord = mqo::shared::scan_orders_full::get();
    const auto& cust = mqo::shared::hash_customer_by_custkey::get();

    // ================================================================
    // Step 0: Load nation lookup (25 rows, direct array by n_nationkey)
    // ================================================================
    std::string nation_names[25];
    {
        MQO_TIME_PHASE("Q10_load_nation");
        std::string ndir = ctx.gendb_dir + "/nation";
        size_t nn = 25;
        const int32_t* n_nationkey = mqo::io::mmap_column<int32_t>(ndir + "/n_nationkey.bin", nn);
        // n_name is dict-encoded: uint8 codes + dictionary
        const uint8_t* n_name_codes = mqo::io::mmap_column<uint8_t>(ndir + "/n_name.bin", nn);
        mqo::io::Dictionary n_name_dict;
        n_name_dict.load(ndir + "/n_name_dict.bin");

        for (size_t i = 0; i < nn; ++i) {
            int32_t nk = n_nationkey[i];
            if (nk >= 0 && nk < 25) {
                nation_names[nk] = n_name_dict.get(n_name_codes[i]);
            }
        }
    }

    // ================================================================
    // Step 1: Filter orders by date range [8674, 8766)
    //         o_orderdate >= 1993-10-01 AND o_orderdate < 1994-01-01
    // Build hash table keyed by o_orderkey -> o_custkey
    // ================================================================
    constexpr int32_t DATE_LO = 8674;   // 1993-10-01
    constexpr int32_t DATE_HI = 8766;   // 1994-01-01

    // Use a flat array for orders hash: orderkey -> custkey
    // Orders has dense_pk, max orderkey ~ 60M for SF10
    // But building a 60M array just for 540K entries is wasteful.
    // Use a compact hash map instead.
    struct OrdersHT {
        // Open addressing hash table: key=o_orderkey, value=o_custkey
        static constexpr size_t CAPACITY = 1 << 20;  // 1M slots (~540K entries, ~50% load)
        static constexpr int32_t EMPTY = -1;
        int32_t keys[CAPACITY];
        int32_t vals[CAPACITY];

        void init() { std::memset(keys, 0xFF, sizeof(keys)); } // -1 fill

        void insert(int32_t key, int32_t val) {
            size_t slot = static_cast<uint32_t>(key) & (CAPACITY - 1);
            while (keys[slot] != EMPTY) {
                slot = (slot + 1) & (CAPACITY - 1);
            }
            keys[slot] = key;
            vals[slot] = val;
        }

        // Returns custkey or EMPTY if not found
        int32_t probe(int32_t key) const {
            size_t slot = static_cast<uint32_t>(key) & (CAPACITY - 1);
            while (true) {
                if (keys[slot] == key) return vals[slot];
                if (keys[slot] == EMPTY) return EMPTY;
                slot = (slot + 1) & (CAPACITY - 1);
            }
        }
    };

    // Heap-allocate to avoid stack overflow (8MB per array × 2)
    auto* oht = new OrdersHT();
    oht->init();

    {
        MQO_TIME_PHASE("Q10_filter_orders_build_hash");
        const size_t n_ord = ord.n_rows;
        for (size_t i = 0; i < n_ord; ++i) {
            int32_t d = ord.o_orderdate[i];
            if (d >= DATE_LO && d < DATE_HI) {
                oht->insert(ord.o_orderkey[i], ord.o_custkey[i]);
            }
        }
    }

    // ================================================================
    // Step 3-6: Scan lineitem, filter returnflag='R', probe orders hash,
    //           probe customer, lookup nation, aggregate by c_custkey
    // ================================================================
    struct AggEntry {
        double   revenue;
        int32_t  c_custkey;
        int32_t  c_nationkey;   // for nation lookup at output
        uint32_t c_row_id;      // customer row for varlen columns
        bool     occupied;
    };

    // Hash aggregation table keyed by c_custkey (int32)
    // ~350K groups expected, use 512K slots
    static constexpr size_t AGG_CAPACITY = 1 << 19;  // 524288
    static constexpr size_t AGG_MASK = AGG_CAPACITY - 1;

    auto* agg = new AggEntry[AGG_CAPACITY]();  // zero-initialized (occupied=false)

    {
        MQO_TIME_PHASE("Q10_main_scan");
        const size_t n_li = li.n_rows;
        const int8_t R_FLAG = static_cast<int8_t>('R');

        for (size_t i = 0; i < n_li; ++i) {
            // Filter: l_returnflag == 'R'
            if (li.l_returnflag[i] != R_FLAG) continue;

            // Probe orders hash
            int32_t okey = li.l_orderkey[i];
            int32_t custkey = oht->probe(okey);
            if (custkey == OrdersHT::EMPTY) continue;

            // Probe customer (dense_pk O(1) lookup)
            if (custkey < 0 || custkey > cust.max_key) continue;
            int32_t c_row = cust.pk_index[custkey];
            if (c_row < 0) continue;

            // Compute revenue contribution
            double rev = li.l_extendedprice[i] * (1.0 - li.l_discount[i]);

            // Aggregate into hash table keyed by custkey
            size_t slot = static_cast<uint32_t>(custkey) & AGG_MASK;
            while (true) {
                if (!agg[slot].occupied) {
                    agg[slot].occupied = true;
                    agg[slot].c_custkey = custkey;
                    agg[slot].c_nationkey = cust.c_nationkey[c_row];
                    agg[slot].c_row_id = static_cast<uint32_t>(c_row);
                    agg[slot].revenue = rev;
                    break;
                }
                if (agg[slot].c_custkey == custkey) {
                    agg[slot].revenue += rev;
                    break;
                }
                slot = (slot + 1) & AGG_MASK;
            }
        }
    }

    delete oht;

    // ================================================================
    // Step 7: Top-20 by revenue DESC using min-heap
    // ================================================================
    struct TopEntry {
        double revenue;
        size_t agg_slot;
        bool operator<(const TopEntry& o) const { return revenue > o.revenue; } // min-heap
    };

    std::vector<TopEntry> heap;
    heap.reserve(21);

    {
        MQO_TIME_PHASE("Q10_topk");
        for (size_t s = 0; s < AGG_CAPACITY; ++s) {
            if (!agg[s].occupied) continue;
            double rev = agg[s].revenue;
            if (heap.size() < 20) {
                heap.push_back({rev, s});
                if (heap.size() == 20) {
                    std::make_heap(heap.begin(), heap.end());
                }
            } else if (rev > heap[0].revenue) {
                std::pop_heap(heap.begin(), heap.end());
                heap.back() = {rev, s};
                std::push_heap(heap.begin(), heap.end());
            }
        }
        // Sort descending by revenue
        std::sort(heap.begin(), heap.end(), [](const TopEntry& a, const TopEntry& b) {
            return a.revenue > b.revenue;
        });
    }

    // ================================================================
    // Step 8: Output CSV
    // ================================================================
    {
        MQO_TIME_PHASE("Q10_output");
        std::string outpath = ctx.output_dir + "/q10.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }

        std::fprintf(fp, "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n");

        for (const auto& te : heap) {
            const auto& a = agg[te.agg_slot];
            uint32_t cr = a.c_row_id;

            std::string_view c_name    = cust.c_name.get(cr);
            std::string_view c_address = cust.c_address.get(cr);
            std::string_view c_phone   = cust.c_phone.get(cr);
            std::string_view c_comment = cust.c_comment.get(cr);
            const char* n_name = nation_names[a.c_nationkey].c_str();

            std::fprintf(fp, "%d,%.*s,%.2f,%.2f,%s,%.*s,%.*s,%.*s\n",
                a.c_custkey,
                (int)c_name.size(), c_name.data(),
                a.revenue,
                cust.c_acctbal[cr],
                n_name,
                (int)c_address.size(), c_address.data(),
                (int)c_phone.size(), c_phone.data(),
                (int)c_comment.size(), c_comment.data());
        }

        std::fclose(fp);
    }

    delete[] agg;
}

}} // namespace mqo::tails
