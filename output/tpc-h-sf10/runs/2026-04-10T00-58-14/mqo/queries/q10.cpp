// Q10 tail — Returned Item Reporting
// Shared inputs: scan_lineitem_full, hash_orders_by_orderkey, hash_customer_by_custkey
// Pipeline: filter(l_returnflag='R') → probe orders(date filter) → probe customer → nation lookup → agg → top-20

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
#include <queue>

namespace mqo::tails {

void run_Q10(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q10_tail");

    // -----------------------------------------------------------------------
    // Step 0: Load nation dimension — direct array indexed by n_nationkey
    // -----------------------------------------------------------------------
    std::string nation_names[25];
    {
        MQO_TIME_PHASE("Q10_load_nation");
        const std::string nb = ctx.gendb_dir + "/nation/";
        const size_t nn = mqo::io::read_row_count(nb + "meta.txt");

        const int32_t* nk = mqo::io::mmap_column<int32_t>(nb + "n_nationkey.bin", nn);
        const uint8_t* nc = mqo::io::mmap_column<uint8_t>(nb + "n_name.bin", nn);
        auto dict = mqo::io::read_dictionary(nb + "n_name_dict.bin");

        for (size_t i = 0; i < nn; ++i) {
            int32_t key = nk[i];
            if (key >= 0 && key < 25) {
                nation_names[key] = dict[nc[i]];
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 1-5: Scan lineitem, filter, probe orders+customer, aggregate
    // -----------------------------------------------------------------------

    // Date constants: o_orderdate >= 1993-10-01 AND < 1994-01-01
    constexpr int32_t DATE_LO = mqo::io::to_epoch_days(1993, 10, 1);
    constexpr int32_t DATE_HI = mqo::io::to_epoch_days(1994, 1, 1);

    // Aggregation entry — direct array indexed by c_custkey (max 1.5M)
    static constexpr int32_t MAX_CUSTKEY = 1500000;

    struct AggEntry {
        double revenue;
        int32_t c_nationkey;
        int32_t row_id;       // for varlen access
        bool    used;
    };

    AggEntry* agg = static_cast<AggEntry*>(
        std::calloc(static_cast<size_t>(MAX_CUSTKEY) + 1, sizeof(AggEntry)));
    if (!agg) {
        std::fprintf(stderr, "[MQO] OOM: Q10 agg array\n");
        std::exit(1);
    }

    {
        MQO_TIME_PHASE("Q10_main_scan");

        const auto& li = mqo::shared::scan_lineitem_full::get_columns(ctx);
        const auto& ord = mqo::shared::hash_orders_by_orderkey::get();
        const auto& cust = mqo::shared::hash_customer_by_custkey::get();

        const size_t n = li.n_rows;
        const int8_t*  rf = li.l_returnflag;
        const int32_t* ok = li.l_orderkey;
        const double*  ep = li.l_extendedprice;
        const double*  dc = li.l_discount;

        for (size_t i = 0; i < n; ++i) {
            // Step 1: filter l_returnflag = 'R' (ASCII 82)
            if (rf[i] != 82) continue;

            // Step 2: probe orders hash, apply date filter
            const auto* oe = ord.probe(ok[i]);
            if (!oe) continue;
            const int32_t od = oe->o_orderdate;
            if (od < DATE_LO || od >= DATE_HI) continue;

            // Step 3: probe customer hash
            const auto* ce = cust.probe(oe->o_custkey);
            if (!ce) continue;

            // Step 5: accumulate revenue
            const int32_t ck = ce->c_custkey;
            double rev = ep[i] * (1.0 - dc[i]);
            AggEntry& a = agg[ck];
            if (!a.used) {
                a.used = true;
                a.revenue = rev;
                a.c_nationkey = ce->c_nationkey;
                a.row_id = ce->row_id;
            } else {
                a.revenue += rev;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Step 6: Top-20 by revenue DESC using min-heap
    // -----------------------------------------------------------------------
    struct TopEntry {
        double revenue;
        int32_t custkey;
        bool operator>(const TopEntry& o) const { return revenue > o.revenue; }
    };

    std::priority_queue<TopEntry, std::vector<TopEntry>, std::greater<TopEntry>> heap;

    {
        MQO_TIME_PHASE("Q10_topk");
        for (int32_t ck = 1; ck <= MAX_CUSTKEY; ++ck) {
            if (!agg[ck].used) continue;
            TopEntry te{agg[ck].revenue, ck};
            if (heap.size() < 20) {
                heap.push(te);
            } else if (te.revenue > heap.top().revenue) {
                heap.pop();
                heap.push(te);
            }
        }
    }

    // Extract top-20 sorted descending
    std::vector<TopEntry> top20;
    top20.reserve(20);
    while (!heap.empty()) {
        top20.push_back(heap.top());
        heap.pop();
    }
    std::sort(top20.begin(), top20.end(), [](const TopEntry& a, const TopEntry& b) {
        return a.revenue > b.revenue;
    });

    // -----------------------------------------------------------------------
    // Step 7: Output CSV
    // -----------------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q10_output");

        const auto& cust = mqo::shared::hash_customer_by_custkey::get();
        const std::string path = ctx.output_dir + "/q10.csv";
        FILE* fp = std::fopen(path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[MQO] Cannot open output: %s\n", path.c_str());
            std::exit(1);
        }

        std::fprintf(fp, "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n");

        for (const auto& te : top20) {
            const int32_t ck = te.custkey;
            const AggEntry& a = agg[ck];
            const int32_t rid = a.row_id;

            auto name    = cust.get_name(rid);
            auto address = cust.get_address(rid);
            auto phone   = cust.get_phone(rid);
            auto comment = cust.get_comment(rid);
            const auto* ce = cust.probe(ck);

            std::fprintf(fp, "%d,%.*s,%.2f,%.2f,%s,%.*s,%.*s,%.*s\n",
                ck,
                (int)name.size(), name.data(),
                te.revenue,
                ce->c_acctbal,
                nation_names[a.c_nationkey].c_str(),
                (int)address.size(), address.data(),
                (int)phone.size(), phone.data(),
                (int)comment.size(), comment.data());
        }

        std::fclose(fp);
    }

    std::free(agg);
}

}  // namespace mqo::tails
