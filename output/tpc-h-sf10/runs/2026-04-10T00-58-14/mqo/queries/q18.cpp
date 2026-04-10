// Q18 tail — Large Volume Customer
// Consumes: scan_lineitem_full, hash_orders_by_orderkey, hash_customer_by_custkey
//
// Strategy: exploit l_orderkey ordering in shared scan for sorted-group-by
// aggregation (no hash table). HAVING filter (sum_qty > 300) is extremely
// selective (~150 rows). Then probe orders + customer hashes, sort, LIMIT 100.

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"
#include "shared/hash_orders_by_orderkey.hpp"
#include "shared/hash_customer_by_custkey.hpp"

#include <algorithm>
#include <cstdio>
#include <string>
#include <vector>

namespace mqo::tails {

void run_Q18(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q18_tail");

    // ---------------------------------------------------------------
    // Step 1: Sorted group-by aggregation on lineitem (ordered by l_orderkey)
    //         + Step 2: HAVING filter (sum_qty > 300)
    // ---------------------------------------------------------------
    struct QualRow {
        int32_t orderkey;
        double  sum_qty;
    };
    std::vector<QualRow> qualifying;
    qualifying.reserve(256);  // expect ~150

    {
        MQO_TIME_PHASE("Q18_agg_having");
        const auto& cols = mqo::shared::scan_lineitem_full::get_columns(ctx);
        const size_t n = cols.n_rows;
        const int32_t* ok = cols.l_orderkey;
        const double*  qty = cols.l_quantity;

        if (n > 0) {
            int32_t cur_key = ok[0];
            double  cur_sum = qty[0];

            for (size_t i = 1; i < n; ++i) {
                const int32_t k = ok[i];
                if (k == cur_key) {
                    cur_sum += qty[i];
                } else {
                    // Flush previous group
                    if (cur_sum > 300.0) {
                        qualifying.push_back({cur_key, cur_sum});
                    }
                    cur_key = k;
                    cur_sum = qty[i];
                }
            }
            // Flush last group
            if (cur_sum > 300.0) {
                qualifying.push_back({cur_key, cur_sum});
            }
        }
    }

    // ---------------------------------------------------------------
    // Steps 3-5: Probe orders hash, probe customer hash, project
    // ---------------------------------------------------------------
    struct ResultRow {
        std::string_view c_name;
        int32_t c_custkey;
        int32_t o_orderkey;
        int32_t o_orderdate;
        double  o_totalprice;
        double  sum_qty;
    };
    std::vector<ResultRow> results;
    results.reserve(qualifying.size());

    {
        MQO_TIME_PHASE("Q18_join_project");
        const auto& orders = mqo::shared::hash_orders_by_orderkey::get();
        const auto& cust   = mqo::shared::hash_customer_by_custkey::get();

        for (const auto& q : qualifying) {
            const auto* oe = orders.probe(q.orderkey);
            if (!oe) continue;
            const auto* ce = cust.probe(oe->o_custkey);
            if (!ce) continue;
            results.push_back({
                cust.get_name(ce->row_id),
                ce->c_custkey,
                q.orderkey,
                oe->o_orderdate,
                oe->o_totalprice,
                q.sum_qty
            });
        }
    }

    // ---------------------------------------------------------------
    // Step 6: Sort by o_totalprice DESC, o_orderdate ASC; LIMIT 100
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q18_sort");
        std::sort(results.begin(), results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.o_totalprice != b.o_totalprice)
                    return a.o_totalprice > b.o_totalprice;
                return a.o_orderdate < b.o_orderdate;
            });
        if (results.size() > 100) results.resize(100);
    }

    // ---------------------------------------------------------------
    // Output CSV
    // ---------------------------------------------------------------
    {
        MQO_TIME_PHASE("Q18_output");
        std::string path = ctx.output_dir + "/q18.csv";
        FILE* f = std::fopen(path.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "[Q18] Cannot open %s\n", path.c_str());
            return;
        }
        std::fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
        for (const auto& r : results) {
            int y, m, d;
            mqo::io::from_epoch_days(r.o_orderdate, y, m, d);
            std::fprintf(f, "%.*s,%d,%d,%04d-%02d-%02d,%.2f,%.2f\n",
                (int)r.c_name.size(), r.c_name.data(),
                r.c_custkey,
                r.o_orderkey,
                y, m, d,
                r.o_totalprice,
                r.sum_qty);
        }
        std::fclose(f);
    }
}

}  // namespace mqo::tails
