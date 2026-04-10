// Q18 tail — Large Volume Customer
// Shared inputs: scan_lineitem_full, scan_orders_full, hash_customer_by_custkey
// Strategy: streaming sorted group-by on lineitem (ordered by l_orderkey),
//           HAVING SUM(l_quantity) > 300, then index lookup into orders,
//           probe customer hash, sort, limit 100.

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"
#include "shared/scan_orders_full.hpp"
#include "shared/hash_customer_by_custkey.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

namespace mqo { namespace tails {

// Convert days-since-epoch to YYYY-MM-DD
static void epoch_days_to_ymd(int32_t days, int& y, int& m, int& d) {
    // Algorithm from Howard Hinnant's date library (civil_from_days)
    days += 719468;
    int era = (days >= 0 ? days : days - 146096) / 146097;
    unsigned doe = static_cast<unsigned>(days - era * 146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int yy = static_cast<int>(yoe) + era * 400;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp = (5*doy + 2) / 153;
    d = static_cast<int>(doy - (153*mp + 2)/5 + 1);
    m = static_cast<int>(mp < 10 ? mp + 3 : mp - 9);
    y = yy + (m <= 2 ? 1 : 0);
}

void run_Q18(const Context& ctx) {
    MQO_TIME_TAIL("Q18_tail");

    // ---- Fetch shared inputs ----
    const auto& li  = mqo::shared::scan_lineitem_full::get();
    const auto& ord = mqo::shared::scan_orders_full::get();
    const auto& cus = mqo::shared::hash_customer_by_custkey::get();

    // ---- Step 1: Streaming sorted group-by on lineitem ----
    // Data is ordered by l_orderkey. Accumulate SUM(l_quantity) per orderkey.
    // HAVING SUM(l_quantity) > 300.
    struct QualifyingOrder {
        int32_t orderkey;
        double  sum_qty;
    };
    std::vector<QualifyingOrder> qualifying;
    qualifying.reserve(128);

    {
        MQO_TIME_PHASE("Q18_subquery_agg");
        const size_t n = li.n_rows;
        if (n > 0) {
            int32_t cur_key = li.l_orderkey[0];
            double  cur_sum = li.l_quantity[0];

            for (size_t i = 1; i < n; ++i) {
                int32_t k = li.l_orderkey[i];
                if (k == cur_key) {
                    cur_sum += li.l_quantity[i];
                } else {
                    if (cur_sum > 300.0) {
                        qualifying.push_back({cur_key, cur_sum});
                    }
                    cur_key = k;
                    cur_sum = li.l_quantity[i];
                }
            }
            // Last group
            if (cur_sum > 300.0) {
                qualifying.push_back({cur_key, cur_sum});
            }
        }
    }

    // ---- Step 2: Index lookup into orders via orders_pk_index ----
    // orders_pk_index.bin: int32_t[max_orderkey+1], mapping orderkey -> row_id
    const int32_t* orders_pk = nullptr;
    {
        size_t pk_sz = 0;
        orders_pk = reinterpret_cast<const int32_t*>(
            mqo::io::mmap_file_raw(ctx.gendb_dir + "/indexes/orders_pk_index.bin", pk_sz));
    }

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
        MQO_TIME_PHASE("Q18_join_and_project");
        for (const auto& q : qualifying) {
            int32_t row_id = orders_pk[q.orderkey];
            if (row_id < 0) continue;  // shouldn't happen for valid FK

            int32_t custkey = ord.o_custkey[row_id];

            // Step 3: Probe customer hash
            if (custkey < 0 || custkey > cus.max_key) continue;
            int32_t c_row = cus.pk_index[custkey];
            if (c_row < 0) continue;

            ResultRow r;
            r.c_name       = cus.c_name.get(static_cast<size_t>(c_row));
            r.c_custkey     = custkey;
            r.o_orderkey    = q.orderkey;
            r.o_orderdate   = ord.o_orderdate[row_id];
            r.o_totalprice  = ord.o_totalprice[row_id];
            r.sum_qty       = q.sum_qty;
            results.push_back(r);
        }
    }

    // ---- Step 4: Sort by o_totalprice DESC, o_orderdate ASC ----
    {
        MQO_TIME_PHASE("Q18_sort");
        std::sort(results.begin(), results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.o_totalprice != b.o_totalprice)
                    return a.o_totalprice > b.o_totalprice;
                return a.o_orderdate < b.o_orderdate;
            });
    }

    // ---- Step 5: Limit 100 ----
    size_t limit = std::min<size_t>(results.size(), 100);

    // ---- Step 6: Output CSV ----
    {
        MQO_TIME_PHASE("Q18_output");
        std::string outpath = ctx.output_dir + "/q18.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }

        std::fprintf(fp, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        for (size_t i = 0; i < limit; ++i) {
            const auto& r = results[i];
            int y, m, d;
            epoch_days_to_ymd(r.o_orderdate, y, m, d);
            std::fprintf(fp, "%.*s,%d,%d,%04d-%02d-%02d,%.2f,%.2f\n",
                         static_cast<int>(r.c_name.size()), r.c_name.data(),
                         r.c_custkey, r.o_orderkey,
                         y, m, d,
                         r.o_totalprice, r.sum_qty);
        }
        std::fclose(fp);
    }
}

}} // namespace mqo::tails
