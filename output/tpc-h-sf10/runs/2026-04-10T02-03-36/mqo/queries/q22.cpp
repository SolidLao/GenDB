// Q22 tail — Global Sales Opportunity
// Shared inputs: hash_customer_by_custkey (customer columns), scan_orders_full (ensures data loaded)
// Operators: scalar subquery (AVG), anti-join via custkey_to_orders index, aggregate, sort, output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/hash_customer_by_custkey.hpp"
#include "shared/scan_orders_full.hpp"

#include <algorithm>
#include <cstdio>
#include <cstring>

namespace mqo { namespace tails {

void run_Q22(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q22_tail");

    const auto& cust = mqo::shared::hash_customer_by_custkey::get();
    // Touch scan_orders_full to ensure dependency is resolved (index files valid)
    (void)mqo::shared::scan_orders_full::get();

    const size_t n_cust = cust.n_rows;

    // Phone prefix lookup: map 2-digit integer (10-39) to bucket 0-6, or -1
    // Codes: 13->0, 17->1, 18->2, 23->3, 29->4, 30->5, 31->6
    int8_t prefix_to_bucket[100];
    std::memset(prefix_to_bucket, -1, sizeof(prefix_to_bucket));
    static const int codes[] = {13, 17, 18, 23, 29, 30, 31};
    static const char* code_strs[] = {"13", "17", "18", "23", "29", "30", "31"};
    for (int i = 0; i < 7; ++i) prefix_to_bucket[codes[i]] = static_cast<int8_t>(i);

    // ---- Step 1: Scalar subquery — AVG(c_acctbal) where c_acctbal > 0 and phone prefix in set ----
    double avg_acctbal;
    {
        MQO_TIME_PHASE("Q22_scalar_avg");
        double sum = 0.0;
        int64_t count = 0;
        for (size_t i = 0; i < n_cust; ++i) {
            double bal = cust.c_acctbal[i];
            if (bal <= 0.0) continue;
            // Extract first 2 chars of phone
            auto phone = cust.c_phone.get(i);
            if (phone.size() < 2) continue;
            int code = (phone[0] - '0') * 10 + (phone[1] - '0');
            if (code < 0 || code >= 100 || prefix_to_bucket[code] < 0) continue;
            sum += bal;
            ++count;
        }
        avg_acctbal = (count > 0) ? (sum / count) : 0.0;
    }

    // ---- Load custkey_to_orders offsets for anti-join ----
    const uint32_t* cto_offsets;
    {
        MQO_TIME_PHASE("Q22_load_antijoin_idx");
        std::string idx_path = ctx.gendb_dir + "/indexes/custkey_to_orders_offsets.bin";
        // max_custkey = 1499999, offsets has max_custkey+2 = 1500001 entries
        size_t n_offsets = 1500001;
        cto_offsets = mqo::io::mmap_column<uint32_t>(idx_path, n_offsets);
    }

    // ---- Step 2+3: Filter + anti-join + aggregate (fused) ----
    struct Bucket {
        int64_t count = 0;
        double sum = 0.0;
    };
    Bucket buckets[7] = {};

    {
        MQO_TIME_PHASE("Q22_filter_antijoin_agg");
        for (size_t i = 0; i < n_cust; ++i) {
            // Phone prefix filter
            auto phone = cust.c_phone.get(i);
            if (phone.size() < 2) continue;
            int code = (phone[0] - '0') * 10 + (phone[1] - '0');
            if (code < 0 || code >= 100) continue;
            int8_t bucket = prefix_to_bucket[code];
            if (bucket < 0) continue;

            // Acctbal filter
            double bal = cust.c_acctbal[i];
            if (bal <= avg_acctbal) continue;

            // Anti-join: NOT EXISTS in orders
            int32_t custkey = cust.c_custkey[i];
            if (custkey >= 0 && custkey <= 1499999) {
                if (cto_offsets[custkey + 1] != cto_offsets[custkey]) continue; // has orders
            }

            buckets[bucket].count++;
            buckets[bucket].sum += bal;
        }
    }

    // ---- Step 4: Sort (already sorted since codes[] is in ascending order) and output ----
    {
        MQO_TIME_PHASE("Q22_output");
        std::string out_path = ctx.output_dir + "/q22.csv";
        FILE* fp = std::fopen(out_path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", out_path.c_str());
            return;
        }
        std::fprintf(fp, "cntrycode,numcust,totacctbal\n");
        // codes[] and code_strs[] are already in ascending order (13,17,18,23,29,30,31)
        for (int i = 0; i < 7; ++i) {
            if (buckets[i].count > 0) {
                std::fprintf(fp, "%s,%ld,%.2f\n",
                             code_strs[i],
                             static_cast<long>(buckets[i].count),
                             buckets[i].sum);
            }
        }
        std::fclose(fp);
    }
}

}} // namespace mqo::tails
