// Q12 tail — Shipping Modes and Order Priority
// Consumes: scan_lineitem_full, hash_orders_by_orderkey
// Operators: residual_filter → hash_probe → aggregate(direct_array) → output

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"
#include "shared/hash_orders_by_orderkey.hpp"

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <omp.h>

namespace mqo::tails {

void run_Q12(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q12_tail");

    // Date constants: 1994-01-01 and 1995-01-01 as days since epoch
    // 1994-01-01 = 8766 days from 1970-01-01
    // 1995-01-01 = 9131 days from 1970-01-01
    constexpr int32_t DATE_LO = 8766;   // 1994-01-01
    constexpr int32_t DATE_HI = 9131;   // 1995-01-01

    // Shipmode dict codes from plan: MAIL=1, SHIP=6
    constexpr uint8_t CODE_MAIL = 1;
    constexpr uint8_t CODE_SHIP = 6;

    // Priority dict codes from plan: 1-URGENT=1, 2-HIGH=3
    constexpr uint8_t PRIO_URGENT = 1;
    constexpr uint8_t PRIO_HIGH   = 3;

    // Get shared inputs
    const auto& li = mqo::shared::scan_lineitem_full::get_columns(ctx);
    const auto& ht = mqo::shared::hash_orders_by_orderkey::get();

    const size_t n = li.n_rows;

    // Shipmode filter bitmask: set bits for codes 1 (MAIL) and 6 (SHIP)
    // Only need to check codes 0-7, use a uint8 bitmask
    constexpr uint8_t SHIPMODE_MASK = (1u << CODE_MAIL) | (1u << CODE_SHIP);

    // Priority classifier: lookup table mapping dict code -> is_high (0 or 1)
    // Dict codes: 0=5-LOW, 1=1-URGENT, 2=4-NOT SPECIFIED, 3=2-HIGH, 4=3-MEDIUM
    uint8_t prio_is_high[8] = {};
    prio_is_high[PRIO_URGENT] = 1;
    prio_is_high[PRIO_HIGH]   = 1;

    // Thread-local aggregation: [shipmode_bucket][0=high, 1=low]
    // shipmode_bucket: CODE_MAIL(1)->0, CODE_SHIP(6)->1
    const int max_threads = omp_get_max_threads();

    // Allocate per-thread accumulators
    // Each thread: int64_t[2][2] = 32 bytes
    struct Accum {
        int64_t high[2] = {};  // [0]=MAIL, [1]=SHIP
        int64_t low[2]  = {};
    };
    std::vector<Accum> tl(max_threads);

    {
        MQO_TIME_PHASE("Q12_scan_filter_agg");

        #pragma omp parallel
        {
            const int tid = omp_get_thread_num();
            Accum& acc = tl[tid];

            // Local copies of column pointers for compiler optimization
            const uint8_t*  shipmode    = li.l_shipmode;
            const int32_t*  receiptdate = li.l_receiptdate;
            const int32_t*  commitdate  = li.l_commitdate;
            const int32_t*  shipdate    = li.l_shipdate;
            const int32_t*  orderkey    = li.l_orderkey;
            const auto*     entries     = ht.entries;

            #pragma omp for schedule(static) nowait
            for (size_t i = 0; i < n; ++i) {
                // Filter 1: l_shipmode IN ('MAIL', 'SHIP') — cheapest, dict uint8 bitmask
                const uint8_t sm = shipmode[i];
                if (!((SHIPMODE_MASK >> sm) & 1u)) continue;

                // Filter 2: l_receiptdate >= 1994-01-01 AND l_receiptdate < 1995-01-01
                const int32_t rd = receiptdate[i];
                if (rd < DATE_LO || rd >= DATE_HI) continue;

                // Filter 3: l_commitdate < l_receiptdate
                const int32_t cd = commitdate[i];
                if (cd >= rd) continue;

                // Filter 4: l_shipdate < l_commitdate
                if (shipdate[i] >= cd) continue;

                // Hash probe: get o_orderpriority
                const int32_t ok = orderkey[i];
                const auto& e = entries[ok];
                // All valid lineitem rows have matching orders (inner join, expect no misses)

                // Classify priority and accumulate
                const uint8_t oprio = e.o_orderpriority;
                // Map shipmode code to bucket: MAIL(1)->0, SHIP(6)->1
                const int bucket = (sm == CODE_SHIP) ? 1 : 0;

                if (prio_is_high[oprio]) {
                    acc.high[bucket]++;
                } else {
                    acc.low[bucket]++;
                }
            }
        }
    }

    // Merge thread-local accumulators
    int64_t high_mail = 0, high_ship = 0;
    int64_t low_mail  = 0, low_ship  = 0;
    for (int t = 0; t < max_threads; ++t) {
        high_mail += tl[t].high[0];
        high_ship += tl[t].high[1];
        low_mail  += tl[t].low[0];
        low_ship  += tl[t].low[1];
    }

    // Output: ordered by l_shipmode ASC → MAIL < SHIP
    {
        MQO_TIME_PHASE("Q12_output");
        std::string path = ctx.output_dir + "/q12.csv";
        FILE* fp = std::fopen(path.c_str(), "w");
        if (!fp) {
            std::fprintf(stderr, "[Q12] Cannot open %s for writing\n", path.c_str());
            return;
        }
        std::fprintf(fp, "l_shipmode,high_line_count,low_line_count\n");
        std::fprintf(fp, "MAIL,%ld,%ld\n", (long)high_mail, (long)low_mail);
        std::fprintf(fp, "SHIP,%ld,%ld\n", (long)high_ship, (long)low_ship);
        std::fclose(fp);
    }
}

}  // namespace mqo::tails
