// Q12 tail — Shipping Modes and Order Priority
// Consumes: scan_lineitem_full (callback), scan_orders_full (materialized)
// Plan: residual filter → index lookup join → direct-array aggregation → output

#include "mqo_profile.hpp"
#include "../shared/scan_lineitem_full.hpp"
#include "../shared/scan_orders_full.hpp"
#include "../shared/mqo_io.hpp"

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

namespace mqo { namespace tails {

void run_Q12(const mqo::Context& ctx) {
    MQO_TIME_TAIL("Q12_tail");

    // --- Fetch shared inputs ---
    const auto& li = mqo::shared::scan_lineitem_full::get();
    const auto& ord = mqo::shared::scan_orders_full::get();

    // --- Load orders_pk_index: dense array orderkey -> row_id ---
    const int32_t* orders_pk = nullptr;
    int32_t pk_max_key = 0;
    {
        MQO_TIME_PHASE("Q12_load_pk_index");
        size_t pk_sz = 0;
        const void* pk_raw = mqo::io::mmap_file_raw(
            ctx.gendb_dir + "/indexes/orders_pk_index.bin", pk_sz);
        // Format: int32 max_key, then int32[max_key] entries
        pk_max_key = *reinterpret_cast<const int32_t*>(pk_raw);
        orders_pk = reinterpret_cast<const int32_t*>(pk_raw) + 1;
    }

    // --- Aggregation accumulators: [shipmode_slot][0=high, 1=low] ---
    // shipmode dict code 1 = MAIL -> slot 0, code 6 = SHIP -> slot 1
    int64_t agg[2][2] = {};  // [slot][high/low]

    {
        MQO_TIME_PHASE("Q12_main_scan");

        const size_t n = li.n_rows;
        const uint8_t* shipmode    = li.l_shipmode;
        const int32_t* receiptdate = li.l_receiptdate;
        const int32_t* commitdate  = li.l_commitdate;
        const int32_t* shipdate    = li.l_shipdate;
        const int32_t* orderkey    = li.l_orderkey;
        const uint8_t* o_prio      = ord.o_orderpriority;

        // Date constants
        constexpr int32_t RECEIPT_LO = 8766;  // 1994-01-01
        constexpr int32_t RECEIPT_HI = 9131;  // 1995-01-01

        // Scan lineitem with residual filters, fused with join + aggregation
        for (size_t i = 0; i < n; ++i) {
            // 1. Cheapest filter first: shipmode IN {1, 6}
            uint8_t sm = shipmode[i];
            if (sm != 1 && sm != 6) continue;

            // 2. Receipt date range [8766, 9131)
            int32_t rd = receiptdate[i];
            if (rd < RECEIPT_LO || rd >= RECEIPT_HI) continue;

            // 3. l_commitdate < l_receiptdate
            int32_t cd = commitdate[i];
            if (cd >= rd) continue;

            // 4. l_shipdate < l_commitdate
            int32_t sd = shipdate[i];
            if (sd >= cd) continue;

            // --- Join: lookup o_orderpriority via pk index ---
            int32_t ok = orderkey[i];
            int32_t row_id = orders_pk[ok];
            uint8_t prio = o_prio[row_id];

            // --- Aggregate ---
            // shipmode slot: code 1 (MAIL) -> 0, code 6 (SHIP) -> 1
            int slot = (sm == 1) ? 0 : 1;
            // high priority: codes 1 (1-URGENT) or 3 (2-HIGH)
            bool is_high = (prio == 1 || prio == 3);
            agg[slot][0] += is_high;      // high_line_count
            agg[slot][1] += !is_high;     // low_line_count
        }
    }

    // --- Output ---
    {
        MQO_TIME_PHASE("Q12_output");
        std::string outpath = ctx.output_dir + "/q12.csv";
        FILE* f = std::fopen(outpath.c_str(), "w");
        if (!f) {
            std::fprintf(stderr, "ERROR: Cannot open %s for writing\n", outpath.c_str());
            return;
        }
        std::fprintf(f, "l_shipmode,high_line_count,low_line_count\n");
        // MAIL (slot 0) before SHIP (slot 1) — alphabetical
        std::fprintf(f, "MAIL,%ld,%ld\n", (long)agg[0][0], (long)agg[0][1]);
        std::fprintf(f, "SHIP,%ld,%ld\n", (long)agg[1][0], (long)agg[1][1]);
        std::fclose(f);
    }
}

}} // namespace mqo::tails
