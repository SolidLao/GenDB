#pragma once
#include "common.hpp"

static inline void fused_scan_orders(Ctx& c, Res& r, uint32_t active) {
    if (!(active & ORD_MASK)) return;
    MQO_TIME_PHASE("fused_scan_orders");

    const size_t N = c.nord;

    // Q13: init per-custkey count array (custkey 1..ncust → index 0..ncust-1)
    if (active & Q13B) r.q13_cnt.assign(c.ncust, 0);

    // Per-thread Q4 accumulators (5 priority buckets)
    const int NT = omp_get_max_threads();
    struct TL { int64_t q4[5] = {}; };
    std::vector<TL> tls(NT);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        TL& t = tls[tid];

        #pragma omp for schedule(static)
        for (size_t i = 0; i < N; i++) {

            // === Q4: orderdate [1993-07-01,1993-10-01) + EXISTS late lineitem ===
            if (active & Q4B) {
                int32_t od = c.o_orderdate[i];
                if (od >= D_930701 && od < D_931001) {
                    int32_t ok = c.o_orderkey_col[i];
                    DRE dr = c.li_ok_idx[ok];
                    bool found = false;
                    for (uint32_t j = dr.start; j < dr.start + dr.count && !found; j++)
                        if (c.l_commitdate[j] < c.l_receiptdate[j]) found = true;
                    if (found) t.q4[c.o_orderpri[i]]++;
                }
            }

            // === Q13: NOT LIKE '%special%requests%' → count per custkey ===
            if (active & Q13B) {
                uint32_t cs = c.o_cmt_off[i], ce = c.o_cmt_off[i + 1];
                const char* s = c.o_cmt_dat + cs;
                size_t sl = ce - cs;
                if (!str_like_ab(s, sl, "special", 7, "requests", 8)) {
                    #pragma omp atomic
                    r.q13_cnt[c.o_custkey[i] - 1]++;
                }
            }
        }
    }

    // Merge Q4
    if (active & Q4B)
        for (int ti = 0; ti < NT; ti++)
            for (int g = 0; g < 5; g++) r.q4[g] += tls[ti].q4[g];

    printf("[MQO] fused_orders done: %zu rows\n", N);
}
