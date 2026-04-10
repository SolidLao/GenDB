#pragma once
#include "common.hpp"

static inline void fused_scan_lineitem(Ctx& c, Res& r, uint32_t active) {
    if (!(active & LI_MASK)) return;
    MQO_TIME_PHASE("fused_scan_lineitem");

    const size_t N = c.nli;

    // === Pre-resolve constants ===
    // Q3: mktsegment = BUILDING
    uint8_t mks_bld = (uint8_t)dfind(c.d_mkseg, "BUILDING");
    // Q5: ASIA region
    int rk_asia = find_regionkey(c, "ASIA");
    bool q5_nat[25] = {};
    if (active & Q5B)
        for (int n = 0; n < 25; n++) q5_nat[n] = (c.n_regionkey[n] == rk_asia);
    // Q7: FRANCE/GERMANY nationkeys
    int nk_fr = find_nationkey(c, "FRANCE");
    int nk_de = find_nationkey(c, "GERMANY");
    // Q12: shipmode / orderpriority codes
    uint8_t sm_mail = (uint8_t)dfind(c.d_shipmode, "MAIL");
    uint8_t sm_ship = (uint8_t)dfind(c.d_shipmode, "SHIP");
    uint8_t op_urg  = (uint8_t)dfind(c.d_ordpri, "1-URGENT");
    uint8_t op_hi   = (uint8_t)dfind(c.d_ordpri, "2-HIGH");
    // Q14: PROMO% p_type codes
    std::vector<uint8_t> is_promo(c.d_ptype.size(), 0);
    if (active & Q14B)
        for (size_t i = 0; i < c.d_ptype.size(); i++)
            if (c.d_ptype[i].size() >= 5 && c.d_ptype[i].compare(0, 5, "PROMO") == 0)
                is_promo[i] = 1;

    // Q15: shared revenue array (suppkey 1..100000)
    if (active & Q15B) r.q15_rev.assign(c.nsupp + 1, 0.0);

    // Q18_sub: shared qty array (orderkey 1..60000000)
    if (active & Q18B) {
        r.q18_qty.resize(60000001);
        #pragma omp parallel for schedule(static)
        for (size_t i = 0; i < 60000001; i++) r.q18_qty[i] = 0.0;
    }

    // === Per-thread local accumulators ===
    const int NT = omp_get_max_threads();
    struct TL {
        Res::Q1G q1[6] = {};
        std::unordered_map<int32_t, Res::Q3G> q3;
        double q5[25] = {};
        double q6 = 0;
        double q7[2][2] = {};
        std::unordered_map<int32_t, double> q10;
        int64_t q12h[2] = {}, q12l[2] = {};
        double q14p = 0, q14t = 0;
    };
    std::vector<TL> tls(NT);

    // === Fused parallel scan ===
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        TL& t = tls[tid];

        #pragma omp for schedule(static)
        for (size_t i = 0; i < N; i++) {
            int32_t sd = c.l_shipdate[i];

            // === Q1: l_shipdate <= 1998-09-02 ===
            if ((active & Q1B) && sd <= D_980902) {
                int ri = (c.l_returnflag[i]=='A') ? 0 : (c.l_returnflag[i]=='N') ? 1 : 2;
                int li = (c.l_linestatus[i]=='F') ? 0 : 1;
                int gi = ri * 2 + li;
                double q = c.l_quantity[i], ep = c.l_extendedprice[i];
                double d = c.l_discount[i], tx = c.l_tax[i];
                double dp = ep * (1.0 - d);
                t.q1[gi].sq += q;  t.q1[gi].sp += ep;
                t.q1[gi].sdp += dp; t.q1[gi].sch += dp * (1.0 + tx);
                t.q1[gi].sd += d;  t.q1[gi].cnt++;
            }

            // === Q6: shipdate [1994-01-01,1995-01-01), discount [0.05,0.07], qty<24 ===
            if ((active & Q6B) && sd >= D_940101 && sd < D_950101) {
                double d = c.l_discount[i];
                if (d >= 0.05 && d <= 0.07 && c.l_quantity[i] < 24.0)
                    t.q6 += c.l_extendedprice[i] * d;
            }

            // === Q14: shipdate [1995-09-01,1995-10-01) ===
            if ((active & Q14B) && sd >= D_950901 && sd < D_951001) {
                double rev = c.l_extendedprice[i] * (1.0 - c.l_discount[i]);
                t.q14t += rev;
                if (is_promo[c.p_type_col[c.l_partkey[i] - 1]]) t.q14p += rev;
            }

            // === Q15: shipdate [1996-01-01,1996-04-01) ===
            if ((active & Q15B) && sd >= D_960101 && sd < D_960401) {
                double rev = c.l_extendedprice[i] * (1.0 - c.l_discount[i]);
                #pragma omp atomic
                r.q15_rev[c.l_suppkey[i]] += rev;
            }

            // === Q18_sub: SUM(qty) GROUP BY orderkey ===
            if (active & Q18B) {
                #pragma omp atomic
                r.q18_qty[c.l_orderkey[i]] += c.l_quantity[i];
            }

            // === Q3: l_shipdate > 1995-03-15 → orders → customer BUILDING ===
            if ((active & Q3B) && sd > D_950315) {
                int32_t ok = c.l_orderkey[i];
                int32_t orow = c.opk_idx[ok];
                if (orow >= 0 && c.o_orderdate[orow] < D_950315) {
                    int32_t ck = c.o_custkey[orow];
                    if (c.c_mktseg[ck - 1] == mks_bld) {
                        double rev = c.l_extendedprice[i] * (1.0 - c.l_discount[i]);
                        auto& g = t.q3[ok];
                        g.rev += rev;
                        g.odate = c.o_orderdate[orow];
                        g.osp = c.o_shippriority[orow];
                    }
                }
            }

            // === Q5: orders date [1994,1995) → cust/supp same ASIA nation ===
            if (active & Q5B) {
                int32_t ok = c.l_orderkey[i];
                int32_t orow = c.opk_idx[ok];
                if (orow >= 0) {
                    int32_t od = c.o_orderdate[orow];
                    if (od >= D_940101 && od < D_950101) {
                        int32_t ck = c.o_custkey[orow];
                        int32_t cnk = c.c_nationkey[ck - 1];
                        if (q5_nat[cnk]) {
                            int32_t snk = c.s_nationkey[c.l_suppkey[i] - 1];
                            if (cnk == snk)
                                t.q5[cnk] += c.l_extendedprice[i] * (1.0 - c.l_discount[i]);
                        }
                    }
                }
            }

            // === Q7: shipdate [1995-01-01,1996-12-31], FR/DE pair ===
            if ((active & Q7B) && sd >= D_950101 && sd <= D_961231) {
                int32_t snk = c.s_nationkey[c.l_suppkey[i] - 1];
                if (snk == nk_fr || snk == nk_de) {
                    int32_t ok = c.l_orderkey[i];
                    int32_t orow = c.opk_idx[ok];
                    if (orow >= 0) {
                        int32_t cnk = c.c_nationkey[c.o_custkey[orow] - 1];
                        int pair = -1;
                        if (snk == nk_fr && cnk == nk_de) pair = 0;
                        else if (snk == nk_de && cnk == nk_fr) pair = 1;
                        if (pair >= 0) {
                            int yi = epoch_year(sd) - 1995;
                            if (yi >= 0 && yi <= 1)
                                t.q7[pair][yi] += c.l_extendedprice[i] * (1.0 - c.l_discount[i]);
                        }
                    }
                }
            }

            // === Q10: returnflag='R' → orders date [1993-10-01,1994-01-01) ===
            if ((active & Q10B) && c.l_returnflag[i] == 'R') {
                int32_t ok = c.l_orderkey[i];
                int32_t orow = c.opk_idx[ok];
                if (orow >= 0) {
                    int32_t od = c.o_orderdate[orow];
                    if (od >= D_931001 && od < D_940101) {
                        int32_t ck = c.o_custkey[orow];
                        t.q10[ck] += c.l_extendedprice[i] * (1.0 - c.l_discount[i]);
                    }
                }
            }

            // === Q12: shipmode MAIL/SHIP, commit<receipt, ship<commit, receipt in [1994,1995) ===
            if (active & Q12B) {
                uint8_t sm = c.l_shipmode_col[i];
                if ((sm == sm_mail || sm == sm_ship) &&
                    c.l_commitdate[i] < c.l_receiptdate[i] &&
                    sd < c.l_commitdate[i] &&
                    c.l_receiptdate[i] >= D_940101 && c.l_receiptdate[i] < D_950101) {
                    int mi = (sm == sm_mail) ? 0 : 1;
                    int32_t orow = c.opk_idx[c.l_orderkey[i]];
                    if (orow >= 0) {
                        uint8_t op = c.o_orderpri[orow];
                        if (op == op_urg || op == op_hi) t.q12h[mi]++;
                        else t.q12l[mi]++;
                    }
                }
            }
        } // end for
    } // end parallel

    // === Merge thread-local results ===
    for (int ti = 0; ti < NT; ti++) {
        if (active & Q1B)
            for (int g = 0; g < 6; g++) {
                r.q1[g].sq += tls[ti].q1[g].sq;  r.q1[g].sp += tls[ti].q1[g].sp;
                r.q1[g].sdp += tls[ti].q1[g].sdp; r.q1[g].sch += tls[ti].q1[g].sch;
                r.q1[g].sd += tls[ti].q1[g].sd;   r.q1[g].cnt += tls[ti].q1[g].cnt;
            }
        if (active & Q3B)
            for (auto& [k, v] : tls[ti].q3) {
                auto& g = r.q3[k]; g.rev += v.rev; g.odate = v.odate; g.osp = v.osp;
            }
        if (active & Q5B)
            for (int n = 0; n < 25; n++) r.q5[n] += tls[ti].q5[n];
        if (active & Q6B) r.q6 += tls[ti].q6;
        if (active & Q7B)
            for (int p = 0; p < 2; p++) for (int y = 0; y < 2; y++) r.q7[p][y] += tls[ti].q7[p][y];
        if (active & Q10B)
            for (auto& [k, v] : tls[ti].q10) r.q10[k] += v;
        if (active & Q12B) {
            r.q12h[0] += tls[ti].q12h[0]; r.q12h[1] += tls[ti].q12h[1];
            r.q12l[0] += tls[ti].q12l[0]; r.q12l[1] += tls[ti].q12l[1];
        }
        if (active & Q14B) { r.q14p += tls[ti].q14p; r.q14t += tls[ti].q14t; }
    }

    printf("[MQO] fused_lineitem done: %zu rows, consumers:", N);
    const char* qn[] = {"Q1","Q3","Q5","Q6","Q7","Q10","Q12","Q14","Q15","Q18"};
    uint32_t qb[] = {Q1B,Q3B,Q5B,Q6B,Q7B,Q10B,Q12B,Q14B,Q15B,Q18B};
    for (int i = 0; i < 10; i++) if (active & qb[i]) printf(" %s", qn[i]);
    printf("\n");
}
