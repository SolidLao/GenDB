#pragma once
#include "common.hpp"

// ================================================================
// Helper: open output file
// ================================================================
static inline FILE* qopen(const Ctx& c, int qn) {
    char buf[512];
    snprintf(buf, sizeof(buf), "%s/Q%d.csv", c.od.c_str(), qn);
    FILE* f = fopen(buf, "w");
    if (!f) { fprintf(stderr, "Cannot open %s\n", buf); exit(1); }
    return f;
}

// ================================================================
// Post-scan dependent stage (Q15_final, Q18_main)
// ================================================================
static inline void run_post_scan(const Ctx& c, Res& r, uint32_t active) {
    if (!(active & PST_MASK)) return;
    MQO_TIME_PHASE("post_scan_dependent");

    // Q15_final: find max revenue supplier
    if (active & Q15B) {
        MQO_TIME_PHASE("Q15_final");
        double max_rev = 0;
        for (size_t sk = 1; sk <= c.nsupp; sk++)
            if (r.q15_rev[sk] > max_rev) max_rev = r.q15_rev[sk];
        for (size_t sk = 1; sk <= c.nsupp; sk++) {
            if (r.q15_rev[sk] == max_rev && max_rev > 0) {
                size_t srow = sk - 1;
                r.q15f.push_back({
                    (int32_t)sk,
                    vl_get(c.s_nm_off, c.s_nm_dat, srow),
                    vl_get(c.s_ad_off, c.s_ad_dat, srow),
                    vl_get(c.s_ph_off, c.s_ph_dat, srow),
                    max_rev
                });
            }
        }
    }

    // Q18_main: qualifying orderkeys (sum qty > 300)
    if (active & Q18B) {
        MQO_TIME_PHASE("Q18_main");
        for (size_t ok = 1; ok <= 60000000; ok++) {
            if (r.q18_qty[ok] > 300.0) {
                int32_t orow = c.opk_idx[ok];
                if (orow < 0) continue;
                int32_t ck = c.o_custkey[orow];
                r.q18.push_back({
                    vl_get(c.c_nm_off, c.c_nm_dat, (size_t)(ck - 1)),
                    ck, (int32_t)ok,
                    c.o_orderdate[orow],
                    c.o_totalprice[orow],
                    r.q18_qty[ok]
                });
            }
        }
    }
}

// ================================================================
// Finalize: sort + limit + CSV output for all 22 queries
// ================================================================
static inline void finalize_output(const Ctx& c, Res& r, uint32_t active) {
    MQO_TIME_PHASE("finalize_output");

    // === Q1 ===
    if (active & Q1B) {
        MQO_TIME_PHASE("Q1_finalize");
        const char rf[] = {'A','N','R'};
        const char ls[] = {'F','O'};
        FILE* f = qopen(c, 1);
        for (int ri = 0; ri < 3; ri++)
            for (int li = 0; li < 2; li++) {
                auto& g = r.q1[ri * 2 + li];
                if (g.cnt == 0) continue;
                fprintf(f, "%c|%c|%.2f|%.2f|%.4f|%.4f|%.2f|%.2f|%.2f|%ld\n",
                    rf[ri], ls[li], g.sq, g.sp, g.sdp, g.sch,
                    g.sq / g.cnt, g.sp / g.cnt, g.sd / g.cnt, (long)g.cnt);
            }
        fclose(f);
    }

    // === Q2 ===
    if (active & Q2B) {
        MQO_TIME_PHASE("Q2_finalize");
        std::sort(r.q2.begin(), r.q2.end(), [](auto& a, auto& b) {
            if (a.s_acctbal != b.s_acctbal) return a.s_acctbal > b.s_acctbal;
            if (a.n_name != b.n_name) return a.n_name < b.n_name;
            if (a.s_name != b.s_name) return a.s_name < b.s_name;
            return a.p_partkey < b.p_partkey;
        });
        int lim = std::min((int)r.q2.size(), 100);
        FILE* f = qopen(c, 2);
        for (int i = 0; i < lim; i++) {
            auto& x = r.q2[i];
            fprintf(f, "%.2f|%s|%s|%d|%s|%s|%s|%s\n",
                x.s_acctbal, x.s_name.c_str(), x.n_name.c_str(), x.p_partkey,
                x.p_mfgr.c_str(), x.s_addr.c_str(), x.s_phone.c_str(), x.s_comment.c_str());
        }
        fclose(f);
    }

    // === Q3 ===
    if (active & Q3B) {
        MQO_TIME_PHASE("Q3_finalize");
        struct Q3S { int32_t ok; double rev; int32_t odate, osp; };
        std::vector<Q3S> q3v;
        for (auto& [k, v] : r.q3) q3v.push_back({k, v.rev, v.odate, v.osp});
        std::sort(q3v.begin(), q3v.end(), [](auto& a, auto& b) {
            if (a.rev != b.rev) return a.rev > b.rev;
            return a.odate < b.odate;
        });
        int lim = std::min((int)q3v.size(), 10);
        FILE* f = qopen(c, 3);
        for (int i = 0; i < lim; i++)
            fprintf(f, "%d|%.2f|%s|%d\n",
                q3v[i].ok, q3v[i].rev, epoch_str(q3v[i].odate).c_str(), q3v[i].osp);
        fclose(f);
    }

    // === Q4 ===
    if (active & Q4B) {
        MQO_TIME_PHASE("Q4_finalize");
        // Alphabetical order of priority strings: 1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW
        // Dict: 0=5-LOW, 1=1-URGENT, 2=4-NOT SPECIFIED, 3=2-HIGH, 4=3-MEDIUM
        int order[] = {1, 3, 4, 2, 0};
        FILE* f = qopen(c, 4);
        for (int i = 0; i < 5; i++)
            fprintf(f, "%s|%ld\n", c.d_ordpri[order[i]].c_str(), (long)r.q4[order[i]]);
        fclose(f);
    }

    // === Q5 ===
    if (active & Q5B) {
        MQO_TIME_PHASE("Q5_finalize");
        struct NR { int nk; double rev; };
        std::vector<NR> q5v;
        for (int n = 0; n < 25; n++)
            if (r.q5[n] != 0) q5v.push_back({n, r.q5[n]});
        std::sort(q5v.begin(), q5v.end(), [](auto& a, auto& b) { return a.rev > b.rev; });
        FILE* f = qopen(c, 5);
        for (auto& x : q5v)
            fprintf(f, "%s|%.2f\n", c.d_nname[c.n_name_col[x.nk]].c_str(), x.rev);
        fclose(f);
    }

    // === Q6 ===
    if (active & Q6B) {
        FILE* f = qopen(c, 6);
        fprintf(f, "%.2f\n", r.q6);
        fclose(f);
    }

    // === Q7 ===
    if (active & Q7B) {
        MQO_TIME_PHASE("Q7_finalize");
        // Pair 0: FRANCE→GERMANY, Pair 1: GERMANY→FRANCE
        // FRANCE < GERMANY alphabetically → pair 0 first
        FILE* f = qopen(c, 7);
        for (int p = 0; p < 2; p++) {
            const char* sn = (p == 0) ? "FRANCE" : "GERMANY";
            const char* cn = (p == 0) ? "GERMANY" : "FRANCE";
            for (int yi = 0; yi < 2; yi++)
                if (r.q7[p][yi] != 0)
                    fprintf(f, "%s|%s|%d|%.2f\n", sn, cn, 1995 + yi, r.q7[p][yi]);
        }
        fclose(f);
    }

    // === Q8 ===
    if (active & Q8B) {
        FILE* f = qopen(c, 8);
        for (int yi = 0; yi < 2; yi++) {
            double share = (r.q8t[yi] != 0) ? r.q8b[yi] / r.q8t[yi] : 0;
            fprintf(f, "%d|%.2f\n", 1995 + yi, share);
        }
        fclose(f);
    }

    // === Q9 ===
    if (active & Q9B) {
        MQO_TIME_PHASE("Q9_finalize");
        struct NP { std::string nation; int year; double profit; };
        std::vector<NP> q9v;
        for (auto& [k, v] : r.q9)
            q9v.push_back({c.d_nname[c.n_name_col[k.first]], k.second, v});
        std::sort(q9v.begin(), q9v.end(), [](auto& a, auto& b) {
            if (a.nation != b.nation) return a.nation < b.nation;
            return a.year > b.year;
        });
        FILE* f = qopen(c, 9);
        for (auto& x : q9v) fprintf(f, "%s|%d|%.2f\n", x.nation.c_str(), x.year, x.profit);
        fclose(f);
    }

    // === Q10 ===
    if (active & Q10B) {
        MQO_TIME_PHASE("Q10_finalize");
        struct Q10S { int32_t ck; double rev; };
        std::vector<Q10S> q10v;
        for (auto& [k, v] : r.q10) q10v.push_back({k, v});
        std::sort(q10v.begin(), q10v.end(), [](auto& a, auto& b) { return a.rev > b.rev; });
        int lim = std::min((int)q10v.size(), 20);
        FILE* f = qopen(c, 10);
        for (int i = 0; i < lim; i++) {
            int32_t ck = q10v[i].ck;
            size_t crow = ck - 1;
            fprintf(f, "%d|%s|%.2f|%.2f|%s|%s|%s|%s\n",
                ck, vl_get(c.c_nm_off, c.c_nm_dat, crow).c_str(),
                q10v[i].rev, c.c_acctbal[crow],
                c.d_nname[c.n_name_col[c.c_nationkey[crow]]].c_str(),
                vl_get(c.c_ad_off, c.c_ad_dat, crow).c_str(),
                vl_get(c.c_ph_off, c.c_ph_dat, crow).c_str(),
                vl_get(c.c_cm_off, c.c_cm_dat, crow).c_str());
        }
        fclose(f);
    }

    // === Q11 ===
    if (active & Q11B) {
        MQO_TIME_PHASE("Q11_finalize");
        // TPC-H Q11 threshold = 0.0001 / SF; SF = ncust / 150000
        double sf = (double)c.ncust / 150000.0;
        double threshold = r.q11_total * (0.0001 / sf);
        std::vector<std::pair<int32_t, double>> q11v;
        for (auto& [k, v] : r.q11)
            if (v > threshold) q11v.push_back({k, v});
        std::sort(q11v.begin(), q11v.end(), [](auto& a, auto& b) { return a.second > b.second; });
        FILE* f = qopen(c, 11);
        for (auto& [pk, val] : q11v) fprintf(f, "%d|%.2f\n", pk, val);
        fclose(f);
    }

    // === Q12 ===
    if (active & Q12B) {
        FILE* f = qopen(c, 12);
        // MAIL (idx 0) < SHIP (idx 1) alphabetically
        fprintf(f, "MAIL|%ld|%ld\n", (long)r.q12h[0], (long)r.q12l[0]);
        fprintf(f, "SHIP|%ld|%ld\n", (long)r.q12h[1], (long)r.q12l[1]);
        fclose(f);
    }

    // === Q13 ===
    if (active & Q13B) {
        MQO_TIME_PHASE("Q13_finalize");
        std::map<int32_t, int64_t> hist;
        for (size_t i = 0; i < c.ncust; i++) hist[r.q13_cnt[i]]++;
        std::vector<std::pair<int32_t, int64_t>> hv(hist.begin(), hist.end());
        std::sort(hv.begin(), hv.end(), [](auto& a, auto& b) {
            if (a.second != b.second) return a.second > b.second;
            return a.first > b.first;
        });
        FILE* f = qopen(c, 13);
        for (auto& [cnt, dist] : hv) fprintf(f, "%d|%ld\n", cnt, (long)dist);
        fclose(f);
    }

    // === Q14 ===
    if (active & Q14B) {
        FILE* f = qopen(c, 14);
        double pct = (r.q14t != 0) ? 100.0 * r.q14p / r.q14t : 0;
        fprintf(f, "%.2f\n", pct);
        fclose(f);
    }

    // === Q15 ===
    if (active & Q15B) {
        MQO_TIME_PHASE("Q15_finalize");
        std::sort(r.q15f.begin(), r.q15f.end(), [](auto& a, auto& b) { return a.sk < b.sk; });
        FILE* f = qopen(c, 15);
        for (auto& x : r.q15f)
            fprintf(f, "%d|%s|%s|%s|%.2f\n", x.sk, x.sname.c_str(),
                x.saddr.c_str(), x.sphone.c_str(), x.rev);
        fclose(f);
    }

    // === Q16 ===
    if (active & Q16B) {
        MQO_TIME_PHASE("Q16_finalize");
        std::sort(r.q16.begin(), r.q16.end(), [](auto& a, auto& b) {
            if (a.cnt != b.cnt) return a.cnt > b.cnt;
            if (a.brand != b.brand) return a.brand < b.brand;
            if (a.type != b.type) return a.type < b.type;
            return a.size < b.size;
        });
        FILE* f = qopen(c, 16);
        for (auto& x : r.q16)
            fprintf(f, "%s|%s|%d|%d\n", x.brand.c_str(), x.type.c_str(), x.size, x.cnt);
        fclose(f);
    }

    // === Q17 ===
    if (active & Q17B) {
        FILE* f = qopen(c, 17);
        fprintf(f, "%.2f\n", r.q17);
        fclose(f);
    }

    // === Q18 ===
    if (active & Q18B) {
        MQO_TIME_PHASE("Q18_finalize");
        std::sort(r.q18.begin(), r.q18.end(), [](auto& a, auto& b) {
            if (a.otp != b.otp) return a.otp > b.otp;
            return a.odate < b.odate;
        });
        int lim = std::min((int)r.q18.size(), 100);
        FILE* f = qopen(c, 18);
        for (int i = 0; i < lim; i++) {
            auto& x = r.q18[i];
            fprintf(f, "%s|%d|%d|%s|%.2f|%.2f\n",
                x.cname.c_str(), x.ck, x.ok,
                epoch_str(x.odate).c_str(), x.otp, x.sq);
        }
        fclose(f);
    }

    // === Q19 ===
    if (active & Q19B) {
        FILE* f = qopen(c, 19);
        fprintf(f, "%.2f\n", r.q19);
        fclose(f);
    }

    // === Q20 ===
    if (active & Q20B) {
        MQO_TIME_PHASE("Q20_finalize");
        std::sort(r.q20.begin(), r.q20.end());
        FILE* f = qopen(c, 20);
        for (auto& [name, addr] : r.q20)
            fprintf(f, "%s|%s\n", name.c_str(), addr.c_str());
        fclose(f);
    }

    // === Q21 ===
    if (active & Q21B) {
        MQO_TIME_PHASE("Q21_finalize");
        struct Q21S { std::string name; int64_t cnt; };
        std::vector<Q21S> q21v;
        for (auto& [k, v] : r.q21) q21v.push_back({k, v});
        std::sort(q21v.begin(), q21v.end(), [](auto& a, auto& b) {
            if (a.cnt != b.cnt) return a.cnt > b.cnt;
            return a.name < b.name;
        });
        int lim = std::min((int)q21v.size(), 100);
        FILE* f = qopen(c, 21);
        for (int i = 0; i < lim; i++)
            fprintf(f, "%s|%ld\n", q21v[i].name.c_str(), (long)q21v[i].cnt);
        fclose(f);
    }

    // === Q22 ===
    if (active & Q22B) {
        FILE* f = qopen(c, 22);
        for (auto& [code, v] : r.q22) // std::map: sorted by key
            fprintf(f, "%s|%ld|%.2f\n", code.c_str(), (long)v.first, v.second);
        fclose(f);
    }

    printf("[MQO] All output written to %s/\n", c.od.c_str());
}
