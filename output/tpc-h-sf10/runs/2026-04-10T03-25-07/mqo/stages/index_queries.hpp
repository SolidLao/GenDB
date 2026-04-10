#pragma once
#include "common.hpp"

// ================================================================
// Q2 — Minimum Cost Supplier (EUROPE, p_size=15, p_type LIKE '%BRASS')
// ================================================================
static inline void run_q2(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q2");
    int rk_europe = find_regionkey(c, "EUROPE");
    bool eu_nation[25] = {};
    for (int n = 0; n < 25; n++) eu_nation[n] = (c.n_regionkey[n] == rk_europe);

    // p_type codes ending with "BRASS"
    std::vector<uint8_t> brass_codes;
    for (size_t i = 0; i < c.d_ptype.size(); i++)
        if (ends_with(c.d_ptype[i], "BRASS")) brass_codes.push_back((uint8_t)i);

    for (size_t pk = 1; pk <= c.npart; pk++) {
        size_t pidx = pk - 1;
        if (c.p_size[pidx] != 15) continue;
        uint8_t tc = c.p_type_col[pidx];
        bool is_brass = false;
        for (auto bc : brass_codes) if (tc == bc) { is_brass = true; break; }
        if (!is_brass) continue;

        // Find min supplycost among EUROPE suppliers for this part
        DRE dr = c.ps_pk_idx[pk];
        double min_cost = 1e18;
        for (uint32_t j = dr.start; j < dr.start + dr.count; j++) {
            int32_t sk = c.ps_suppkey[j];
            if (eu_nation[c.s_nationkey[sk - 1]] && c.ps_supplycost[j] < min_cost)
                min_cost = c.ps_supplycost[j];
        }
        if (min_cost >= 1e18) continue;

        // Collect all EUROPE suppliers matching min cost
        for (uint32_t j = dr.start; j < dr.start + dr.count; j++) {
            int32_t sk = c.ps_suppkey[j];
            int32_t snk = c.s_nationkey[sk - 1];
            if (eu_nation[snk] && c.ps_supplycost[j] == min_cost) {
                size_t srow = sk - 1;
                r.q2.push_back({
                    c.s_acctbal[srow],
                    vl_get(c.s_nm_off, c.s_nm_dat, srow),
                    c.d_nname[c.n_name_col[snk]],
                    (int32_t)pk,
                    c.d_pmfgr[c.p_mfgr_col[pidx]],
                    vl_get(c.s_ad_off, c.s_ad_dat, srow),
                    vl_get(c.s_ph_off, c.s_ph_dat, srow),
                    vl_get(c.s_cm_off, c.s_cm_dat, srow)
                });
            }
        }
    }
}

// ================================================================
// Q8 — National Market Share (ECONOMY ANODIZED STEEL, AMERICA, BRAZIL)
// ================================================================
static inline void run_q8(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q8");
    int ptype_code = dfind(c.d_ptype, "ECONOMY ANODIZED STEEL");
    int rk_america = find_regionkey(c, "AMERICA");
    int nk_brazil  = find_nationkey(c, "BRAZIL");
    bool am_nation[25] = {};
    for (int n = 0; n < 25; n++) am_nation[n] = (c.n_regionkey[n] == rk_america);

    for (size_t pk = 1; pk <= c.npart; pk++) {
        if (c.p_type_col[pk - 1] != (uint8_t)ptype_code) continue;

        uint32_t li_s = c.li_pk_off[pk], li_e = c.li_pk_off[pk + 1];
        for (uint32_t j = li_s; j < li_e; j++) {
            uint32_t lr = c.li_pk_rows[j];
            int32_t orow = c.opk_idx[c.l_orderkey[lr]];
            if (orow < 0) continue;
            int32_t od = c.o_orderdate[orow];
            if (od < D_950101 || od > D_961231) continue;

            int32_t cnk = c.c_nationkey[c.o_custkey[orow] - 1];
            if (!am_nation[cnk]) continue;

            int32_t snk = c.s_nationkey[c.l_suppkey[lr] - 1];
            double vol = c.l_extendedprice[lr] * (1.0 - c.l_discount[lr]);
            int yi = epoch_year(od) - 1995;
            if (yi >= 0 && yi <= 1) {
                r.q8t[yi] += vol;
                if (snk == nk_brazil) r.q8b[yi] += vol;
            }
        }
    }
}

// ================================================================
// Q9 — Product Type Profit (p_name LIKE '%green%')
// ================================================================
static inline void run_q9(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q9");
    for (size_t pk = 1; pk <= c.npart; pk++) {
        size_t pidx = pk - 1;
        const char* nm = c.p_nm_dat + c.p_nm_off[pidx];
        size_t nl = c.p_nm_off[pidx + 1] - c.p_nm_off[pidx];
        if (!str_contains(nm, nl, "green", 5)) continue;

        uint32_t li_s = c.li_pk_off[pk], li_e = c.li_pk_off[pk + 1];
        for (uint32_t j = li_s; j < li_e; j++) {
            uint32_t lr = c.li_pk_rows[j];
            int32_t sk = c.l_suppkey[lr];
            int32_t snk = c.s_nationkey[sk - 1];

            int32_t orow = c.opk_idx[c.l_orderkey[lr]];
            if (orow < 0) continue;
            int yr = epoch_year(c.o_orderdate[orow]);

            // partsupp supplycost via composite hash
            int32_t psrow = ps_hash_lookup(c.ps_hash, c.ps_hmask, (int32_t)pk, sk);
            double ps_cost = (psrow >= 0) ? c.ps_supplycost[psrow] : 0;

            double amount = c.l_extendedprice[lr] * (1.0 - c.l_discount[lr])
                          - ps_cost * c.l_quantity[lr];
            r.q9[{snk, yr}] += amount;
        }
    }
}

// ================================================================
// Q11 — Important Stock (GERMANY, threshold 0.0001)
// ================================================================
static inline void run_q11(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q11");
    int nk_de = find_nationkey(c, "GERMANY");
    uint32_t s_start = c.sup_nk_off[nk_de], s_end = c.sup_nk_off[nk_de + 1];

    for (uint32_t si = s_start; si < s_end; si++) {
        uint32_t srow = c.sup_nk_rows[si];
        int32_t sk = (int32_t)srow + 1;
        uint32_t ps_s = c.ps_sk_off[sk], ps_e = c.ps_sk_off[sk + 1];
        for (uint32_t psi = ps_s; psi < ps_e; psi++) {
            uint32_t psrow = c.ps_sk_rows[psi];
            double val = c.ps_supplycost[psrow] * c.ps_availqty[psrow];
            r.q11[(int32_t)c.ps_partkey[psrow]] += val;
            r.q11_total += val;
        }
    }
}

// ================================================================
// Q16 — Parts/Supplier Relationship
// (Brand#45 excluded, MEDIUM POLISHED% excluded, sizes 49,14,23,45,19,3,36,9)
// ================================================================
static inline void run_q16(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q16");

    // Supplier blacklist: s_comment LIKE '%Customer%Complaints%'
    std::vector<bool> blacklist(c.nsupp + 1, false);
    for (size_t i = 0; i < c.nsupp; i++) {
        const char* cm = c.s_cm_dat + c.s_cm_off[i];
        size_t cl = c.s_cm_off[i + 1] - c.s_cm_off[i];
        if (str_like_ab(cm, cl, "Customer", 8, "Complaints", 10))
            blacklist[i + 1] = true;
    }

    uint8_t brand45 = (uint8_t)dfind(c.d_pbrand, "Brand#45");

    // Bad type codes: MEDIUM POLISHED%
    std::vector<uint8_t> bad_types;
    for (size_t i = 0; i < c.d_ptype.size(); i++)
        if (c.d_ptype[i].size() >= 15 && c.d_ptype[i].compare(0, 15, "MEDIUM POLISHED") == 0)
            bad_types.push_back((uint8_t)i);

    bool good_size[51] = {};
    int sizes[] = {49,14,23,45,19,3,36,9};
    for (int s : sizes) if (s >= 1 && s <= 50) good_size[s] = true;

    struct Q16K {
        uint8_t brand, type; int32_t size;
        bool operator<(const Q16K& o) const {
            if (brand != o.brand) return brand < o.brand;
            if (type != o.type) return type < o.type;
            return size < o.size;
        }
    };
    std::map<Q16K, std::set<int32_t>> groups;

    for (size_t pk = 1; pk <= c.npart; pk++) {
        size_t pidx = pk - 1;
        if (c.p_brand_col[pidx] == brand45) continue;
        uint8_t tc = c.p_type_col[pidx];
        bool bad = false;
        for (auto bt : bad_types) if (tc == bt) { bad = true; break; }
        if (bad) continue;
        int32_t sz = c.p_size[pidx];
        if (sz < 1 || sz > 50 || !good_size[sz]) continue;

        Q16K key{c.p_brand_col[pidx], tc, sz};
        DRE dr = c.ps_pk_idx[pk];
        for (uint32_t j = dr.start; j < dr.start + dr.count; j++) {
            int32_t sk = c.ps_suppkey[j];
            if (!blacklist[sk]) groups[key].insert(sk);
        }
    }

    for (auto& [k, s] : groups)
        r.q16.push_back({c.d_pbrand[k.brand], c.d_ptype[k.type], k.size, (int)s.size()});
}

// ================================================================
// Q17 — Small-Quantity-Order Revenue (Brand#23, MED BOX)
// ================================================================
static inline void run_q17(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q17");
    uint8_t brand_code = (uint8_t)dfind(c.d_pbrand, "Brand#23");
    uint8_t cont_code  = (uint8_t)dfind(c.d_pcont, "MED BOX");
    double total = 0;

    for (size_t pk = 1; pk <= c.npart; pk++) {
        size_t pidx = pk - 1;
        if (c.p_brand_col[pidx] != brand_code || c.p_cont_col[pidx] != cont_code) continue;

        uint32_t li_s = c.li_pk_off[pk], li_e = c.li_pk_off[pk + 1];
        uint32_t cnt = li_e - li_s;
        if (cnt == 0) continue;

        // Pass 1: avg(l_quantity)
        double sq = 0;
        for (uint32_t j = li_s; j < li_e; j++) sq += c.l_quantity[c.li_pk_rows[j]];
        double thresh = 0.2 * (sq / cnt);

        // Pass 2: sum extendedprice where qty < 0.2 * avg
        for (uint32_t j = li_s; j < li_e; j++) {
            uint32_t lr = c.li_pk_rows[j];
            if (c.l_quantity[lr] < thresh) total += c.l_extendedprice[lr];
        }
    }
    r.q17 = total / 7.0;
}

// ================================================================
// Q19 — Discounted Revenue (3 brand/container/size groups)
// ================================================================
static inline void run_q19(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q19");

    // Brand codes
    uint8_t br1 = (uint8_t)dfind(c.d_pbrand, "Brand#12");
    uint8_t br2 = (uint8_t)dfind(c.d_pbrand, "Brand#23");
    uint8_t br3 = (uint8_t)dfind(c.d_pbrand, "Brand#34");

    // Container codes per group
    auto cc = [&](const char* n) -> uint8_t { return (uint8_t)dfind(c.d_pcont, n); };
    uint8_t sm_c[] = {cc("SM CASE"), cc("SM BOX"), cc("SM PACK"), cc("SM PKG")};
    uint8_t md_c[] = {cc("MED BAG"), cc("MED BOX"), cc("MED PKG"), cc("MED PACK")};
    uint8_t lg_c[] = {cc("LG CASE"), cc("LG BOX"), cc("LG PACK"), cc("LG PKG")};

    auto in4 = [](uint8_t v, const uint8_t* arr) {
        return v == arr[0] || v == arr[1] || v == arr[2] || v == arr[3];
    };

    // Shipmode: AIR or REG AIR
    uint8_t sm_air = (uint8_t)dfind(c.d_shipmode, "AIR");
    uint8_t sm_reg = (uint8_t)dfind(c.d_shipmode, "REG AIR");
    // Shipinstruct: DELIVER IN PERSON
    uint8_t si_dip = (uint8_t)dfind(c.d_shipinst, "DELIVER IN PERSON");

    double total = 0;

    for (size_t pk = 1; pk <= c.npart; pk++) {
        size_t pidx = pk - 1;
        uint8_t brand = c.p_brand_col[pidx];
        uint8_t cont  = c.p_cont_col[pidx];
        int32_t sz    = c.p_size[pidx];

        // Determine which group this part matches (0=none, 1/2/3)
        int grp = 0;
        if (brand == br1 && in4(cont, sm_c) && sz >= 1 && sz <= 5) grp = 1;
        else if (brand == br2 && in4(cont, md_c) && sz >= 1 && sz <= 10) grp = 2;
        else if (brand == br3 && in4(cont, lg_c) && sz >= 1 && sz <= 15) grp = 3;
        if (grp == 0) continue;

        double qlo = (grp == 1) ? 1.0 : (grp == 2) ? 10.0 : 20.0;
        double qhi = qlo + 10.0;

        uint32_t li_s = c.li_pk_off[pk], li_e = c.li_pk_off[pk + 1];
        for (uint32_t j = li_s; j < li_e; j++) {
            uint32_t lr = c.li_pk_rows[j];
            uint8_t sm = c.l_shipmode_col[lr];
            if ((sm != sm_air && sm != sm_reg) || c.l_shipinst_col[lr] != si_dip) continue;
            double q = c.l_quantity[lr];
            if (q >= qlo && q <= qhi)
                total += c.l_extendedprice[lr] * (1.0 - c.l_discount[lr]);
        }
    }
    r.q19 = total;
}

// ================================================================
// Q20 — Potential Part Promotion (forest%, CANADA, 1994)
// ================================================================
static inline void run_q20(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q20");
    int nk_canada = find_nationkey(c, "CANADA");

    // Forest parts
    std::set<int32_t> forest_parts;
    for (size_t pk = 1; pk <= c.npart; pk++) {
        size_t pidx = pk - 1;
        const char* nm = c.p_nm_dat + c.p_nm_off[pidx];
        size_t nl = c.p_nm_off[pidx + 1] - c.p_nm_off[pidx];
        if (nl >= 6 && memcmp(nm, "forest", 6) == 0)
            forest_parts.insert((int32_t)pk);
    }

    // CANADA suppliers
    uint32_t s_start = c.sup_nk_off[nk_canada], s_end = c.sup_nk_off[nk_canada + 1];

    for (uint32_t si = s_start; si < s_end; si++) {
        uint32_t srow = c.sup_nk_rows[si];
        int32_t sk = (int32_t)srow + 1;
        bool qualifies = false;

        // Iterate partsupp entries for this supplier
        uint32_t ps_s = c.ps_sk_off[sk], ps_e = c.ps_sk_off[sk + 1];
        for (uint32_t psi = ps_s; psi < ps_e && !qualifies; psi++) {
            uint32_t psrow = c.ps_sk_rows[psi];
            int32_t pk = c.ps_partkey[psrow];
            if (forest_parts.count(pk) == 0) continue;

            int32_t avail = c.ps_availqty[psrow];
            // Sum l_quantity for (pk, sk) with shipdate [1994-01-01, 1995-01-01)
            double sum_qty = 0;
            bool has_rows = false;
            uint32_t li_s = c.li_pk_off[pk], li_e = c.li_pk_off[pk + 1];
            for (uint32_t lj = li_s; lj < li_e; lj++) {
                uint32_t lr = c.li_pk_rows[lj];
                if (c.l_suppkey[lr] == sk &&
                    c.l_shipdate[lr] >= D_940101 && c.l_shipdate[lr] < D_950101) {
                    sum_qty += c.l_quantity[lr];
                    has_rows = true;
                }
            }
            if (has_rows && avail > 0.5 * sum_qty) qualifies = true;
        }

        if (qualifies) {
            r.q20.push_back({
                vl_get(c.s_nm_off, c.s_nm_dat, srow),
                vl_get(c.s_ad_off, c.s_ad_dat, srow)
            });
        }
    }
}

// ================================================================
// Q21 — Suppliers Who Kept Orders Waiting (SAUDI ARABIA)
// ================================================================
static inline void run_q21(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q21");
    int nk_sa = find_nationkey(c, "SAUDI ARABIA");
    uint32_t s_start = c.sup_nk_off[nk_sa], s_end = c.sup_nk_off[nk_sa + 1];

    for (uint32_t si = s_start; si < s_end; si++) {
        uint32_t srow = c.sup_nk_rows[si];
        int32_t sk = (int32_t)srow + 1;
        int64_t wait_count = 0;

        uint32_t li_s = c.li_sk_off[sk], li_e = c.li_sk_off[sk + 1];
        for (uint32_t li_i = li_s; li_i < li_e; li_i++) {
            uint32_t lr = c.li_sk_rows[li_i];
            if (c.l_receiptdate[lr] <= c.l_commitdate[lr]) continue;

            int32_t ok = c.l_orderkey[lr];
            int32_t orow = c.opk_idx[ok];
            if (orow < 0 || c.o_orderstatus[orow] != 'F') continue;

            // EXISTS l2 different suppkey; NOT EXISTS l3 different + late
            DRE dr = c.li_ok_idx[ok];
            bool exists_other = false, exists_other_late = false;
            for (uint32_t j = dr.start; j < dr.start + dr.count; j++) {
                if (c.l_suppkey[j] != sk) {
                    exists_other = true;
                    if (c.l_receiptdate[j] > c.l_commitdate[j]) {
                        exists_other_late = true;
                        break;
                    }
                }
            }
            if (exists_other && !exists_other_late) wait_count++;
        }

        if (wait_count > 0) {
            std::string sname = vl_get(c.s_nm_off, c.s_nm_dat, srow);
            r.q21[sname] += wait_count;
        }
    }
}

// ================================================================
// Q22 — Global Sales Opportunity (phone codes 13,31,23,29,30,18,17)
// ================================================================
static inline void run_q22(const Ctx& c, Res& r) {
    MQO_TIME_PHASE("Q22");

    const char* codes[] = {"13","31","23","29","30","18","17"};
    auto is_target = [&](const char* ph) -> bool {
        for (int i = 0; i < 7; i++)
            if (ph[0] == codes[i][0] && ph[1] == codes[i][1]) return true;
        return false;
    };

    // Pass 1: avg acctbal for targets with positive balance
    double sum_bal = 0; int64_t cnt = 0;
    for (size_t i = 0; i < c.ncust; i++) {
        const char* ph = c.c_ph_dat + c.c_ph_off[i];
        if (!is_target(ph)) continue;
        if (c.c_acctbal[i] > 0.0) { sum_bal += c.c_acctbal[i]; cnt++; }
    }
    double avg_bal = cnt > 0 ? sum_bal / cnt : 0;

    // Pass 2: filter + NOT EXISTS orders
    for (size_t i = 0; i < c.ncust; i++) {
        const char* ph = c.c_ph_dat + c.c_ph_off[i];
        if (!is_target(ph)) continue;
        if (c.c_acctbal[i] <= avg_bal) continue;
        // NOT EXISTS orders: custkey_to_orders uses 0-based key (key = custkey-1 = i)
        if (c.cust_ok_off[i] < c.cust_ok_off[i + 1]) continue; // has orders
        std::string cc(ph, 2);
        r.q22[cc].first++;
        r.q22[cc].second += c.c_acctbal[i];
    }
}

// ================================================================
// Dispatcher
// ================================================================
static inline void run_index_queries(const Ctx& c, Res& r, uint32_t active) {
    if (!(active & IDX_MASK)) return;
    MQO_TIME_PHASE("index_queries");
    if (active & Q2B)  run_q2(c, r);
    if (active & Q8B)  run_q8(c, r);
    if (active & Q9B)  run_q9(c, r);
    if (active & Q11B) run_q11(c, r);
    if (active & Q16B) run_q16(c, r);
    if (active & Q17B) run_q17(c, r);
    if (active & Q19B) run_q19(c, r);
    if (active & Q20B) run_q20(c, r);
    if (active & Q21B) run_q21(c, r);
    if (active & Q22B) run_q22(c, r);
}
