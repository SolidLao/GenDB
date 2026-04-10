#pragma once
// Shared component: scan_lineitem_shipdate_1995_1996
// Kind: filtered_scan   Mode: materialize
// Filter: l_shipdate >= 1995-01-01 AND l_shipdate <= 1996-12-31
// Consumers: Q7, Q14, Q15
// Derived as a side-output from scan_lineitem_full.

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"
#include "shared/scan_lineitem_full.hpp"

namespace mqo::shared::scan_lineitem_shipdate_1995_1996 {

static constexpr int32_t DATE_LO = mqo::io::to_epoch_days(1995, 1, 1);  // 9131
static constexpr int32_t DATE_HI = mqo::io::to_epoch_days(1997, 1, 1);  // 9862  (exclusive)

struct Output {
    size_t   n_rows          = 0;
    int32_t* l_orderkey      = nullptr;
    int32_t* l_partkey       = nullptr;
    int32_t* l_suppkey       = nullptr;
    double*  l_quantity      = nullptr;
    double*  l_extendedprice = nullptr;
    double*  l_discount      = nullptr;
    int32_t* l_shipdate      = nullptr;
};

inline Output& get() {
    static Output out;
    return out;
}

inline void build(const mqo::Context& ctx) {
    MQO_TIME_SHARED("scan_lineitem_shipdate_1995_1996");
    const auto& full = mqo::shared::scan_lineitem_full::get_columns(ctx);
    const size_t n = full.n_rows;

    // Pass 1: count qualifying rows
    size_t count = 0;
    for (size_t i = 0; i < n; ++i) {
        const int32_t sd = full.l_shipdate[i];
        if (sd >= DATE_LO && sd < DATE_HI) ++count;
    }

    // Allocate
    Output& out   = get();
    out.n_rows          = count;
    out.l_orderkey      = static_cast<int32_t*>(std::malloc(count * sizeof(int32_t)));
    out.l_partkey       = static_cast<int32_t*>(std::malloc(count * sizeof(int32_t)));
    out.l_suppkey       = static_cast<int32_t*>(std::malloc(count * sizeof(int32_t)));
    out.l_quantity      = static_cast<double*> (std::malloc(count * sizeof(double)));
    out.l_extendedprice = static_cast<double*> (std::malloc(count * sizeof(double)));
    out.l_discount      = static_cast<double*> (std::malloc(count * sizeof(double)));
    out.l_shipdate      = static_cast<int32_t*>(std::malloc(count * sizeof(int32_t)));

    // Pass 2: materialize
    size_t j = 0;
    for (size_t i = 0; i < n; ++i) {
        const int32_t sd = full.l_shipdate[i];
        if (sd >= DATE_LO && sd < DATE_HI) {
            out.l_orderkey[j]      = full.l_orderkey[i];
            out.l_partkey[j]       = full.l_partkey[i];
            out.l_suppkey[j]       = full.l_suppkey[i];
            out.l_quantity[j]      = full.l_quantity[i];
            out.l_extendedprice[j] = full.l_extendedprice[i];
            out.l_discount[j]      = full.l_discount[i];
            out.l_shipdate[j]      = sd;
            ++j;
        }
    }
}

}  // namespace mqo::shared::scan_lineitem_shipdate_1995_1996
