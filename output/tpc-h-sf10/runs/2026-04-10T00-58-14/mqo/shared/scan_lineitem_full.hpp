#pragma once
// Shared component: scan_lineitem_full
// Kind: filtered_scan (full table)   Mode: callback
// Consumers: Q1,Q3,Q4,Q5,Q6,Q7,Q8,Q9,Q10,Q12,Q14,Q15,Q17,Q18,Q19,Q20,Q21
//
// Mmaps all lineitem columns. Provides:
//   - get_columns(ctx): lazy-init, returns mmap'd column pointers
//   - run_callback<Consumer>(ctx, consumer): iterate all rows

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo::shared::scan_lineitem_full {

struct Columns {
    size_t n_rows = 0;
    const int32_t*  l_orderkey      = nullptr;
    const int32_t*  l_partkey       = nullptr;
    const int32_t*  l_suppkey       = nullptr;
    const int32_t*  l_linenumber    = nullptr;
    const double*   l_quantity      = nullptr;
    const double*   l_extendedprice = nullptr;
    const double*   l_discount      = nullptr;
    const double*   l_tax           = nullptr;
    const int8_t*   l_returnflag    = nullptr;
    const int8_t*   l_linestatus    = nullptr;
    const int32_t*  l_shipdate      = nullptr;
    const int32_t*  l_commitdate    = nullptr;
    const int32_t*  l_receiptdate   = nullptr;
    const uint8_t*  l_shipinstruct  = nullptr;
    const uint8_t*  l_shipmode      = nullptr;
};

inline Columns& get_columns(const mqo::Context& ctx) {
    static Columns cols;
    static bool inited = false;
    if (!inited) {
        MQO_TIME_SHARED("scan_lineitem_full_init");
        const std::string b = ctx.gendb_dir + "/lineitem/";
        cols.n_rows = mqo::io::read_row_count(b + "meta.txt");
        const size_t n = cols.n_rows;
        cols.l_orderkey      = mqo::io::mmap_column<int32_t>(b + "l_orderkey.bin", n);
        cols.l_partkey       = mqo::io::mmap_column<int32_t>(b + "l_partkey.bin", n);
        cols.l_suppkey       = mqo::io::mmap_column<int32_t>(b + "l_suppkey.bin", n);
        cols.l_linenumber    = mqo::io::mmap_column<int32_t>(b + "l_linenumber.bin", n);
        cols.l_quantity      = mqo::io::mmap_column<double> (b + "l_quantity.bin", n);
        cols.l_extendedprice = mqo::io::mmap_column<double> (b + "l_extendedprice.bin", n);
        cols.l_discount      = mqo::io::mmap_column<double> (b + "l_discount.bin", n);
        cols.l_tax           = mqo::io::mmap_column<double> (b + "l_tax.bin", n);
        cols.l_returnflag    = mqo::io::mmap_column<int8_t> (b + "l_returnflag.bin", n);
        cols.l_linestatus    = mqo::io::mmap_column<int8_t> (b + "l_linestatus.bin", n);
        cols.l_shipdate      = mqo::io::mmap_column<int32_t>(b + "l_shipdate.bin", n);
        cols.l_commitdate    = mqo::io::mmap_column<int32_t>(b + "l_commitdate.bin", n);
        cols.l_receiptdate   = mqo::io::mmap_column<int32_t>(b + "l_receiptdate.bin", n);
        cols.l_shipinstruct  = mqo::io::mmap_column<uint8_t>(b + "l_shipinstruct.bin", n);
        cols.l_shipmode      = mqo::io::mmap_column<uint8_t>(b + "l_shipmode.bin", n);
        inited = true;
    }
    return cols;
}

/// Callback-fusion API: iterate all lineitem rows, calling
/// consumer(row_index, columns_ref) for each.
template <typename Consumer>
void run_callback(const mqo::Context& ctx, Consumer&& consumer) {
    MQO_TIME_SHARED("scan_lineitem_full");
    const Columns& cols = get_columns(ctx);
    const size_t n = cols.n_rows;
    for (size_t i = 0; i < n; ++i) {
        consumer(i, cols);
    }
}

}  // namespace mqo::shared::scan_lineitem_full
