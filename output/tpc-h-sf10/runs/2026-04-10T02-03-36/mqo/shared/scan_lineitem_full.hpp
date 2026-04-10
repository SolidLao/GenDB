#pragma once

// Shared component: scan_lineitem_full
// Kind: filtered_scan  |  Fusion mode: callback
// Consumers: Q1, Q3, Q5, Q6, Q7, Q8, Q10, Q12, Q14, Q15, Q18
// Cardinality: 59,986,052 rows (full lineitem table)

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo { namespace shared { namespace scan_lineitem_full {

// Column-oriented output schema (all columns mmap'd; pointers valid until
// process exit).  Consumer iterates [0, n_rows) applying residual filters.
struct Columns {
    size_t          n_rows           = 0;
    const int32_t*  l_orderkey       = nullptr;
    const int32_t*  l_partkey        = nullptr;
    const int32_t*  l_suppkey        = nullptr;
    const double*   l_quantity       = nullptr;
    const double*   l_extendedprice  = nullptr;
    const double*   l_discount       = nullptr;
    const double*   l_tax            = nullptr;
    const int8_t*   l_returnflag     = nullptr;   // raw char stored as int8
    const int8_t*   l_linestatus     = nullptr;   // raw char stored as int8
    const int32_t*  l_shipdate       = nullptr;   // days_since_epoch_1970
    const int32_t*  l_commitdate     = nullptr;   // days_since_epoch_1970
    const int32_t*  l_receiptdate    = nullptr;   // days_since_epoch_1970
    const uint8_t*  l_shipmode       = nullptr;   // dictionary-encoded
};

// Callback-fusion API: mmaps all lineitem columns, passes Columns struct to
// consumer, then returns.  Mmap'd pages remain valid (no munmap).
template <typename Consumer>
void run_callback(const mqo::Context& ctx, Consumer&& consumer) {
    MQO_TIME_SHARED("scan_lineitem_full");
    std::string dir = ctx.gendb_dir + "/lineitem";
    size_t n = mqo::io::read_row_count(dir);

    Columns cols;
    cols.n_rows          = n;
    cols.l_orderkey      = mqo::io::mmap_column<int32_t>(dir + "/l_orderkey.bin", n);
    cols.l_partkey       = mqo::io::mmap_column<int32_t>(dir + "/l_partkey.bin", n);
    cols.l_suppkey       = mqo::io::mmap_column<int32_t>(dir + "/l_suppkey.bin", n);
    cols.l_quantity      = mqo::io::mmap_column<double> (dir + "/l_quantity.bin", n);
    cols.l_extendedprice = mqo::io::mmap_column<double> (dir + "/l_extendedprice.bin", n);
    cols.l_discount      = mqo::io::mmap_column<double> (dir + "/l_discount.bin", n);
    cols.l_tax           = mqo::io::mmap_column<double> (dir + "/l_tax.bin", n);
    cols.l_returnflag    = mqo::io::mmap_column<int8_t> (dir + "/l_returnflag.bin", n);
    cols.l_linestatus    = mqo::io::mmap_column<int8_t> (dir + "/l_linestatus.bin", n);
    cols.l_shipdate      = mqo::io::mmap_column<int32_t>(dir + "/l_shipdate.bin", n);
    cols.l_commitdate    = mqo::io::mmap_column<int32_t>(dir + "/l_commitdate.bin", n);
    cols.l_receiptdate   = mqo::io::mmap_column<int32_t>(dir + "/l_receiptdate.bin", n);
    cols.l_shipmode      = mqo::io::mmap_column<uint8_t>(dir + "/l_shipmode.bin", n);

    consumer(cols);
}

// Getter — populated by the dispatcher after run_callback; defined in mqo_main.cpp.
const Columns& get();

}}} // namespace mqo::shared::scan_lineitem_full
