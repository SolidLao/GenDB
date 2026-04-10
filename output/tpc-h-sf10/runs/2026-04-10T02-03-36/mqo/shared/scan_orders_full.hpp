#pragma once

// Shared component: scan_orders_full
// Kind: filtered_scan  |  Fusion mode: materialize
// Consumers: Q3, Q4, Q5, Q7, Q8, Q9, Q10, Q12, Q13, Q18, Q21, Q22
// Cardinality: 15,000,000 rows (full orders table)

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo { namespace shared { namespace scan_orders_full {

struct Output {
    size_t          n_rows          = 0;
    const int32_t*  o_orderkey      = nullptr;
    const int32_t*  o_custkey       = nullptr;
    const int8_t*   o_orderstatus   = nullptr;   // raw char stored as int8
    const double*   o_totalprice    = nullptr;
    const int32_t*  o_orderdate     = nullptr;   // days_since_epoch_1970
    const uint8_t*  o_orderpriority = nullptr;   // dictionary-encoded
    const int32_t*  o_shippriority  = nullptr;
    mqo::io::VarlenColumn o_comment;             // offsets + data pool
};

inline Output build(const mqo::Context& ctx) {
    MQO_TIME_SHARED("scan_orders_full");
    std::string dir = ctx.gendb_dir + "/orders";
    size_t n = mqo::io::read_row_count(dir);

    Output out;
    out.n_rows          = n;
    out.o_orderkey      = mqo::io::mmap_column<int32_t>(dir + "/o_orderkey.bin", n);
    out.o_custkey       = mqo::io::mmap_column<int32_t>(dir + "/o_custkey.bin", n);
    out.o_orderstatus   = mqo::io::mmap_column<int8_t> (dir + "/o_orderstatus.bin", n);
    out.o_totalprice    = mqo::io::mmap_column<double> (dir + "/o_totalprice.bin", n);
    out.o_orderdate     = mqo::io::mmap_column<int32_t>(dir + "/o_orderdate.bin", n);
    out.o_orderpriority = mqo::io::mmap_column<uint8_t>(dir + "/o_orderpriority.bin", n);
    out.o_shippriority  = mqo::io::mmap_column<int32_t>(dir + "/o_shippriority.bin", n);
    out.o_comment       = mqo::io::mmap_varlen(dir + "/o_comment", n);
    return out;
}

// Getter — populated by the dispatcher; defined in mqo_main.cpp.
const Output& get();

}}} // namespace mqo::shared::scan_orders_full
