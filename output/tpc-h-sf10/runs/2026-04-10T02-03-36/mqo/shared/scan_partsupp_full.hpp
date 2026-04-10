#pragma once

// Shared component: scan_partsupp_full
// Kind: filtered_scan  |  Fusion mode: materialize
// Consumers: Q2, Q9, Q11, Q16, Q20
// Cardinality: 8,000,000 rows (full partsupp table)

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo { namespace shared { namespace scan_partsupp_full {

struct Output {
    size_t          n_rows        = 0;
    const int32_t*  ps_partkey    = nullptr;
    const int32_t*  ps_suppkey    = nullptr;
    const int32_t*  ps_availqty   = nullptr;
    const double*   ps_supplycost = nullptr;
};

inline Output build(const mqo::Context& ctx) {
    MQO_TIME_SHARED("scan_partsupp_full");
    std::string dir = ctx.gendb_dir + "/partsupp";
    size_t n = mqo::io::read_row_count(dir);

    Output out;
    out.n_rows        = n;
    out.ps_partkey    = mqo::io::mmap_column<int32_t>(dir + "/ps_partkey.bin", n);
    out.ps_suppkey    = mqo::io::mmap_column<int32_t>(dir + "/ps_suppkey.bin", n);
    out.ps_availqty   = mqo::io::mmap_column<int32_t>(dir + "/ps_availqty.bin", n);
    out.ps_supplycost = mqo::io::mmap_column<double> (dir + "/ps_supplycost.bin", n);
    return out;
}

// Getter — populated by the dispatcher; defined in mqo_main.cpp.
const Output& get();

}}} // namespace mqo::shared::scan_partsupp_full
