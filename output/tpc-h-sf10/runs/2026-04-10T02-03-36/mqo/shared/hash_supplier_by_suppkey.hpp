#pragma once

// Shared component: hash_supplier_by_suppkey
// Kind: hash_build  |  Fusion mode: materialize
// Build column: s_suppkey (dense_pk, max_key=100000 => O(1) array lookup)
// Consumers: Q2, Q5, Q7, Q8, Q9, Q11, Q15, Q16, Q20, Q21
// Cardinality: 100,000 rows

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo { namespace shared { namespace hash_supplier_by_suppkey {

struct Output {
    size_t          n_rows     = 0;      // 100,000
    int32_t         max_key    = 0;      // 100,000
    const int32_t*  pk_index   = nullptr; // pk_index[suppkey] = row_id (-1 if absent)

    // Payload columns (indexed by row_id 0..n_rows-1):
    const int32_t*  s_suppkey   = nullptr;
    const int32_t*  s_nationkey = nullptr;
    const double*   s_acctbal   = nullptr;
    mqo::io::VarlenColumn s_name;
    mqo::io::VarlenColumn s_address;
    mqo::io::VarlenColumn s_phone;
    mqo::io::VarlenColumn s_comment;
};

inline Output build(const mqo::Context& ctx) {
    MQO_TIME_SHARED("hash_supplier_by_suppkey");
    std::string tbl = ctx.gendb_dir + "/supplier";
    std::string idx = ctx.gendb_dir + "/indexes";
    size_t n = mqo::io::read_row_count(tbl);

    Output out;
    out.n_rows      = n;
    out.max_key     = 100000;
    out.pk_index    = mqo::io::mmap_column<int32_t>(idx + "/supplier_pk_index.bin",
                                                     static_cast<size_t>(out.max_key) + 1);
    out.s_suppkey   = mqo::io::mmap_column<int32_t>(tbl + "/s_suppkey.bin", n);
    out.s_nationkey = mqo::io::mmap_column<int32_t>(tbl + "/s_nationkey.bin", n);
    out.s_acctbal   = mqo::io::mmap_column<double> (tbl + "/s_acctbal.bin", n);
    out.s_name      = mqo::io::mmap_varlen(tbl + "/s_name", n);
    out.s_address   = mqo::io::mmap_varlen(tbl + "/s_address", n);
    out.s_phone     = mqo::io::mmap_varlen(tbl + "/s_phone", n);
    out.s_comment   = mqo::io::mmap_varlen(tbl + "/s_comment", n);
    return out;
}

// Getter — populated by the dispatcher; defined in mqo_main.cpp.
const Output& get();

}}} // namespace mqo::shared::hash_supplier_by_suppkey
