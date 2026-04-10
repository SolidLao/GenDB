#pragma once

// Shared component: hash_customer_by_custkey
// Kind: hash_build  |  Fusion mode: materialize
// Build column: c_custkey (dense_pk, max_key=1500000 => O(1) array lookup)
// Consumers: Q3, Q5, Q7, Q8, Q10, Q13, Q18, Q22
// Cardinality: 1,500,000 rows

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo { namespace shared { namespace hash_customer_by_custkey {

struct Output {
    size_t          n_rows     = 0;      // 1,500,000
    int32_t         max_key    = 0;      // 1,500,000
    const int32_t*  pk_index   = nullptr; // pk_index[custkey] = row_id (-1 if absent)

    // Payload columns (indexed by row_id 0..n_rows-1):
    const int32_t*  c_custkey    = nullptr;
    const int32_t*  c_nationkey  = nullptr;
    const double*   c_acctbal    = nullptr;
    const uint8_t*  c_mktsegment = nullptr;  // dictionary-encoded
    mqo::io::VarlenColumn c_name;
    mqo::io::VarlenColumn c_address;
    mqo::io::VarlenColumn c_phone;
    mqo::io::VarlenColumn c_comment;
};

inline Output build(const mqo::Context& ctx) {
    MQO_TIME_SHARED("hash_customer_by_custkey");
    std::string tbl = ctx.gendb_dir + "/customer";
    std::string idx = ctx.gendb_dir + "/indexes";
    size_t n = mqo::io::read_row_count(tbl);

    Output out;
    out.n_rows     = n;
    out.max_key    = 1500000;
    out.pk_index   = mqo::io::mmap_column<int32_t>(idx + "/customer_pk_index.bin",
                                                    static_cast<size_t>(out.max_key) + 1);
    out.c_custkey    = mqo::io::mmap_column<int32_t>(tbl + "/c_custkey.bin", n);
    out.c_nationkey  = mqo::io::mmap_column<int32_t>(tbl + "/c_nationkey.bin", n);
    out.c_acctbal    = mqo::io::mmap_column<double> (tbl + "/c_acctbal.bin", n);
    out.c_mktsegment = mqo::io::mmap_column<uint8_t>(tbl + "/c_mktsegment.bin", n);
    out.c_name       = mqo::io::mmap_varlen(tbl + "/c_name", n);
    out.c_address    = mqo::io::mmap_varlen(tbl + "/c_address", n);
    out.c_phone      = mqo::io::mmap_varlen(tbl + "/c_phone", n);
    out.c_comment    = mqo::io::mmap_varlen(tbl + "/c_comment", n);
    return out;
}

// Getter — populated by the dispatcher; defined in mqo_main.cpp.
const Output& get();

}}} // namespace mqo::shared::hash_customer_by_custkey
