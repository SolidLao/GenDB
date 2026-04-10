#pragma once

// Shared component: hash_part_by_partkey
// Kind: hash_build  |  Fusion mode: materialize
// Build column: p_partkey (dense_pk, max_key=2000000 => O(1) array lookup)
// Consumers: Q2, Q8, Q9, Q14, Q16, Q17, Q19, Q20
// Cardinality: 2,000,000 rows

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo { namespace shared { namespace hash_part_by_partkey {

struct Output {
    size_t          n_rows     = 0;      // 2,000,000
    int32_t         max_key    = 0;      // 2,000,000
    const int32_t*  pk_index   = nullptr; // pk_index[partkey] = row_id (-1 if absent)

    // Payload columns (indexed by row_id 0..n_rows-1):
    const int32_t*  p_partkey     = nullptr;
    const int32_t*  p_size        = nullptr;
    const double*   p_retailprice = nullptr;
    const uint8_t*  p_mfgr        = nullptr;  // dictionary-encoded
    const uint8_t*  p_brand       = nullptr;  // dictionary-encoded
    const uint8_t*  p_type        = nullptr;  // dictionary-encoded
    const uint8_t*  p_container   = nullptr;  // dictionary-encoded
    mqo::io::VarlenColumn p_name;
};

inline Output build(const mqo::Context& ctx) {
    MQO_TIME_SHARED("hash_part_by_partkey");
    std::string tbl = ctx.gendb_dir + "/part";
    std::string idx = ctx.gendb_dir + "/indexes";
    size_t n = mqo::io::read_row_count(tbl);

    Output out;
    out.n_rows       = n;
    out.max_key      = 2000000;
    out.pk_index     = mqo::io::mmap_column<int32_t>(idx + "/part_pk_index.bin",
                                                      static_cast<size_t>(out.max_key) + 1);
    out.p_partkey     = mqo::io::mmap_column<int32_t>(tbl + "/p_partkey.bin", n);
    out.p_size        = mqo::io::mmap_column<int32_t>(tbl + "/p_size.bin", n);
    out.p_retailprice = mqo::io::mmap_column<double> (tbl + "/p_retailprice.bin", n);
    out.p_mfgr        = mqo::io::mmap_column<uint8_t>(tbl + "/p_mfgr.bin", n);
    out.p_brand       = mqo::io::mmap_column<uint8_t>(tbl + "/p_brand.bin", n);
    out.p_type        = mqo::io::mmap_column<uint8_t>(tbl + "/p_type.bin", n);
    out.p_container   = mqo::io::mmap_column<uint8_t>(tbl + "/p_container.bin", n);
    out.p_name        = mqo::io::mmap_varlen(tbl + "/p_name", n);
    return out;
}

// Getter — populated by the dispatcher; defined in mqo_main.cpp.
const Output& get();

}}} // namespace mqo::shared::hash_part_by_partkey
