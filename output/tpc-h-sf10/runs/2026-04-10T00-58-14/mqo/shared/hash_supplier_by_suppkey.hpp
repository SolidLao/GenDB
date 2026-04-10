#pragma once
// Shared component: hash_supplier_by_suppkey
// Kind: hash_build   Mode: materialize
// Key: s_suppkey (max 100 000, dense)   Rows: 100 000
// Consumers: Q2,Q5,Q7,Q8,Q9,Q11,Q15,Q21
//
// Direct-indexed array (~2.4 MB, fits in L3 cache).
// Includes VarlenAccessors for s_name, s_address, s_phone, s_comment.

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo::shared::hash_supplier_by_suppkey {

static constexpr int32_t MAX_KEY = 100000;

struct Entry {
    double  s_acctbal;       // 8B  offset 0
    int32_t s_suppkey;       // 4B  offset 8   (0 = empty)
    int32_t s_nationkey;     // 4B  offset 12
    int32_t row_id;          // 4B  offset 16  (for varlen access)
    int32_t _pad;            // 4B  offset 20
    // sizeof = 24
};

struct Output {
    Entry*  entries   = nullptr;
    size_t  n_entries = 0;
    mqo::io::VarlenAccessor va_name;
    mqo::io::VarlenAccessor va_address;
    mqo::io::VarlenAccessor va_phone;
    mqo::io::VarlenAccessor va_comment;

    const Entry* probe(int32_t key) const {
        if (key < 1 || key > MAX_KEY) return nullptr;
        const Entry& e = entries[key];
        return e.s_suppkey != 0 ? &e : nullptr;
    }

    std::string_view get_name(int32_t rid)    const { return va_name.get(rid); }
    std::string_view get_address(int32_t rid) const { return va_address.get(rid); }
    std::string_view get_phone(int32_t rid)   const { return va_phone.get(rid); }
    std::string_view get_comment(int32_t rid) const { return va_comment.get(rid); }
};

inline Output& get() {
    static Output out;
    return out;
}

inline void build(const mqo::Context& ctx) {
    MQO_TIME_SHARED("hash_supplier_by_suppkey");
    const std::string b = ctx.gendb_dir + "/supplier/";
    const size_t n = mqo::io::read_row_count(b + "meta.txt");

    const int32_t* sk = mqo::io::mmap_column<int32_t>(b + "s_suppkey.bin", n);
    const int32_t* nk = mqo::io::mmap_column<int32_t>(b + "s_nationkey.bin", n);
    const double*  ab = mqo::io::mmap_column<double> (b + "s_acctbal.bin", n);

    Entry* entries = static_cast<Entry*>(
        std::calloc(static_cast<size_t>(MAX_KEY) + 1, sizeof(Entry)));
    if (!entries) {
        std::fprintf(stderr, "[MQO] OOM: supplier direct array\n");
        std::exit(1);
    }

    for (size_t i = 0; i < n; ++i) {
        const int32_t key = sk[i];
        Entry& e       = entries[key];
        e.s_suppkey    = key;
        e.s_nationkey  = nk[i];
        e.s_acctbal    = ab[i];
        e.row_id       = static_cast<int32_t>(i);
    }

    Output& out    = get();
    out.entries    = entries;
    out.n_entries  = n;
    out.va_name    = mqo::io::mmap_varlen(b, "s_name");
    out.va_address = mqo::io::mmap_varlen(b, "s_address");
    out.va_phone   = mqo::io::mmap_varlen(b, "s_phone");
    out.va_comment = mqo::io::mmap_varlen(b, "s_comment");
}

}  // namespace mqo::shared::hash_supplier_by_suppkey
