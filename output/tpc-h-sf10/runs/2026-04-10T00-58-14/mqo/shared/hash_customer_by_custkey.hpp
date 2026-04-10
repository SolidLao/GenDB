#pragma once
// Shared component: hash_customer_by_custkey
// Kind: hash_build   Mode: materialize
// Key: c_custkey (max 1 500 000, dense)   Rows: 1 500 000
// Consumers: Q3,Q5,Q7,Q8,Q10,Q13,Q18
//
// Direct-indexed array (~36 MB). All custkeys 1..1500000 present.
// Includes VarlenAccessors for c_name, c_address, c_phone, c_comment
// so tails can look up varlen fields by row_id.

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo::shared::hash_customer_by_custkey {

static constexpr int32_t MAX_KEY = 1500000;

struct Entry {
    double  c_acctbal;       // 8B  offset 0
    int32_t c_custkey;       // 4B  offset 8   (0 = empty)
    int32_t c_nationkey;     // 4B  offset 12
    int32_t row_id;          // 4B  offset 16  (for varlen access)
    uint8_t c_mktsegment;   // 1B  offset 20  (dict code)
    uint8_t _pad[3];        // 3B  offset 21
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
        return e.c_custkey != 0 ? &e : nullptr;
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
    MQO_TIME_SHARED("hash_customer_by_custkey");
    const std::string b = ctx.gendb_dir + "/customer/";
    const size_t n = mqo::io::read_row_count(b + "meta.txt");

    const int32_t* ck = mqo::io::mmap_column<int32_t>(b + "c_custkey.bin", n);
    const int32_t* nk = mqo::io::mmap_column<int32_t>(b + "c_nationkey.bin", n);
    const double*  ab = mqo::io::mmap_column<double> (b + "c_acctbal.bin", n);
    const uint8_t* ms = mqo::io::mmap_column<uint8_t>(b + "c_mktsegment.bin", n);

    Entry* entries = static_cast<Entry*>(
        std::calloc(static_cast<size_t>(MAX_KEY) + 1, sizeof(Entry)));
    if (!entries) {
        std::fprintf(stderr, "[MQO] OOM: customer direct array\n");
        std::exit(1);
    }

    for (size_t i = 0; i < n; ++i) {
        const int32_t key = ck[i];
        Entry& e       = entries[key];
        e.c_custkey    = key;
        e.c_nationkey  = nk[i];
        e.c_acctbal    = ab[i];
        e.c_mktsegment = ms[i];
        e.row_id       = static_cast<int32_t>(i);
    }

    Output& out    = get();
    out.entries    = entries;
    out.n_entries  = n;
    out.va_name    = mqo::io::mmap_varlen(b, "c_name");
    out.va_address = mqo::io::mmap_varlen(b, "c_address");
    out.va_phone   = mqo::io::mmap_varlen(b, "c_phone");
    out.va_comment = mqo::io::mmap_varlen(b, "c_comment");
}

}  // namespace mqo::shared::hash_customer_by_custkey
