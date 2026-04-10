#pragma once
// Shared component: hash_orders_by_orderkey
// Kind: hash_build   Mode: materialize
// Key: o_orderkey (max 60 000 000)   Rows: 15 000 000
// Consumers: Q3,Q5,Q7,Q8,Q9,Q10,Q12,Q18,Q21
//
// Uses a direct-indexed array (key -> entry) for O(1) lookup.
// lineitem is sorted by l_orderkey, so probes are cache-sequential.
// Total memory: ~1.92 GB (60M entries x 32B).

#include "mqo_profile.hpp"
#include "shared/mqo_io.hpp"

namespace mqo::shared::hash_orders_by_orderkey {

static constexpr int32_t MAX_KEY = 60000000;

struct Entry {
    double  o_totalprice;     // 8B  offset 0
    int32_t o_orderkey;       // 4B  offset 8   (0 = empty slot)
    int32_t o_custkey;        // 4B  offset 12
    int32_t o_orderdate;      // 4B  offset 16
    int32_t o_shippriority;   // 4B  offset 20
    int8_t  o_orderstatus;    // 1B  offset 24
    uint8_t o_orderpriority;  // 1B  offset 25
    int16_t _pad;             // 2B  offset 26
    // sizeof = 32  (28 + 4 trailing pad for alignof(double)=8)
};

struct Output {
    Entry*  entries   = nullptr;
    size_t  n_entries = 0;    // populated count

    const Entry* probe(int32_t key) const {
        if (key < 1 || key > MAX_KEY) return nullptr;
        const Entry& e = entries[key];
        return e.o_orderkey != 0 ? &e : nullptr;
    }
};

inline Output& get() {
    static Output out;
    return out;
}

inline void build(const mqo::Context& ctx) {
    MQO_TIME_SHARED("hash_orders_by_orderkey");
    const std::string b = ctx.gendb_dir + "/orders/";
    const size_t n = mqo::io::read_row_count(b + "meta.txt");

    const int32_t* ok = mqo::io::mmap_column<int32_t>(b + "o_orderkey.bin", n);
    const int32_t* ck = mqo::io::mmap_column<int32_t>(b + "o_custkey.bin", n);
    const int32_t* od = mqo::io::mmap_column<int32_t>(b + "o_orderdate.bin", n);
    const int8_t*  os = mqo::io::mmap_column<int8_t> (b + "o_orderstatus.bin", n);
    const double*  tp = mqo::io::mmap_column<double> (b + "o_totalprice.bin", n);
    const uint8_t* op = mqo::io::mmap_column<uint8_t>(b + "o_orderpriority.bin", n);
    const int32_t* sp = mqo::io::mmap_column<int32_t>(b + "o_shippriority.bin", n);

    Entry* entries = static_cast<Entry*>(
        std::calloc(static_cast<size_t>(MAX_KEY) + 1, sizeof(Entry)));
    if (!entries) {
        std::fprintf(stderr, "[MQO] OOM: orders direct array (%zu B)\n",
                     (static_cast<size_t>(MAX_KEY) + 1) * sizeof(Entry));
        std::exit(1);
    }

    for (size_t i = 0; i < n; ++i) {
        const int32_t key = ok[i];
        Entry& e          = entries[key];
        e.o_orderkey      = key;
        e.o_custkey       = ck[i];
        e.o_orderdate     = od[i];
        e.o_orderstatus   = os[i];
        e.o_totalprice    = tp[i];
        e.o_orderpriority = op[i];
        e.o_shippriority  = sp[i];
    }

    Output& out  = get();
    out.entries   = entries;
    out.n_entries = n;
}

}  // namespace mqo::shared::hash_orders_by_orderkey
