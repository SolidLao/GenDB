// Q3: Shipping Priority — GenDB iteration 0
// Strategy: customer bitset → orders zone-map scan + HT build + bloom filter
//           → lineitem zone-map scan + bloom check + HT probe → agg → top-10
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <algorithm>
#include <vector>
#include <string>
#include <fstream>
#include <iostream>
#include <thread>
#include <omp.h>
#include <sys/types.h>
#include "date_utils.h"
#include "timing_utils.h"

// ---------------------------------------------------------------------------
// mmap helper — returns typed pointer + element count
// ---------------------------------------------------------------------------
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    n = st.st_size / sizeof(T);
    if (n == 0) { close(fd); return nullptr; }
    auto* p = reinterpret_cast<const T*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    return p;
}

// ---------------------------------------------------------------------------
// Zone map
// ---------------------------------------------------------------------------
struct ZoneBlock { int32_t mn, mx; uint32_t bsz; };

static std::vector<ZoneBlock> load_zonemap(const std::string& path) {
    std::vector<ZoneBlock> zones;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return zones;
    struct stat st; fstat(fd, &st);
    if (st.st_size < 4) { close(fd); return zones; }
    auto* data = reinterpret_cast<const uint8_t*>(
        mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);
    if (data == MAP_FAILED) return zones;
    uint32_t nb;
    memcpy(&nb, data, 4);
    zones.resize(nb);
    const uint8_t* p = data + 4;
    for (uint32_t b = 0; b < nb; b++) {
        memcpy(&zones[b].mn, p, 4); p += 4;
        memcpy(&zones[b].mx, p, 4); p += 4;
        memcpy(&zones[b].bsz, p, 4); p += 4;
    }
    munmap((void*)data, st.st_size);
    return zones;
}

// ---------------------------------------------------------------------------
// Open-addressing hash table: int32_t key → {orderdate, shippriority}
// Sentinel: key == 0 means empty (o_orderkey >= 1 in TPC-H)
// ---------------------------------------------------------------------------
struct OrdersHT {
    struct Slot { int32_t key, orderdate, shippriority, _pad; };
    Slot*    slots = nullptr;
    uint32_t mask  = 0;

    void init(uint32_t cap) {
        uint32_t sz = 1;
        while (sz < cap) sz <<= 1;
        mask  = sz - 1;
        slots = new Slot[sz]();   // zero-init → key=0 everywhere
    }
    void insert(int32_t key, int32_t date, int32_t prio) {
        uint32_t idx = ((uint32_t)key * 2654435761u) & mask;
        while (slots[idx].key != 0 && slots[idx].key != key)
            idx = (idx + 1) & mask;
        slots[idx] = {key, date, prio, 0};
    }
    const Slot* lookup(int32_t key) const {
        uint32_t idx = ((uint32_t)key * 2654435761u) & mask;
        while (slots[idx].key != 0) {
            if (slots[idx].key == key) return &slots[idx];
            idx = (idx + 1) & mask;
        }
        return nullptr;
    }
    ~OrdersHT() { delete[] slots; }
};

// ---------------------------------------------------------------------------
// Bloom filter: 32M bits (4 MB) — fits in L3, 4 hash functions
// FPR ≈ 0.06% for 1.35M elements (10 bits/element at 32M bits, optimal k≈4)
// ---------------------------------------------------------------------------
struct BloomFilter {
    static constexpr uint32_t NBITS = 1u << 25; // 32M bits = 4 MB
    static constexpr uint32_t NMASK = NBITS - 1;
    uint64_t* bits;

    BloomFilter()  { bits = new uint64_t[NBITS / 64](); }
    ~BloomFilter() { delete[] bits; }

    void add(uint32_t x) {
        uint32_t a = x * 2654435761u;
        uint32_t b = x * 2246822519u;
        uint32_t c = x * 3266489917u;
        uint32_t d = (a ^ b) * 2654435761u;
        bits[(a & NMASK) >> 6] |= 1ULL << (a & 63);
        bits[(b & NMASK) >> 6] |= 1ULL << (b & 63);
        bits[(c & NMASK) >> 6] |= 1ULL << (c & 63);
        bits[(d & NMASK) >> 6] |= 1ULL << (d & 63);
    }
    bool test(uint32_t x) const {
        uint32_t a = x * 2654435761u;
        uint32_t b = x * 2246822519u;
        uint32_t c = x * 3266489917u;
        uint32_t d = (a ^ b) * 2654435761u;
        return ((bits[(a & NMASK) >> 6] >> (a & 63)) & 1) &
               ((bits[(b & NMASK) >> 6] >> (b & 63)) & 1) &
               ((bits[(c & NMASK) >> 6] >> (c & 63)) & 1) &
               ((bits[(d & NMASK) >> 6] >> (d & 63)) & 1);
    }
};

// ---------------------------------------------------------------------------
// Aggregation hash table: int32_t orderkey → {revenue, orderdate, shippriority}
// ---------------------------------------------------------------------------
struct AggHT {
    struct Entry {
        int32_t key       = 0;
        int32_t orderdate = 0;
        int32_t shipprio  = 0;
        int32_t _pad      = 0;
        int64_t revenue   = 0;  // scaled x10000
    };
    Entry*   slots = nullptr;
    uint32_t mask  = 0;

    void init(uint32_t cap) {
        uint32_t sz = 1;
        while (sz < cap) sz <<= 1;
        mask  = sz - 1;
        slots = new Entry[sz]();
    }
    void update(int32_t key, int32_t date, int32_t prio, int64_t rev) {
        uint32_t idx = ((uint32_t)key * 2654435761u) & mask;
        while (slots[idx].key != 0 && slots[idx].key != key)
            idx = (idx + 1) & mask;
        if (slots[idx].key == 0)
            slots[idx] = {key, date, prio, 0, rev};
        else
            slots[idx].revenue += rev;
    }
    uint32_t size() const { return mask + 1; }
    ~AggHT() { delete[] slots; }
};

// ---------------------------------------------------------------------------
// Main query
// ---------------------------------------------------------------------------
void run_Q3(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // 1995-03-15 = epoch day 9204
    const int32_t CUTOFF = 9204;
    const int NT = std::min((int)std::thread::hardware_concurrency(), 64);

    // =====================================================================
    // Phase 1: Customer scan — build cust_valid bitset
    // =====================================================================
    std::vector<uint8_t> cust_valid(1500001, 0);
    {
        GENDB_PHASE("dim_filter");

        size_t n = 0;
        auto* ckeys = mmap_col<int32_t>(gendb_dir + "/customer/c_custkey.bin",    n);
        auto* csegs = mmap_col<int32_t>(gendb_dir + "/customer/c_mktsegment.bin", n);

        // Resolve 'BUILDING' to its dictionary code
        int32_t building_code = -1;
        {
            std::ifstream df(gendb_dir + "/customer/c_mktsegment_dict.txt");
            std::string word; int32_t code = 0;
            while (std::getline(df, word)) {
                if (word == "BUILDING") { building_code = code; break; }
                ++code;
            }
        }

        if (building_code >= 0) {
            // Sequential scan — 1.5M rows, fast enough; avoids false-sharing
            for (size_t i = 0; i < n; i++) {
                if (csegs[i] == building_code)
                    cust_valid[(uint32_t)ckeys[i]] = 1;
            }
        }
    }

    // =====================================================================
    // Phase 2: Orders scan + build join HT + bloom filter
    // =====================================================================
    OrdersHT  orders_ht;
    BloomFilter bloom;

    {
        GENDB_PHASE("build_joins");

        size_t n = 0;
        auto* o_okey   = mmap_col<int32_t>(gendb_dir + "/orders/o_orderkey.bin",    n);
        auto* o_ckey   = mmap_col<int32_t>(gendb_dir + "/orders/o_custkey.bin",     n);
        auto* o_odate  = mmap_col<int32_t>(gendb_dir + "/orders/o_orderdate.bin",   n);
        auto* o_shipri = mmap_col<int32_t>(gendb_dir + "/orders/o_shippriority.bin",n);
        size_t n_orders = n;

        // Load orders zone map
        auto ozones = load_zonemap(
            gendb_dir + "/indexes/orders_o_orderdate_zonemap.bin");

        // Pre-compute block start offsets
        std::vector<size_t> obstart;
        if (!ozones.empty()) {
            obstart.resize(ozones.size() + 1);
            obstart[0] = 0;
            for (size_t b = 0; b < ozones.size(); b++)
                obstart[b+1] = obstart[b] + ozones[b].bsz;
        }

        // Thread-local collection of qualifying orders
        struct ORow { int32_t okey, odate, prio; };
        std::vector<std::vector<ORow>> tlocal(NT);
        for (int t = 0; t < NT; t++) tlocal[t].reserve(32000);

        #pragma omp parallel num_threads(NT)
        {
            int tid = omp_get_thread_num();
            auto& loc = tlocal[tid];

            if (!ozones.empty()) {
                #pragma omp for schedule(dynamic, 2) nowait
                for (int b = 0; b < (int)ozones.size(); b++) {
                    int32_t mn = ozones[b].mn, mx = ozones[b].mx;
                    // Skip entire block if all dates >= CUTOFF
                    if (mn >= CUTOFF) continue;
                    size_t s = obstart[b], e = obstart[b+1];
                    bool all_ok = (mx < CUTOFF);
                    for (size_t i = s; i < e; i++) {
                        if ((all_ok || o_odate[i] < CUTOFF) &&
                            cust_valid[(uint32_t)o_ckey[i]])
                            loc.push_back({o_okey[i], o_odate[i], o_shipri[i]});
                    }
                }
            } else {
                // Fallback: no zone map
                #pragma omp for schedule(static) nowait
                for (size_t i = 0; i < n_orders; i++) {
                    if (o_odate[i] < CUTOFF && cust_valid[(uint32_t)o_ckey[i]])
                        loc.push_back({o_okey[i], o_odate[i], o_shipri[i]});
                }
            }
        }

        // Count qualifying rows, size HT
        size_t total_orders = 0;
        for (auto& v : tlocal) total_orders += v.size();

        // Build orders HT + bloom filter (single-threaded, ~1.35M inserts)
        orders_ht.init((uint32_t)(total_orders * 2 + 64));
        for (auto& v : tlocal) {
            for (auto& r : v) {
                orders_ht.insert(r.okey, r.odate, r.prio);
                bloom.add((uint32_t)r.okey);
            }
        }
    }

    // =====================================================================
    // Phase 3: Lineitem scan + join probe + aggregate
    // =====================================================================
    // Thread-local match buffers, then single-threaded final aggregation
    struct LiMatch { int32_t ok, odate, prio; int64_t rev; };
    std::vector<std::vector<LiMatch>> tmatches(NT);
    for (int t = 0; t < NT; t++) tmatches[t].reserve(100000);

    {
        GENDB_PHASE("main_scan");

        size_t n = 0;
        auto* l_okey   = mmap_col<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin",       n);
        auto* l_eprice = mmap_col<int64_t>(gendb_dir + "/lineitem/l_extendedprice.bin",   n);
        auto* l_disc   = mmap_col<int64_t>(gendb_dir + "/lineitem/l_discount.bin",        n);
        auto* l_sdate  = mmap_col<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin",        n);
        size_t n_li = n;

        // Load lineitem zone map
        auto lzones = load_zonemap(
            gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");

        std::vector<size_t> lbstart;
        if (!lzones.empty()) {
            lbstart.resize(lzones.size() + 1);
            lbstart[0] = 0;
            for (size_t b = 0; b < lzones.size(); b++)
                lbstart[b+1] = lbstart[b] + lzones[b].bsz;
        }

        #pragma omp parallel num_threads(NT)
        {
            int tid = omp_get_thread_num();
            auto& lm = tmatches[tid];

            if (!lzones.empty()) {
                #pragma omp for schedule(dynamic, 4) nowait
                for (int b = 0; b < (int)lzones.size(); b++) {
                    int32_t mn = lzones[b].mn, mx = lzones[b].mx;
                    // Skip block if all dates <= CUTOFF (need l_shipdate > CUTOFF)
                    if (mx <= CUTOFF) continue;
                    size_t s = lbstart[b], e = lbstart[b+1];
                    bool all_ok = (mn > CUTOFF);
                    for (size_t i = s; i < e; i++) {
                        if (!all_ok && l_sdate[i] <= CUTOFF) continue;
                        int32_t ok = l_okey[i];
                        if (!bloom.test((uint32_t)ok)) continue;
                        const auto* slot = orders_ht.lookup(ok);
                        if (!slot) continue;
                        // revenue scaled: l_extendedprice(x100) * (100 - l_discount(x100)) = x10000
                        int64_t rev = l_eprice[i] * (100LL - l_disc[i]);
                        lm.push_back({ok, slot->orderdate, slot->shippriority, rev});
                    }
                }
            } else {
                #pragma omp for schedule(static) nowait
                for (size_t i = 0; i < n_li; i++) {
                    if (l_sdate[i] <= CUTOFF) continue;
                    int32_t ok = l_okey[i];
                    if (!bloom.test((uint32_t)ok)) continue;
                    const auto* slot = orders_ht.lookup(ok);
                    if (!slot) continue;
                    int64_t rev = l_eprice[i] * (100LL - l_disc[i]);
                    lm.push_back({ok, slot->orderdate, slot->shippriority, rev});
                }
            }
        }
    }

    // =====================================================================
    // Phase 4: Merge aggregation + top-10 + output
    // =====================================================================
    {
        GENDB_PHASE("output");

        // Count total matches to size the aggregation HT
        size_t total_matches = 0;
        for (auto& v : tmatches) total_matches += v.size();

        // Size final agg HT: at least 2x total distinct groups (~1.35M)
        uint32_t agg_cap = (uint32_t)std::max(total_matches * 2 + 64, (size_t)4096);
        AggHT final_agg;
        final_agg.init(agg_cap);

        for (auto& v : tmatches)
            for (auto& m : v)
                final_agg.update(m.ok, m.odate, m.prio, m.rev);

        // Collect non-empty entries
        struct Result {
            int32_t l_orderkey;
            int64_t revenue;       // scaled x10000
            int32_t o_orderdate;
            int32_t o_shippriority;
        };
        std::vector<Result> results;
        results.reserve(1500000);
        for (uint32_t i = 0; i < final_agg.size(); i++) {
            auto& e = final_agg.slots[i];
            if (e.key != 0)
                results.push_back({e.key, e.revenue, e.orderdate, e.shipprio});
        }

        // Partial sort for top-10: revenue DESC, o_orderdate ASC
        auto cmp = [](const Result& a, const Result& b) {
            if (a.revenue != b.revenue) return a.revenue > b.revenue;
            return a.o_orderdate < b.o_orderdate;
        };
        if ((int)results.size() > 10) {
            std::partial_sort(results.begin(), results.begin() + 10,
                              results.end(), cmp);
            results.resize(10);
        } else {
            std::sort(results.begin(), results.end(), cmp);
        }

        // Ensure results directory exists
        {
            std::string cmd = "mkdir -p " + results_dir;
            system(cmd.c_str());
        }

        // Write CSV
        std::string out_path = results_dir + "/Q3.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); exit(1); }

        fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
        char date_buf[12];
        for (auto& r : results) {
            gendb::epoch_days_to_date_str(r.o_orderdate, date_buf);
            // Convert scaled revenue to double for formatted 2dp output
            double rev_out = (double)r.revenue / 10000.0;
            fprintf(f, "%d,%.2f,%s,%d\n",
                    r.l_orderkey, rev_out, date_buf, r.o_shippriority);
        }
        fclose(f);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0]
                  << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_Q3(gendb_dir, results_dir);
    return 0;
}
#endif
