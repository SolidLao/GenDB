// Q18: Large Volume Customer
// Strategy: Parallel subquery agg on lineitem → qualifying_orderkeys hash set
//           Index probes on orders/customer/lineitem → assemble top-100

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>
#include <omp.h>

#include "date_utils.h"
#include "timing_utils.h"

// ---- Constants & Helpers --------------------------------------------------------

static constexpr int32_t SENTINEL = INT32_MIN;

inline uint32_t hash32(int32_t key, uint32_t mask) {
    return (uint32_t)((uint64_t)(uint32_t)key * 0x9E3779B97F4A7C15ULL >> 32) & mask;
}

// Interleaved aggregation slot: key + val in the same 64-byte cache line
// → single DRAM access covers both fields (eliminates the 2nd cache miss vs. separate arrays)
struct RawAggSlot {
    int32_t key;
    int32_t _pad;  // alignment padding so val is 8-byte aligned
    int64_t val;   // accumulate SUM(l_quantity) as int64_t (l_quantity is integer-valued 1-50)
};
static_assert(sizeof(RawAggSlot) == 16, "RawAggSlot must be 16 bytes");
static_assert(offsetof(RawAggSlot, val) == 8, "val must be at offset 8");

struct AtomAggSlot {
    std::atomic<int32_t> key;
    int32_t _pad;
    std::atomic<int64_t> val;
};
static_assert(sizeof(AtomAggSlot) == 16, "AtomAggSlot must be 16 bytes");

// ---- mmap Column File -----------------------------------------------------------

template<typename T>
struct ColFile {
    const T* data = nullptr;
    size_t count = 0;
    size_t bytes = 0;
    int fd = -1;

    // populate=true: MAP_POPULATE (eager fault-in, good for fully-scanned columns)
    // populate=false: demand paging only (better for sparsely-accessed files in hot runs)
    void open(const std::string& path, bool populate = true, bool sequential = true) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        bytes = st.st_size;
        count = bytes / sizeof(T);
        int mflags = MAP_PRIVATE;
        if (populate) mflags |= MAP_POPULATE;
        data = reinterpret_cast<const T*>(
            mmap(nullptr, bytes, PROT_READ, mflags, fd, 0));
        if (data == MAP_FAILED) { perror("mmap"); exit(1); }
        posix_fadvise(fd, 0, bytes,
            sequential ? POSIX_FADV_SEQUENTIAL : POSIX_FADV_RANDOM);
    }

    ~ColFile() {
        if (data && data != MAP_FAILED) munmap(const_cast<T*>(data), bytes);
        if (fd >= 0) ::close(fd);
    }
};

// ---- Pre-Built Hash Index -------------------------------------------------------

struct HtSlot { int32_t key; uint32_t offset; uint32_t count; };  // 12 bytes

struct IndexFile {
    const char* raw = nullptr;
    size_t bytes = 0;
    int fd = -1;
    uint32_t ht_size = 0;
    const HtSlot* ht = nullptr;
    const uint32_t* positions = nullptr;

    // Index files are probed for ~2400 keys — don't eagerly fault all pages (no MAP_POPULATE)
    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        bytes = st.st_size;
        raw = reinterpret_cast<const char*>(
            mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE, fd, 0));
        if (raw == MAP_FAILED) { perror("mmap index"); exit(1); }
        posix_fadvise(fd, 0, bytes, POSIX_FADV_RANDOM);
        ht_size = *reinterpret_cast<const uint32_t*>(raw);
        // raw[4] = num_positions (unused directly)
        ht = reinterpret_cast<const HtSlot*>(raw + 8);
        positions = reinterpret_cast<const uint32_t*>(raw + 8 + (size_t)ht_size * 12);
    }

    // Returns true if found; sets out_offset, out_count
    bool probe(int32_t key, uint32_t& out_offset, uint32_t& out_count) const {
        uint32_t mask = ht_size - 1;
        uint32_t h = hash32(key, mask);
        for (uint32_t p = 0; p < ht_size; p++) {
            uint32_t idx = (h + p) & mask;
            if (ht[idx].key == key) {
                out_offset = ht[idx].offset;
                out_count  = ht[idx].count;
                return true;
            }
            if (ht[idx].key == SENTINEL) return false;
        }
        return false;
    }

    ~IndexFile() {
        if (raw && raw != MAP_FAILED) munmap(const_cast<char*>(raw), bytes);
        if (fd >= 0) ::close(fd);
    }
};

// ---- Qualifying Orderkeys Hash Set ----------------------------------------------
// next_power_of_2(2400 * 2) = 8192; load factor ~29%

struct QualSet {
    static constexpr uint32_t CAP = 8192;
    static constexpr uint32_t MASK = CAP - 1;
    int32_t slots[CAP];

    QualSet() { std::fill(slots, slots + CAP, SENTINEL); }

    void insert(int32_t key) {
        uint32_t h = hash32(key, MASK);
        for (uint32_t p = 0; p < CAP; p++) {
            uint32_t idx = (h + p) & MASK;
            if (slots[idx] == SENTINEL || slots[idx] == key) {
                slots[idx] = key;
                return;
            }
        }
        // Should never overflow with adequate capacity
    }

    bool contains(int32_t key) const {
        uint32_t h = hash32(key, MASK);
        for (uint32_t p = 0; p < CAP; p++) {
            uint32_t idx = (h + p) & MASK;
            if (slots[idx] == key) return true;
            if (slots[idx] == SENTINEL) return false;
        }
        return false;
    }
};

// ---- Result Row -----------------------------------------------------------------

struct ResultRow {
    std::string c_name;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double o_totalprice;
    double sum_qty;
};

// ---- Main Query Function --------------------------------------------------------

void run_q18(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();  // C1/C11: required before any date decode

    // ---- Data Loading -----------------------------------------------------------
    ColFile<int32_t> l_orderkey_f, o_custkey_f, o_orderdate_f, c_custkey_f;
    ColFile<double>  l_quantity_f, o_totalprice_f;
    ColFile<char>    c_name_f;
    IndexFile orders_idx, customer_idx, lineitem_idx;

    {
        GENDB_PHASE("data_loading");
        // Fully-scanned columns: MAP_POPULATE + SEQUENTIAL prefetch
        l_orderkey_f.open(gendb_dir + "/lineitem/l_orderkey.bin", /*populate=*/true,  /*seq=*/true);
        l_quantity_f.open(gendb_dir + "/lineitem/l_quantity.bin",  /*populate=*/true,  /*seq=*/true);
        // Sparsely-accessed (~2400 rows): no MAP_POPULATE, RANDOM fadvise avoids read-ahead waste
        o_custkey_f.open(gendb_dir + "/orders/o_custkey.bin",      /*populate=*/false, /*seq=*/false);
        o_orderdate_f.open(gendb_dir + "/orders/o_orderdate.bin",  /*populate=*/false, /*seq=*/false);
        o_totalprice_f.open(gendb_dir + "/orders/o_totalprice.bin",/*populate=*/false, /*seq=*/false);
        c_custkey_f.open(gendb_dir + "/customer/c_custkey.bin",    /*populate=*/false, /*seq=*/false);
        c_name_f.open(gendb_dir + "/customer/c_name.bin",          /*populate=*/false, /*seq=*/false);
        // Index files: no MAP_POPULATE (probed for ~2400 keys, open() uses FADV_RANDOM)
        orders_idx.open(gendb_dir + "/indexes/orders_orderkey_hash.bin");
        customer_idx.open(gendb_dir + "/indexes/customer_custkey_hash.bin");
        lineitem_idx.open(gendb_dir + "/indexes/lineitem_orderkey_hash.bin");
    }

    const size_t L = l_orderkey_f.count;
    const int32_t* l_orderkey = l_orderkey_f.data;
    const double*  l_quantity  = l_quantity_f.data;

    // ---- Build Joins: Parallel Subquery Aggregation + Extract Qualifying Keys ---
    // Shared open-addressing hash map: capacity=33554432 (next_power_of_2(15M*2))
    // Keys: atomic<int32_t> sentinel=INT32_MIN; Vals: atomic<uint64_t> (double bits)

    QualSet qual_set;

    {
        GENDB_PHASE("build_joins");

        constexpr uint32_t AGG_CAP  = 33554432;  // next_power_of_2(15M * 2)
        constexpr uint32_t AGG_MASK = AGG_CAP - 1;

        // Interleaved struct: key + val in same 16-byte slot → same 64-byte cache line.
        // Eliminates the 2nd DRAM miss vs. separate key/val arrays (each probe was 2 misses).
        // C20: std::fill / loop for INT32_MIN sentinel (never memset for multi-byte values)
        RawAggSlot* raw_slots = new RawAggSlot[AGG_CAP];

        // Parallel init (C20: loop, not memset, for SENTINEL)
        #pragma omp parallel for schedule(static) num_threads(64)
        for (uint32_t i = 0; i < AGG_CAP; i++) {
            raw_slots[i].key = SENTINEL;
            raw_slots[i]._pad = 0;
            raw_slots[i].val = 0LL;
        }

        // Reinterpret for lock-free access (same layout, same size)
        auto* agg = reinterpret_cast<AtomAggSlot*>(raw_slots);

        // Parallel scan lineitem: aggregate SUM(l_quantity) per l_orderkey.
        // int64_t fetch_add → single 'lock xadd' instruction (no CAS retry loop).
        // l_quantity values are integer-valued (1.0–50.0) → safe int64_t cast.
        // C24: bounded probing (for-loop, not while)
        #pragma omp parallel for schedule(static) num_threads(64)
        for (size_t i = 0; i < L; i++) {
            int32_t key = l_orderkey[i];
            int64_t qty = (int64_t)l_quantity[i];  // exact: values are 1–50 integer-valued doubles
            uint32_t h  = hash32(key, AGG_MASK);

            for (uint32_t probe = 0; probe < AGG_CAP; probe++) {
                uint32_t idx = (h + probe) & AGG_MASK;
                AtomAggSlot& slot = agg[idx];
                int32_t cur = slot.key.load(std::memory_order_relaxed);

                if (cur == key) {
                    // Hot path: key already present → atomic int64 add (lock xadd, no retry)
                    slot.val.fetch_add(qty, std::memory_order_relaxed);
                    break;
                }
                if (cur == SENTINEL) {
                    int32_t expected = SENTINEL;
                    if (slot.key.compare_exchange_strong(
                            expected, key, std::memory_order_relaxed)) {
                        // We claimed this slot
                        slot.val.fetch_add(qty, std::memory_order_relaxed);
                        break;
                    }
                    // CAS failed: expected now has the actual current value
                    if (expected == key) {
                        // Another thread just inserted our key
                        slot.val.fetch_add(qty, std::memory_order_relaxed);
                        break;
                    }
                    // Another thread inserted a different key — probe next
                }
            }
        }

        // Parallel extraction of qualifying orderkeys: SUM(l_quantity) > 300
        // (integer threshold: 300LL, exact for integer-valued l_quantity)
        // Critical section hit ~2400 times out of 33M → negligible contention
        #pragma omp parallel for schedule(static) num_threads(64)
        for (uint32_t i = 0; i < AGG_CAP; i++) {
            int32_t k = agg[i].key.load(std::memory_order_relaxed);
            if (k != SENTINEL && agg[i].val.load(std::memory_order_relaxed) > 300LL) {
                #pragma omp critical
                qual_set.insert(k);
            }
        }

        delete[] raw_slots;
    }

    // ---- Main Scan: Probe Indexes, Assemble Results ----------------------------
    std::vector<ResultRow> results;
    results.reserve(2400);

    {
        GENDB_PHASE("main_scan");

        for (uint32_t qi = 0; qi < QualSet::CAP; qi++) {
            int32_t ok = qual_set.slots[qi];
            if (ok == SENTINEL) continue;

            // Probe orders index → get order row(s)
            uint32_t ooff, ocnt;
            if (!orders_idx.probe(ok, ooff, ocnt)) continue;

            // Probe lineitem index → compute SUM(l_quantity) for this orderkey
            double sum_qty = 0.0;
            {
                uint32_t loff, lcnt;
                if (lineitem_idx.probe(ok, loff, lcnt)) {
                    for (uint32_t j = 0; j < lcnt; j++) {
                        uint32_t lrow = lineitem_idx.positions[loff + j];
                        sum_qty += l_quantity[lrow];
                    }
                }
            }

            // For each matching order row (unique in TPC-H, ocnt == 1 normally)
            for (uint32_t j = 0; j < ocnt; j++) {
                uint32_t orow     = orders_idx.positions[ooff + j];
                int32_t  cust_key = o_custkey_f.data[orow];
                int32_t  odate    = o_orderdate_f.data[orow];
                double   otprice  = o_totalprice_f.data[orow];

                // Probe customer index → get c_name, c_custkey
                std::string c_name_str;
                int32_t c_custkey_val = cust_key;
                {
                    uint32_t coff, ccnt;
                    if (customer_idx.probe(cust_key, coff, ccnt)) {
                        uint32_t crow = customer_idx.positions[coff];
                        c_custkey_val = c_custkey_f.data[crow];
                        const char* name = c_name_f.data + (size_t)crow * 26;
                        c_name_str = std::string(name, strnlen(name, 26));
                    }
                }

                results.push_back({c_name_str, c_custkey_val, ok, odate, otprice, sum_qty});
            }
        }
    }

    // ---- Output: Partial Sort Top-100 + CSV Write ------------------------------
    {
        GENDB_PHASE("output");

        // Partial sort: top-100 by (o_totalprice DESC, o_orderdate ASC) — O(n log k)
        size_t top_k = std::min((size_t)100, results.size());
        std::partial_sort(results.begin(), results.begin() + top_k, results.end(),
            [](const ResultRow& a, const ResultRow& b) {
                if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
                return a.o_orderdate < b.o_orderdate;
            });
        results.resize(top_k);

        std::filesystem::create_directories(results_dir);
        std::ofstream out(results_dir + "/Q18.csv");
        out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";
        out << std::fixed << std::setprecision(2);
        for (const auto& r : results) {
            out << r.c_name << ","
                << r.c_custkey << ","
                << r.o_orderkey << ","
                << [&]{ char buf[12]; gendb::epoch_days_to_date_str(r.o_orderdate, buf); return std::string(buf); }() << ","
                << r.o_totalprice << ","
                << r.sum_qty << "\n";
        }
    }
}

// ---- Entry Point ---------------------------------------------------------------

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q18(gendb_dir, results_dir);
    return 0;
}
#endif
