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

// Atomic double accumulation via uint64_t CAS
inline void atomic_double_add(std::atomic<uint64_t>& target, double delta) {
    uint64_t old_bits = target.load(std::memory_order_relaxed);
    uint64_t new_bits;
    do {
        double old_val;
        std::memcpy(&old_val, &old_bits, 8);
        double new_val = old_val + delta;
        std::memcpy(&new_bits, &new_val, 8);
    } while (!target.compare_exchange_weak(old_bits, new_bits, std::memory_order_relaxed));
}

// ---- mmap Column File -----------------------------------------------------------

template<typename T>
struct ColFile {
    const T* data = nullptr;
    size_t count = 0;
    size_t bytes = 0;
    int fd = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        bytes = st.st_size;
        count = bytes / sizeof(T);
        data = reinterpret_cast<const T*>(
            mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
        if (data == MAP_FAILED) { perror("mmap"); exit(1); }
        posix_fadvise(fd, 0, bytes, POSIX_FADV_SEQUENTIAL);
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

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        bytes = st.st_size;
        raw = reinterpret_cast<const char*>(
            mmap(nullptr, bytes, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0));
        if (raw == MAP_FAILED) { perror("mmap index"); exit(1); }
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
        l_orderkey_f.open(gendb_dir + "/lineitem/l_orderkey.bin");
        l_quantity_f.open(gendb_dir + "/lineitem/l_quantity.bin");
        o_custkey_f.open(gendb_dir + "/orders/o_custkey.bin");
        o_orderdate_f.open(gendb_dir + "/orders/o_orderdate.bin");
        o_totalprice_f.open(gendb_dir + "/orders/o_totalprice.bin");
        c_custkey_f.open(gendb_dir + "/customer/c_custkey.bin");
        c_name_f.open(gendb_dir + "/customer/c_name.bin");
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

        // Allocate raw arrays; reinterpret as atomics (lock-free, same size/align)
        // C20: Use loop for INT32_MIN sentinel — NEVER memset for multi-byte sentinel
        int32_t*  raw_keys = new int32_t[AGG_CAP];
        uint64_t* raw_vals = new uint64_t[AGG_CAP];

        // Parallel init of keys (INT32_MIN sentinel)
        #pragma omp parallel for schedule(static) num_threads(64)
        for (uint32_t i = 0; i < AGG_CAP; i++) raw_keys[i] = SENTINEL;

        // Val init: 0 bytes = 0.0 double = 0ULL (safe use of memset for zero)
        std::memset(raw_vals, 0, (size_t)AGG_CAP * 8);

        auto* agg_keys = reinterpret_cast<std::atomic<int32_t>*>(raw_keys);
        auto* agg_vals = reinterpret_cast<std::atomic<uint64_t>*>(raw_vals);

        // Parallel scan lineitem: aggregate SUM(l_quantity) per l_orderkey
        // C24: bounded probing (for-loop, not while)
        #pragma omp parallel for schedule(static) num_threads(64)
        for (size_t i = 0; i < L; i++) {
            int32_t key = l_orderkey[i];
            double  qty = l_quantity[i];
            uint32_t h  = hash32(key, AGG_MASK);

            for (uint32_t probe = 0; probe < AGG_CAP; probe++) {
                uint32_t idx = (h + probe) & AGG_MASK;
                int32_t cur = agg_keys[idx].load(std::memory_order_relaxed);

                if (cur == key) {
                    atomic_double_add(agg_vals[idx], qty);
                    break;
                }
                if (cur == SENTINEL) {
                    int32_t expected = SENTINEL;
                    if (agg_keys[idx].compare_exchange_strong(
                            expected, key, std::memory_order_relaxed)) {
                        // We claimed this slot
                        atomic_double_add(agg_vals[idx], qty);
                        break;
                    }
                    // CAS failed: expected now has the actual value
                    if (expected == key) {
                        // Another thread just inserted our key
                        atomic_double_add(agg_vals[idx], qty);
                        break;
                    }
                    // Another thread inserted a different key — probe next
                }
            }
        }

        // Extract qualifying orderkeys: SUM(l_quantity) > 300
        for (uint32_t i = 0; i < AGG_CAP; i++) {
            int32_t k = agg_keys[i].load(std::memory_order_relaxed);
            if (k != SENTINEL) {
                uint64_t bits = agg_vals[i].load(std::memory_order_relaxed);
                double s;
                std::memcpy(&s, &bits, 8);
                if (s > 300.0) {
                    qual_set.insert(k);
                }
            }
        }

        delete[] raw_keys;
        delete[] raw_vals;
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
