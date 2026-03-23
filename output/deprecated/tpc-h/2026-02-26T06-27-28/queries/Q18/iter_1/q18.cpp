// Q18: Large Volume Customer — iter_1
// Optimization: Phase 1 uses sequential sort-group scan of lineitem_orderkey_sorted.bin
// instead of per-thread hash maps. Eliminates 512MB hash table + 11s merge overhead.
// Each thread handles disjoint complete groups — no hash table, no heap allocation.
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <climits>
#include <algorithm>
#include <string>
#include <vector>
#include <filesystem>
#include <fstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"

// ─── date helper ─────────────────────────────────────────────────────────────
static inline std::string epoch_days_to_str(int32_t days) {
    int z = days + 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    int doe = z - era * 146097;
    int yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int y = yoe + era * 400;
    int doy = doe - (365*yoe + yoe/4 - yoe/100);
    int mp = (5*doy + 2) / 153;
    int d = doy - (153*mp + 2)/5 + 1;
    int m = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2);
    char buf[16];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, d);
    return buf;
}

// ─── mmap helper ─────────────────────────────────────────────────────────────
struct MmapFile {
    void* ptr = nullptr;
    size_t sz  = 0;
    int fd     = -1;

    bool open(const std::string& path, int advise = MADV_RANDOM) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        sz = st.st_size;
        ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { ::close(fd); ptr = nullptr; sz = 0; return false; }
        madvise(ptr, sz, advise);
        return true;
    }
    template<typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
    ~MmapFile() {
        if (ptr && ptr != MAP_FAILED) munmap(ptr, sz);
        if (fd >= 0) ::close(fd);
    }
};

// ─── PK hash index ────────────────────────────────────────────────────────────
struct HashIndex {
    struct Slot { int32_t key; int32_t row_id; };
    uint64_t bucket_count = 0;
    const Slot* slots = nullptr;

    void init(const void* data) {
        const uint64_t* hdr = reinterpret_cast<const uint64_t*>(data);
        bucket_count = hdr[1];
        slots = reinterpret_cast<const Slot*>(hdr + 2);
    }

    int32_t probe(int32_t key) const {
        uint64_t h = ((uint64_t)(uint32_t)key * 2654435761ULL) % bucket_count;
        for (;;) {
            int32_t k = slots[h].key;
            if (k == key) return slots[h].row_id;
            if (k == INT32_MIN) return -1;
            h = (h + 1 < bucket_count) ? h + 1 : 0;
        }
    }
};

// ─── sorted pair ─────────────────────────────────────────────────────────────
struct SortedPair { int32_t key; int32_t row_id; };

// ─── result row ──────────────────────────────────────────────────────────────
struct ResultRow {
    std::string c_name;
    int32_t     c_custkey;
    int32_t     o_orderkey;
    int32_t     o_orderdate;
    double      o_totalprice;
    double      sum_qty;
};

// ─── main ─────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    { GENDB_PHASE("total");

    // ── data loading ──────────────────────────────────────────────────────────
    MmapFile f_li_qty;
    MmapFile f_o_ck, f_o_od, f_o_tp;
    MmapFile f_c_ck, f_c_name_off, f_c_name_data;
    MmapFile f_ord_hash, f_cust_hash, f_li_sorted;

    { GENDB_PHASE("data_loading");

    // sorted index: sequential read in phase1 (advise SEQUENTIAL)
    f_li_sorted.open(gendb_dir + "/indexes/lineitem_orderkey_sorted.bin", MADV_SEQUENTIAL);
    // l_quantity: random access by row_id
    f_li_qty.open(gendb_dir + "/lineitem/l_quantity.bin", MADV_RANDOM);
    f_o_ck.open(gendb_dir  + "/orders/o_custkey.bin",    MADV_RANDOM);
    f_o_od.open(gendb_dir  + "/orders/o_orderdate.bin",  MADV_RANDOM);
    f_o_tp.open(gendb_dir  + "/orders/o_totalprice.bin", MADV_RANDOM);
    f_c_ck.open(gendb_dir  + "/customer/c_custkey.bin",  MADV_RANDOM);
    f_c_name_off.open(gendb_dir + "/customer/c_name.offsets", MADV_RANDOM);
    f_c_name_data.open(gendb_dir + "/customer/c_name.data",   MADV_RANDOM);
    f_ord_hash.open(gendb_dir   + "/indexes/orders_orderkey_hash.bin",   MADV_RANDOM);
    f_cust_hash.open(gendb_dir  + "/indexes/customer_custkey_hash.bin",  MADV_RANDOM);

    } // data_loading

    const double*  li_quantity  = f_li_qty.as<double>();
    const int32_t* o_custkey    = f_o_ck.as<int32_t>();
    const int32_t* o_orderdate  = f_o_od.as<int32_t>();
    const double*  o_totalprice = f_o_tp.as<double>();
    const int32_t* c_custkey    = f_c_ck.as<int32_t>();
    const int32_t* c_name_off   = f_c_name_off.as<int32_t>();
    const char*    c_name_data  = f_c_name_data.as<char>();

    HashIndex ord_idx, cust_idx;
    ord_idx.init(f_ord_hash.ptr);
    cust_idx.init(f_cust_hash.ptr);

    const uint64_t    N_PAIRS = *f_li_sorted.as<uint64_t>();
    const SortedPair* pairs   = reinterpret_cast<const SortedPair*>(
                                    f_li_sorted.as<uint64_t>() + 1);

    // ── phase 1: sequential sort-group scan → hot_keys ────────────────────────
    // Divide N_PAIRS into nthreads equal chunks; each thread advances its start/end
    // to complete group boundaries (key-change boundaries). No hash table needed —
    // since pairs are sorted by l_orderkey, all rows of a group are contiguous.
    std::vector<int32_t> hot_keys;
    { GENDB_PHASE("dim_filter");

    const int nthreads = omp_get_max_threads();

    // Pre-compute thread start boundaries (serial, O(nthreads) work).
    // thread_start[t] = first pair index for thread t, aligned to a group start.
    std::vector<size_t> thread_start(nthreads + 1);
    {
        const size_t chunk = (N_PAIRS + (size_t)nthreads - 1) / (size_t)nthreads;
        thread_start[0] = 0;
        for (int t = 1; t < nthreads; t++) {
            size_t raw = (size_t)t * chunk;
            if (raw >= N_PAIRS) { thread_start[t] = N_PAIRS; continue; }
            // advance forward until we're at the start of a new group
            while (raw < N_PAIRS && pairs[raw].key == pairs[raw - 1].key)
                ++raw;
            thread_start[t] = raw;
        }
        thread_start[nthreads] = N_PAIRS;
    }

    // Per-thread hot-key vectors (no synchronization needed during scan)
    std::vector<std::vector<int32_t>> tl_hot(nthreads);
    for (auto& v : tl_hot) v.reserve(64);

    #pragma omp parallel num_threads(nthreads)
    {
        const int tid = omp_get_thread_num();
        const size_t s = thread_start[tid];
        const size_t e = thread_start[tid + 1];

        if (s < e) {
            auto& local_hot = tl_hot[tid];
            int32_t cur_key = pairs[s].key;
            double  sum     = 0.0;

            for (size_t i = s; i < e; i++) {
                const int32_t k = pairs[i].key;
                if (__builtin_expect(k != cur_key, 0)) {
                    if (sum > 300.0) local_hot.push_back(cur_key);
                    cur_key = k;
                    sum = 0.0;
                }
                sum += li_quantity[pairs[i].row_id];
            }
            // flush last group in this thread's range
            if (sum > 300.0) local_hot.push_back(cur_key);
        }
    }

    // Merge per-thread hot keys (trivial — ~3000 total)
    size_t total_hot = 0;
    for (auto& v : tl_hot) total_hot += v.size();
    hot_keys.reserve(total_hot);
    for (auto& v : tl_hot)
        for (int32_t k : v) hot_keys.push_back(k);

    } // dim_filter

    // After phase 1, sorted index used for binary search only → switch access hint
    madvise(f_li_sorted.ptr, f_li_sorted.sz, MADV_RANDOM);

    // ── phase 2: join + aggregate per hot orderkey ────────────────────────────
    const int nthreads2 = omp_get_max_threads();
    std::vector<std::vector<ResultRow>> tl_results(nthreads2);
    { GENDB_PHASE("build_joins");

    const size_t n_hot = hot_keys.size();

    #pragma omp parallel for schedule(dynamic, 32) num_threads(nthreads2)
    for (size_t i = 0; i < n_hot; i++) {
        const int     tid = omp_get_thread_num();
        const int32_t ok  = hot_keys[i];

        // probe orders hash → orders row
        const int32_t o_row = ord_idx.probe(ok);
        if (o_row < 0) continue;

        const int32_t ck = o_custkey[o_row];
        const int32_t od = o_orderdate[o_row];
        const double  tp = o_totalprice[o_row];

        // probe customer hash → customer row
        const int32_t c_row = cust_idx.probe(ck);
        if (c_row < 0) continue;

        const int32_t the_ck = c_custkey[c_row];
        const int32_t noff   = c_name_off[c_row];
        const int32_t nlen   = c_name_off[c_row + 1] - noff;
        std::string name(c_name_data + noff, (size_t)nlen);

        // binary-search sorted index for this orderkey → sum l_quantity
        size_t lo = 0, hi = N_PAIRS;
        while (lo < hi) {
            size_t mid = (lo + hi) >> 1;
            if (pairs[mid].key < ok) lo = mid + 1;
            else hi = mid;
        }
        double sum_qty = 0.0;
        for (size_t p = lo; p < N_PAIRS && pairs[p].key == ok; p++)
            sum_qty += li_quantity[pairs[p].row_id];

        tl_results[tid].push_back({std::move(name), the_ck, ok, od, tp, sum_qty});
    }

    } // build_joins

    // Flatten per-thread results
    std::vector<ResultRow> results;
    results.reserve(hot_keys.size());
    for (auto& tr : tl_results)
        for (auto& r : tr) results.push_back(std::move(r));

    // ── phase 3: sort + top-100 ───────────────────────────────────────────────
    { GENDB_PHASE("main_scan");

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
        return a.o_orderdate < b.o_orderdate;
    });
    if (results.size() > 100) results.resize(100);

    } // main_scan

    // ── output ────────────────────────────────────────────────────────────────
    { GENDB_PHASE("output");

    std::ofstream out(results_dir + "/Q18.csv");
    out << "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n";
    out << std::fixed;
    out.precision(2);
    for (const auto& r : results) {
        out << r.c_name << ","
            << r.c_custkey << ","
            << r.o_orderkey << ","
            << epoch_days_to_str(r.o_orderdate) << ","
            << r.o_totalprice << ","
            << r.sum_qty << "\n";
    }
    out.close();

    } // output

    } // total
    return 0;
}
