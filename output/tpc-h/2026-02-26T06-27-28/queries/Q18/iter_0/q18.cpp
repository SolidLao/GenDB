// Q18: Large Volume Customer
// Pipeline: phase1=parallel lineitem scan → hot_keys, phase2=point-lookup joins, phase3=sort+limit
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <climits>
#include <cassert>
#include <algorithm>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <omp.h>

#include "timing_utils.h"
#include "mmap_utils.h"

// ─── date helper ─────────────────────────────────────────────────────────────
static std::string epoch_days_to_str(int32_t days) {
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
    size_t sz = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        sz = st.st_size;
        ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { ::close(fd); ptr = nullptr; return false; }
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
        // hdr[0] = num_entries, hdr[1] = bucket_count
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

// ─── sorted pair for lineitem index ──────────────────────────────────────────
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
    const std::string gendb_dir = argv[1];
    const std::string results_dir = argv[2];
    std::filesystem::create_directories(results_dir);

    { GENDB_PHASE("total");

    // ── data loading ──────────────────────────────────────────────────────────
    MmapFile f_li_ok, f_li_qty;
    MmapFile f_o_ck, f_o_od, f_o_tp;
    MmapFile f_c_ck, f_c_name_off, f_c_name_data;
    MmapFile f_ord_hash, f_cust_hash, f_li_sorted;

    { GENDB_PHASE("data_loading");

    f_li_ok.open(gendb_dir + "/lineitem/l_orderkey.bin");
    f_li_qty.open(gendb_dir + "/lineitem/l_quantity.bin");
    f_o_ck.open(gendb_dir + "/orders/o_custkey.bin");
    f_o_od.open(gendb_dir + "/orders/o_orderdate.bin");
    f_o_tp.open(gendb_dir + "/orders/o_totalprice.bin");
    f_c_ck.open(gendb_dir + "/customer/c_custkey.bin");
    f_c_name_off.open(gendb_dir + "/customer/c_name.offsets");
    f_c_name_data.open(gendb_dir + "/customer/c_name.data");
    f_ord_hash.open(gendb_dir + "/indexes/orders_orderkey_hash.bin");
    f_cust_hash.open(gendb_dir + "/indexes/customer_custkey_hash.bin");
    f_li_sorted.open(gendb_dir + "/indexes/lineitem_orderkey_sorted.bin");

    } // data_loading

    const int32_t* li_orderkey  = f_li_ok.as<int32_t>();
    const double*  li_quantity  = f_li_qty.as<double>();
    const size_t   N_LI         = f_li_ok.sz / sizeof(int32_t);

    const int32_t* o_custkey    = f_o_ck.as<int32_t>();
    const int32_t* o_orderdate  = f_o_od.as<int32_t>();
    const double*  o_totalprice = f_o_tp.as<double>();

    const int32_t* c_custkey    = f_c_ck.as<int32_t>();
    const int32_t* c_name_off   = f_c_name_off.as<int32_t>();
    const char*    c_name_data  = f_c_name_data.as<char>();

    HashIndex ord_idx, cust_idx;
    ord_idx.init(f_ord_hash.ptr);
    cust_idx.init(f_cust_hash.ptr);

    const uint64_t    li_sorted_n     = *f_li_sorted.as<uint64_t>();
    const SortedPair* li_sorted_pairs = reinterpret_cast<const SortedPair*>(
        f_li_sorted.as<uint64_t>() + 1);

    // prefetch hints
    madvise(f_li_ok.ptr,   f_li_ok.sz,   MADV_SEQUENTIAL);
    madvise(f_li_qty.ptr,  f_li_qty.sz,  MADV_SEQUENTIAL);
    madvise(f_li_sorted.ptr, f_li_sorted.sz, MADV_RANDOM);

    // ── phase 1: parallel scan → hot_keys ─────────────────────────────────────
    std::unordered_set<int32_t> hot_keys;
    { GENDB_PHASE("dim_filter");

    int nthreads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t,double>> tl_maps(nthreads);
    for (auto& m : tl_maps) m.reserve(1 << 20);

    const size_t MORSEL   = 1000000UL;
    const size_t n_morsels = (N_LI + MORSEL - 1) / MORSEL;

    #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
    for (size_t m = 0; m < n_morsels; m++) {
        int tid = omp_get_thread_num();
        auto& mp  = tl_maps[tid];
        size_t s  = m * MORSEL;
        size_t e  = std::min(s + MORSEL, N_LI);
        for (size_t i = s; i < e; i++) {
            mp[li_orderkey[i]] += li_quantity[i];
        }
    }

    // merge
    std::unordered_map<int32_t,double> global_qty;
    global_qty.reserve(1 << 24);
    for (auto& mp : tl_maps) {
        for (auto& [k, v] : mp) global_qty[k] += v;
        std::unordered_map<int32_t,double>().swap(mp);
    }

    hot_keys.reserve(4096);
    for (auto& [k, v] : global_qty)
        if (v > 300.0) hot_keys.insert(k);

    } // dim_filter

    std::vector<int32_t> hot_vec(hot_keys.begin(), hot_keys.end());

    // ── phase 2: join + aggregate per hot orderkey ────────────────────────────
    int nthreads = omp_get_max_threads();
    std::vector<std::vector<ResultRow>> tl_results(nthreads);
    { GENDB_PHASE("build_joins");

    #pragma omp parallel for schedule(dynamic, 32) num_threads(nthreads)
    for (size_t i = 0; i < hot_vec.size(); i++) {
        int     tid = omp_get_thread_num();
        int32_t ok  = hot_vec[i];

        // probe orders hash
        int32_t o_row = ord_idx.probe(ok);
        if (o_row < 0) continue;

        int32_t ck = o_custkey[o_row];
        int32_t od = o_orderdate[o_row];
        double  tp = o_totalprice[o_row];

        // probe customer hash
        int32_t c_row = cust_idx.probe(ck);
        if (c_row < 0) continue;

        int32_t the_ck  = c_custkey[c_row];
        int32_t noff    = c_name_off[c_row];
        int32_t nlen    = c_name_off[c_row+1] - noff;
        std::string name(c_name_data + noff, nlen);

        // binary search lineitem sorted index
        size_t lo = 0, hi = li_sorted_n;
        while (lo < hi) {
            size_t mid = (lo + hi) >> 1;
            if (li_sorted_pairs[mid].key < ok) lo = mid + 1;
            else hi = mid;
        }
        double sum_qty = 0.0;
        for (size_t p = lo; p < li_sorted_n && li_sorted_pairs[p].key == ok; p++)
            sum_qty += li_quantity[li_sorted_pairs[p].row_id];

        tl_results[tid].push_back({std::move(name), the_ck, ok, od, tp, sum_qty});
    }

    } // build_joins

    // flatten results
    std::vector<ResultRow> results;
    results.reserve(hot_vec.size());
    for (auto& tr : tl_results)
        for (auto& r : tr) results.push_back(std::move(r));

    // ── phase 3: sort + limit ─────────────────────────────────────────────────
    { GENDB_PHASE("main_scan");  // reuse label for sort phase

    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
        return a.o_orderdate < b.o_orderdate;
    });
    if (results.size() > 100) results.resize(100);

    } // sort

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
