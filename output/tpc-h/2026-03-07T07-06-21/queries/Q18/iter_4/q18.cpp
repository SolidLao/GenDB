// Q18: Large Volume Customer — iter_4
//
// Pipeline:
//   Pass 1 — Sorted streaming group-by (parallel, 64 threads):
//     Sequential scan of sorted (l_orderkey, l_quantity) column versions
//     Register-only accumulator (cur_key, cur_sum) — no random writes, no cache misses
//     ~64 boundary keys reconciled O(1). Emits ~2520 qualifying (okey, sum_qty) pairs.
//
//   Build filter:
//     is_big[60M] char array from 2520 qualifying keys (trivial, 57MB)
//     sum_qty_map: unordered_map<int32_t,int32_t> with 2520 entries (fits in L1/L2)
//
//   Pass 2 — Parallel orders scan (64 threads):
//     Sequential scan of all orders columns with is_big[] filter
//     Speculative execution handles the 57MB random filter array efficiently
//     For ~2520 hits: join to customer via cust_idx (5.7MB, L3-resident)
//     sum_qty looked up from sum_qty_map (in L1/L2)
//
//   Sort: partial_sort top-100 on ~2520 entries (trivial)
//
// Advantage over direct orders_by_orderkey index lookup (iter_3 approach):
//   orders_by_orderkey is 240MB → 2520 random accesses = 2 levels of dependent DRAM latency
//   Parallel orders scan is sequential → hardware-prefetched, bandwidth-bound, faster
//
// iter_3 had correct logic but used [TIMING] format; this version uses [GENDB_PHASE].

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <vector>
#include <thread>
#include <string>
#include <chrono>
#include <unordered_map>

// ── GENDB_PHASE timing ────────────────────────────────────────────────────
#ifdef GENDB_PROFILE
struct PhaseTimer {
    const char* name;
    std::chrono::high_resolution_clock::time_point t0;
    explicit PhaseTimer(const char* n)
        : name(n), t0(std::chrono::high_resolution_clock::now()) {}
    ~PhaseTimer() {
        auto t1 = std::chrono::high_resolution_clock::now();
        double ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
        fprintf(stderr, "[GENDB_PHASE] %s: %.3f ms\n", name, ms);
    }
};
#define GENDB_PHASE(name) PhaseTimer _phase_timer_##__LINE__(name)
#else
#define GENDB_PHASE(name)
#endif

// ── mmap helper ───────────────────────────────────────────────────────────
struct MmapFile {
    void*  data = nullptr;
    size_t size = 0;
    int    fd   = -1;

    bool open(const char* path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { perror(path); return false; }
        struct stat st; fstat(fd, &st);
        size = (size_t)st.st_size;
        if (size == 0) return true;
        data = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; return false; }
        return true;
    }
    void advise_seq()      { if (data && size) madvise(data, size, MADV_SEQUENTIAL); }
    void advise_rand()     { if (data && size) madvise(data, size, MADV_RANDOM); }
    void advise_willneed() { if (data && size) madvise(data, size, MADV_WILLNEED); }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
};

// ── Result struct ─────────────────────────────────────────────────────────
struct Q18Result {
    char    c_name[26];
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double  o_totalprice;
    int32_t sum_qty;
};

// ── Date decode: days-since-epoch → "YYYY-MM-DD" (Howard Hinnant) ─────────
static void epoch_days_to_date_str(int32_t z, char* buf) {
    int32_t zz  = z + 719468;
    int32_t era = (zz >= 0 ? zz : zz - 146096) / 146097;
    int32_t doe = (int32_t)(unsigned)(zz - era * 146097);
    int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int32_t y   = yoe + era * 400;
    int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    int32_t mp  = (5*doy + 2) / 153;
    int32_t d   = doy - (153*mp + 2)/5 + 1;
    int32_t m   = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2) ? 1 : 0;
    snprintf(buf, 11, "%04d-%02d-%02d", (int)y, (int)m, (int)d);
}

// ── Per-thread boundary data from sorted scan pass ────────────────────────
struct ThreadBoundary {
    int32_t first_key = -1;
    int32_t first_sum = 0;
    int32_t last_key  = -1;
    int32_t last_sum  = 0;
    std::vector<std::pair<int32_t,int32_t>> interior;  // (okey, sum) for interior qualifying keys
};

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    auto t_total_start = std::chrono::high_resolution_clock::now();

    // ── Phase: data_loading ───────────────────────────────────────────────
    MmapFile f_sorted_okey, f_sorted_qty;
    MmapFile f_o_orderkey, f_o_custkey, f_o_orderdate, f_o_totalprice;
    MmapFile f_cust_idx, f_c_custkey, f_c_name;
    {
        GENDB_PHASE("data_loading");

        auto p = [&](const char* rel) { return gendb_dir + "/" + rel; };

        // Sorted lineitem: large sequential scan → MADV_WILLNEED fires async readahead
        if (!f_sorted_okey.open(p("column_versions/lineitem.l_orderkey_qty.sorted/sorted_orderkey.bin").c_str())) return 1;
        if (!f_sorted_qty.open(p("column_versions/lineitem.l_orderkey_qty.sorted/sorted_quantity.bin").c_str()))   return 1;
        f_sorted_okey.advise_seq();
        f_sorted_qty.advise_seq();
        f_sorted_okey.advise_willneed();
        f_sorted_qty.advise_willneed();

        // Orders: sequential scan in Pass 2 with 64 threads
        if (!f_o_orderkey.open(p("orders/o_orderkey.bin").c_str()))     return 1;
        if (!f_o_custkey.open(p("orders/o_custkey.bin").c_str()))        return 1;
        if (!f_o_orderdate.open(p("orders/o_orderdate.bin").c_str()))    return 1;
        if (!f_o_totalprice.open(p("orders/o_totalprice.bin").c_str()))  return 1;
        f_o_orderkey.advise_willneed();
        f_o_custkey.advise_willneed();
        f_o_orderdate.advise_willneed();
        f_o_totalprice.advise_willneed();

        // Customer: small, warm into L3 immediately
        if (!f_cust_idx.open(p("customer/customer_by_custkey.bin").c_str()))  return 1;
        if (!f_c_custkey.open(p("customer/c_custkey.bin").c_str()))            return 1;
        if (!f_c_name.open(p("customer/c_name.bin").c_str()))                  return 1;
        f_cust_idx.advise_willneed();
        f_c_custkey.advise_rand();
        f_c_name.advise_rand();
    }

    const int32_t* sorted_okey   = (const int32_t*)f_sorted_okey.data;
    const int8_t*  sorted_qty    = (const int8_t*) f_sorted_qty.data;
    const int32_t* o_orderkey    = (const int32_t*)f_o_orderkey.data;
    const int32_t* o_custkey     = (const int32_t*)f_o_custkey.data;
    const int32_t* o_orderdate   = (const int32_t*)f_o_orderdate.data;
    const double*  o_totalprice  = (const double*) f_o_totalprice.data;
    const int32_t* cust_idx      = (const int32_t*)f_cust_idx.data;
    const int32_t* c_custkey_col = (const int32_t*)f_c_custkey.data;
    const char*    c_name_base   = (const char*)   f_c_name.data;

    const size_t N_lineitem = f_sorted_okey.size / sizeof(int32_t);  // 59,986,052
    const size_t N_orders   = f_o_orderkey.size  / sizeof(int32_t);  // 15,000,000

    const int nthreads = (int)std::thread::hardware_concurrency();

    // ── Phase: main_scan (pass1_lineitem_scan_aggregate) ──────────────────
    // Parallel streaming group-by on sorted (orderkey, quantity) column versions.
    // Each thread scans its row range with register-only state:
    //   cur_key, cur_sum — no accumulator array, no random writes, no cache misses.
    // Interior keys (fully within one thread's range) emitted directly.
    // Boundary key partials stored for reconciliation.
    std::vector<ThreadBoundary> thread_data(nthreads);
    {
        GENDB_PHASE("main_scan");

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            const size_t lo = (size_t)t * N_lineitem / nthreads;
            const size_t hi = (size_t)(t + 1) * N_lineitem / nthreads;

            threads.emplace_back([&thread_data, sorted_okey, sorted_qty, lo, hi, t]() {
                ThreadBoundary& td = thread_data[t];
                if (lo >= hi) return;

                const int32_t first_key = sorted_okey[lo];
                const int32_t last_key  = sorted_okey[hi - 1];

                int32_t cur_key   = first_key;
                int32_t cur_sum   = 0;
                int32_t first_sum = 0;
                bool first_done   = false;

                for (size_t i = lo; i < hi; i++) {
                    const int32_t k = sorted_okey[i];
                    if (k != cur_key) {
                        if (!first_done) {
                            first_sum  = cur_sum;
                            first_done = true;
                        } else if (cur_key != last_key) {
                            if (cur_sum > 300) {
                                td.interior.emplace_back(cur_key, cur_sum);
                            }
                        }
                        cur_key = k;
                        cur_sum = 0;
                    }
                    cur_sum += (int32_t)sorted_qty[i];
                }

                td.first_key = first_key;
                td.last_key  = last_key;
                td.last_sum  = cur_sum;
                if (!first_done) {
                    td.first_sum = cur_sum;  // entire range is one key
                } else {
                    td.first_sum = first_sum;
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: dim_filter (boundary_key_merge + build is_big + sum_qty_map) ──
    // Reconcile ~64 boundary key partials. Combine with interior qualifying keys.
    // Build is_big[] char array (57MB) for fast orders scan filter.
    // Build sum_qty_map (2520 entries, fits in L1/L2) for O(1) sum lookup in Pass 2.
    constexpr size_t ARR_SIZE = 60000001;
    char* is_big = nullptr;
    std::unordered_map<int32_t, int32_t> sum_qty_map;
    {
        GENDB_PHASE("dim_filter");

        is_big = (char*)calloc(ARR_SIZE, 1);
        if (!is_big) { fprintf(stderr, "OOM: is_big alloc failed\n"); return 1; }
        sum_qty_map.reserve(4096);

        // Collect interior qualifying keys
        for (int t = 0; t < nthreads; t++) {
            for (const auto& [okey, sty] : thread_data[t].interior) {
                is_big[(uint32_t)okey] = 1;
                sum_qty_map[okey] = sty;
            }
        }

        // Boundary merge: build sorted list of (first_key, sum) and (last_key, sum) per thread
        std::vector<std::pair<int32_t,int32_t>> blist;
        blist.reserve(nthreads * 2);
        for (int t = 0; t < nthreads; t++) {
            const ThreadBoundary& td = thread_data[t];
            if (td.first_key < 0) continue;
            blist.emplace_back(td.first_key, td.first_sum);
            if (td.first_key != td.last_key) {
                blist.emplace_back(td.last_key, td.last_sum);
            }
        }

        // Merge consecutive same-key boundary entries, emit qualifying ones
        if (!blist.empty()) {
            int32_t cur_key = blist[0].first;
            int32_t cur_sum = blist[0].second;
            for (size_t i = 1; i < blist.size(); i++) {
                if (blist[i].first == cur_key) {
                    cur_sum += blist[i].second;
                } else {
                    if (cur_sum > 300) {
                        is_big[(uint32_t)cur_key] = 1;
                        sum_qty_map[cur_key] = cur_sum;
                    }
                    cur_key = blist[i].first;
                    cur_sum = blist[i].second;
                }
            }
            if (cur_sum > 300) {
                is_big[(uint32_t)cur_key] = 1;
                sum_qty_map[cur_key] = cur_sum;
            }
        }
    }

    // ── Phase: pass2_orders_scan_filter + join_orders_to_customer ─────────
    // 64 threads scan all orders columns sequentially.
    // is_big[okey] is O(1) char lookup (57MB, speculative execution handles misses).
    // On hit (2520 out of 15M rows):
    //   cust_idx[o_custkey[j]] → customer row (5.7MB, L3-resident)
    //   sum_qty_map[okey] → sum_qty (~2520-entry map, in L1/L2)
    // Avoids random access into 240MB orders_by_orderkey index entirely.
    std::vector<std::vector<Q18Result>> tl_results(nthreads);
    {
        GENDB_PHASE("pass2_orders_scan_filter");

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        const size_t chunk = (N_orders + nthreads - 1) / nthreads;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                const size_t lo = (size_t)t * chunk;
                const size_t hi = std::min(lo + chunk, N_orders);
                if (lo >= hi) return;

                auto& local = tl_results[t];
                local.reserve(64);

                for (size_t j = lo; j < hi; j++) {
                    const int32_t okey = o_orderkey[j];
                    // Fast O(1) filter: 57MB char array, speculative loads hide latency
                    if (!is_big[(uint32_t)okey]) continue;

                    // Hit: join to customer via customer_by_custkey (5.7MB, L3-resident)
                    const int32_t ckey = o_custkey[j];
                    const int32_t crow = cust_idx[(uint32_t)ckey];
                    if (__builtin_expect(crow < 0, 0)) continue;

                    // sum_qty: O(1) lookup from tiny in-cache map (~2520 entries)
                    const auto it = sum_qty_map.find(okey);
                    if (__builtin_expect(it == sum_qty_map.end(), 0)) continue;

                    Q18Result r;
                    memcpy(r.c_name, c_name_base + (size_t)crow * 26, 26);
                    r.c_name[25]   = '\0';
                    r.c_custkey    = c_custkey_col[crow];
                    r.o_orderkey   = okey;
                    r.o_orderdate  = o_orderdate[j];
                    r.o_totalprice = o_totalprice[j];
                    r.sum_qty      = it->second;
                    local.push_back(r);
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    free(is_big);

    // ── Phase: assemble_result_structs ────────────────────────────────────
    std::vector<Q18Result> results;
    {
        GENDB_PHASE("assemble_result_structs");
        size_t total = 0;
        for (const auto& v : tl_results) total += v.size();
        results.reserve(total);
        for (auto& v : tl_results) {
            results.insert(results.end(), v.begin(), v.end());
        }
    }

    // ── Phase: topk_sort ──────────────────────────────────────────────────
    {
        GENDB_PHASE("topk_sort");
        const size_t K = std::min(results.size(), (size_t)100);
        std::partial_sort(results.begin(), results.begin() + (ptrdiff_t)K, results.end(),
            [](const Q18Result& a, const Q18Result& b) {
                if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
                return a.o_orderdate < b.o_orderdate;
            });
        results.resize(K);
    }

    // ── Phase: output ─────────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        {
            std::string cmd = "mkdir -p \"" + results_dir + "\"";
            (void)system(cmd.c_str());
        }

        const std::string out_path = results_dir + "/Q18.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return 1; }

        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[11];
        for (const auto& r : results) {
            epoch_days_to_date_str(r.o_orderdate, date_buf);
            // Ground truth format: sum_qty as %.2f (e.g., "318.00")
            fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
                    r.c_name,
                    r.c_custkey,
                    r.o_orderkey,
                    date_buf,
                    r.o_totalprice,
                    (double)r.sum_qty);
        }
        fclose(f);
    }

    // ── Total timing ──────────────────────────────────────────────────────
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_total_end - t_total_start).count();
    fprintf(stderr, "[GENDB_PHASE] total: %.3f ms\n", total_ms);

    return 0;
}
