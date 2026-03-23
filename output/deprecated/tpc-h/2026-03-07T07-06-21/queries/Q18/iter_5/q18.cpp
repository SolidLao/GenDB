// Q18: Large Volume Customer — iter_5
//
// Pipeline (per plan.json):
//   Pass 1 — Sorted streaming group-by (parallel, nproc threads):
//     Sequential scan of sorted (l_orderkey, l_quantity) column versions
//     Register-only accumulator (cur_key, cur_sum) per thread — no heap allocation, no cache misses
//     ~nthreads boundary key partials reconciled O(nthreads). Emits ~2520 qualifying pairs.
//
//   Build filter (dim_filter):
//     std::bitset<60000001> (7.5MB, L3-resident) — KEY OPTIMIZATION vs iter_4
//       Replaces char is_big[60000001] (57MB > L3) from prior approach
//       8x smaller → 117K cache lines vs 939K; fits entirely in 44MB L3
//       Eliminates ~70ms DRAM traffic from 15M-row orders scan
//     sum_qty_map: unordered_map<int32_t,int32_t> with ~2520 entries (L1/L2-resident)
//
//   Pass 2 — Parallel orders scan (nproc threads):
//     Sequential scan of all 4 orders columns together
//     bitset probe: 7.5MB working set, L3-resident after first-thread warmup
//     All 15M probes hit L3 at ~1ns vs DRAM at ~100ns → major speedup
//     For ~2520 hits: O(1) join to customer via dense customer_by_custkey index (5.7MB)
//     sum_qty looked up from sum_qty_map (L1/L2, ~2520 entries)
//
//   Sort: partial_sort top-100 on ~2520 entries (trivial)
//
// Expected hot time: ~3-5ms vs 66ms baseline

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
#include <bitset>

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
    int32_t o_orderdate;   // days since epoch; decoded at output
    double  o_totalprice;
    int32_t sum_qty;
};

// ── Date decode: days-since-epoch → "YYYY-MM-DD" (Howard Hinnant) ─────────
static void epoch_days_to_date_str(int32_t z, char* buf) {
    int32_t zz  = z + 719468;
    int32_t era = (zz >= 0 ? zz : zz - 146096) / 146097;
    unsigned doe = (unsigned)(zz - era * 146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int32_t y    = (int32_t)yoe + era * 400;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp  = (5*doy + 2) / 153;
    unsigned d   = doy - (153*mp + 2)/5 + 1;
    unsigned m   = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2) ? 1 : 0;
    snprintf(buf, 11, "%04d-%02d-%02d", (int)y, (int)m, (int)d);
}

// ── Per-thread boundary data from Pass 1 sorted scan ─────────────────────
struct ThreadBoundary {
    int32_t first_key = -1;  // key at lo boundary; partial sum may be split across prev thread
    int32_t first_sum = 0;
    int32_t last_key  = -1;  // key at hi-1 boundary; partial sum may continue in next thread
    int32_t last_sum  = 0;
    std::vector<std::pair<int32_t,int32_t>> interior;  // (okey, sum_qty) for interior qualifying keys
};

// ── Bitset: std::bitset<60000001> = 7.5MB, L3-resident ───────────────────
static constexpr size_t BITSET_SIZE = 60000001;
using BigBitset = std::bitset<BITSET_SIZE>;

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

        // Sorted lineitem column versions (Pass 1): 286MB sequential read
        if (!f_sorted_okey.open((gendb_dir + "/column_versions/lineitem.l_orderkey_qty.sorted/sorted_orderkey.bin").c_str())) return 1;
        if (!f_sorted_qty.open ((gendb_dir + "/column_versions/lineitem.l_orderkey_qty.sorted/sorted_quantity.bin").c_str()))  return 1;
        f_sorted_okey.advise_seq();
        f_sorted_qty.advise_seq();
        f_sorted_okey.advise_willneed();
        f_sorted_qty.advise_willneed();

        // Orders columns (Pass 2): 4 columns, 285MB total, sequential
        if (!f_o_orderkey.open  ((gendb_dir + "/orders/o_orderkey.bin").c_str()))    return 1;
        if (!f_o_custkey.open   ((gendb_dir + "/orders/o_custkey.bin").c_str()))     return 1;
        if (!f_o_orderdate.open ((gendb_dir + "/orders/o_orderdate.bin").c_str()))   return 1;
        if (!f_o_totalprice.open((gendb_dir + "/orders/o_totalprice.bin").c_str()))  return 1;
        f_o_orderkey.advise_willneed();
        f_o_custkey.advise_willneed();
        f_o_orderdate.advise_willneed();
        f_o_totalprice.advise_willneed();

        // Customer: small, warm into L3
        if (!f_cust_idx.open((gendb_dir + "/customer/customer_by_custkey.bin").c_str())) return 1;
        if (!f_c_custkey.open((gendb_dir + "/customer/c_custkey.bin").c_str()))          return 1;
        if (!f_c_name.open   ((gendb_dir + "/customer/c_name.bin").c_str()))             return 1;
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

    // ── Phase: main_scan (Pass 1: sorted streaming group-by) ─────────────
    // Parallel scan of sorted_orderkey.bin + sorted_quantity.bin.
    // Each thread: register-only state (cur_key, cur_sum, first_sum, last_sum).
    // No accumulator array, no random writes, no cache misses during scan.
    // Interior keys (fully within one thread's range): emitted directly if sum > 300.
    // Boundary keys (at thread edges): deferred to boundary merge phase.
    std::vector<ThreadBoundary> thread_data(nthreads);
    {
        GENDB_PHASE("main_scan");

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        for (int t = 0; t < nthreads; t++) {
            const size_t lo = (size_t)t       * N_lineitem / (size_t)nthreads;
            const size_t hi = (size_t)(t + 1) * N_lineitem / (size_t)nthreads;

            threads.emplace_back([&thread_data, sorted_okey, sorted_qty, lo, hi, t]() {
                ThreadBoundary& td = thread_data[t];
                if (lo >= hi) return;

                const int32_t first_key = sorted_okey[lo];
                const int32_t last_key  = sorted_okey[hi - 1];

                int32_t cur_key   = first_key;
                int32_t cur_sum   = 0;
                int32_t first_sum = 0;
                bool first_done   = false;  // true after we've transitioned past first_key

                for (size_t i = lo; i < hi; i++) {
                    const int32_t k = sorted_okey[i];
                    if (__builtin_expect(k != cur_key, 0)) {
                        if (!first_done) {
                            // Just finished accumulating first_key; save partial for boundary merge
                            first_sum  = cur_sum;
                            first_done = true;
                        } else if (cur_key != last_key) {
                            // Interior key: fully owned by this thread; emit if qualifying
                            if (cur_sum > 300) {
                                td.interior.emplace_back(cur_key, cur_sum);
                            }
                        }
                        // cur_key == last_key: defer to boundary merge (may continue in next thread)
                        cur_key = k;
                        cur_sum = 0;
                    }
                    cur_sum += (int32_t)sorted_qty[i];
                }

                // Save boundary sums
                td.first_key = first_key;
                td.last_key  = last_key;
                td.last_sum  = cur_sum;
                if (!first_done) {
                    // Entire range is one key
                    td.first_sum = cur_sum;
                } else {
                    td.first_sum = first_sum;
                }
            });
        }
        for (auto& th : threads) th.join();
    }

    // ── Phase: dim_filter (boundary merge + build bitset + sum_qty_map) ───
    // 1. Collect interior qualifying keys from all threads.
    // 2. Reconcile ~nthreads boundary key partials (O(nthreads) single-threaded work).
    // 3. Build std::bitset<60000001> (7.5MB, L3-resident).
    //    KEY CHANGE vs iter_4: replaces char[60M] (57MB) → 8x smaller → L3-resident
    //    After bitset warms in L3, all 15M orders probes hit at ~1ns (vs DRAM at ~100ns)
    // 4. Build sum_qty_map (~2520 entries, fits in L1/L2).
    BigBitset* is_big = new BigBitset();  // heap-alloc, zero-initialized, 7.5MB
    std::unordered_map<int32_t, int32_t> sum_qty_map;
    sum_qty_map.reserve(4096);
    {
        GENDB_PHASE("dim_filter");

        // Collect interior qualifying keys
        for (int t = 0; t < nthreads; t++) {
            for (const auto& [okey, sty] : thread_data[t].interior) {
                is_big->set((size_t)okey);
                sum_qty_map[okey] = sty;
            }
        }

        // Boundary merge: list (first_key, first_sum) and (last_key, last_sum) per thread
        // Since data is globally sorted, these appear in ascending key order
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

        // Merge consecutive entries with the same key; emit qualifying keys
        if (!blist.empty()) {
            int32_t cur_key = blist[0].first;
            int32_t cur_sum = blist[0].second;
            for (size_t i = 1; i < blist.size(); i++) {
                if (blist[i].first == cur_key) {
                    cur_sum += blist[i].second;
                } else {
                    if (cur_sum > 300) {
                        is_big->set((size_t)cur_key);
                        sum_qty_map[cur_key] = cur_sum;
                    }
                    cur_key = blist[i].first;
                    cur_sum = blist[i].second;
                }
            }
            // Flush last boundary key
            if (cur_sum > 300) {
                is_big->set((size_t)cur_key);
                sum_qty_map[cur_key] = cur_sum;
            }
        }
    }

    // ── Phase: pass2_orders_scan + join_orders_to_customer ────────────────
    // nthreads scan all 4 orders columns sequentially.
    // is_big->test(okey): probe 7.5MB bitset (L3-resident after first-thread warmup).
    // ~2520 hits out of 15M rows: join to customer via dense index (5.7MB, L3-resident).
    // sum_qty from unordered_map (~2520 entries, effectively L1/L2 resident).
    std::vector<std::vector<Q18Result>> tl_results(nthreads);
    {
        GENDB_PHASE("pass2_orders_scan");

        std::vector<std::thread> threads;
        threads.reserve(nthreads);
        const size_t chunk = (N_orders + (size_t)nthreads - 1) / (size_t)nthreads;

        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&, t]() {
                const size_t lo = (size_t)t * chunk;
                const size_t hi = std::min(lo + chunk, N_orders);
                if (lo >= hi) return;

                auto& local = tl_results[t];
                local.reserve(64);

                for (size_t j = lo; j < hi; j++) {
                    const int32_t okey = o_orderkey[j];
                    // Bitset probe: 7.5MB, L3-resident → ~1ns per access after warmup
                    if (!is_big->test((size_t)okey)) continue;

                    // Join to customer: 5.7MB dense index, L3-resident
                    const int32_t ckey = o_custkey[j];
                    const int32_t crow = cust_idx[(uint32_t)ckey];
                    if (__builtin_expect(crow < 0, 0)) continue;

                    // sum_qty: O(1) map lookup, ~2520-entry map (L1/L2 resident)
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

    delete is_big;

    // ── Phase: build_joins (merge per-thread result vectors) ─────────────
    std::vector<Q18Result> results;
    {
        GENDB_PHASE("build_joins");
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
            // sum_qty as %.2f per correctness anchor (e.g., "318.00")
            // c_name: fixed 26-byte field, print up to 25 chars
            fprintf(f, "%.25s,%d,%d,%s,%.2f,%.2f\n",
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
