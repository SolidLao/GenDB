// Q18: Large Volume Customer — GenDB iter_3
// Strategy: sorted (l_orderkey, l_quantity) column versions → parallel streaming group-by
//   - 64 threads each scan a contiguous row range of sorted arrays
//   - Register-only accumulator: cur_key + cur_sum per thread (no hash table, no accumulator array)
//   - ~64 boundary keys reconciled in O(1) after thread join
//   - ~2520 qualifying orderkeys → direct O(1) index lookup via orders_by_orderkey
//   - Customer join via customer_by_custkey (5.7MB, L3-resident)

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

#include "timing_utils.h"
#include "mmap_utils.h"

// ---------------------------------------------------------------------------
// Date decode: days since 1970-01-01 → YYYY-MM-DD  (Howard Hinnant algorithm)
// ---------------------------------------------------------------------------
static void epoch_days_to_date_str(int32_t z, char* buf) {
    int32_t zz  = z + 719468;
    int32_t era = (zz >= 0 ? zz : zz - 146096) / 146097;
    int32_t doe = zz - era * 146097;
    int32_t yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    int32_t y   = yoe + era * 400;
    int32_t doy = doe - (365*yoe + yoe/4 - yoe/100);
    int32_t mp  = (5*doy + 2) / 153;
    int32_t d   = doy - (153*mp + 2)/5 + 1;
    int32_t m   = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2) ? 1 : 0;
    snprintf(buf, 11, "%04d-%02d-%02d", (int)y, (int)m, (int)d);
}

// ---------------------------------------------------------------------------
// Result struct
// ---------------------------------------------------------------------------
struct Q18Result {
    char    c_name[26];
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double  o_totalprice;
    int32_t sum_qty;
};

// ---------------------------------------------------------------------------
// Per-thread boundary data from sorted scan pass
// ---------------------------------------------------------------------------
struct ThreadBoundary {
    int32_t first_key = -1;
    int32_t first_sum = 0;  // partial sum of first_key rows in [lo, hi)
    int32_t last_key  = -1;
    int32_t last_sum  = 0;  // partial sum of last_key rows in [lo, hi)
    std::vector<std::pair<int32_t,int32_t>> interior;  // complete interior keys (sum > 300)
};

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    mkdir(results_dir.c_str(), 0755);

    auto t_total_start = std::chrono::high_resolution_clock::now();

    // -----------------------------------------------------------------------
    // Phase: data_loading
    // -----------------------------------------------------------------------
    gendb::MmapColumn<int32_t> sorted_okey;
    gendb::MmapColumn<int8_t>  sorted_qty;
    gendb::MmapColumn<int32_t> orders_idx;       // orders_by_orderkey[60000001]
    gendb::MmapColumn<int32_t> customer_idx;     // customer_by_custkey[1500001]
    gendb::MmapColumn<int32_t> o_custkey_col;
    gendb::MmapColumn<int32_t> o_orderdate_col;
    gendb::MmapColumn<double>  o_totalprice_col;
    gendb::MmapColumn<int32_t> c_custkey_col;

    const char* c_name_data = nullptr;
    size_t      c_name_size = 0;
    int         c_name_fd   = -1;

    {
        GENDB_PHASE("data_loading");

        // Large sequential sorted arrays: fire async readahead immediately
        sorted_okey.open(gendb_dir + "/column_versions/lineitem.l_orderkey_qty.sorted/sorted_orderkey.bin");
        sorted_qty.open(gendb_dir  + "/column_versions/lineitem.l_orderkey_qty.sorted/sorted_quantity.bin");
        sorted_okey.advise_sequential();
        sorted_qty.advise_sequential();
        sorted_okey.prefetch();
        sorted_qty.prefetch();

        // Index files: random access during join phase
        orders_idx.open(gendb_dir + "/orders/orders_by_orderkey.bin");
        orders_idx.advise_random();

        customer_idx.open(gendb_dir + "/customer/customer_by_custkey.bin");
        customer_idx.prefetch();  // 5.7MB → pull into L3

        // Orders payload columns (accessed for ~2520 rows only)
        o_custkey_col.open(gendb_dir   + "/orders/o_custkey.bin");
        o_orderdate_col.open(gendb_dir + "/orders/o_orderdate.bin");
        o_totalprice_col.open(gendb_dir+ "/orders/o_totalprice.bin");
        o_custkey_col.advise_random();
        o_orderdate_col.advise_random();
        o_totalprice_col.advise_random();

        // Customer payload columns
        c_custkey_col.open(gendb_dir + "/customer/c_custkey.bin");
        c_custkey_col.advise_random();

        // c_name: fixed 26-byte records — mmap raw
        std::string c_name_path = gendb_dir + "/customer/c_name.bin";
        c_name_fd = ::open(c_name_path.c_str(), O_RDONLY);
        if (c_name_fd < 0) { perror(c_name_path.c_str()); return 1; }
        struct stat st;
        fstat(c_name_fd, &st);
        c_name_size = (size_t)st.st_size;
        c_name_data = (const char*)mmap(nullptr, c_name_size, PROT_READ, MAP_PRIVATE, c_name_fd, 0);
        if (c_name_data == MAP_FAILED) { perror("mmap c_name"); return 1; }
        madvise((void*)c_name_data, c_name_size, MADV_RANDOM);
    }

    const size_t N        = sorted_okey.count;   // 59986052
    const int    nthreads = (int)std::min((unsigned)std::thread::hardware_concurrency(), 64u);

    // -----------------------------------------------------------------------
    // Phase: main_scan
    // Parallel streaming group-by on sorted (orderkey, quantity) arrays.
    // Each thread scans [lo, hi) with register-only state (cur_key, cur_sum).
    // Interior qualifying keys emitted directly; boundary partials stored.
    // -----------------------------------------------------------------------
    std::vector<ThreadBoundary> thread_data(nthreads);

    {
        GENDB_PHASE("main_scan");

        std::vector<std::thread> threads;
        threads.reserve(nthreads);

        const int32_t* const okey_ptr = sorted_okey.data;
        const int8_t*  const qty_ptr  = sorted_qty.data;

        for (int t = 0; t < nthreads; t++) {
            const size_t lo = (size_t)t * N / nthreads;
            const size_t hi = (size_t)(t + 1) * N / nthreads;

            threads.emplace_back([&thread_data, okey_ptr, qty_ptr, lo, hi, t]() {
                ThreadBoundary& td = thread_data[t];
                if (lo >= hi) return;

                const int32_t first_key = okey_ptr[lo];
                const int32_t last_key  = okey_ptr[hi - 1];

                int32_t cur_key    = first_key;
                int32_t cur_sum    = 0;
                int32_t first_sum  = 0;
                bool    first_done = false;

                for (size_t i = lo; i < hi; i++) {
                    const int32_t k = okey_ptr[i];
                    if (k != cur_key) {
                        if (!first_done) {
                            // Completed the first key group in this chunk
                            first_sum  = cur_sum;
                            first_done = true;
                        } else if (cur_key != last_key) {
                            // Interior key: fully contained in this thread's range
                            if (cur_sum > 300) {
                                td.interior.push_back({cur_key, cur_sum});
                            }
                        }
                        // Note: cur_key == last_key transition is impossible
                        // (last_key is max in chunk → no row after it has a different key)
                        cur_key = k;
                        cur_sum = 0;
                    }
                    cur_sum += (int32_t)qty_ptr[i];  // int8_t values 1-50, always positive
                }

                // After loop: cur_key == last_key, cur_sum = last_key's partial sum
                td.first_key = first_key;
                td.last_key  = last_key;
                td.last_sum  = cur_sum;
                if (!first_done) {
                    // Entire chunk is one key: first_key == last_key
                    td.first_sum = cur_sum;
                } else {
                    td.first_sum = first_sum;
                }
            });
        }

        for (auto& th : threads) th.join();
    }

    // -----------------------------------------------------------------------
    // Phase: boundary_key_merge
    // At most 2*nthreads boundary entries. Merge adjacent same-key partials,
    // emit qualifying keys (sum > 300). Combine with interior results.
    // -----------------------------------------------------------------------
    std::vector<std::pair<int32_t,int32_t>> qualifying;  // (orderkey, sum_qty)
    qualifying.reserve(4096);

    {
        GENDB_PHASE("boundary_key_merge");

        // Gather all complete interior qualifying keys from every thread
        for (int t = 0; t < nthreads; t++) {
            for (const auto& p : thread_data[t].interior) {
                qualifying.push_back(p);
            }
        }

        // Build ordered boundary list (sorted ascending since threads cover sorted ranges)
        std::vector<std::pair<int32_t,int32_t>> blist;
        blist.reserve(nthreads * 2);
        for (int t = 0; t < nthreads; t++) {
            const ThreadBoundary& td = thread_data[t];
            if (td.first_key < 0) continue;  // empty chunk
            blist.push_back({td.first_key, td.first_sum});
            if (td.first_key != td.last_key) {
                blist.push_back({td.last_key, td.last_sum});
            }
        }

        // Merge consecutive same-key entries, emit qualifying
        if (!blist.empty()) {
            int32_t cur_key = blist[0].first;
            int32_t cur_sum = blist[0].second;
            for (size_t i = 1; i < blist.size(); i++) {
                if (blist[i].first == cur_key) {
                    cur_sum += blist[i].second;
                } else {
                    if (cur_sum > 300) qualifying.push_back({cur_key, cur_sum});
                    cur_key = blist[i].first;
                    cur_sum = blist[i].second;
                }
            }
            if (cur_sum > 300) qualifying.push_back({cur_key, cur_sum});
        }
    }

    // -----------------------------------------------------------------------
    // Phase: build_joins
    // Direct O(1) index lookup for ~2520 qualifying orderkeys,
    // immediate customer join via customer_by_custkey dense array.
    // -----------------------------------------------------------------------
    std::vector<Q18Result> results;
    results.reserve(qualifying.size());

    {
        GENDB_PHASE("build_joins");

        const int32_t* const oidx   = orders_idx.data;
        const int32_t* const cidx   = customer_idx.data;
        const int32_t* const ocust  = o_custkey_col.data;
        const int32_t* const odate  = o_orderdate_col.data;
        const double*  const oprice = o_totalprice_col.data;
        const int32_t* const ccust  = c_custkey_col.data;

        for (const auto& [okey_val, sum_qty] : qualifying) {
            const int32_t orow = oidx[okey_val];
            if (orow < 0) continue;  // sentinel

            const int32_t ckey = ocust[orow];
            const int32_t crow = cidx[ckey];
            if (crow < 0) continue;  // sentinel

            Q18Result r;
            memcpy(r.c_name, c_name_data + (size_t)crow * 26, 26);
            r.c_name[25]   = '\0';  // ensure null-termination
            r.c_custkey    = ccust[crow];
            r.o_orderkey   = okey_val;
            r.o_orderdate  = odate[orow];
            r.o_totalprice = oprice[orow];
            r.sum_qty      = sum_qty;
            results.push_back(r);
        }
    }

    // -----------------------------------------------------------------------
    // Phase: topk_sort
    // ~2520 entries, partial_sort for top 100
    // ORDER BY o_totalprice DESC, o_orderdate ASC
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("topk_sort");

        const size_t K = std::min(results.size(), (size_t)100);
        if (results.size() <= 100) {
            std::sort(results.begin(), results.end(),
                [](const Q18Result& a, const Q18Result& b) {
                    if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
                    return a.o_orderdate < b.o_orderdate;
                });
        } else {
            std::partial_sort(results.begin(), results.begin() + (ptrdiff_t)K, results.end(),
                [](const Q18Result& a, const Q18Result& b) {
                    if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
                    return a.o_orderdate < b.o_orderdate;
                });
            results.resize(K);
        }
    }

    // -----------------------------------------------------------------------
    // Phase: output — write Q18.csv
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        const std::string out_path = results_dir + "/Q18.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror(out_path.c_str()); return 1; }

        fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char date_buf[11];
        for (const auto& r : results) {
            epoch_days_to_date_str(r.o_orderdate, date_buf);
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

    // Cleanup
    if (c_name_data && c_name_data != MAP_FAILED) munmap((void*)c_name_data, c_name_size);
    if (c_name_fd >= 0) ::close(c_name_fd);

    // Total timing
    auto t_total_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double,std::milli>(t_total_end - t_total_start).count();
#ifdef GENDB_PROFILE
    fprintf(stderr, "[TIMING] total: %.2f ms\n", total_ms);
#endif
    (void)total_ms;

    return 0;
}
