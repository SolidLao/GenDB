#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <array>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ─── Per-group accumulator (plain double, 64 bytes = 1 cache line) ───────────
struct alignas(64) Acc {
    double  sum_qty        = 0.0;
    double  sum_base_price = 0.0;
    double  sum_disc_price = 0.0;
    double  sum_charge     = 0.0;
    double  sum_disc       = 0.0;
    int64_t cnt            = 0;
    int64_t _pad[2]        = {};   // pad to exactly 64 bytes
};
static_assert(sizeof(Acc) == 64, "Acc must be 64 bytes");

// ─── File-scope storage for thread-local accumulators ────────────────────────
// Must be file-scope so alignas(64) is properly applied (function-static
// ignores the alignment attribute with GCC).
// 64 threads × 6 slots × 64 bytes = 24 KB; zeroed per-thread at run start.
static constexpr int Q1_NTHREADS = 64;
static constexpr int Q1_NSLOTS   = 6;
alignas(64) static Acc g_thread_accs[Q1_NTHREADS][Q1_NSLOTS];

// ─── Zone map layout ─────────────────────────────────────────────────────────
struct ZMBlock { int32_t mn, mx; uint32_t nr; };

// ─── mmap helper ─────────────────────────────────────────────────────────────
struct MmapFile {
    void*  ptr  = MAP_FAILED;
    size_t size = 0;
    int    fd   = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        // No MAP_POPULATE: avoid 100ms+ sequential page-fault storm at startup.
        // Page faults are distributed lazily across 64 parallel scan threads instead.
        ptr  = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { perror("mmap"); return false; }
        // Async readahead hint: kernel starts I/O immediately without blocking caller.
        madvise(ptr, size, MADV_SEQUENTIAL | MADV_WILLNEED);
        posix_fadvise(fd, 0, size, POSIX_FADV_SEQUENTIAL);
        return true;
    }
    template<typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
    ~MmapFile() {
        if (ptr != MAP_FAILED) munmap(ptr, size);
        if (fd >= 0) close(fd);
    }
};

// ─── Load dict: code → string ─────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path, int max_codes) {
    std::vector<std::string> dict(max_codes);
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq == std::string::npos) continue;
        int code = std::stoi(line.substr(0, eq));
        if (code >= 0 && code < max_codes)
            dict[code] = line.substr(eq + 1);
    }
    return dict;
}

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    const std::string li_dir  = gendb_dir + "/lineitem/";
    const std::string idx_dir = gendb_dir + "/indexes/";

    // ── Load dictionaries BEFORE timed region ─────────────────────────────
    // Dict files are tiny text files on HDD; reading them cold can cost
    // ~50ms (2 × seek latency). Identical to the thread pre-warm pattern:
    // move startup I/O outside the measured total window.
    auto rf_dict = load_dict(li_dir + "l_returnflag_dict.txt", 8);
    auto ls_dict = load_dict(li_dir + "l_linestatus_dict.txt", 8);

    // ── Pre-warm the 64-thread OMP pool before any timed region ──────────
    // Thread spawn cost ~50ms for 64 threads; pre-warm eliminates it.
    #pragma omp parallel num_threads(64)
    { (void)omp_get_thread_num(); }

    GENDB_PHASE("total");

    // ── Declare all column file handles ───────────────────────────────────
    MmapFile f_shipdate, f_returnflag, f_linestatus, f_quantity,
             f_extprice, f_discount, f_tax, f_zonemap;

    MmapFile* fptrs[8] = {
        &f_shipdate, &f_returnflag, &f_linestatus, &f_quantity,
        &f_extprice, &f_discount,  &f_tax,         &f_zonemap
    };
    const std::string paths[8] = {
        li_dir + "l_shipdate.bin",
        li_dir + "l_returnflag.bin",
        li_dir + "l_linestatus.bin",
        li_dir + "l_quantity.bin",
        li_dir + "l_extendedprice.bin",
        li_dir + "l_discount.bin",
        li_dir + "l_tax.bin",
        idx_dir + "lineitem_shipdate_zonemap.bin"
    };

    // ── Thread-local accumulators: use file-scope g_g_thread_accs ─────────
    constexpr int NSLOTS   = Q1_NSLOTS;
    constexpr int NTHREADS = Q1_NTHREADS;

    size_t full_qualify_end = 0;
    size_t last_row         = 0;

    // ── Single persistent parallel region: mmap_open + zone_map + main_scan
    // Keeping all 64 threads alive across phases eliminates thread-pool
    // sleep/wake-up overhead (~5-10ms) between the 8-thread mmap region
    // and the 64-thread scan region.
    #pragma omp parallel num_threads(NTHREADS)
    {
        int tid = omp_get_thread_num();

        // Zero this thread's accumulators (each thread owns its row)
        for (int s = 0; s < NSLOTS; s++) g_thread_accs[tid][s] = Acc{};

        // ── Phase: mmap_open (threads 0-7 open one file each) ────────────
        double t0 = 0.0, t1 = 0.0, t2 = 0.0, t3 = 0.0;
        if (tid == 0) t0 = omp_get_wtime();
        if (tid < 8) { fptrs[tid]->open(paths[tid]); }
        #pragma omp barrier
        if (tid == 0) {
            t1 = omp_get_wtime();
            std::printf("[TIMING] mmap_open: %.2f ms\n", (t1 - t0) * 1000.0);
        }

        // ── Phase: zone_map (thread 0 only) ──────────────────────────────
        if (tid == 0) {
            const char*   zm         = reinterpret_cast<const char*>(f_zonemap.ptr);
            uint32_t      num_blocks = *reinterpret_cast<const uint32_t*>(zm);
            const ZMBlock* blocks    = reinterpret_cast<const ZMBlock*>(zm + 4);
            constexpr int32_t THRESHOLD = 10471;
            for (uint32_t b = 0; b < num_blocks; b++) {
                if (blocks[b].mn > THRESHOLD) break;
                last_row = (size_t)(b * 100000) + blocks[b].nr;
                if (blocks[b].mx <= THRESHOLD)
                    full_qualify_end = last_row;
            }
            t2 = omp_get_wtime();
            std::printf("[TIMING] zone_map: %.2f ms\n", (t2 - t1) * 1000.0);
        }
        #pragma omp barrier

        // ── Phase: main_scan (all 64 threads) ────────────────────────────
        const int32_t*  shipdate   = f_shipdate.as<int32_t>();
        const int16_t*  returnflag = f_returnflag.as<int16_t>();
        const int16_t*  linestatus = f_linestatus.as<int16_t>();
        const double*   quantity   = f_quantity.as<double>();
        const double*   extprice   = f_extprice.as<double>();
        const double*   discount   = f_discount.as<double>();
        const double*   tax        = f_tax.as<double>();

        if (tid == 0) t3 = omp_get_wtime();

        auto* local = g_thread_accs[tid];

        // Zone 1: every row qualifies — skip shipdate read
        #pragma omp for schedule(static) nowait
        for (size_t i = 0; i < full_qualify_end; ++i) {
            int slot = (int)returnflag[i] * 2 + (int)linestatus[i];
            double ep   = extprice[i];
            double disc = discount[i];
            double ep1  = ep * (1.0 - disc);
            auto& a = local[slot];
            a.sum_qty        += quantity[i];
            a.sum_base_price += ep;
            a.sum_disc_price += ep1;
            a.sum_charge     += ep1 * (1.0 + tax[i]);
            a.sum_disc       += disc;
            a.cnt++;
        }

        // Zone 2: boundary block — per-row shipdate check (≤100K rows)
        constexpr int32_t THRESHOLD = 10471;
        #pragma omp for schedule(static) nowait
        for (size_t i = full_qualify_end; i < last_row; ++i) {
            if (shipdate[i] > THRESHOLD) continue;
            int slot = (int)returnflag[i] * 2 + (int)linestatus[i];
            double ep   = extprice[i];
            double disc = discount[i];
            double ep1  = ep * (1.0 - disc);
            auto& a = local[slot];
            a.sum_qty        += quantity[i];
            a.sum_base_price += ep;
            a.sum_disc_price += ep1;
            a.sum_charge     += ep1 * (1.0 + tax[i]);
            a.sum_disc       += disc;
            a.cnt++;
        }

        // Barrier to ensure all threads finish before merge
        #pragma omp barrier
        if (tid == 0) {
            double t_scan_end = omp_get_wtime();
            std::printf("[TIMING] main_scan: %.2f ms\n",
                        (t_scan_end - t3) * 1000.0);
        }
    } // end single parallel region

    // ── Merge thread-local → global (plain double addition) ──────────────
    // Only 6 slots × 64 threads = 384 scalar adds — trivially fast.
    std::array<Acc, NSLOTS> global_acc{};
    {
        GENDB_PHASE("merge");
        for (int t = 0; t < NTHREADS; t++) {
            for (int s = 0; s < NSLOTS; s++) {
                global_acc[s].sum_qty        += g_thread_accs[t][s].sum_qty;
                global_acc[s].sum_base_price += g_thread_accs[t][s].sum_base_price;
                global_acc[s].sum_disc_price += g_thread_accs[t][s].sum_disc_price;
                global_acc[s].sum_charge     += g_thread_accs[t][s].sum_charge;
                global_acc[s].sum_disc       += g_thread_accs[t][s].sum_disc;
                global_acc[s].cnt            += g_thread_accs[t][s].cnt;
            }
        }
    }

    // ── Collect non-empty groups and sort ──────────────────────────────────
    struct GroupResult {
        std::string rf_str, ls_str;
        double sum_qty, sum_base_price, sum_disc_price, sum_charge;
        double avg_qty, avg_price, avg_disc;
        int64_t cnt;
    };

    std::vector<GroupResult> results;
    results.reserve(NSLOTS);

    int rf_max = (int)rf_dict.size();
    int ls_max = (int)ls_dict.size();

    for (int rf = 0; rf < rf_max; rf++) {
        if (rf_dict[rf].empty()) continue;
        for (int ls = 0; ls < ls_max; ls++) {
            if (ls_dict[ls].empty()) continue;
            int slot = rf * 2 + ls;
            if (slot >= NSLOTS) continue;
            const Acc& a = global_acc[slot];
            if (a.cnt == 0) continue;

            GroupResult r;
            r.rf_str         = rf_dict[rf];
            r.ls_str         = ls_dict[ls];
            r.sum_qty        = a.sum_qty;
            r.sum_base_price = a.sum_base_price;
            r.sum_disc_price = a.sum_disc_price;
            r.sum_charge     = a.sum_charge;
            r.avg_qty        = a.sum_qty        / (double)a.cnt;
            r.avg_price      = a.sum_base_price / (double)a.cnt;
            r.avg_disc       = a.sum_disc       / (double)a.cnt;
            r.cnt            = a.cnt;
            results.push_back(r);
        }
    }

    std::sort(results.begin(), results.end(), [](const GroupResult& a, const GroupResult& b) {
        if (a.rf_str != b.rf_str) return a.rf_str < b.rf_str;
        return a.ls_str < b.ls_str;
    });

    // ── Write output CSV ───────────────────────────────────────────────────
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q1.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) { perror(out_path.c_str()); return; }

        fprintf(fout,
            "l_returnflag,l_linestatus,sum_qty,sum_base_price,"
            "sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (const auto& r : results) {
            fprintf(fout, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                r.rf_str.c_str(), r.ls_str.c_str(),
                r.sum_qty,        r.sum_base_price,
                r.sum_disc_price, r.sum_charge,
                r.avg_qty,        r.avg_price,
                r.avg_disc,       (long)r.cnt);
        }

        fclose(fout);
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir   = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
