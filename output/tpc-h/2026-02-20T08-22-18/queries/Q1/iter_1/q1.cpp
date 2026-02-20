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

// ─── Kahan accumulator helper ────────────────────────────────────────────────
struct KahanSum {
    double sum = 0.0;
    double c   = 0.0;
    inline void add(double v) {
        double y = v - c;
        double t = sum + y;
        c   = (t - sum) - y;
        sum = t;
    }
};

// ─── Per-group accumulator ────────────────────────────────────────────────────
struct Acc {
    KahanSum sum_qty;
    KahanSum sum_base_price;
    KahanSum sum_disc_price;
    KahanSum sum_charge;
    KahanSum sum_disc;
    int64_t  cnt = 0;
};

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
    GENDB_PHASE("total");

    const std::string li_dir  = gendb_dir + "/lineitem/";
    const std::string idx_dir = gendb_dir + "/indexes/";

    // ── Open all columns ──────────────────────────────────────────────────
    MmapFile f_shipdate, f_returnflag, f_linestatus, f_quantity,
             f_extprice, f_discount, f_tax, f_zonemap;

    {
        GENDB_PHASE("mmap_open");
        // Open all 8 files in parallel: each thread's MADV_WILLNEED starts
        // async prefetch concurrently, overlapping I/O across all columns.
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
        #pragma omp parallel for num_threads(8) schedule(static, 1)
        for (int i = 0; i < 8; i++) {
            fptrs[i]->open(paths[i]);
        }
    }

    const int32_t*  shipdate   = f_shipdate.as<int32_t>();
    const int16_t*  returnflag = f_returnflag.as<int16_t>();
    const int16_t*  linestatus = f_linestatus.as<int16_t>();
    const double*   quantity   = f_quantity.as<double>();
    const double*   extprice   = f_extprice.as<double>();
    const double*   discount   = f_discount.as<double>();
    const double*   tax        = f_tax.as<double>();

    // ── Zone map: find last qualifying row ────────────────────────────────
    size_t last_row = 0;
    {
        GENDB_PHASE("zone_map");
        const char* zm = reinterpret_cast<const char*>(f_zonemap.ptr);
        uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(zm);
        const ZMBlock* blocks = reinterpret_cast<const ZMBlock*>(zm + 4);
        const int32_t THRESHOLD = 10471;

        for (uint32_t b = 0; b < num_blocks; b++) {
            if (blocks[b].mn > THRESHOLD) break;
            last_row = (size_t)(b * 100000) + blocks[b].nr;
        }
    }

    // ── Load dictionaries ─────────────────────────────────────────────────
    auto rf_dict = load_dict(li_dir + "l_returnflag_dict.txt", 8);
    auto ls_dict = load_dict(li_dir + "l_linestatus_dict.txt", 8);

    // ── Parallel aggregation: morsel-driven with thread-local flat arrays ──
    constexpr int NSLOTS = 6;
    const int nthreads = omp_get_max_threads();
    // thread_accs[thread][slot]
    std::vector<std::array<Acc, NSLOTS>> thread_accs(nthreads);

    {
        GENDB_PHASE("main_scan");
        const int32_t THRESHOLD = 10471;
        const size_t  MORSEL    = 16384;
        const size_t  nrows     = last_row;

        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            auto& local = thread_accs[tid];

            #pragma omp for schedule(dynamic, 1) nowait
            for (size_t base = 0; base < nrows; base += MORSEL) {
                size_t end = std::min(base + MORSEL, nrows);

                for (size_t i = base; i < end; ++i) {
                    if (shipdate[i] > THRESHOLD) continue;

                    int slot = (int)returnflag[i] * 2 + (int)linestatus[i];

                    double qty  = quantity[i];
                    double ep   = extprice[i];
                    double disc = discount[i];
                    double tx   = tax[i];
                    double ep1  = ep * (1.0 - disc);

                    auto& a = local[slot];
                    a.sum_qty.add(qty);
                    a.sum_base_price.add(ep);
                    a.sum_disc_price.add(ep1);
                    a.sum_charge.add(ep1 * (1.0 + tx));
                    a.sum_disc.add(disc);
                    a.cnt++;
                }
            }
        }
    }

    // ── Merge thread-local → global (Kahan-aware) ────────────────────────
    // Re-use Kahan across thread merge too
    struct MergeAcc {
        KahanSum sum_qty, sum_base_price, sum_disc_price, sum_charge, sum_disc;
        int64_t cnt = 0;
    };
    std::array<MergeAcc, NSLOTS> global_acc{};
    {
        GENDB_PHASE("merge");
        for (int t = 0; t < nthreads; t++) {
            for (int s = 0; s < NSLOTS; s++) {
                global_acc[s].sum_qty.add(thread_accs[t][s].sum_qty.sum);
                global_acc[s].sum_base_price.add(thread_accs[t][s].sum_base_price.sum);
                global_acc[s].sum_disc_price.add(thread_accs[t][s].sum_disc_price.sum);
                global_acc[s].sum_charge.add(thread_accs[t][s].sum_charge.sum);
                global_acc[s].sum_disc.add(thread_accs[t][s].sum_disc.sum);
                global_acc[s].cnt += thread_accs[t][s].cnt;
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
            const MergeAcc& a = global_acc[slot];
            if (a.cnt == 0) continue;

            GroupResult r;
            r.rf_str         = rf_dict[rf];
            r.ls_str         = ls_dict[ls];
            r.sum_qty        = a.sum_qty.sum;
            r.sum_base_price = a.sum_base_price.sum;
            r.sum_disc_price = a.sum_disc_price.sum;
            r.sum_charge     = a.sum_charge.sum;
            r.avg_qty        = a.sum_qty.sum        / (double)a.cnt;
            r.avg_price      = a.sum_base_price.sum / (double)a.cnt;
            r.avg_disc       = a.sum_disc.sum       / (double)a.cnt;
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
