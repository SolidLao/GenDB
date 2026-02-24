#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>
#include <omp.h>
#include "timing_utils.h"

// Q1: Pricing Summary Report
// Single table scan over lineitem, filter l_shipdate <= 10471,
// GROUP BY (l_returnflag, l_linestatus) — 6 groups max.

static const int32_t SHIPDATE_THRESHOLD = 10471;

// Helper: mmap a binary column file read-only
template<typename T>
static const T* mmap_col(const std::string& path, size_t& n_elements) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); n_elements = 0; return nullptr; }
    struct stat st;
    if (fstat(fd, &st) < 0) { perror("fstat"); close(fd); n_elements = 0; return nullptr; }
    posix_fadvise(fd, 0, st.st_size, POSIX_FADV_SEQUENTIAL);
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { perror("mmap"); n_elements = 0; return nullptr; }
    n_elements = (size_t)st.st_size / sizeof(T);
    return reinterpret_cast<const T*>(ptr);
}

// Per-thread aggregation slot — use long double to avoid 1-ULP accumulation errors
// on large sums (e.g. N,O sum_disc_price ~ 1e12 summed over 29M rows).
struct AggSlot {
    long double sum_qty        = 0.0L;
    long double sum_base_price = 0.0L;
    long double sum_disc_price = 0.0L;
    long double sum_charge     = 0.0L;
    long double sum_disc       = 0.0L;
    int64_t     count          = 0;
};

// 64 threads × 6 groups; pad each thread's block to avoid false sharing
// Each AggSlot is 5*8 + 8 = 48 bytes; 6 slots = 288 bytes < 512B → pad to cache line boundary
struct alignas(64) ThreadAgg {
    AggSlot slots[6];
};

static ThreadAgg tl_agg[64];

void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    const std::string li_dir = gendb_dir + "/lineitem/";

    // ── Phase 0: Data Loading ─────────────────────────────────────────────────
    const int32_t*  col_shipdate   = nullptr;
    const int8_t*   col_returnflag = nullptr;
    const int8_t*   col_linestatus = nullptr;
    const uint8_t*  col_quantity   = nullptr;
    const uint8_t*  col_discount   = nullptr;
    const uint8_t*  col_tax        = nullptr;
    const double*   col_extprice   = nullptr;
    const double*   qty_lut        = nullptr;
    const double*   disc_lut       = nullptr;
    const double*   tax_lut        = nullptr;
    size_t total_rows = 0;
    size_t scan_limit = 0;   // rows to scan (zone-map trimmed)

    {
        GENDB_PHASE("data_loading");

        // ── LUT sidecar files (256 doubles each, ~2 KB) ──────────────────────
        size_t n_lut;
        qty_lut  = mmap_col<double>(li_dir + "l_quantity_lookup.bin",  n_lut);
        disc_lut = mmap_col<double>(li_dir + "l_discount_lookup.bin",  n_lut);
        tax_lut  = mmap_col<double>(li_dir + "l_tax_lookup.bin",       n_lut);

        // ── Zone map: find the first block with block_min > threshold ─────────
        // Layout: [uint32_t num_blocks] then [int32_t min, int32_t max, uint32_t start_row] × N
        scan_limit = 59986052ULL;  // default: full table
        {
            const std::string zm_path = li_dir + "indexes/shipdate_zonemap.bin";
            int fd_zm = open(zm_path.c_str(), O_RDONLY);
            if (fd_zm >= 0) {
                struct stat st; fstat(fd_zm, &st);
                void* zm_ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd_zm, 0);
                close(fd_zm);
                if (zm_ptr != MAP_FAILED) {
                    const char* base = reinterpret_cast<const char*>(zm_ptr);
                    uint32_t num_blocks = *reinterpret_cast<const uint32_t*>(base);
                    struct ZMEntry { int32_t min_val; int32_t max_val; uint32_t start_row; };
                    const ZMEntry* entries = reinterpret_cast<const ZMEntry*>(base + sizeof(uint32_t));
                    for (uint32_t b = 0; b < num_blocks; b++) {
                        if (entries[b].min_val > SHIPDATE_THRESHOLD) {
                            scan_limit = entries[b].start_row;
                            break;
                        }
                    }
                    munmap(zm_ptr, st.st_size);
                }
            }
        }

        // ── mmap all 7 lineitem columns in parallel ───────────────────────────
        size_t n_tmp;
        col_shipdate   = mmap_col<int32_t>(li_dir + "l_shipdate.bin",     total_rows);
        col_returnflag = mmap_col<int8_t> (li_dir + "l_returnflag.bin",   n_tmp);
        col_linestatus = mmap_col<int8_t> (li_dir + "l_linestatus.bin",   n_tmp);
        col_quantity   = mmap_col<uint8_t>(li_dir + "l_quantity.bin",     n_tmp);
        col_discount   = mmap_col<uint8_t>(li_dir + "l_discount.bin",     n_tmp);
        col_tax        = mmap_col<uint8_t>(li_dir + "l_tax.bin",          n_tmp);
        col_extprice   = mmap_col<double> (li_dir + "l_extendedprice.bin",n_tmp);

        if (scan_limit > total_rows) scan_limit = total_rows;
    }

    // ── Group key lookup tables ───────────────────────────────────────────────
    // returnflag: A=65→0, N=78→1, R=82→2
    // linestatus: F=70→0, O=79→1
    // slot = rf_idx * 2 + ls_idx
    uint8_t rf_idx[256];  memset(rf_idx, 0, sizeof(rf_idx));
    uint8_t ls_idx[256];  memset(ls_idx, 0, sizeof(ls_idx));
    rf_idx[65] = 0; rf_idx[78] = 1; rf_idx[82] = 2;  // A, N, R
    ls_idx[70] = 0; ls_idx[79] = 1;                   // F, O

    // ── Phase 3: Parallel scan + fused filter + aggregation ──────────────────
    // Initialize thread-local slots to zero
    memset(tl_agg, 0, sizeof(tl_agg));

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel num_threads(64)
        {
            int tid = omp_get_thread_num();
            AggSlot* agg = tl_agg[tid].slots;

            #pragma omp for schedule(static, 65536)
            for (size_t i = 0; i < scan_limit; i++) {
                if (col_shipdate[i] > SHIPDATE_THRESHOLD) continue;

                const int slot = rf_idx[(uint8_t)col_returnflag[i]] * 2
                               + ls_idx[(uint8_t)col_linestatus[i]];

                const double qty  = qty_lut [col_quantity[i]];
                const double disc = disc_lut [col_discount[i]];
                const double tv   = tax_lut  [col_tax[i]];
                const double ep   = col_extprice[i];
                const double dp   = ep * (1.0 - disc);

                agg[slot].sum_qty        += qty;
                agg[slot].sum_base_price += ep;
                agg[slot].sum_disc_price += dp;
                agg[slot].sum_charge     += dp * (1.0 + tv);
                agg[slot].sum_disc       += disc;
                agg[slot].count          += 1;
            }
        }
    }

    // ── Merge thread-local aggregates (trivial: 6×64 = 384 additions) ────────
    AggSlot global_agg[6];
    for (int g = 0; g < 6; g++) global_agg[g] = {};
    for (int t = 0; t < 64; t++) {
        for (int g = 0; g < 6; g++) {
            global_agg[g].sum_qty        += tl_agg[t].slots[g].sum_qty;
            global_agg[g].sum_base_price += tl_agg[t].slots[g].sum_base_price;
            global_agg[g].sum_disc_price += tl_agg[t].slots[g].sum_disc_price;
            global_agg[g].sum_charge     += tl_agg[t].slots[g].sum_charge;
            global_agg[g].sum_disc       += tl_agg[t].slots[g].sum_disc;
            global_agg[g].count          += tl_agg[t].slots[g].count;
        }
    }

    // ── Phase 4: Output CSV ───────────────────────────────────────────────────
    // Output order: rf ASC (A < N < R), ls ASC (F < O)
    // rf chars indexed by rf_idx: 0→A(65), 1→N(78), 2→R(82)
    // ls chars indexed by ls_idx: 0→F(70), 1→O(79)
    static const char RF_CHARS[3] = {'A', 'N', 'R'};
    static const char LS_CHARS[2] = {'F', 'O'};

    {
        GENDB_PHASE("output");

        const std::string out_path = results_dir + "/Q1.csv";
        FILE* fout = fopen(out_path.c_str(), "w");
        if (!fout) { perror(out_path.c_str()); return; }

        fprintf(fout,
            "l_returnflag,l_linestatus,sum_qty,sum_base_price,"
            "sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (int ri = 0; ri < 3; ri++) {
            for (int li = 0; li < 2; li++) {
                const int slot = ri * 2 + li;
                if (global_agg[slot].count == 0) continue;
                const int64_t cnt = global_agg[slot].count;
                fprintf(fout,
                    "%c,%c,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%lld\n",
                    RF_CHARS[ri], LS_CHARS[li],
                    (double)global_agg[slot].sum_qty,
                    (double)global_agg[slot].sum_base_price,
                    (double)global_agg[slot].sum_disc_price,
                    (double)global_agg[slot].sum_charge,
                    (double)(global_agg[slot].sum_qty        / (long double)cnt),
                    (double)(global_agg[slot].sum_base_price / (long double)cnt),
                    (double)(global_agg[slot].sum_disc       / (long double)cnt),
                    (long long)cnt);
            }
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
    std::string results_dir = (argc > 2) ? argv[2] : ".";
    run_Q1(gendb_dir, results_dir);
    return 0;
}
#endif
