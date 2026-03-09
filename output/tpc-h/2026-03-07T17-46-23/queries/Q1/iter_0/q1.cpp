// Q1: Pricing Summary Report — GenDB generated code
// Single-table scan with date filter, grouped aggregation, ordered output
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ─── mmap helper ───
static void* mmap_file(const std::string& path, size_t& len) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    len = st.st_size;
    void* p = mmap(nullptr, len, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (p == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    madvise(p, len, MADV_SEQUENTIAL);
    return p;
}

// ─── Accumulator (long double to avoid precision loss from parallel merge) ───
struct Acc {
    long double sum_qty;
    long double sum_base_price;
    long double sum_disc_price;
    long double sum_charge;
    long double sum_discount;
    int64_t count;
};

static constexpr int AGG_SIZE = 65536;
static constexpr int32_t SHIPDATE_THRESHOLD = 10471; // 1998-09-02

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // ─── Phase: data_loading ───
    size_t len;
    const int32_t* col_shipdate;
    const uint8_t* col_returnflag;
    const uint8_t* col_linestatus;
    const double*  col_quantity;
    const double*  col_extendedprice;
    const double*  col_discount;
    const double*  col_tax;
    size_t num_rows;

    {
        GENDB_PHASE("data_loading");
        col_shipdate     = (const int32_t*) mmap_file(gendb_dir + "/lineitem/l_shipdate.bin", len);
        num_rows = len / sizeof(int32_t);
        col_returnflag   = (const uint8_t*) mmap_file(gendb_dir + "/lineitem/l_returnflag.bin", len);
        col_linestatus   = (const uint8_t*) mmap_file(gendb_dir + "/lineitem/l_linestatus.bin", len);
        col_quantity     = (const double*)  mmap_file(gendb_dir + "/lineitem/l_quantity.bin", len);
        col_extendedprice= (const double*)  mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", len);
        col_discount     = (const double*)  mmap_file(gendb_dir + "/lineitem/l_discount.bin", len);
        col_tax          = (const double*)  mmap_file(gendb_dir + "/lineitem/l_tax.bin", len);
    }

    // ─── Phase: zonemap loading ───
    // Load zonemap for block skipping
    struct ZoneEntry { int32_t min_date; int32_t max_date; };
    uint32_t zm_num_blocks = 0;
    uint32_t zm_block_size = 0;
    const ZoneEntry* zones = nullptr;
    {
        size_t zm_len;
        const uint8_t* zm_data = (const uint8_t*) mmap_file(gendb_dir + "/indexes/lineitem_shipdate_zonemap.bin", zm_len);
        zm_num_blocks = *(const uint32_t*)(zm_data + 0);
        zm_block_size = *(const uint32_t*)(zm_data + 4);
        zones = (const ZoneEntry*)(zm_data + 8);
    }

    // ─── Phase: main_scan (parallel morsel-driven) ───
    int nthreads = omp_get_max_threads();
    // Allocate per-thread accumulators
    std::vector<std::vector<Acc>> thread_accs(nthreads, std::vector<Acc>(AGG_SIZE));
    for (int t = 0; t < nthreads; t++) {
        memset(thread_accs[t].data(), 0, AGG_SIZE * sizeof(Acc));
    }

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            Acc* __restrict__ local = thread_accs[tid].data();

            #pragma omp for schedule(dynamic, 1)
            for (uint32_t blk = 0; blk < zm_num_blocks; blk++) {
                // Zonemap skip check
                if (zones[blk].min_date > SHIPDATE_THRESHOLD) continue;

                size_t start = (size_t)blk * zm_block_size;
                size_t end = start + zm_block_size;
                if (end > num_rows) end = num_rows;

                // If entire block passes (max_date <= threshold), skip per-row date check
                if (zones[blk].max_date <= SHIPDATE_THRESHOLD) {
                    for (size_t i = start; i < end; i++) {
                        uint16_t key = ((uint16_t)col_returnflag[i] << 8) | col_linestatus[i];
                        Acc& a = local[key];
                        double qty = col_quantity[i];
                        double price = col_extendedprice[i];
                        double disc = col_discount[i];
                        double tax = col_tax[i];
                        double disc_price = price * (1.0 - disc);
                        a.sum_qty += qty;
                        a.sum_base_price += price;
                        a.sum_disc_price += disc_price;
                        a.sum_charge += disc_price * (1.0 + tax);
                        a.sum_discount += disc;
                        a.count++;
                    }
                } else {
                    for (size_t i = start; i < end; i++) {
                        if (col_shipdate[i] > SHIPDATE_THRESHOLD) continue;
                        uint16_t key = ((uint16_t)col_returnflag[i] << 8) | col_linestatus[i];
                        Acc& a = local[key];
                        double qty = col_quantity[i];
                        double price = col_extendedprice[i];
                        double disc = col_discount[i];
                        double tax = col_tax[i];
                        double disc_price = price * (1.0 - disc);
                        a.sum_qty += qty;
                        a.sum_base_price += price;
                        a.sum_disc_price += disc_price;
                        a.sum_charge += disc_price * (1.0 + tax);
                        a.sum_discount += disc;
                        a.count++;
                    }
                }
            }
        }
    }

    // ─── Phase: merge thread accumulators ───
    std::vector<Acc> global(AGG_SIZE);
    memset(global.data(), 0, AGG_SIZE * sizeof(Acc));
    {
        GENDB_PHASE("merge");
        for (int t = 0; t < nthreads; t++) {
            const Acc* local = thread_accs[t].data();
            for (int k = 0; k < AGG_SIZE; k++) {
                if (local[k].count == 0) continue;
                Acc& g = global[k];
                g.sum_qty += local[k].sum_qty;
                g.sum_base_price += local[k].sum_base_price;
                g.sum_disc_price += local[k].sum_disc_price;
                g.sum_charge += local[k].sum_charge;
                g.sum_discount += local[k].sum_discount;
                g.count += local[k].count;
            }
        }
    }

    // ─── Phase: output ───
    {
        GENDB_PHASE("output");

        // Collect active groups
        struct Result {
            char returnflag;
            char linestatus;
            uint16_t key;
        };
        std::vector<Result> results;
        for (int k = 0; k < AGG_SIZE; k++) {
            if (global[k].count > 0) {
                Result r;
                r.returnflag = (char)(k >> 8);
                r.linestatus = (char)(k & 0xFF);
                r.key = (uint16_t)k;
                results.push_back(r);
            }
        }

        // Sort by returnflag ASC, linestatus ASC
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
            return a.linestatus < b.linestatus;
        });

        // Write CSV
        std::string outpath = results_dir + "/Q1.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        if (!f) { perror(outpath.c_str()); return 1; }

        fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (auto& r : results) {
            const Acc& a = global[r.key];
            double avg_qty = (double)(a.sum_qty / a.count);
            double avg_price = (double)(a.sum_base_price / a.count);
            double avg_disc = (double)(a.sum_discount / a.count);
            fprintf(f, "%c,%c,%.2f,%.2f,%.4f,%.6f,%.2f,%.2f,%.2f,%ld\n",
                    r.returnflag, r.linestatus,
                    (double)a.sum_qty, (double)a.sum_base_price,
                    (double)a.sum_disc_price, (double)a.sum_charge,
                    avg_qty, avg_price, avg_disc,
                    (long)a.count);
        }
        fclose(f);
    }

    return 0;
}
