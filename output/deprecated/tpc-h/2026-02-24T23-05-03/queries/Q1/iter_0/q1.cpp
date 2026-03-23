#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <cmath>

#include "date_utils.h"
#include "timing_utils.h"

// ── Aggregation slot ──────────────────────────────────────────────────────────
struct AggSlot {
    int16_t     rf_code;
    int16_t     ls_code;
    double      sum_qty;
    long double sum_base_price;
    long double sum_disc_price;  // C35: derived expression, must use long double
    long double sum_charge;      // C35: derived expression, must use long double
    double      sum_disc;
    int64_t     count;
};

// ── mmap helper ───────────────────────────────────────────────────────────────
static const void* mmap_ro(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return ptr;
}

// ── Dictionary loader ─────────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { perror(path.c_str()); exit(1); }
    char buf[256];
    while (fgets(buf, sizeof(buf), f)) {
        size_t len = strlen(buf);
        while (len > 0 && (buf[len-1] == '\n' || buf[len-1] == '\r')) --len;
        buf[len] = '\0';
        dict.emplace_back(buf);
    }
    fclose(f);
    return dict;
}

// ── Zone map block ────────────────────────────────────────────────────────────
struct ZMBlock { int32_t mn, mx; uint32_t cnt; };

// ── Main query ────────────────────────────────────────────────────────────────
void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");

    gendb::init_date_tables();  // C11

    // Date threshold: DATE '1998-12-01' - INTERVAL '90' DAY (C1, C7)
    int32_t threshold = gendb::add_days(gendb::date_str_to_epoch_days("1998-12-01"), -90);

    const std::string li = gendb_dir + "/lineitem";

    // ── Data loading ───────────────────────────────────────────────────────────
    size_t sz_sd, sz_rf, sz_ls, sz_qty, sz_ep, sz_disc, sz_tax, sz_zm;

    const int32_t*  l_shipdate;
    const int16_t*  l_returnflag;
    const int16_t*  l_linestatus;
    const double*   l_quantity;
    const double*   l_extendedprice;
    const double*   l_discount;
    const double*   l_tax;
    const uint32_t* zm_raw;
    std::vector<std::string> rf_dict, ls_dict;

    {
        GENDB_PHASE("data_loading");

        l_shipdate      = (const int32_t*)mmap_ro(li + "/l_shipdate.bin",      sz_sd);
        l_returnflag    = (const int16_t*)mmap_ro(li + "/l_returnflag.bin",    sz_rf);
        l_linestatus    = (const int16_t*)mmap_ro(li + "/l_linestatus.bin",    sz_ls);
        l_quantity      = (const double*) mmap_ro(li + "/l_quantity.bin",      sz_qty);
        l_extendedprice = (const double*) mmap_ro(li + "/l_extendedprice.bin", sz_ep);
        l_discount      = (const double*) mmap_ro(li + "/l_discount.bin",      sz_disc);
        l_tax           = (const double*) mmap_ro(li + "/l_tax.bin",           sz_tax);
        zm_raw          = (const uint32_t*)mmap_ro(li + "/indexes/l_shipdate_zone_map.bin", sz_zm);

        madvise((void*)l_shipdate,      sz_sd,   MADV_SEQUENTIAL);
        madvise((void*)l_returnflag,    sz_rf,   MADV_SEQUENTIAL);
        madvise((void*)l_linestatus,    sz_ls,   MADV_SEQUENTIAL);
        madvise((void*)l_quantity,      sz_qty,  MADV_SEQUENTIAL);
        madvise((void*)l_extendedprice, sz_ep,   MADV_SEQUENTIAL);
        madvise((void*)l_discount,      sz_disc, MADV_SEQUENTIAL);
        madvise((void*)l_tax,           sz_tax,  MADV_SEQUENTIAL);

        // C2: load dictionaries at runtime
        rf_dict = load_dict(li + "/l_returnflag_dict.txt");
        ls_dict = load_dict(li + "/l_linestatus_dict.txt");
    }

    // Parse zone map
    uint32_t num_blocks = zm_raw[0];
    const ZMBlock* blocks = (const ZMBlock*)(zm_raw + 1);
    const size_t BLOCK_SIZE = 65536;
    size_t total_rows = sz_sd / sizeof(int32_t);

    // ── dim_filter (nothing for Q1 — no dimension tables) ────────────────────
    { GENDB_PHASE("dim_filter"); }

    // ── build_joins (no joins for Q1) ─────────────────────────────────────────
    { GENDB_PHASE("build_joins"); }

    // ── Main scan: parallel morsel-driven with thread-local aggregation ────────
    // 6-slot direct array indexed by rf_code*2 + ls_code
    // rf has 3 codes [0,1,2], ls has 2 codes [0,1] → slots [0..5], only 4 valid
    const int NSLOTS = 6;
    int nthreads = 1;
    #pragma omp parallel
    { nthreads = omp_get_num_threads(); }

    // Allocate thread-local aggregation arrays
    std::vector<std::array<AggSlot, NSLOTS>> tl_agg(nthreads);
    // Zero-initialize all thread-local slots
    for (int t = 0; t < nthreads; ++t) {
        for (int s = 0; s < NSLOTS; ++s) {
            tl_agg[t][s] = {0, 0, 0.0, 0.0L, 0.0L, 0.0L, 0.0, 0};
        }
    }

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 4)
        for (uint32_t b = 0; b < num_blocks; ++b) {
            const ZMBlock& blk = blocks[b];

            // C19: zone-map skip — skip block if min > threshold (no rows pass)
            if (blk.mn > threshold) continue;

            int tid = omp_get_thread_num();
            AggSlot* local = tl_agg[tid].data();

            size_t row_start = (size_t)b * BLOCK_SIZE;
            size_t row_end   = row_start + blk.cnt;
            if (row_end > total_rows) row_end = total_rows;

            bool full_pass = (blk.mx <= threshold);  // all rows pass

            if (full_pass) {
                // No per-row date check needed
                for (size_t i = row_start; i < row_end; ++i) {
                    int16_t rf  = l_returnflag[i];
                    int16_t ls  = l_linestatus[i];
                    int      idx = (int)rf * 2 + (int)ls;
                    double   qty = l_quantity[i];
                    double   ep  = l_extendedprice[i];
                    double   disc= l_discount[i];
                    double   tax = l_tax[i];

                    AggSlot& s = local[idx];
                    s.rf_code        = rf;
                    s.ls_code        = ls;
                    s.sum_qty        += qty;
                    s.sum_base_price += (long double)ep;
                    s.sum_disc_price += (long double)ep * (1.0L - (long double)disc);
                    s.sum_charge     += (long double)ep * (1.0L - (long double)disc) * (1.0L + (long double)tax);
                    s.sum_disc       += disc;
                    s.count          += 1;
                }
            } else {
                // Partial block: per-row date check
                for (size_t i = row_start; i < row_end; ++i) {
                    if (l_shipdate[i] > threshold) continue;

                    int16_t rf  = l_returnflag[i];
                    int16_t ls  = l_linestatus[i];
                    int      idx = (int)rf * 2 + (int)ls;
                    double   qty = l_quantity[i];
                    double   ep  = l_extendedprice[i];
                    double   disc= l_discount[i];
                    double   tax = l_tax[i];

                    AggSlot& s = local[idx];
                    s.rf_code        = rf;
                    s.ls_code        = ls;
                    s.sum_qty        += qty;
                    s.sum_base_price += (long double)ep;
                    s.sum_disc_price += (long double)ep * (1.0L - (long double)disc);
                    s.sum_charge     += (long double)ep * (1.0L - (long double)disc) * (1.0L + (long double)tax);
                    s.sum_disc       += disc;
                    s.count          += 1;
                }
            }
        }
    }

    // ── Thread-local merge ─────────────────────────────────────────────────────
    {
        GENDB_PHASE("aggregation_merge");

        AggSlot global_agg[NSLOTS] = {};
        for (int t = 0; t < nthreads; ++t) {
            for (int s = 0; s < NSLOTS; ++s) {
                AggSlot& g = global_agg[s];
                AggSlot& l = tl_agg[t][s];
                g.rf_code         = l.rf_code;
                g.ls_code         = l.ls_code;
                g.sum_qty        += l.sum_qty;
                g.sum_base_price += l.sum_base_price;
                g.sum_disc_price += l.sum_disc_price;
                g.sum_charge     += l.sum_charge;
                g.sum_disc       += l.sum_disc;
                g.count          += l.count;
            }
        }

        // ── Output ─────────────────────────────────────────────────────────────
        {
            GENDB_PHASE("output");

            // Collect non-empty slots
            struct OutRow {
                std::string rf_str;
                std::string ls_str;
                double      sum_qty;
                long double sum_base_price;
                long double sum_disc_price;
                long double sum_charge;
                double      avg_qty;
                long double avg_price;
                double      avg_disc;
                int64_t     count;
            };

            std::vector<OutRow> rows;
            for (int s = 0; s < NSLOTS; ++s) {
                if (global_agg[s].count == 0) continue;
                AggSlot& g = global_agg[s];
                OutRow r;
                r.rf_str          = rf_dict[g.rf_code];   // C18
                r.ls_str          = ls_dict[g.ls_code];   // C18
                r.sum_qty         = g.sum_qty;
                r.sum_base_price  = g.sum_base_price;
                r.sum_disc_price  = g.sum_disc_price;
                r.sum_charge      = g.sum_charge;
                r.avg_qty         = g.count > 0 ? g.sum_qty / g.count : 0.0;
                r.avg_price       = g.count > 0 ? g.sum_base_price / (long double)g.count : 0.0L;
                r.avg_disc        = g.count > 0 ? g.sum_disc / g.count : 0.0;
                r.count           = g.count;
                rows.push_back(r);
            }

            // Sort by (l_returnflag ASC, l_linestatus ASC) — string labels
            std::sort(rows.begin(), rows.end(), [](const OutRow& a, const OutRow& b) {
                if (a.rf_str != b.rf_str) return a.rf_str < b.rf_str;
                return a.ls_str < b.ls_str;
            });

            // Write CSV
            std::string out_path = results_dir + "/Q1.csv";
            FILE* fp = fopen(out_path.c_str(), "w");
            if (!fp) { perror(out_path.c_str()); exit(1); }

            fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
            for (const auto& r : rows) {
                // C31: double-quote ALL string columns
                fprintf(fp, "\"%s\",\"%s\",%.2f,%.2Lf,%.2Lf,%.2Lf,%.2f,%.2Lf,%.2f,%lld\n",
                    r.rf_str.c_str(),
                    r.ls_str.c_str(),
                    r.sum_qty,
                    r.sum_base_price,
                    r.sum_disc_price,
                    r.sum_charge,
                    r.avg_qty,
                    r.avg_price,
                    r.avg_disc,
                    (long long)r.count);
            }
            fclose(fp);
        }
    }
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir  = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q1(gendb_dir, results_dir);
    return 0;
}
#endif
