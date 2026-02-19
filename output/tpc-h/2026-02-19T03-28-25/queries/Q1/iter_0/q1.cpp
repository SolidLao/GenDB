// Q1: Pricing Summary Report — GenDB iteration 0
// Strategy: zone-map pruning + morsel-driven parallel scan + thread-local flat-array aggregation

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>

#include "date_utils.h"
#include "timing_utils.h"

// ── Aggregation slot ──────────────────────────────────────────────────────────
struct AggSlot {
    int64_t sum_qty_raw       = 0;
    int64_t sum_base_price_raw= 0;
    int64_t sum_disc_price_raw= 0;
    int64_t sum_charge_raw    = 0;
    int64_t sum_discount_raw  = 0;
    int64_t count_order       = 0;
};

// ── Dictionary loader ─────────────────────────────────────────────────────────
static std::vector<std::string> load_dict(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream f(path);
    std::string line;
    while (std::getline(f, line)) {
        // strip \r if present
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (!line.empty()) dict.push_back(line);
    }
    return dict;
}

// ── mmap helper ──────────────────────────────────────────────────────────────
static const void* mmap_file(const std::string& path, size_t& byte_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); byte_size = 0; return nullptr; }
    struct stat st;
    fstat(fd, &st);
    byte_size = (size_t)st.st_size;
    void* ptr = mmap(nullptr, byte_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { perror("mmap"); byte_size = 0; return nullptr; }
    return ptr;
}

// ── Main query function ───────────────────────────────────────────────────────
void run_Q1(const std::string& gendb_dir, const std::string& results_dir) {
    GENDB_PHASE("total");
    gendb::init_date_tables();

    // l_shipdate <= DATE '1998-09-02'  →  epoch day 10471
    const int32_t SHIPDATE_THRESHOLD = gendb::date_str_to_epoch_days("1998-09-02");
    const size_t  NOMINAL_BLOCK_SIZE  = 100000;

    // ── Load dictionaries ────────────────────────────────────────────────────
    auto rf_dict = load_dict(gendb_dir + "/lineitem/l_returnflag_dict.txt");
    auto ls_dict = load_dict(gendb_dir + "/lineitem/l_linestatus_dict.txt");
    int rf_size  = (int)rf_dict.size();   // 3  (A, N, R)
    int ls_size  = (int)ls_dict.size();   // 2  (F, O)

    // ── Load zone map ────────────────────────────────────────────────────────
    // Layout: [uint32_t num_blocks] then [int32_t min, int32_t max, uint32_t block_size] × num_blocks
    uint32_t num_blocks = 0;
    std::vector<int32_t>  zone_min, zone_max;
    std::vector<uint32_t> zone_blocksize;

    {
        GENDB_PHASE("zone_map_load");
        size_t zm_bytes = 0;
        const void* zm_ptr = mmap_file(gendb_dir + "/lineitem/l_shipdate_zone.idx", zm_bytes);
        if (zm_ptr && zm_bytes >= 4) {
            const uint8_t* p = reinterpret_cast<const uint8_t*>(zm_ptr);
            num_blocks = *reinterpret_cast<const uint32_t*>(p);
            p += 4;
            zone_min.resize(num_blocks);
            zone_max.resize(num_blocks);
            zone_blocksize.resize(num_blocks);
            for (uint32_t i = 0; i < num_blocks; i++) {
                zone_min[i]       = *reinterpret_cast<const int32_t*>(p);  p += 4;
                zone_max[i]       = *reinterpret_cast<const int32_t*>(p);  p += 4;
                zone_blocksize[i] = *reinterpret_cast<const uint32_t*>(p); p += 4;
            }
            munmap(const_cast<void*>(zm_ptr), zm_bytes);
        }
    }

    // ── mmap all required columns ────────────────────────────────────────────
    size_t b_shipdate, b_returnflag, b_linestatus, b_quantity, b_extprice, b_discount, b_tax;
    const int32_t* l_shipdate      = reinterpret_cast<const int32_t*>(
        mmap_file(gendb_dir + "/lineitem/l_shipdate.bin",      b_shipdate));
    const int16_t* l_returnflag    = reinterpret_cast<const int16_t*>(
        mmap_file(gendb_dir + "/lineitem/l_returnflag.bin",    b_returnflag));
    const int16_t* l_linestatus    = reinterpret_cast<const int16_t*>(
        mmap_file(gendb_dir + "/lineitem/l_linestatus.bin",    b_linestatus));
    const int64_t* l_quantity      = reinterpret_cast<const int64_t*>(
        mmap_file(gendb_dir + "/lineitem/l_quantity.bin",      b_quantity));
    const int64_t* l_extendedprice = reinterpret_cast<const int64_t*>(
        mmap_file(gendb_dir + "/lineitem/l_extendedprice.bin", b_extprice));
    const int64_t* l_discount      = reinterpret_cast<const int64_t*>(
        mmap_file(gendb_dir + "/lineitem/l_discount.bin",      b_discount));
    const int64_t* l_tax           = reinterpret_cast<const int64_t*>(
        mmap_file(gendb_dir + "/lineitem/l_tax.bin",           b_tax));

    size_t total_rows = b_shipdate / sizeof(int32_t);

    // If zone map didn't load, synthesize one full block
    if (num_blocks == 0) {
        num_blocks = 1;
        zone_min.push_back(0);
        zone_max.push_back(INT32_MAX); // Force row-level filtering; actual data spans beyond SHIPDATE_THRESHOLD
        zone_blocksize.push_back((uint32_t)total_rows);
    }

    // ── Thread-local flat aggregation arrays ─────────────────────────────────
    // [nthreads][rf_size][ls_size]
    int nthreads = omp_get_max_threads();
    // Pad to avoid false sharing: use cache-line aligned slots
    // Each AggSlot = 6 × int64_t = 48 bytes; for 3×2=6 groups per thread that's 288 bytes — fits in one cache line block
    std::vector<std::vector<AggSlot>> local_agg(
        (size_t)nthreads * rf_size * ls_size);
    // Flat layout: [tid * rf_size * ls_size + rf * ls_size + ls]
    // We'll allocate as a flat vector and zero-init
    std::vector<AggSlot> flat_agg((size_t)nthreads * rf_size * ls_size);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel for schedule(dynamic, 1) num_threads(nthreads)
        for (int64_t blk = 0; blk < (int64_t)num_blocks; blk++) {
            // Zone-map pruning: skip blocks where all shipdates > threshold
            if (zone_min[blk] > SHIPDATE_THRESHOLD) continue;

            int tid = omp_get_thread_num();
            AggSlot* my_agg = &flat_agg[(size_t)tid * rf_size * ls_size];

            size_t row_start = (size_t)blk * NOMINAL_BLOCK_SIZE;
            size_t row_end   = row_start + (size_t)zone_blocksize[blk];
            if (row_end > total_rows) row_end = total_rows;

            // If zone max <= threshold, every row in the block qualifies — no branch needed
            if (zone_max[blk] <= SHIPDATE_THRESHOLD) {
                for (size_t i = row_start; i < row_end; i++) {
                    int rf = l_returnflag[i];
                    int ls = l_linestatus[i];

                    int64_t price = l_extendedprice[i];
                    int64_t disc  = l_discount[i];
                    int64_t tax   = l_tax[i];

                    // disc_price = price * (100 - disc) / 100   [result in scale 100]
                    int64_t disc_price = price * (100LL - disc) / 100LL;
                    // charge = disc_price * (100 + tax) / 100   [result in scale 100]
                    int64_t charge     = disc_price * (100LL + tax) / 100LL;

                    AggSlot& slot = my_agg[rf * ls_size + ls];
                    slot.sum_qty_raw        += l_quantity[i];
                    slot.sum_base_price_raw += price;
                    slot.sum_disc_price_raw += disc_price;
                    slot.sum_charge_raw     += charge;
                    slot.sum_discount_raw   += disc;
                    slot.count_order        += 1;
                }
            } else {
                // Mixed block: need row-level filter
                for (size_t i = row_start; i < row_end; i++) {
                    if (l_shipdate[i] > SHIPDATE_THRESHOLD) continue;

                    int rf = l_returnflag[i];
                    int ls = l_linestatus[i];

                    int64_t price = l_extendedprice[i];
                    int64_t disc  = l_discount[i];
                    int64_t tax   = l_tax[i];

                    int64_t disc_price = price * (100LL - disc) / 100LL;
                    int64_t charge     = disc_price * (100LL + tax) / 100LL;

                    AggSlot& slot = my_agg[rf * ls_size + ls];
                    slot.sum_qty_raw        += l_quantity[i];
                    slot.sum_base_price_raw += price;
                    slot.sum_disc_price_raw += disc_price;
                    slot.sum_charge_raw     += charge;
                    slot.sum_discount_raw   += disc;
                    slot.count_order        += 1;
                }
            }
        }
    }

    // ── Merge thread-local aggregates into global ────────────────────────────
    std::vector<AggSlot> global_agg((size_t)rf_size * ls_size);

    {
        GENDB_PHASE("merge_aggregates");
        for (int t = 0; t < nthreads; t++) {
            AggSlot* src = &flat_agg[(size_t)t * rf_size * ls_size];
            for (int g = 0; g < rf_size * ls_size; g++) {
                global_agg[g].sum_qty_raw        += src[g].sum_qty_raw;
                global_agg[g].sum_base_price_raw += src[g].sum_base_price_raw;
                global_agg[g].sum_disc_price_raw += src[g].sum_disc_price_raw;
                global_agg[g].sum_charge_raw     += src[g].sum_charge_raw;
                global_agg[g].sum_discount_raw   += src[g].sum_discount_raw;
                global_agg[g].count_order        += src[g].count_order;
            }
        }
    }

    // ── Sort and write output ────────────────────────────────────────────────
    {
        GENDB_PHASE("output");

        struct OutRow {
            std::string returnflag, linestatus;
            int rf_code, ls_code;
        };
        std::vector<OutRow> rows;
        rows.reserve(rf_size * ls_size);
        for (int rf = 0; rf < rf_size; rf++)
            for (int ls = 0; ls < ls_size; ls++)
                if (global_agg[rf * ls_size + ls].count_order > 0)
                    rows.push_back({rf_dict[rf], ls_dict[ls], rf, ls});

        std::sort(rows.begin(), rows.end(), [](const OutRow& a, const OutRow& b) {
            if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
            return a.linestatus < b.linestatus;
        });

        // Ensure results directory exists
        {
            std::string cmd = "mkdir -p " + results_dir;
            (void)system(cmd.c_str());
        }

        std::string out_path = results_dir + "/Q1.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { perror(out_path.c_str()); return; }

        fprintf(fp, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                    "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (auto& row : rows) {
            const AggSlot& s = global_agg[row.rf_code * ls_size + row.ls_code];
            int64_t cnt = s.count_order;

            // All raw sums are in scale 100 (SQL value × 100)
            double sum_qty        = (double)s.sum_qty_raw        / 100.0;
            double sum_base_price = (double)s.sum_base_price_raw / 100.0;
            double sum_disc_price = (double)s.sum_disc_price_raw / 100.0;
            double sum_charge     = (double)s.sum_charge_raw     / 100.0;
            double avg_qty        = (double)s.sum_qty_raw        / (double)cnt / 100.0;
            double avg_price      = (double)s.sum_base_price_raw / (double)cnt / 100.0;
            double avg_disc       = (double)s.sum_discount_raw   / (double)cnt / 100.0;

            fprintf(fp, "%s,%s,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%lld\n",
                row.returnflag.c_str(), row.linestatus.c_str(),
                sum_qty, sum_base_price, sum_disc_price, sum_charge,
                avg_qty, avg_price, avg_disc, (long long)cnt);
        }
        fclose(fp);
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
