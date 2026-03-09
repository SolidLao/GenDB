// Q1: Pricing Summary Report — GenDB iter_0
// Strategy: zone_map_prune → parallel_scan_filter → aggregate → merge → sort_output
// Parallelism: morsel-driven, one block per task, thread-local GroupAcc[6] merged at end

#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <vector>
#include <string>
#include <atomic>
#include <thread>
#include <algorithm>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "timing_utils.h"
#include "mmap_utils.h"

// ---------------------------------------------------------------------------
// GroupAcc: thread-local accumulator for one (returnflag, linestatus) group
//
// sum_disc_price and sum_charge use integer arithmetic to avoid FP accumulation
// error over 60M rows:
//   sum_disc_price_int = Σ lround(price*100) × (100 - discount_raw)
//       → actual sum_disc_price = sum_disc_price_int / 10000.0
//   sum_charge_int = Σ lround(price*100) × (100 - discount_raw) × (100 + tax_raw)
//       → actual sum_charge = sum_charge_int / 1000000.0
//
// Overflow analysis (SF=10, max extprice≈104949.50, max lround≈10494950):
//   sum_disc_price_int per row ≤ 10494950 × 100 = 1.05e9
//   sum over 30M rows (largest group) ≤ 3.1e16 < INT64_MAX (9.22e18) ✓
//   sum_charge_int per row ≤ 1.05e9 × 108 = 1.13e11
//   sum over 30M rows ≤ 3.4e18 < INT64_MAX ✓
// ---------------------------------------------------------------------------
struct GroupAcc {
    int64_t  sum_qty            = 0;
    double   sum_price          = 0.0;
    int64_t  sum_disc_price_int = 0;  // ×10000
    int64_t  sum_charge_int     = 0;  // ×1000000
    int64_t  sum_disc           = 0;  // raw hundredths
    int64_t  count              = 0;
};

// ---------------------------------------------------------------------------
// Zone map entry (guide format): 8 bytes per block {int32_t mn, int32_t mx}
// ---------------------------------------------------------------------------
struct ZoneEntry {
    int32_t mn;
    int32_t mx;
};

static const int32_t THRESHOLD = 10471; // date_to_days("1998-09-02")

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    const std::string gendb_dir   = argv[1];
    const std::string results_dir = argv[2];

    // Ensure output directory exists
    std::filesystem::create_directories(results_dir);

    GENDB_PHASE_MS("total", ms_total);

    // -----------------------------------------------------------------------
    // Phase: data_loading
    // -----------------------------------------------------------------------
    uint32_t num_blocks = 0, block_size = 0;
    std::vector<ZoneEntry> zones;
    char rf_dict[3];   // l_returnflag dict: code→char
    char ls_dict[2];   // l_linestatus dict: code→char

    gendb::MmapColumn<int32_t> col_shipdate;
    gendb::MmapColumn<int8_t>  col_returnflag;
    gendb::MmapColumn<int8_t>  col_linestatus;
    gendb::MmapColumn<int8_t>  col_quantity;
    gendb::MmapColumn<double>  col_extprice;
    gendb::MmapColumn<int8_t>  col_discount;
    gendb::MmapColumn<int8_t>  col_tax;

    {
        GENDB_PHASE("data_loading");

        // Load zone map (guide format: uint32_t num_blocks, uint32_t block_size, ZE[]{mn,mx})
        {
            std::string zm_path = gendb_dir + "/lineitem/l_shipdate_zone_map.bin";
            FILE* zf = fopen(zm_path.c_str(), "rb");
            if (!zf) { fprintf(stderr, "Cannot open zone map: %s\n", zm_path.c_str()); return 1; }
            (void)fread(&num_blocks, 4, 1, zf);
            (void)fread(&block_size, 4, 1, zf);
            zones.resize(num_blocks);
            (void)fread(zones.data(), sizeof(ZoneEntry), num_blocks, zf);
            fclose(zf);
        }

        // Load dict sidecars
        {
            std::string rf_path = gendb_dir + "/lineitem/l_returnflag_dict.bin";
            FILE* f = fopen(rf_path.c_str(), "rb");
            if (!f) { fprintf(stderr, "Cannot open returnflag dict: %s\n", rf_path.c_str()); return 1; }
            (void)fread(rf_dict, 1, 3, f); fclose(f);
        }
        {
            std::string ls_path = gendb_dir + "/lineitem/l_linestatus_dict.bin";
            FILE* f = fopen(ls_path.c_str(), "rb");
            if (!f) { fprintf(stderr, "Cannot open linestatus dict: %s\n", ls_path.c_str()); return 1; }
            (void)fread(ls_dict, 1, 2, f); fclose(f);
        }

        // mmap all columns (zero-copy, MADV_SEQUENTIAL)
        col_shipdate.open  (gendb_dir + "/lineitem/l_shipdate.bin");
        col_returnflag.open(gendb_dir + "/lineitem/l_returnflag.bin");
        col_linestatus.open(gendb_dir + "/lineitem/l_linestatus.bin");
        col_quantity.open  (gendb_dir + "/lineitem/l_quantity.bin");
        col_extprice.open  (gendb_dir + "/lineitem/l_extendedprice.bin");
        col_discount.open  (gendb_dir + "/lineitem/l_discount.bin");
        col_tax.open       (gendb_dir + "/lineitem/l_tax.bin");

        // Fire MADV_WILLNEED on all columns to overlap I/O with setup
        mmap_prefetch_all(col_shipdate, col_returnflag, col_linestatus,
                          col_quantity, col_extprice, col_discount, col_tax);
    }

    const size_t total_rows = col_shipdate.count;

    // Pre-build list of qualifying block indices via zone map
    // lineitem sorted ASC by l_shipdate → only tail blocks have mn > THRESHOLD
    std::vector<uint32_t> qualifying_blocks;
    qualifying_blocks.reserve(num_blocks);
    for (uint32_t b = 0; b < num_blocks; b++) {
        if (zones[b].mn <= THRESHOLD) {
            qualifying_blocks.push_back(b);
        }
    }

    // -----------------------------------------------------------------------
    // Phase: main_scan (parallel morsel-driven)
    // -----------------------------------------------------------------------
    // gid = (rf_code<<1)|ls_code, rf_code ∈ {0,1,2}, ls_code ∈ {0,1} → gid ∈ 0..5
    const int num_threads = (int)std::thread::hardware_concurrency();
    std::vector<GroupAcc> thread_groups(num_threads * 6);  // [tid*6 + gid]

    {
        GENDB_PHASE("main_scan");

        std::atomic<uint32_t> next_block{0};
        const uint32_t num_qual = (uint32_t)qualifying_blocks.size();

        // Raw pointers for inner loop — avoid member access overhead
        const int32_t* __restrict__ shipdate   = col_shipdate.data;
        const int8_t*  __restrict__ returnflag = col_returnflag.data;
        const int8_t*  __restrict__ linestatus = col_linestatus.data;
        const int8_t*  __restrict__ quantity   = col_quantity.data;
        const double*  __restrict__ extprice   = col_extprice.data;
        const int8_t*  __restrict__ discount   = col_discount.data;
        const int8_t*  __restrict__ tax        = col_tax.data;

        auto worker = [&](int tid) {
            GroupAcc* __restrict__ local = &thread_groups[tid * 6];

            while (true) {
                uint32_t qi = next_block.fetch_add(1, std::memory_order_relaxed);
                if (qi >= num_qual) break;

                uint32_t b = qualifying_blocks[qi];
                size_t row_start = (size_t)b * block_size;
                size_t row_end   = std::min(row_start + (size_t)block_size, total_rows);

                // Inner loop — critical path
                // Per row: filter → group → integer-based accumulate
                for (size_t i = row_start; i < row_end; i++) {
                    if (shipdate[i] > THRESHOLD) continue;

                    int gid = ((uint8_t)returnflag[i] << 1) | (uint8_t)linestatus[i];

                    double   price  = extprice[i];
                    int      d_raw  = (uint8_t)discount[i];   // 0..10
                    int      t_raw  = (uint8_t)tax[i];        // 0..8

                    // Integer rep of price×100 (l_extendedprice is DECIMAL(15,2))
                    int64_t  N_price = (int64_t)(price * 100.0 + 0.5);
                    int64_t  nd      = 100 - d_raw;   // 90..100 (exact)
                    int64_t  nt      = 100 + t_raw;   // 100..108 (exact)

                    local[gid].sum_qty            += (uint8_t)quantity[i];
                    local[gid].sum_price          += price;
                    local[gid].sum_disc_price_int += N_price * nd;
                    local[gid].sum_charge_int     += N_price * nd * nt;
                    local[gid].sum_disc           += d_raw;
                    local[gid].count++;
                }
            }
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);
        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& th : threads) th.join();
    }

    // -----------------------------------------------------------------------
    // Phase: merge — sum thread-local GroupAcc[6] into global groups[6]
    // -----------------------------------------------------------------------
    GroupAcc groups[6] = {};
    {
        GENDB_PHASE("merge");
        for (int t = 0; t < num_threads; t++) {
            for (int g = 0; g < 6; g++) {
                const GroupAcc& src = thread_groups[t * 6 + g];
                groups[g].sum_qty            += src.sum_qty;
                groups[g].sum_price          += src.sum_price;
                groups[g].sum_disc_price_int += src.sum_disc_price_int;
                groups[g].sum_charge_int     += src.sum_charge_int;
                groups[g].sum_disc           += src.sum_disc;
                groups[g].count              += src.count;
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase: output — iterate groups 0..5 in lexicographic order
    // gid = (rf<<1)|ls: 0=(A,F),1=(A,O),2=(N,F),3=(N,O),4=(R,F),5=(R,O)
    // Dict codes preserve ORDER BY: A<N<R (0<1<2), F<O (0<1) — no sort needed
    // -----------------------------------------------------------------------
    {
        GENDB_PHASE("output");

        std::string out_path = results_dir + "/Q1.csv";
        FILE* out = fopen(out_path.c_str(), "w");
        if (!out) { fprintf(stderr, "Cannot open output: %s\n", out_path.c_str()); return 1; }

        fprintf(out, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
                     "sum_charge,avg_qty,avg_price,avg_disc,count_order\n");

        for (int rf = 0; rf <= 2; rf++) {
            for (int ls = 0; ls <= 1; ls++) {
                int gid = (rf << 1) | ls;
                const GroupAcc& g = groups[gid];
                if (g.count == 0) continue;

                double cnt        = (double)g.count;
                double disc_price = (double)g.sum_disc_price_int / 10000.0;
                double charge     = (double)g.sum_charge_int     / 1000000.0;
                double avg_qty    = (double)g.sum_qty   / cnt;
                double avg_price  = g.sum_price          / cnt;
                double avg_disc   = (double)g.sum_disc * 0.01 / cnt;

                char rf_char = rf_dict[rf];
                char ls_char = ls_dict[ls];

                fprintf(out, "%c,%c,%ld,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
                        rf_char, ls_char,
                        (long)g.sum_qty,
                        g.sum_price,
                        disc_price,
                        charge,
                        avg_qty,
                        avg_price,
                        avg_disc,
                        (long)g.count);
            }
        }

        fclose(out);
    }

    return 0;
}
