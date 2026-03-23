#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <array>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

static const int64_t TOTAL_ROWS = 59986052;
static const int BLOCK_SIZE = 65536;

struct Accum {
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_disc_price_c; // Kahan compensation
    double sum_charge;
    double sum_charge_c;     // Kahan compensation
    double sum_discount;
    int64_t count;
};

#define KAHAN_ADD(sum, comp, val) do { \
    double y = (val) - (comp); \
    double t = (sum) + y; \
    (comp) = (t - (sum)) - y; \
    (sum) = t; \
} while(0)

template<typename T>
static T* mmap_col(const std::string& path, size_t num_rows) {
    int fd = open(path.c_str(), O_RDONLY);
    size_t sz = num_rows * sizeof(T);
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    madvise(ptr, sz, MADV_SEQUENTIAL);
    madvise(ptr, sz, MADV_HUGEPAGE);
    return (T*)ptr;
}

int main(int argc, char* argv[]) {
    gendb::init_date_tables();

    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    int32_t param_date = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper",
        gendb::date_str_to_epoch_days("1998-12-01"));
    int32_t threshold = gendb::add_days(param_date, -90);

    GENDB_PHASE("total");

    const int32_t* shipdate;
    const int8_t* returnflag;
    const int8_t* linestatus;
    const double* quantity;
    const double* extendedprice;
    const double* discount;
    const double* tax;

    uint64_t zm_num_blocks;
    const int32_t* zm_body;
    void* zm_ptr;
    size_t zm_size;

    {
        GENDB_PHASE("data_loading");
        shipdate     = mmap_col<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", TOTAL_ROWS);
        returnflag   = mmap_col<int8_t>(gendb_dir + "/lineitem/l_returnflag.bin", TOTAL_ROWS);
        linestatus   = mmap_col<int8_t>(gendb_dir + "/lineitem/l_linestatus.bin", TOTAL_ROWS);
        quantity     = mmap_col<double>(gendb_dir + "/lineitem/l_quantity.bin", TOTAL_ROWS);
        extendedprice= mmap_col<double>(gendb_dir + "/lineitem/l_extendedprice.bin", TOTAL_ROWS);
        discount     = mmap_col<double>(gendb_dir + "/lineitem/l_discount.bin", TOTAL_ROWS);
        tax          = mmap_col<double>(gendb_dir + "/lineitem/l_tax.bin", TOTAL_ROWS);

        std::string zm_path = gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin";
        int fd = open(zm_path.c_str(), O_RDONLY);
        struct stat st;
        fstat(fd, &st);
        zm_size = st.st_size;
        zm_ptr = mmap(nullptr, zm_size, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        zm_num_blocks = *reinterpret_cast<uint64_t*>(zm_ptr);
        zm_body = reinterpret_cast<const int32_t*>((char*)zm_ptr + 12);
    }

    // Compact key lookup: rf_map['A']=0, 'N'=1, 'R'=2; ls_map['F']=0, 'O'=1
    // key = rf_idx * 2 + ls_idx
    uint8_t rf_map[256] = {};
    uint8_t ls_map[256] = {};
    rf_map['A'] = 0; rf_map['N'] = 1; rf_map['R'] = 2;
    ls_map['F'] = 0; ls_map['O'] = 1;

    int nthreads = omp_get_max_threads();

    // Thread-local accumulators - dynamically sized, cache-line padded
    // Each thread slot is padded to avoid false sharing
    struct alignas(64) ThreadSlot {
        Accum groups[6];
    };
    std::vector<ThreadSlot> thread_accs(nthreads);
    memset(thread_accs.data(), 0, nthreads * sizeof(ThreadSlot));

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            Accum* local = thread_accs[tid].groups;

            #pragma omp for schedule(dynamic, 4)
            for (int64_t blk = 0; blk < (int64_t)zm_num_blocks; blk++) {
                int32_t zm_min = zm_body[blk * 2];
                int32_t zm_max = zm_body[blk * 2 + 1];

                if (zm_min > threshold) continue;

                int64_t start = blk * BLOCK_SIZE;
                int64_t end = start + BLOCK_SIZE;
                if (end > TOTAL_ROWS) end = TOTAL_ROWS;

                if (zm_max <= threshold) {
                    // All rows pass - no per-row date check
                    for (int64_t i = start; i < end; i++) {
                        uint8_t key = rf_map[(uint8_t)returnflag[i]] * 2
                                    + ls_map[(uint8_t)linestatus[i]];
                        Accum& a = local[key];
                        double ep = extendedprice[i];
                        double disc = discount[i];
                        double dp = ep * (1.0 - disc);
                        a.sum_qty += quantity[i];
                        a.sum_base_price += ep;
                        KAHAN_ADD(a.sum_disc_price, a.sum_disc_price_c, dp);
                        KAHAN_ADD(a.sum_charge, a.sum_charge_c, dp * (1.0 + tax[i]));
                        a.sum_discount += disc;
                        a.count++;
                    }
                } else {
                    // Partial block - per-row date check
                    for (int64_t i = start; i < end; i++) {
                        if (shipdate[i] > threshold) continue;
                        uint8_t key = rf_map[(uint8_t)returnflag[i]] * 2
                                    + ls_map[(uint8_t)linestatus[i]];
                        Accum& a = local[key];
                        double ep = extendedprice[i];
                        double disc = discount[i];
                        double dp = ep * (1.0 - disc);
                        a.sum_qty += quantity[i];
                        a.sum_base_price += ep;
                        KAHAN_ADD(a.sum_disc_price, a.sum_disc_price_c, dp);
                        KAHAN_ADD(a.sum_charge, a.sum_charge_c, dp * (1.0 + tax[i]));
                        a.sum_discount += disc;
                        a.count++;
                    }
                }
            }
        }
    }

    // Merge thread-local accumulators
    Accum global[6] = {};
    {
        GENDB_PHASE("aggregation");
        for (int t = 0; t < nthreads; t++) {
            for (int g = 0; g < 6; g++) {
                global[g].sum_qty += thread_accs[t].groups[g].sum_qty;
                global[g].sum_base_price += thread_accs[t].groups[g].sum_base_price;
                KAHAN_ADD(global[g].sum_disc_price, global[g].sum_disc_price_c,
                    thread_accs[t].groups[g].sum_disc_price);
                KAHAN_ADD(global[g].sum_charge, global[g].sum_charge_c,
                    thread_accs[t].groups[g].sum_charge);
                global[g].sum_discount += thread_accs[t].groups[g].sum_discount;
                global[g].count += thread_accs[t].groups[g].count;
            }
        }
    }

    // Output
    {
        GENDB_PHASE("output");
        const char rf_chars[] = {'A', 'A', 'N', 'N', 'R', 'R'};
        const char ls_chars[] = {'F', 'O', 'F', 'O', 'F', 'O'};

        std::string outpath = results_dir + "/Q1.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
        for (int g = 0; g < 6; g++) {
            if (global[g].count == 0) continue;
            Accum& a = global[g];
            fprintf(f, "%c,%c,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n",
                rf_chars[g], ls_chars[g],
                a.sum_qty, a.sum_base_price, a.sum_disc_price, a.sum_charge,
                a.sum_qty / a.count, a.sum_base_price / a.count,
                a.sum_discount / a.count, (double)a.count);
        }
        fclose(f);
    }

    // Cleanup
    munmap((void*)shipdate, TOTAL_ROWS * sizeof(int32_t));
    munmap((void*)returnflag, TOTAL_ROWS * sizeof(int8_t));
    munmap((void*)linestatus, TOTAL_ROWS * sizeof(int8_t));
    munmap((void*)quantity, TOTAL_ROWS * sizeof(double));
    munmap((void*)extendedprice, TOTAL_ROWS * sizeof(double));
    munmap((void*)discount, TOTAL_ROWS * sizeof(double));
    munmap((void*)tax, TOTAL_ROWS * sizeof(double));
    munmap(zm_ptr, zm_size);

    return 0;
}
