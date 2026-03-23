#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
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
T* mmap_file(const std::string& path, size_t& len) {
    int fd = open(path.c_str(), O_RDONLY);
    struct stat st;
    fstat(fd, &st);
    len = st.st_size;
    void* ptr = mmap(nullptr, len, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    madvise(ptr, len, MADV_SEQUENTIAL);
    close(fd);
    return reinterpret_cast<T*>(ptr);
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();
    int32_t param_date = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper",
        gendb::date_str_to_epoch_days("1998-12-01"));
    int32_t threshold = gendb::add_days(param_date, -90);

    GENDB_PHASE("total");

    size_t len;
    const int32_t* shipdate;
    const int8_t* returnflag;
    const int8_t* linestatus;
    const double* quantity;
    const double* extendedprice;
    const double* discount;
    const double* tax;

    uint64_t zm_num_blocks;
    const int32_t* zm_body;

    {
        GENDB_PHASE("data_loading");
        shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", len);
        returnflag = mmap_file<int8_t>(gendb_dir + "/lineitem/l_returnflag.bin", len);
        linestatus = mmap_file<int8_t>(gendb_dir + "/lineitem/l_linestatus.bin", len);
        quantity = mmap_file<double>(gendb_dir + "/lineitem/l_quantity.bin", len);
        extendedprice = mmap_file<double>(gendb_dir + "/lineitem/l_extendedprice.bin", len);
        discount = mmap_file<double>(gendb_dir + "/lineitem/l_discount.bin", len);
        tax = mmap_file<double>(gendb_dir + "/lineitem/l_tax.bin", len);

        int fd = open((gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin").c_str(), O_RDONLY);
        struct stat st;
        fstat(fd, &st);
        void* zm_ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        close(fd);
        zm_num_blocks = *reinterpret_cast<uint64_t*>(zm_ptr);
        (void)*reinterpret_cast<uint32_t*>((char*)zm_ptr + 8); // block_size, unused
        zm_body = reinterpret_cast<const int32_t*>((char*)zm_ptr + 12);
    }

    // Lookup tables for compact key
    int rf_map[256] = {};
    rf_map['A'] = 0; rf_map['N'] = 1; rf_map['R'] = 2;
    int ls_map[256] = {};
    ls_map['F'] = 0; ls_map['O'] = 1;

    int num_threads = omp_get_max_threads();

    // Thread-local accumulators
    alignas(64) Accum thread_accs[256][6];
    memset(thread_accs, 0, sizeof(thread_accs));

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            Accum* local = thread_accs[tid];

            #pragma omp for schedule(dynamic, 4)
            for (int64_t blk = 0; blk < (int64_t)zm_num_blocks; blk++) {
                int32_t zm_min = zm_body[blk * 2];
                int32_t zm_max = zm_body[blk * 2 + 1];

                if (zm_min > threshold) continue;

                int64_t start = blk * BLOCK_SIZE;
                int64_t end = start + BLOCK_SIZE;
                if (end > TOTAL_ROWS) end = TOTAL_ROWS;

                if (zm_max <= threshold) {
                    for (int64_t i = start; i < end; i++) {
                        int key = rf_map[(unsigned char)returnflag[i]] * 2 + ls_map[(unsigned char)linestatus[i]];
                        double ep = extendedprice[i];
                        double disc = discount[i];
                        double one_minus_disc = 1.0 - disc;
                        double dp = ep * one_minus_disc;

                        Accum& a = local[key];
                        a.sum_qty += quantity[i];
                        a.sum_base_price += ep;
                        KAHAN_ADD(a.sum_disc_price, a.sum_disc_price_c, dp);
                        KAHAN_ADD(a.sum_charge, a.sum_charge_c, dp * (1.0 + tax[i]));
                        a.sum_discount += disc;
                        a.count++;
                    }
                } else {
                    for (int64_t i = start; i < end; i++) {
                        if (shipdate[i] > threshold) continue;
                        int key = rf_map[(unsigned char)returnflag[i]] * 2 + ls_map[(unsigned char)linestatus[i]];
                        double ep = extendedprice[i];
                        double disc = discount[i];
                        double one_minus_disc = 1.0 - disc;
                        double dp = ep * one_minus_disc;

                        Accum& a = local[key];
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

    // Merge
    Accum global[6] = {};
    {
        GENDB_PHASE("aggregation");
        for (int t = 0; t < num_threads; t++) {
            for (int g = 0; g < 6; g++) {
                global[g].sum_qty += thread_accs[t][g].sum_qty;
                global[g].sum_base_price += thread_accs[t][g].sum_base_price;
                KAHAN_ADD(global[g].sum_disc_price, global[g].sum_disc_price_c,
                    thread_accs[t][g].sum_disc_price);
                KAHAN_ADD(global[g].sum_charge, global[g].sum_charge_c,
                    thread_accs[t][g].sum_charge);
                global[g].sum_discount += thread_accs[t][g].sum_discount;
                global[g].count += thread_accs[t][g].count;
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
            double avg_qty = a.sum_qty / a.count;
            double avg_price = a.sum_base_price / a.count;
            double avg_disc = a.sum_discount / a.count;
            fprintf(f, "%c,%c,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f\n",
                rf_chars[g], ls_chars[g],
                a.sum_qty, a.sum_base_price, a.sum_disc_price, a.sum_charge,
                avg_qty, avg_price, avg_disc, (double)a.count);
        }
        fclose(f);
    }

    return 0;
}
