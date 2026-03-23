#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>
#include <limits>
#include <omp.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

template<typename T>
static T* mmap_file(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    size_t sz = st.st_size;
    count = sz / sizeof(T);
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    close(fd);
    return reinterpret_cast<T*>(ptr);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params...]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();

    // Parse parameters — l_shipdate_upper default is base date, add 1 year for actual bound
    int32_t p_shipdate_lower = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower", 8766);
    int32_t p_shipdate_upper_base = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper", 8766);
    int32_t p_shipdate_upper = gendb::add_years(p_shipdate_upper_base, 1);
    // Round discount bounds by 1 ULP outward to handle floating-point edge cases
    // (e.g., CLI passes 0.06999999999999999 which is 1 ULP below 0.07)
    double p_discount_lower = std::nextafter(
        gendb::parse_double_arg(argc, argv, "--l_discount_lower", 0.05),
        -std::numeric_limits<double>::infinity());
    double p_discount_upper = std::nextafter(
        gendb::parse_double_arg(argc, argv, "--l_discount_upper", 0.07),
        std::numeric_limits<double>::infinity());
    double p_quantity_upper = gendb::parse_double_arg(argc, argv, "--l_quantity_upper", 24.0);

    GENDB_PHASE("total");

    // Data loading
    const int32_t* l_shipdate;
    const double* l_discount;
    const double* l_quantity;
    const double* l_extendedprice;
    size_t num_rows;

    // Zonemap
    uint64_t zm_num_blocks;
    uint32_t zm_block_size;
    const int32_t* zm_data;

    {
        GENDB_PHASE("data_loading");
        size_t n1, n2, n3, n4;
        l_shipdate = mmap_file<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", n1);
        l_discount = mmap_file<double>(gendb_dir + "/lineitem/l_discount.bin", n2);
        l_quantity = mmap_file<double>(gendb_dir + "/lineitem/l_quantity.bin", n3);
        l_extendedprice = mmap_file<double>(gendb_dir + "/lineitem/l_extendedprice.bin", n4);
        num_rows = n1;

        // Load zonemap
        int fd = open((gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin").c_str(), O_RDONLY);
        if (fd < 0) { perror("zonemap open"); exit(1); }
        struct stat st;
        fstat(fd, &st);
        void* zm_ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (zm_ptr == MAP_FAILED) { perror("zonemap mmap"); exit(1); }
        close(fd);

        // Header: uint64_t num_blocks, uint32_t block_size
        zm_num_blocks = *reinterpret_cast<const uint64_t*>(zm_ptr);
        zm_block_size = *reinterpret_cast<const uint32_t*>(reinterpret_cast<const char*>(zm_ptr) + 8);
        zm_data = reinterpret_cast<const int32_t*>(reinterpret_cast<const char*>(zm_ptr) + 12);
    }

    double final_revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        int num_threads = omp_get_max_threads();
        double* thread_sums = new double[num_threads]();

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double local_sum = 0.0;

            #pragma omp for schedule(dynamic, 4)
            for (int64_t b = 0; b < (int64_t)zm_num_blocks; b++) {
                // Zonemap check: skip if block is entirely outside date range
                int32_t block_min = zm_data[b * 2];
                int32_t block_max = zm_data[b * 2 + 1];
                if (block_min >= p_shipdate_upper || block_max < p_shipdate_lower) continue;

                size_t row_start = (size_t)b * zm_block_size;
                size_t row_end = row_start + zm_block_size;
                if (row_end > num_rows) row_end = num_rows;

                for (size_t i = row_start; i < row_end; i++) {
                    // Filter order: shipdate (0.15) → discount (0.27) → quantity (0.46)
                    int32_t sd = l_shipdate[i];
                    if (sd < p_shipdate_lower || sd >= p_shipdate_upper) continue;
                    double disc = l_discount[i];
                    if (disc < p_discount_lower || disc > p_discount_upper) continue;
                    if (l_quantity[i] >= p_quantity_upper) continue;
                    local_sum += l_extendedprice[i] * disc;
                }
            }

            thread_sums[tid] = local_sum;
        }

        for (int t = 0; t < num_threads; t++) {
            final_revenue += thread_sums[t];
        }
        delete[] thread_sums;
    }

    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { perror("fopen"); return 1; }
        fprintf(f, "revenue\n%.4f\n", final_revenue);
        fclose(f);
    }

    return 0;
}
