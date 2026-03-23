#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
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
static T* mmap_column(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st;
    fstat(fd, &st);
    count = st.st_size / sizeof(T);
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
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

    // Parse parameters
    int32_t p_shipdate_lower = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower", 8766);
    int32_t p_shipdate_upper_base = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper", 8766);
    // Correctness anchor: add_years(p_shipdate_upper_base, 1)
    int32_t p_shipdate_upper = gendb::add_years(p_shipdate_upper_base, 1);
    double p_discount_lower = std::nextafter(
        gendb::parse_double_arg(argc, argv, "--l_discount_lower", 0.05),
        -std::numeric_limits<double>::infinity());
    double p_discount_upper = std::nextafter(
        gendb::parse_double_arg(argc, argv, "--l_discount_upper", 0.07),
        std::numeric_limits<double>::infinity());
    double p_quantity_upper = gendb::parse_double_arg(argc, argv, "--l_quantity_upper", 24.0);

    GENDB_PHASE("total");

    const int32_t* l_shipdate;
    const double* l_discount;
    const double* l_quantity;
    const double* l_extendedprice;
    size_t num_rows;
    uint64_t zm_num_blocks;
    uint32_t zm_block_size;
    const int32_t* zm_data;

    {
        GENDB_PHASE("data_loading");
        size_t tmp;
        l_shipdate = mmap_column<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", num_rows);
        l_discount = mmap_column<double>(gendb_dir + "/lineitem/l_discount.bin", tmp);
        l_quantity = mmap_column<double>(gendb_dir + "/lineitem/l_quantity.bin", tmp);
        l_extendedprice = mmap_column<double>(gendb_dir + "/lineitem/l_extendedprice.bin", tmp);

        // Load zonemap
        int fd = open((gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin").c_str(), O_RDONLY);
        struct stat st;
        fstat(fd, &st);
        void* zm_ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        close(fd);
        zm_num_blocks = *reinterpret_cast<const uint64_t*>(zm_ptr);
        zm_block_size = *reinterpret_cast<const uint32_t*>(reinterpret_cast<const char*>(zm_ptr) + 8);
        zm_data = reinterpret_cast<const int32_t*>(reinterpret_cast<const char*>(zm_ptr) + 12);

        // Hint sequential access for all columns
        madvise((void*)l_shipdate, num_rows * sizeof(int32_t), MADV_SEQUENTIAL);
        madvise((void*)l_discount, num_rows * sizeof(double), MADV_SEQUENTIAL);
        madvise((void*)l_quantity, num_rows * sizeof(double), MADV_SEQUENTIAL);
        madvise((void*)l_extendedprice, num_rows * sizeof(double), MADV_SEQUENTIAL);
    }

    // Zonemap filter — identify qualifying blocks
    std::vector<uint32_t> qual_blocks;
    {
        GENDB_PHASE("zonemap_filter");
        qual_blocks.reserve(256);
        for (uint64_t b = 0; b < zm_num_blocks; b++) {
            int32_t bmin = zm_data[b * 2];
            int32_t bmax = zm_data[b * 2 + 1];
            if (bmin >= p_shipdate_upper || bmax < p_shipdate_lower) continue;
            qual_blocks.push_back((uint32_t)b);
        }
    }

    double final_revenue = 0.0;

    {
        GENDB_PHASE("main_scan");

        const int n_qual = (int)qual_blocks.size();

        #pragma omp parallel reduction(+:final_revenue)
        {
            double local_sum = 0.0;

            #pragma omp for schedule(dynamic, 2) nowait
            for (int qi = 0; qi < n_qual; qi++) {
                uint32_t b = qual_blocks[qi];
                size_t row_start = (size_t)b * zm_block_size;
                size_t row_end = row_start + zm_block_size;
                if (row_end > num_rows) row_end = num_rows;

                const int32_t* __restrict__ sd = l_shipdate + row_start;
                const double* __restrict__ disc = l_discount + row_start;
                const double* __restrict__ qty = l_quantity + row_start;
                const double* __restrict__ ep = l_extendedprice + row_start;
                const size_t cnt = row_end - row_start;

                for (size_t i = 0; i < cnt; i++) {
                    int32_t s = sd[i];
                    if (s >= p_shipdate_lower && s < p_shipdate_upper) {
                        double d = disc[i];
                        if (d >= p_discount_lower && d <= p_discount_upper) {
                            if (qty[i] < p_quantity_upper) {
                                local_sum += ep[i] * d;
                            }
                        }
                    }
                }
            }

            final_revenue += local_sum;
        }
    }

    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q6.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        fprintf(f, "revenue\n%.4f\n", final_revenue);
        fclose(f);
    }

    return 0;
}
