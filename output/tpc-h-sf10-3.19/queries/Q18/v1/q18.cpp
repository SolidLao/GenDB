#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

// Simple mmap helper returning pointer and size
struct MmapFile {
    void* ptr = nullptr;
    size_t size = 0;
    int fd = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path.c_str()); exit(1); }
    }

    void advise_seq() { if (ptr && size > 0) madvise(ptr, size, MADV_SEQUENTIAL); }
    void advise_random() { if (ptr && size > 0) madvise(ptr, size, MADV_RANDOM); }

    ~MmapFile() {
        if (ptr && ptr != MAP_FAILED) munmap(ptr, size);
        if (fd >= 0) ::close(fd);
    }
};

struct ResultTuple {
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double o_totalprice;
    double sum_qty;
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [--limit N] [--lower N]\n", argv[0]);
        return 1;
    }

    gendb::init_date_tables();

    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    int64_t param_limit = gendb::parse_int_arg(argc, argv, "--limit", 100);
    int64_t param_lower = gendb::parse_int_arg(argc, argv, "--lower", 300);
    double lower_threshold = (double)param_lower;
    int limit = (int)param_limit;

    GENDB_PHASE("total");

    // === Data Loading ===
    MmapFile f_grouped, f_l_quantity;
    MmapFile f_orders_lookup, f_o_custkey, f_o_orderdate, f_o_totalprice;
    MmapFile f_c_name_offsets, f_c_name_data;

    {
        GENDB_PHASE("data_loading");

        f_grouped.open(gendb_dir + "/indexes/lineitem_l_orderkey_grouped.bin");
        f_l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        f_l_quantity.advise_seq();

        f_orders_lookup.open(gendb_dir + "/indexes/orders_o_orderkey_lookup.bin");
        f_o_custkey.open(gendb_dir + "/orders/o_custkey.bin");
        f_o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
        f_o_totalprice.open(gendb_dir + "/orders/o_totalprice.bin");
        f_o_custkey.advise_random();
        f_o_orderdate.advise_random();
        f_o_totalprice.advise_random();

        f_c_name_offsets.open(gendb_dir + "/customer/c_name_offsets.bin");
        f_c_name_data.open(gendb_dir + "/customer/c_name_data.bin");
    }

    // Parse index headers
    uint64_t grouped_num_entries = *(const uint64_t*)f_grouped.ptr;
    const uint32_t* grouped_body = (const uint32_t*)((const char*)f_grouped.ptr + sizeof(uint64_t));

    uint64_t lookup_num_entries = *(const uint64_t*)f_orders_lookup.ptr;
    const int32_t* orders_lookup = (const int32_t*)((const char*)f_orders_lookup.ptr + sizeof(uint64_t));

    const double* l_quantity = (const double*)f_l_quantity.ptr;
    const int32_t* o_custkey = (const int32_t*)f_o_custkey.ptr;
    const int32_t* o_orderdate = (const int32_t*)f_o_orderdate.ptr;
    const double* o_totalprice = (const double*)f_o_totalprice.ptr;

    const uint32_t* c_name_offsets = (const uint32_t*)f_c_name_offsets.ptr;
    const char* c_name_data = (const char*)f_c_name_data.ptr;

    // === Subquery: Find orderkeys with SUM(l_quantity) > threshold ===
    std::vector<ResultTuple> results;

    {
        GENDB_PHASE("main_scan");

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<std::pair<int32_t, double>>> thread_qualifying(nthreads);

        // Pre-reserve
        for (auto& v : thread_qualifying) v.reserve(20000 / nthreads + 1000);

        uint64_t n = grouped_num_entries;

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local = thread_qualifying[tid];

            #pragma omp for schedule(dynamic, 16384)
            for (uint64_t k = 0; k < n; k++) {
                uint32_t count = grouped_body[k * 2 + 1];
                if (count == 0) continue;

                uint32_t start = grouped_body[k * 2];
                double sum = 0.0;
                const double* qptr = l_quantity + start;

                // Vectorizable sum
                for (uint32_t i = 0; i < count; i++) {
                    sum += qptr[i];
                }

                if (sum > lower_threshold) {
                    local.emplace_back((int32_t)k, sum);
                }
            }
        }

        // Merge thread results and do orders + customer lookup
        size_t total_qualifying = 0;
        for (auto& v : thread_qualifying) total_qualifying += v.size();
        results.reserve(total_qualifying);

        for (auto& v : thread_qualifying) {
            for (auto& [okey, sum_qty] : v) {
                // Orders lookup
                if ((uint64_t)okey >= lookup_num_entries) continue;
                int32_t row_id = orders_lookup[okey];
                if (row_id < 0) continue;

                ResultTuple rt;
                rt.o_orderkey = okey;
                rt.sum_qty = sum_qty;
                rt.c_custkey = o_custkey[row_id];
                rt.o_orderdate = o_orderdate[row_id];
                rt.o_totalprice = o_totalprice[row_id];
                results.push_back(rt);
            }
        }
    }

    // === Top-K Sort ===
    {
        GENDB_PHASE("topk_sort");

        int k = std::min(limit, (int)results.size());
        if (k > 0) {
            std::partial_sort(results.begin(), results.begin() + k, results.end(),
                [](const ResultTuple& a, const ResultTuple& b) {
                    if (a.o_totalprice != b.o_totalprice)
                        return a.o_totalprice > b.o_totalprice;
                    if (a.o_orderdate != b.o_orderdate)
                        return a.o_orderdate < b.o_orderdate;
                    return a.o_orderkey < b.o_orderkey;
                });
            results.resize(k);
        }
    }

    // === Output ===
    {
        GENDB_PHASE("output");

        std::string outpath = results_dir + "/Q18.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        if (!fp) { fprintf(stderr, "Cannot open output %s\n", outpath.c_str()); return 1; }

        fprintf(fp, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        char datebuf[16];
        for (auto& r : results) {
            // Decode c_name for this row
            int32_t ck = r.c_custkey;
            int row = ck - 1; // dense 1-based PKs
            uint32_t off_start = c_name_offsets[row];
            uint32_t off_end = c_name_offsets[row + 1];

            gendb::epoch_days_to_date_str(r.o_orderdate, datebuf);

            fprintf(fp, "\"%.*s\",%d,%d,%s,%.2f,%.2f\n",
                (int)(off_end - off_start), c_name_data + off_start,
                r.c_custkey,
                r.o_orderkey,
                datebuf,
                r.o_totalprice,
                r.sum_qty);
        }

        fclose(fp);
    }

    return 0;
}
