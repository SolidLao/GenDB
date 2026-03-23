#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <algorithm>

#include "timing_utils.h"
#include "cli_params.h"

struct MmapFile {
    void* data;
    size_t size;
    MmapFile() : data(nullptr), size(0) {}
    void open(const std::string& path, int advice = MADV_SEQUENTIAL) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::perror(path.c_str()); std::exit(1); }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
        madvise(data, size, advice);
        ::close(fd);
    }
    ~MmapFile() { if (data && data != MAP_FAILED) munmap(data, size); }
};

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    double c_acctbal_lower = gendb::parse_double_arg(argc, argv, "--c_acctbal_lower", 0.0);

    // Mmap all files outside timing scope (avoid TLB shootdown in munmap)
    MmapFile mf_o_custkey, mf_c_custkey, mf_c_acctbal, mf_c_phone_offsets, mf_c_phone_data;
    mf_o_custkey.open(gendb_dir + "/orders/o_custkey.bin", MADV_SEQUENTIAL);
    mf_c_custkey.open(gendb_dir + "/customer/c_custkey.bin", MADV_SEQUENTIAL);
    mf_c_acctbal.open(gendb_dir + "/customer/c_acctbal.bin", MADV_SEQUENTIAL);
    mf_c_phone_offsets.open(gendb_dir + "/customer/c_phone_offsets.bin", MADV_SEQUENTIAL);
    mf_c_phone_data.open(gendb_dir + "/customer/c_phone_data.bin", MADV_RANDOM);

    const int32_t* o_custkey = (const int32_t*)mf_o_custkey.data;
    const int64_t num_orders = mf_o_custkey.size / sizeof(int32_t);

    const int32_t* c_custkey = (const int32_t*)mf_c_custkey.data;
    const double* c_acctbal = (const double*)mf_c_acctbal.data;
    const uint32_t* c_phone_offsets = (const uint32_t*)mf_c_phone_offsets.data;
    const char* c_phone_data = (const char*)mf_c_phone_data.data;
    const int64_t num_customers = mf_c_acctbal.size / sizeof(double);

    // Country code lookup: bool[40]
    bool cc_valid[40] = {};
    cc_valid[13] = true; cc_valid[17] = true; cc_valid[18] = true;
    cc_valid[23] = true; cc_valid[29] = true; cc_valid[30] = true; cc_valid[31] = true;

    // Map country code int -> bucket index (0-6) for aggregation
    int cc_to_bucket[40];
    std::memset(cc_to_bucket, -1, sizeof(cc_to_bucket));
    int codes[] = {13, 17, 18, 23, 29, 30, 31}; // sorted
    for (int i = 0; i < 7; i++) cc_to_bucket[codes[i]] = i;

    // Find max custkey for bitset sizing
    int64_t max_custkey = 1500000; // TPC-H SF10
    // Be safe: check from metadata
    if (num_customers > max_custkey) max_custkey = num_customers;
    int64_t bitset_words = (max_custkey + 64) / 64;

    {
    GENDB_PHASE("total");

    // Phase 1: Build has_orders bitset
    uint64_t* has_orders = nullptr;
    {
        GENDB_PHASE("build_has_orders_bitset");
        has_orders = (uint64_t*)std::calloc(bitset_words, sizeof(uint64_t));

        #pragma omp parallel
        {
            // Thread-local bitset
            uint64_t* local_bs = (uint64_t*)std::calloc(bitset_words, sizeof(uint64_t));

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < num_orders; i++) {
                int32_t ck = o_custkey[i];
                local_bs[ck >> 6] |= (1ULL << (ck & 63));
            }

            // Merge
            #pragma omp critical
            {
                for (int64_t w = 0; w < bitset_words; w++)
                    has_orders[w] |= local_bs[w];
            }

            std::free(local_bs);
        }
    }

    // Phase 2: Compute avg(c_acctbal) where acctbal > c_acctbal_lower AND cc in target
    double avg_acctbal = 0.0;
    {
        GENDB_PHASE("compute_avg_acctbal");
        double total_sum = 0.0;
        int64_t total_count = 0;

        #pragma omp parallel reduction(+:total_sum,total_count)
        {
            #pragma omp for schedule(static)
            for (int64_t i = 0; i < num_customers; i++) {
                uint32_t off = c_phone_offsets[i];
                int cc = (c_phone_data[off] - '0') * 10 + (c_phone_data[off + 1] - '0');
                if (cc < 40 && cc_valid[cc] && c_acctbal[i] > c_acctbal_lower) {
                    total_sum += c_acctbal[i];
                    total_count++;
                }
            }
        }

        if (total_count > 0) avg_acctbal = total_sum / total_count;
    }

    // Phase 3: Main scan - filter and aggregate
    struct Bucket { int64_t count; double sum; };
    Bucket results[7] = {};
    {
        GENDB_PHASE("filter_aggregate");

        #pragma omp parallel
        {
            Bucket local_res[7] = {};

            #pragma omp for schedule(static)
            for (int64_t i = 0; i < num_customers; i++) {
                // Country code check first (cheapest)
                uint32_t off = c_phone_offsets[i];
                int cc = (c_phone_data[off] - '0') * 10 + (c_phone_data[off + 1] - '0');
                if (cc >= 40 || !cc_valid[cc]) continue;

                // Acctbal check
                double bal = c_acctbal[i];
                if (bal <= avg_acctbal) continue;

                // Anti-join: customer must NOT have orders
                int32_t ck = c_custkey[i];
                if (has_orders[ck >> 6] & (1ULL << (ck & 63))) continue;

                int bucket = cc_to_bucket[cc];
                local_res[bucket].count++;
                local_res[bucket].sum += bal;
            }

            #pragma omp critical
            {
                for (int b = 0; b < 7; b++) {
                    results[b].count += local_res[b].count;
                    results[b].sum += local_res[b].sum;
                }
            }
        }
    }

    // Phase 4: Output
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q22.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        std::fprintf(fp, "cntrycode,numcust,totacctbal\n");
        // codes[] is already sorted: 13,17,18,23,29,30,31
        for (int i = 0; i < 7; i++) {
            std::fprintf(fp, "%d,%ld,%.2f\n", codes[i], results[i].count, results[i].sum);
        }
        std::fclose(fp);
    }

    std::free(has_orders);
    } // end total

    return 0;
}
