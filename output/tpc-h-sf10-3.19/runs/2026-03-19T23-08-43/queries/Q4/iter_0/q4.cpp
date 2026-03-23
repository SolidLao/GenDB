#include <cstdio>
#include <cstdint>
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

struct MmapFile {
    void* data;
    size_t size;
    int fd;
    MmapFile() : data(nullptr), size(0), fd(-1) {}
    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::perror(path.c_str()); std::exit(1); }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
    }
    void advise(int advice) {
        madvise(data, size, advice);
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) ::close(fd);
    }
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [--param val ...]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();
    int32_t o_orderdate_lower = gendb::parse_date_arg(argc, argv, "--o_orderdate_lower", 8582);
    int32_t o_orderdate_upper_base = gendb::parse_date_arg(argc, argv, "--o_orderdate_upper", 8582);
    // The upper bound param is the base date; add 3 months for the interval
    int32_t o_orderdate_upper = gendb::add_months(o_orderdate_upper_base, 3);

    // Mmap all files BEFORE timing scope (exclude munmap TLB shootdown)
    MmapFile f_o_orderdate, f_o_orderkey, f_o_orderpriority;
    MmapFile f_zonemap, f_grouped_idx;
    MmapFile f_l_commitdate, f_l_receiptdate;
    MmapFile f_dict;

    f_o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
    f_o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
    f_o_orderpriority.open(gendb_dir + "/orders/o_orderpriority.bin");
    f_zonemap.open(gendb_dir + "/indexes/orders_o_orderdate_zonemap.bin");
    f_grouped_idx.open(gendb_dir + "/indexes/lineitem_l_orderkey_grouped.bin");
    f_l_commitdate.open(gendb_dir + "/lineitem/l_commitdate.bin");
    f_l_receiptdate.open(gendb_dir + "/lineitem/l_receiptdate.bin");
    f_dict.open(gendb_dir + "/orders/o_orderpriority_dict.bin");

    f_o_orderdate.advise(MADV_SEQUENTIAL | MADV_HUGEPAGE);
    f_o_orderkey.advise(MADV_SEQUENTIAL | MADV_HUGEPAGE);
    f_o_orderpriority.advise(MADV_SEQUENTIAL | MADV_HUGEPAGE);
    f_l_commitdate.advise(MADV_SEQUENTIAL | MADV_HUGEPAGE);
    f_l_receiptdate.advise(MADV_SEQUENTIAL | MADV_HUGEPAGE);

    const int32_t* o_orderdate = (const int32_t*)f_o_orderdate.data;
    const int32_t* o_orderkey = (const int32_t*)f_o_orderkey.data;
    const int8_t*  o_orderpriority = (const int8_t*)f_o_orderpriority.data;
    const int32_t* l_commitdate = (const int32_t*)f_l_commitdate.data;
    const int32_t* l_receiptdate = (const int32_t*)f_l_receiptdate.data;

    uint64_t num_orders = f_o_orderdate.size / sizeof(int32_t);

    // Zonemap header
    const uint8_t* zm_raw = (const uint8_t*)f_zonemap.data;
    uint64_t zm_num_blocks = *(const uint64_t*)zm_raw;
    uint32_t zm_block_size = *(const uint32_t*)(zm_raw + 8);
    const int32_t* zm_body = (const int32_t*)(zm_raw + 12);

    // Grouped index header
    const uint8_t* gi_raw = (const uint8_t*)f_grouped_idx.data;
    uint64_t gi_num_entries = *(const uint64_t*)gi_raw;
    const uint32_t* gi_body = (const uint32_t*)(gi_raw + 8);

    // Load dictionary
    const uint8_t* dict_raw = (const uint8_t*)f_dict.data;
    uint32_t dict_count = *(const uint32_t*)dict_raw;
    std::vector<std::string> dict_strings(dict_count);
    const uint8_t* dp = dict_raw + 4;
    for (uint32_t i = 0; i < dict_count; i++) {
        uint16_t len = *(const uint16_t*)dp;
        dp += 2;
        dict_strings[i] = std::string((const char*)dp, len);
        dp += len;
    }

    {
        GENDB_PHASE("total");

        // Step 1: Zonemap filter - find qualifying blocks
        std::vector<uint32_t> qual_blocks;
        {
            GENDB_PHASE("zonemap_filter");
            qual_blocks.reserve(zm_num_blocks);
            for (uint64_t b = 0; b < zm_num_blocks; b++) {
                int32_t bmin = zm_body[b * 2];
                int32_t bmax = zm_body[b * 2 + 1];
                if (bmax >= o_orderdate_lower && bmin < o_orderdate_upper) {
                    qual_blocks.push_back((uint32_t)b);
                }
            }
        }

        // Since orders table is sorted by o_orderkey, and blocks are sequential,
        // qualifying rows are already in orderkey order. Skip the sort entirely.
        // Single pass: scan qualifying blocks, for each qualifying row immediately
        // probe lineitem index and aggregate.

        uint32_t num_qual_blocks = (uint32_t)qual_blocks.size();
        int64_t global_counts[5] = {0, 0, 0, 0, 0};

        {
            GENDB_PHASE("main_scan");
            #pragma omp parallel
            {
                alignas(64) int64_t local_counts[8] = {0};
                #pragma omp for schedule(static)
                for (uint32_t bi = 0; bi < num_qual_blocks; bi++) {
                    uint64_t b = qual_blocks[bi];
                    uint64_t row_start = b * zm_block_size;
                    uint64_t row_end = std::min(row_start + (uint64_t)zm_block_size, num_orders);
                    for (uint64_t r = row_start; r < row_end; r++) {
                        int32_t d = o_orderdate[r];
                        if (d >= o_orderdate_lower && d < o_orderdate_upper) {
                            int32_t ok = o_orderkey[r];
                            if (ok >= 0 && (uint64_t)ok < gi_num_entries) {
                                uint32_t start = gi_body[ok * 2];
                                uint32_t count = gi_body[ok * 2 + 1];
                                for (uint32_t j = 0; j < count; j++) {
                                    uint32_t row = start + j;
                                    if (l_commitdate[row] < l_receiptdate[row]) {
                                        local_counts[o_orderpriority[r]]++;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                #pragma omp critical
                {
                    for (int k = 0; k < 5; k++) global_counts[k] += local_counts[k];
                }
            }
        }

        // Step 6: Output - sort by priority string, write CSV
        {
            GENDB_PHASE("output");
            // Create (string, count) pairs and sort by string
            std::vector<std::pair<std::string, int64_t>> results;
            for (uint32_t i = 0; i < dict_count; i++) {
                results.push_back({dict_strings[i], global_counts[i]});
            }
            std::sort(results.begin(), results.end());

            std::string outpath = results_dir + "/Q4.csv";
            FILE* fout = std::fopen(outpath.c_str(), "w");
            if (!fout) { std::perror("fopen output"); return 1; }
            std::fprintf(fout, "o_orderpriority,order_count\n");
            for (auto& [prio, cnt] : results) {
                std::fprintf(fout, "%s,%ld\n", prio.c_str(), cnt);
            }
            std::fclose(fout);
        }
    }

    return 0;
}
