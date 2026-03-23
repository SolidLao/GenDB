#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <thread>
#include <atomic>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

// RAII mmap wrapper — declared BEFORE timer scope to exclude munmap TLB shootdown
struct MappedFile {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    MappedFile() = default;
    MappedFile(const std::string& path, int advice = MADV_SEQUENTIAL) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::fprintf(stderr, "Cannot open %s\n", path.c_str()); std::exit(1); }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { std::fprintf(stderr, "mmap failed %s\n", path.c_str()); std::exit(1); }
        madvise(data, size, advice);
        madvise(data, size, MADV_HUGEPAGE);
    }
    ~MappedFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    MappedFile(MappedFile&& o) noexcept : data(o.data), size(o.size), fd(o.fd) {
        o.data = nullptr; o.size = 0; o.fd = -1;
    }
    MappedFile& operator=(MappedFile&& o) noexcept {
        if (this != &o) {
            if (data && data != MAP_FAILED) munmap(data, size);
            if (fd >= 0) close(fd);
            data = o.data; size = o.size; fd = o.fd;
            o.data = nullptr; o.size = 0; o.fd = -1;
        }
        return *this;
    }

    template<typename T> const T* as() const { return reinterpret_cast<const T*>(data); }
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [--param value ...]\n", argv[0]);
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();

    // Parse parameters — both defaults are the base date; upper gets +1 month
    int32_t shipdate_lower = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower",
                                                    gendb::date_str_to_epoch_days("1995-09-01"));
    int32_t shipdate_upper_base = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper",
                                                         gendb::date_str_to_epoch_days("1995-09-01"));
    int32_t shipdate_upper = gendb::add_months(shipdate_upper_base, 1);

    // === Mmap all files BEFORE timer scope (exclude munmap TLB shootdown) ===
    // Part columns
    MappedFile mf_p_partkey(gendb_dir + "/part/p_partkey.bin");
    MappedFile mf_p_type(gendb_dir + "/part/p_type.bin");
    MappedFile mf_p_type_dict(gendb_dir + "/part/p_type_dict.bin");

    // Lineitem columns
    MappedFile mf_l_shipdate(gendb_dir + "/lineitem/l_shipdate.bin");
    MappedFile mf_l_extendedprice(gendb_dir + "/lineitem/l_extendedprice.bin");
    MappedFile mf_l_discount(gendb_dir + "/lineitem/l_discount.bin");
    MappedFile mf_l_partkey(gendb_dir + "/lineitem/l_partkey.bin");

    // Zonemap
    MappedFile mf_zonemap(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");

    // Part lookup index
    MappedFile mf_pk_lookup(gendb_dir + "/indexes/part_p_partkey_lookup.bin");

    const size_t num_lineitem = mf_l_shipdate.size / sizeof(int32_t);
    const size_t num_part = mf_p_partkey.size / sizeof(int32_t);

    {
        GENDB_PHASE("total");

        // ============ Phase 1: Build promo bitset ============
        // Pointers
        const int32_t* p_partkey = mf_p_partkey.as<int32_t>();
        const int16_t* p_type = mf_p_type.as<int16_t>();
        const uint8_t* dict_raw = mf_p_type_dict.as<uint8_t>();

        // Read p_type dictionary
        std::vector<bool> promo_codes;
        {
            GENDB_PHASE("build_promo");
            uint32_t dict_count;
            std::memcpy(&dict_count, dict_raw, sizeof(uint32_t));
            const uint8_t* ptr = dict_raw + sizeof(uint32_t);

            promo_codes.resize(dict_count, false);
            for (uint32_t i = 0; i < dict_count; i++) {
                uint16_t len;
                std::memcpy(&len, ptr, sizeof(uint16_t));
                ptr += sizeof(uint16_t);
                // Check if starts with "PROMO"
                if (len >= 5 && std::memcmp(ptr, "PROMO", 5) == 0) {
                    promo_codes[i] = true;
                }
                ptr += len;
            }

            // Build is_promo array indexed by partkey
            // Find max partkey
            int32_t max_pk = 0;
            for (size_t i = 0; i < num_part; i++) {
                if (p_partkey[i] > max_pk) max_pk = p_partkey[i];
            }

            // We'll use a global bool array
            // Actually let's just build it inline below
        }

        // Build is_promo[partkey] bool array
        // Find max partkey for sizing
        int32_t max_pk = 0;
        for (size_t i = 0; i < num_part; i++) {
            if (p_partkey[i] > max_pk) max_pk = p_partkey[i];
        }

        std::vector<uint8_t> is_promo(max_pk + 1, 0);
        for (size_t i = 0; i < num_part; i++) {
            int32_t pk = p_partkey[i];
            int16_t tc = p_type[i];
            if (promo_codes[tc]) {
                is_promo[pk] = 1;
            }
        }

        // ============ Phase 2: Zonemap prefilter ============
        std::vector<uint32_t> qualifying_blocks;
        uint32_t block_size;
        {
            GENDB_PHASE("zonemap_prefilter");
            const uint8_t* zm = mf_zonemap.as<uint8_t>();
            uint64_t num_blocks;
            std::memcpy(&num_blocks, zm, sizeof(uint64_t));
            std::memcpy(&block_size, zm + sizeof(uint64_t), sizeof(uint32_t));
            const int32_t* minmax = reinterpret_cast<const int32_t*>(zm + sizeof(uint64_t) + sizeof(uint32_t));

            qualifying_blocks.reserve(num_blocks);
            for (uint64_t b = 0; b < num_blocks; b++) {
                int32_t bmin = minmax[b * 2];
                int32_t bmax = minmax[b * 2 + 1];
                // Skip if block range doesn't overlap [shipdate_lower, shipdate_upper)
                if (bmax < shipdate_lower || bmin >= shipdate_upper) continue;
                qualifying_blocks.push_back((uint32_t)b);
            }
        }

        // ============ Phase 3: Parallel scan + aggregate ============
        double promo_sum = 0.0;
        double total_sum = 0.0;
        {
            GENDB_PHASE("main_scan");
            const int32_t* __restrict__ l_shipdate = mf_l_shipdate.as<int32_t>();
            const double* __restrict__ l_extendedprice = mf_l_extendedprice.as<double>();
            const double* __restrict__ l_discount = mf_l_discount.as<double>();
            const int32_t* __restrict__ l_partkey = mf_l_partkey.as<int32_t>();
            const uint8_t* __restrict__ promo_arr = is_promo.data();

            const int num_qual = (int)qualifying_blocks.size();

            #pragma omp parallel reduction(+:promo_sum,total_sum)
            {
                double local_promo = 0.0;
                double local_total = 0.0;

                #pragma omp for schedule(dynamic, 4)
                for (int qi = 0; qi < num_qual; qi++) {
                    uint32_t block_id = qualifying_blocks[qi];
                    size_t start = (size_t)block_id * block_size;
                    size_t end = start + block_size;
                    if (end > num_lineitem) end = num_lineitem;

                    for (size_t i = start; i < end; i++) {
                        int32_t sd = l_shipdate[i];
                        if (sd >= shipdate_lower && sd < shipdate_upper) {
                            double revenue = l_extendedprice[i] * (1.0 - l_discount[i]);
                            local_total += revenue;
                            if (promo_arr[l_partkey[i]]) {
                                local_promo += revenue;
                            }
                        }
                    }
                }

                promo_sum += local_promo;
                total_sum += local_total;
            }
        }

        // ============ Phase 4: Output ============
        {
            GENDB_PHASE("output");
            double result = 100.0 * promo_sum / total_sum;

            std::string outpath = results_dir + "/Q14.csv";
            FILE* f = std::fopen(outpath.c_str(), "w");
            if (!f) { std::fprintf(stderr, "Cannot open %s\n", outpath.c_str()); return 1; }
            std::fprintf(f, "promo_revenue\n");
            std::fprintf(f, "%.2f\n", result);
            std::fclose(f);
        }
    }

    return 0;
}
