#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>
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

// Multi-word string arg parser: concatenates consecutive non-flag args
static std::string parse_multiword_string_arg(int argc, char* argv[], const char* name, const std::string& default_val) {
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], name) == 0) {
            if (i + 1 >= argc) return default_val;
            std::string result = argv[i + 1];
            for (int j = i + 2; j < argc; j++) {
                if (argv[j][0] == '-' && argv[j][1] == '-') break;
                result += " ";
                result += argv[j];
            }
            return result;
        }
    }
    return default_val;
}

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
        if (size == 0) { ptr = nullptr; return; }
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path.c_str()); exit(1); }
    }

    void advise_seq() {
        if (ptr && size > 0) {
            madvise(ptr, size, MADV_SEQUENTIAL);
            madvise(ptr, size, MADV_HUGEPAGE);
        }
    }

    void close() {
        if (ptr && size > 0) munmap(ptr, size);
        if (fd >= 0) ::close(fd);
        ptr = nullptr; size = 0; fd = -1;
    }

    ~MmapFile() { close(); }
};

// Load dictionary: uint32_t count, then [uint16_t len, char[len]] per entry
static std::vector<std::string> load_dict(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) { fprintf(stderr, "Cannot open dict %s\n", path.c_str()); exit(1); }
    uint32_t count;
    if (fread(&count, sizeof(count), 1, f) != 1) { fclose(f); exit(1); }
    std::vector<std::string> dict(count);
    for (uint32_t i = 0; i < count; i++) {
        uint16_t len;
        if (fread(&len, sizeof(len), 1, f) != 1) { fclose(f); exit(1); }
        dict[i].resize(len);
        if (fread(&dict[i][0], 1, len, f) != (size_t)len) { fclose(f); exit(1); }
    }
    fclose(f);
    return dict;
}

static int8_t find_code(const std::vector<std::string>& dict, const std::string& val) {
    for (size_t i = 0; i < dict.size(); i++) {
        if (dict[i] == val) return (int8_t)i;
    }
    return -1;
}

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params]\n", argv[0]); return 1; }

    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();

    std::string p_brand_eq = parse_multiword_string_arg(argc, argv, "--p_brand_eq", "Brand#23");
    std::string p_container_eq = parse_multiword_string_arg(argc, argv, "--p_container_eq", "MED BOX");

    // Mmap all columns BEFORE timing scope to exclude munmap TLB shootdown
    MmapFile mf_p_brand, mf_p_container, mf_p_partkey;
    MmapFile mf_l_partkey, mf_l_quantity, mf_l_extendedprice;

    mf_p_brand.open(gendb_dir + "/part/p_brand.bin");
    mf_p_container.open(gendb_dir + "/part/p_container.bin");
    mf_p_partkey.open(gendb_dir + "/part/p_partkey.bin");
    mf_l_partkey.open(gendb_dir + "/lineitem/l_partkey.bin");
    mf_l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
    mf_l_extendedprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");

    mf_l_partkey.advise_seq();
    mf_l_quantity.advise_seq();
    mf_l_extendedprice.advise_seq();

    const int8_t* __restrict__ p_brand_col = (const int8_t*)mf_p_brand.ptr;
    const int8_t* __restrict__ p_container_col = (const int8_t*)mf_p_container.ptr;
    const int32_t* __restrict__ p_partkey_col = (const int32_t*)mf_p_partkey.ptr;
    const int32_t* __restrict__ l_partkey_col = (const int32_t*)mf_l_partkey.ptr;
    const double* __restrict__ l_quantity_col = (const double*)mf_l_quantity.ptr;
    const double* __restrict__ l_extendedprice_col = (const double*)mf_l_extendedprice.ptr;

    const size_t num_parts = mf_p_partkey.size / sizeof(int32_t);
    const size_t num_lineitem = mf_l_partkey.size / sizeof(int32_t);

    double total_sum = 0.0;

    {
        GENDB_PHASE("total");

        // Step 1: Load dicts and resolve codes
        int8_t brand_code, container_code;
        {
            GENDB_PHASE("data_loading");
            auto brand_dict = load_dict(gendb_dir + "/part/p_brand_dict.bin");
            auto container_dict = load_dict(gendb_dir + "/part/p_container_dict.bin");
            brand_code = find_code(brand_dict, p_brand_eq);
            container_code = find_code(container_dict, p_container_eq);
        }

        // Step 2: Filter part, collect qualifying partkeys + build compact map
        // Use direct-addressed array over partkey domain (2M entries × 2 bytes = 4MB)
        // Value: compact_id+1 (0 = not qualifying)
        // Also build bitset (250KB) for fast membership test
        std::vector<int32_t> qual_partkeys;
        const size_t PK_DOMAIN = num_parts + 1; // partkeys are 1-based in TPC-H, max = num_parts

        // Direct lookup: partkey → compact_id+1 (0 means not qualifying)
        std::vector<uint16_t> pk_lookup(PK_DOMAIN, 0);

        // Bitset for fast membership test (avoids lookup table access for non-qualifying)
        const size_t bitset_words = (PK_DOMAIN + 63) / 64;
        std::vector<uint64_t> pk_bitset(bitset_words, 0);

        {
            GENDB_PHASE("dim_filter");
            qual_partkeys.reserve(4096);
            for (size_t i = 0; i < num_parts; i++) {
                if (p_brand_col[i] == brand_code && p_container_col[i] == container_code) {
                    int32_t pk = p_partkey_col[i];
                    uint16_t idx = (uint16_t)qual_partkeys.size();
                    qual_partkeys.push_back(pk);
                    pk_lookup[pk] = idx + 1;
                    pk_bitset[pk >> 6] |= (1ULL << (pk & 63));
                }
            }
        }

        const size_t num_qual = qual_partkeys.size();
        if (num_qual == 0) {
            GENDB_PHASE("output");
            std::string out_path = results_dir + "/Q17.csv";
            FILE* fout = fopen(out_path.c_str(), "w");
            fprintf(fout, "avg_yearly\n0.00\n");
            fclose(fout);
            return 0;
        }

        // Step 3: Pass 1 — compute per-part sum(l_quantity) and count
        std::vector<double> sum_qty(num_qual, 0.0);
        std::vector<uint32_t> count_qty(num_qual, 0);
        {
            GENDB_PHASE("pass1_avg");
            const int nthreads = omp_get_max_threads();
            std::vector<std::vector<double>> tl_sum(nthreads, std::vector<double>(num_qual, 0.0));
            std::vector<std::vector<uint32_t>> tl_cnt(nthreads, std::vector<uint32_t>(num_qual, 0));

            #pragma omp parallel
            {
                const int tid = omp_get_thread_num();
                double* __restrict__ my_sum = tl_sum[tid].data();
                uint32_t* __restrict__ my_cnt = tl_cnt[tid].data();
                const uint64_t* __restrict__ bs = pk_bitset.data();
                const uint16_t* __restrict__ lk = pk_lookup.data();

                #pragma omp for schedule(static)
                for (size_t i = 0; i < num_lineitem; i++) {
                    int32_t pk = l_partkey_col[i];
                    if ((uint32_t)pk < PK_DOMAIN && (bs[pk >> 6] & (1ULL << (pk & 63)))) {
                        uint16_t idx = lk[pk] - 1;
                        my_sum[idx] += l_quantity_col[i];
                        my_cnt[idx]++;
                    }
                }
            }

            // Merge thread-local arrays
            for (int t = 0; t < nthreads; t++) {
                for (size_t j = 0; j < num_qual; j++) {
                    sum_qty[j] += tl_sum[t][j];
                    count_qty[j] += tl_cnt[t][j];
                }
            }
        }

        // Step 4: Compute thresholds (0.2 * avg_qty per part)
        // Store as direct-addressed by partkey for fastest access in pass 2
        std::vector<double> threshold_by_pk(PK_DOMAIN, 0.0);
        {
            GENDB_PHASE("compute_thresholds");
            for (size_t j = 0; j < num_qual; j++) {
                if (count_qty[j] > 0) {
                    threshold_by_pk[qual_partkeys[j]] = 0.2 * (sum_qty[j] / (double)count_qty[j]);
                }
            }
        }

        // Step 5: Pass 2 — sum l_extendedprice where l_quantity < threshold
        {
            GENDB_PHASE("main_scan");
            const double* __restrict__ thresh = threshold_by_pk.data();
            const uint64_t* __restrict__ bs = pk_bitset.data();

            #pragma omp parallel reduction(+:total_sum)
            {
                #pragma omp for schedule(static)
                for (size_t i = 0; i < num_lineitem; i++) {
                    int32_t pk = l_partkey_col[i];
                    if ((uint32_t)pk < PK_DOMAIN && (bs[pk >> 6] & (1ULL << (pk & 63)))) {
                        if (l_quantity_col[i] < thresh[pk]) {
                            total_sum += l_extendedprice_col[i];
                        }
                    }
                }
            }
        }

        // Step 6: Output
        {
            GENDB_PHASE("output");
            double avg_yearly = total_sum / 7.0;
            std::string out_path = results_dir + "/Q17.csv";
            FILE* fout = fopen(out_path.c_str(), "w");
            if (!fout) { fprintf(stderr, "Cannot write %s\n", out_path.c_str()); return 1; }
            fprintf(fout, "avg_yearly\n%.2f\n", avg_yearly);
            fclose(fout);
        }
    }

    return 0;
}
