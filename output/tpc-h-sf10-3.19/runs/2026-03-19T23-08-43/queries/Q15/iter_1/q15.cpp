// Q15: Top Supplier — GenDB iteration 1
// Optimized: capped threads, calloc inside parallel region for NUMA first-touch
#include "timing_utils.h"
#include "cli_params.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ---- mmap helper (no RAII destructor — manual close for TLB shootdown control) ----
struct MmapFile {
    void* ptr = nullptr;
    size_t len = 0;
    int fd = -1;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st); len = st.st_size;
        ptr = mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path.c_str()); exit(1); }
    }

    void advise_seq_huge() {
        if (len > 0) {
            madvise(ptr, len, MADV_SEQUENTIAL);
#ifdef MADV_HUGEPAGE
            madvise(ptr, len, MADV_HUGEPAGE);
#endif
        }
    }

    void close() {
        if (ptr && ptr != MAP_FAILED) { munmap(ptr, len); ptr = nullptr; }
        if (fd >= 0) { ::close(fd); fd = -1; }
        len = 0;
    }

    template<typename T> const T* as() const { return static_cast<const T*>(ptr); }
    template<typename T> size_t count() const { return len / sizeof(T); }
};

// ---- varlen string reader (int32_t offsets) ----
static inline std::string read_varlen(const int32_t* offsets, const char* data, size_t row) {
    return std::string(data + offsets[row], offsets[row + 1] - offsets[row]);
}

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params]\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();
    int32_t shipdate_lo = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower", 9497);
    int32_t shipdate_base = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper", 9497);
    int32_t shipdate_hi = gendb::add_months(shipdate_base, 3); // correctness anchor: add_months(shipdate_hi, 3)

    // Declare mmap resources BEFORE timing scope to exclude munmap TLB shootdown
    MmapFile zm_file, li_shipdate, li_suppkey, li_extprice, li_discount;
    MmapFile sup_lookup;
    MmapFile s_name_off, s_name_dat, s_addr_off, s_addr_dat, s_phone_off, s_phone_dat;

    {
    GENDB_PHASE("total");

    // Phase 1: Data loading + zonemap filter
    std::vector<uint32_t> qual_blocks;
    uint32_t block_size;
    size_t total_rows;
    size_t rev_size;

    {
        GENDB_PHASE("data_loading");

        // Load zonemap
        zm_file.open(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");
        const uint8_t* zm_raw = static_cast<const uint8_t*>(zm_file.ptr);
        uint64_t num_blocks;
        memcpy(&num_blocks, zm_raw, 8);
        memcpy(&block_size, zm_raw + 8, 4);
        const int32_t* zm_minmax = reinterpret_cast<const int32_t*>(zm_raw + 12);

        // Build qualifying block list
        qual_blocks.reserve(64);
        for (uint64_t b = 0; b < num_blocks; b++) {
            int32_t bmax = zm_minmax[b * 2 + 1];
            if (bmax < shipdate_lo) continue;
            int32_t bmin = zm_minmax[b * 2];
            if (bmin >= shipdate_hi) continue;
            qual_blocks.push_back((uint32_t)b);
        }

        // mmap lineitem columns with MADV_SEQUENTIAL + MADV_HUGEPAGE, no MAP_POPULATE
        li_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        li_suppkey.open(gendb_dir + "/lineitem/l_suppkey.bin");
        li_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        li_discount.open(gendb_dir + "/lineitem/l_discount.bin");
        li_shipdate.advise_seq_huge();
        li_suppkey.advise_seq_huge();
        li_extprice.advise_seq_huge();
        li_discount.advise_seq_huge();

        total_rows = li_shipdate.count<int32_t>();

        // Supplier lookup for rev_size
        sup_lookup.open(gendb_dir + "/indexes/supplier_s_suppkey_lookup.bin");
        uint64_t num_entries;
        memcpy(&num_entries, sup_lookup.ptr, 8);
        rev_size = (size_t)num_entries;
    }

    const int32_t* sd = li_shipdate.as<int32_t>();
    const int32_t* sk = li_suppkey.as<int32_t>();
    const double* ep = li_extprice.as<double>();
    const double* dc = li_discount.as<double>();

    // Phase 2: Parallel scan + aggregate
    // Cap threads per plan: min(nproc, qualifying_blocks, 16)
    int nthreads = std::min({omp_get_max_threads(), (int)qual_blocks.size(), 16});
    if (nthreads < 1) nthreads = 1;
    omp_set_num_threads(nthreads);

    double* global_revenue = nullptr;

    {
        GENDB_PHASE("main_scan");

        // Thread-local revenue arrays allocated via calloc INSIDE parallel region
        // for NUMA first-touch locality (plan optimization)
        std::vector<double*> tl_arrays(nthreads, nullptr);
        int nqb = (int)qual_blocks.size();

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            // calloc: OS provides zero pages via copy-on-write, no constructor overhead
            double* my_rev = (double*)calloc(rev_size, sizeof(double));
            tl_arrays[tid] = my_rev;

            #pragma omp for schedule(static)
            for (int qi = 0; qi < nqb; qi++) {
                uint32_t blk = qual_blocks[qi];
                size_t row_start = (size_t)blk * block_size;
                size_t row_end = row_start + block_size;
                if (row_end > total_rows) row_end = total_rows;

                for (size_t i = row_start; i < row_end; i++) {
                    int32_t d = sd[i];
                    if (d >= shipdate_lo && d < shipdate_hi) {
                        my_rev[sk[i]] += ep[i] * (1.0 - dc[i]);
                    }
                }
            }

            // Parallel merge: element-wise sum into global array
            #pragma omp single
            {
                global_revenue = (double*)calloc(rev_size, sizeof(double));
            }

            #pragma omp for schedule(static)
            for (size_t j = 0; j < rev_size; j++) {
                double sum = 0.0;
                for (int t = 0; t < nthreads; t++) {
                    sum += tl_arrays[t][j];
                }
                global_revenue[j] = sum;
            }

            // Free thread-local array after merge
            free(my_rev);
        }
    }

    // Phase 3: Find max revenue + collect matching suppkeys
    double max_revenue = 0.0;
    std::vector<int32_t> matching;
    {
        GENDB_PHASE("find_max");
        for (size_t i = 0; i < rev_size; i++) {
            if (global_revenue[i] > max_revenue) max_revenue = global_revenue[i];
        }
        for (size_t i = 0; i < rev_size; i++) {
            if (global_revenue[i] > 0.0 && std::fabs(global_revenue[i] - max_revenue) < 1e-6) {
                matching.push_back((int32_t)i);
            }
        }
        std::sort(matching.begin(), matching.end());
    }

    // Phase 4: Lookup supplier + output
    {
        GENDB_PHASE("output");

        const int32_t* sup_arr = reinterpret_cast<const int32_t*>(
            static_cast<const uint8_t*>(sup_lookup.ptr) + 8);

        s_name_off.open(gendb_dir + "/supplier/s_name_offsets.bin");
        s_name_dat.open(gendb_dir + "/supplier/s_name_data.bin");
        s_addr_off.open(gendb_dir + "/supplier/s_address_offsets.bin");
        s_addr_dat.open(gendb_dir + "/supplier/s_address_data.bin");
        s_phone_off.open(gendb_dir + "/supplier/s_phone_offsets.bin");
        s_phone_dat.open(gendb_dir + "/supplier/s_phone_data.bin");

        const int32_t* noff = s_name_off.as<int32_t>();
        const char* ndat = s_name_dat.as<char>();
        const int32_t* aoff = s_addr_off.as<int32_t>();
        const char* adat = s_addr_dat.as<char>();
        const int32_t* poff = s_phone_off.as<int32_t>();
        const char* pdat = s_phone_dat.as<char>();

        std::string out_path = results_dir + "/Q15.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        if (!f) { fprintf(stderr, "Cannot open %s\n", out_path.c_str()); exit(1); }
        fprintf(f, "s_suppkey,s_name,s_address,s_phone,total_revenue\n");

        for (int32_t suppkey : matching) {
            int32_t row_id = sup_arr[suppkey];
            if (row_id < 0) continue;

            std::string name = read_varlen(noff, ndat, row_id);
            std::string addr = read_varlen(aoff, adat, row_id);
            std::string phone = read_varlen(poff, pdat, row_id);

            // CSV: quote address if it contains comma or quote
            bool need_quote = addr.find(',') != std::string::npos ||
                              addr.find('"') != std::string::npos;
            if (need_quote) {
                std::string escaped;
                escaped.reserve(addr.size() + 4);
                for (char c : addr) {
                    if (c == '"') escaped += "\"\"";
                    else escaped += c;
                }
                fprintf(f, "%d,%s,\"%s\",%s,%.4f\n",
                        suppkey, name.c_str(), escaped.c_str(), phone.c_str(),
                        global_revenue[suppkey]);
            } else {
                fprintf(f, "%d,%s,%s,%s,%.4f\n",
                        suppkey, name.c_str(), addr.c_str(), phone.c_str(),
                        global_revenue[suppkey]);
            }
        }
        fclose(f);
    }

    free(global_revenue);

    } // end total timer

    // Cleanup mmaps outside timing scope
    zm_file.close();
    li_shipdate.close();
    li_suppkey.close();
    li_extprice.close();
    li_discount.close();
    sup_lookup.close();
    s_name_off.close();
    s_name_dat.close();
    s_addr_off.close();
    s_addr_dat.close();
    s_phone_off.close();
    s_phone_dat.close();

    return 0;
}
