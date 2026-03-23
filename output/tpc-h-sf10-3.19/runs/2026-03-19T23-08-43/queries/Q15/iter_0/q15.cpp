// Q15: Top Supplier — GenDB iteration 0
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

// ---- mmap helper ----
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
    void advise_seq() { if (len > 0) madvise(ptr, len, MADV_SEQUENTIAL); }
    void advise_hugepage() {
#ifdef MADV_HUGEPAGE
        if (len > 0) madvise(ptr, len, MADV_HUGEPAGE);
#endif
    }
    ~MmapFile() {
        if (ptr && ptr != MAP_FAILED) munmap(ptr, len);
        if (fd >= 0) ::close(fd);
    }
    template<typename T> const T* as() const { return static_cast<const T*>(ptr); }
    template<typename T> size_t count() const { return len / sizeof(T); }
};

// ---- varlen string reader ----
static std::string read_varlen(const int32_t* offsets, const char* data, size_t row) {
    int32_t start = offsets[row];
    int32_t end = offsets[row + 1];
    return std::string(data + start, end - start);
}

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params]\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();
    int32_t shipdate_lo = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower", 9497);  // 1996-01-01
    int32_t shipdate_hi = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper", 9497);  // default same
    // The upper bound param is the base date; actual upper = base + 3 months
    shipdate_hi = gendb::add_months(shipdate_hi, 3);

    // Declare mmap resources BEFORE timing scope (exclude munmap TLB shootdown)
    MmapFile zm_file;
    MmapFile li_shipdate, li_suppkey, li_extprice, li_discount;
    MmapFile sup_lookup;
    MmapFile s_name_off, s_name_dat, s_addr_off, s_addr_dat, s_phone_off, s_phone_dat;

    {
    GENDB_PHASE("total");

    // Phase 1: Load data
    {
        GENDB_PHASE("data_loading");
        zm_file.open(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");
        li_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
        li_suppkey.open(gendb_dir + "/lineitem/l_suppkey.bin");
        li_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        li_discount.open(gendb_dir + "/lineitem/l_discount.bin");

        li_shipdate.advise_seq(); li_shipdate.advise_hugepage();
        li_suppkey.advise_seq(); li_suppkey.advise_hugepage();
        li_extprice.advise_seq(); li_extprice.advise_hugepage();
        li_discount.advise_seq(); li_discount.advise_hugepage();

        sup_lookup.open(gendb_dir + "/indexes/supplier_s_suppkey_lookup.bin");
        s_name_off.open(gendb_dir + "/supplier/s_name_offsets.bin");
        s_name_dat.open(gendb_dir + "/supplier/s_name_data.bin");
        s_addr_off.open(gendb_dir + "/supplier/s_address_offsets.bin");
        s_addr_dat.open(gendb_dir + "/supplier/s_address_data.bin");
        s_phone_off.open(gendb_dir + "/supplier/s_phone_offsets.bin");
        s_phone_dat.open(gendb_dir + "/supplier/s_phone_data.bin");
    }

    // Parse zonemap
    // Format: uint64_t num_blocks, uint32_t block_size, then int32_t[num_blocks*2] min/max pairs
    const uint8_t* zm_raw = static_cast<const uint8_t*>(zm_file.ptr);
    uint64_t num_blocks;
    memcpy(&num_blocks, zm_raw, 8);
    uint32_t block_size;
    memcpy(&block_size, zm_raw + 8, 4);
    const int32_t* zm_minmax = reinterpret_cast<const int32_t*>(zm_raw + 12);

    // Build qualifying block list
    std::vector<uint64_t> qual_blocks;
    {
        GENDB_PHASE("zonemap_filter");
        qual_blocks.reserve(num_blocks);
        for (uint64_t b = 0; b < num_blocks; b++) {
            int32_t bmin = zm_minmax[b * 2];
            int32_t bmax = zm_minmax[b * 2 + 1];
            if (bmax < shipdate_lo || bmin >= shipdate_hi) continue;
            qual_blocks.push_back(b);
        }
    }

    const int32_t* sd = li_shipdate.as<int32_t>();
    const int32_t* sk = li_suppkey.as<int32_t>();
    const double* ep = li_extprice.as<double>();
    const double* dc = li_discount.as<double>();
    size_t total_rows = li_shipdate.count<int32_t>();

    // Find max suppkey for array sizing
    // Supplier count is known (~100K), but let's use lookup header
    uint64_t num_supp_entries;
    memcpy(&num_supp_entries, sup_lookup.ptr, 8);
    size_t rev_size = num_supp_entries; // revenue array size

    // Phase 2: Parallel scan + aggregate
    std::vector<double> revenue(rev_size, 0.0);
    {
        GENDB_PHASE("main_scan");
        int nthreads = omp_get_max_threads();
        // Thread-local arrays
        std::vector<std::vector<double>> tl_rev(nthreads, std::vector<double>(rev_size, 0.0));

        size_t nqb = qual_blocks.size();
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double* my_rev = tl_rev[tid].data();

            #pragma omp for schedule(static)
            for (size_t qi = 0; qi < nqb; qi++) {
                uint64_t b = qual_blocks[qi];
                size_t row_start = b * block_size;
                size_t row_end = row_start + block_size;
                if (row_end > total_rows) row_end = total_rows;

                for (size_t i = row_start; i < row_end; i++) {
                    int32_t d = sd[i];
                    if (d >= shipdate_lo && d < shipdate_hi) {
                        my_rev[sk[i]] += ep[i] * (1.0 - dc[i]);
                    }
                }
            }
        }

        // Merge thread-local arrays
        {
            GENDB_PHASE("merge");
            #pragma omp parallel for schedule(static)
            for (size_t i = 0; i < rev_size; i++) {
                double sum = 0.0;
                for (int t = 0; t < nthreads; t++) {
                    sum += tl_rev[t][i];
                }
                revenue[i] = sum;
            }
        }
    }

    // Phase 3: Find max revenue
    double max_revenue = 0.0;
    std::vector<int32_t> matching_suppkeys;
    {
        GENDB_PHASE("find_max");
        for (size_t i = 0; i < rev_size; i++) {
            if (revenue[i] > max_revenue) max_revenue = revenue[i];
        }
        for (size_t i = 0; i < rev_size; i++) {
            if (std::fabs(revenue[i] - max_revenue) < 1e-6 && revenue[i] > 0.0) {
                matching_suppkeys.push_back((int32_t)i);
            }
        }
        std::sort(matching_suppkeys.begin(), matching_suppkeys.end());
    }

    // Phase 4: Lookup supplier and output
    {
        GENDB_PHASE("output");
        const int32_t* sup_arr = reinterpret_cast<const int32_t*>(
            static_cast<const uint8_t*>(sup_lookup.ptr) + 8);
        const int32_t* name_off = s_name_off.as<int32_t>();
        const char* name_dat = s_name_dat.as<char>();
        const int32_t* addr_off = s_addr_off.as<int32_t>();
        const char* addr_dat = s_addr_dat.as<char>();
        const int32_t* phone_off = s_phone_off.as<int32_t>();
        const char* phone_dat = s_phone_dat.as<char>();

        std::string out_path = results_dir + "/Q15.csv";
        FILE* f = fopen(out_path.c_str(), "w");
        fprintf(f, "s_suppkey,s_name,s_address,s_phone,total_revenue\n");

        for (int32_t suppkey : matching_suppkeys) {
            int32_t row_id = sup_arr[suppkey];
            if (row_id < 0) continue;

            std::string name = read_varlen(name_off, name_dat, row_id);
            std::string addr = read_varlen(addr_off, addr_dat, row_id);
            std::string phone = read_varlen(phone_off, phone_dat, row_id);

            // Check if address contains comma — if so, quote it
            bool need_quote = addr.find(',') != std::string::npos ||
                              addr.find('"') != std::string::npos;
            if (need_quote) {
                // Escape quotes in address
                std::string escaped;
                for (char c : addr) {
                    if (c == '"') escaped += "\"\"";
                    else escaped += c;
                }
                fprintf(f, "%d,%s,\"%s\",%s,%.4f\n",
                        suppkey, name.c_str(), escaped.c_str(), phone.c_str(),
                        revenue[suppkey]);
            } else {
                fprintf(f, "%d,%s,%s,%s,%.4f\n",
                        suppkey, name.c_str(), addr.c_str(), phone.c_str(),
                        revenue[suppkey]);
            }
        }
        fclose(f);
    }

    } // end total timer

    return 0;
}
