// Q7: Volume Shipping — GenDB generated code
#include <cstdio>
#include <cstdint>
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

// ---------- mmap helper ----------
struct MMap {
    void* ptr;
    size_t len;
    MMap() : ptr(nullptr), len(0) {}
    ~MMap() { if (ptr && ptr != MAP_FAILED) munmap(ptr, len); }
    MMap(const MMap&) = delete;
    MMap& operator=(const MMap&) = delete;
};

template<typename T>
static const T* mmap_col(const std::string& path, size_t& count, MMap& holder, int advice = MADV_SEQUENTIAL) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); std::exit(1); }
    struct stat st; fstat(fd, &st);
    holder.len = st.st_size;
    holder.ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    if (holder.ptr == MAP_FAILED) { perror("mmap"); std::exit(1); }
    if (st.st_size > 4096) madvise(holder.ptr, st.st_size, advice);
    count = st.st_size / sizeof(T);
    return static_cast<const T*>(holder.ptr);
}

// ---------- zonemap (guide format: uint64_t num_blocks, uint32_t block_size, int32_t[num_blocks*2] min/max) ----------
struct ZoneBlock { int32_t min_val, max_val; };

static void load_zonemap(const std::string& path, uint64_t& num_blocks, uint32_t& block_size,
                         std::vector<ZoneBlock>& blocks) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); std::exit(1); }
    ::read(fd, &num_blocks, 8);
    ::read(fd, &block_size, 4);
    blocks.resize(num_blocks);
    ::read(fd, blocks.data(), num_blocks * sizeof(ZoneBlock));
    ::close(fd);
}

// ---------- lookup index (dense_pk_array: uint64_t num_entries header, int32_t[] body) ----------
template<typename T>
static const T* mmap_lookup(const std::string& path, uint64_t& num_entries, MMap& holder, int advice = MADV_SEQUENTIAL) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); std::exit(1); }
    struct stat st; fstat(fd, &st);
    holder.len = st.st_size;
    holder.ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    if (holder.ptr == MAP_FAILED) { perror("mmap"); std::exit(1); }
    num_entries = *static_cast<const uint64_t*>(holder.ptr);
    if (st.st_size > 4096) madvise(holder.ptr, st.st_size, advice);
    return reinterpret_cast<const T*>(static_cast<const char*>(holder.ptr) + 8);
}

// ---------- varlen string helper ----------
static std::string read_varlen(const uint32_t* offsets, const char* data, int row) {
    return std::string(data + offsets[row], data + offsets[row + 1]);
}

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params...]\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();

    // Parse CLI parameters
    int32_t shipdate_lo = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower", 9131);
    int32_t shipdate_hi = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper", 9861);
    std::string nation1 = gendb::parse_string_arg(argc, argv, "--n_name_eq", "FRANCE");
    std::string nation2 = gendb::parse_string_arg(argc, argv, "--n_name_eq_2", "GERMANY");
    std::string nation3 = gendb::parse_string_arg(argc, argv, "--n_name_eq_3", "GERMANY");
    std::string nation4 = gendb::parse_string_arg(argc, argv, "--n_name_eq_4", "FRANCE");

    int year_lo = gendb::extract_year(shipdate_lo);
    int year_hi = gendb::extract_year(shipdate_hi);

    // ========== mmap all resources BEFORE timing scope ==========
    // Nation
    MMap mm_nkey, mm_noff, mm_ndata;
    size_t n_nation;
    const int32_t* n_nationkey = mmap_col<int32_t>(gendb_dir + "/nation/n_nationkey.bin", n_nation, mm_nkey);
    size_t n_noff;
    const uint32_t* n_name_off = mmap_col<uint32_t>(gendb_dir + "/nation/n_name_offsets.bin", n_noff, mm_noff);
    size_t n_ndata;
    const char* n_name_data = mmap_col<char>(gendb_dir + "/nation/n_name_data.bin", n_ndata, mm_ndata);

    // Supplier
    MMap mm_snk;
    size_t n_supp;
    const int32_t* s_nationkey = mmap_col<int32_t>(gendb_dir + "/supplier/s_nationkey.bin", n_supp, mm_snk);

    // Supplier lookup (suppkey → row_id)
    MMap mm_supp_lkp;
    uint64_t supp_lkp_entries;
    const int32_t* supp_lookup = mmap_lookup<int32_t>(gendb_dir + "/indexes/supplier_s_suppkey_lookup.bin",
                                                       supp_lkp_entries, mm_supp_lkp);

    // Customer
    MMap mm_cnk;
    size_t n_cust;
    const int32_t* c_nationkey = mmap_col<int32_t>(gendb_dir + "/customer/c_nationkey.bin", n_cust, mm_cnk);

    // Customer lookup
    MMap mm_cust_lkp;
    uint64_t cust_lkp_entries;
    const int32_t* cust_lookup = mmap_lookup<int32_t>(gendb_dir + "/indexes/customer_c_custkey_lookup.bin",
                                                       cust_lkp_entries, mm_cust_lkp);

    // Orders lookup + o_custkey
    MMap mm_ord_lkp, mm_ocustkey;
    uint64_t ord_lkp_entries;
    const int32_t* ord_lookup = mmap_lookup<int32_t>(gendb_dir + "/indexes/orders_o_orderkey_lookup.bin",
                                                      ord_lkp_entries, mm_ord_lkp);
    size_t n_orders;
    const int32_t* o_custkey = mmap_col<int32_t>(gendb_dir + "/orders/o_custkey.bin", n_orders, mm_ocustkey);

    // Lineitem columns
    MMap mm_lok, mm_lsk, mm_lsd, mm_lep, mm_ld;
    size_t n_li;
    const int32_t* l_orderkey = mmap_col<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin", n_li, mm_lok);
    size_t tmp;
    const int32_t* l_suppkey = mmap_col<int32_t>(gendb_dir + "/lineitem/l_suppkey.bin", tmp, mm_lsk);
    const int32_t* l_shipdate = mmap_col<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin", tmp, mm_lsd);
    const double* l_extendedprice = mmap_col<double>(gendb_dir + "/lineitem/l_extendedprice.bin", tmp, mm_lep);
    const double* l_discount = mmap_col<double>(gendb_dir + "/lineitem/l_discount.bin", tmp, mm_ld);

    // Zonemap
    uint64_t zm_num_blocks;
    uint32_t zm_block_size;
    std::vector<ZoneBlock> zm_blocks;
    load_zonemap(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin", zm_num_blocks, zm_block_size, zm_blocks);

    // ========== BEGIN TIMED SECTION ==========
    {
    GENDB_PHASE("total");

    // --- Step 1: Find nationkeys for the 4 nation params ---
    int32_t nk1 = -1, nk2 = -1, nk3 = -1, nk4 = -1;
    std::string name1_str, name2_str, name3_str, name4_str;
    {
        GENDB_PHASE("nation_filter");
        for (size_t i = 0; i < n_nation; i++) {
            std::string nm = read_varlen(n_name_off, n_name_data, (int)i);
            int32_t nk = n_nationkey[i];
            if (nm == nation1) { nk1 = nk; name1_str = nm; }
            if (nm == nation2) { nk2 = nk; name2_str = nm; }
            if (nm == nation3) { nk3 = nk; name3_str = nm; }
            if (nm == nation4) { nk4 = nk; name4_str = nm; }
        }
    }

    // Nation pair encoding:
    // Pair 0: supplier=nation1, customer=nation2
    // Pair 1: supplier=nation3, customer=nation4
    // supp_nk_flag[nationkey]: bit0 = matches nation1, bit1 = matches nation3
    // cust_nk_flag[nationkey]: bit0 = matches nation2, bit1 = matches nation4
    uint8_t supp_nk_flag[25] = {};
    uint8_t cust_nk_flag[25] = {};
    if (nk1 >= 0 && nk1 < 25) supp_nk_flag[nk1] |= 1;
    if (nk3 >= 0 && nk3 < 25) supp_nk_flag[nk3] |= 2;
    if (nk2 >= 0 && nk2 < 25) cust_nk_flag[nk2] |= 1;
    if (nk4 >= 0 && nk4 < 25) cust_nk_flag[nk4] |= 2;

    // --- Step 2: Build supp_nation_flag[suppkey] using lookup index ---
    std::vector<uint8_t> supp_flag(supp_lkp_entries, 0);
    {
        GENDB_PHASE("build_supp_flag");
        for (uint64_t sk = 0; sk < supp_lkp_entries; sk++) {
            int32_t rid = supp_lookup[sk];
            if (rid < 0 || (size_t)rid >= n_supp) continue;
            int32_t nk = s_nationkey[rid];
            if (nk >= 0 && nk < 25 && supp_nk_flag[nk]) {
                supp_flag[sk] = supp_nk_flag[nk];
            }
        }
    }

    // --- Step 3: Build cust_nation_flag[custkey] ---
    std::vector<uint8_t> cust_flag(cust_lkp_entries, 0);
    {
        GENDB_PHASE("build_cust_flag");
        for (uint64_t ck = 0; ck < cust_lkp_entries; ck++) {
            int32_t rid = cust_lookup[ck];
            if (rid >= 0 && (size_t)rid < n_cust) {
                int32_t nk = c_nationkey[rid];
                if (nk >= 0 && nk < 25 && cust_nk_flag[nk]) {
                    cust_flag[ck] = cust_nk_flag[nk];
                }
            }
        }
    }

    // --- Step 4: Build qualifying block ranges from zonemap ---
    struct BlockRange { uint32_t start, end; };
    std::vector<BlockRange> ranges;
    {
        GENDB_PHASE("zonemap_filter");
        for (uint64_t b = 0; b < zm_num_blocks; b++) {
            if (zm_blocks[b].max_val < shipdate_lo || zm_blocks[b].min_val > shipdate_hi) continue;
            uint32_t start = (uint32_t)(b * zm_block_size);
            uint32_t end = start + zm_block_size;
            if (end > (uint32_t)n_li) end = (uint32_t)n_li;
            ranges.push_back({start, end});
        }
    }

    // --- Step 5: Parallel scan with aggregation ---
    int num_years = year_hi - year_lo + 1;
    int num_buckets = 2 * num_years; // 2 pairs × num_years

    int nthreads = omp_get_max_threads();
    std::vector<std::vector<double>> tl_agg(nthreads, std::vector<double>(num_buckets, 0.0));

    {
        GENDB_PHASE("main_scan");
        int nr = (int)ranges.size();

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            double* my_agg = tl_agg[tid].data();

            #pragma omp for schedule(dynamic, 4)
            for (int ri = 0; ri < nr; ri++) {
                uint32_t rstart = ranges[ri].start;
                uint32_t rend = ranges[ri].end;

                for (uint32_t i = rstart; i < rend; i++) {
                    int32_t sd = l_shipdate[i];
                    if (sd < shipdate_lo || sd > shipdate_hi) continue;

                    int32_t sk = l_suppkey[i];
                    if (sk < 0 || (uint64_t)sk >= supp_lkp_entries) continue;
                    uint8_t sf = supp_flag[sk];
                    if (!sf) continue;

                    // Two-step lookup: orderkey -> order row_id -> custkey -> cust_flag
                    int32_t ok = l_orderkey[i];
                    if (ok < 0 || (uint64_t)ok >= ord_lkp_entries) continue;
                    int32_t o_rid = ord_lookup[ok];
                    if (o_rid < 0) continue;
                    int32_t ck = o_custkey[o_rid];
                    if (ck < 0 || (uint64_t)ck >= cust_lkp_entries) continue;
                    uint8_t cf = cust_flag[ck];
                    if (!cf) continue;

                    // Check valid pairs
                    // pair0: (sf & 1) && (cf & 1) => supplier=nation1, customer=nation2
                    // pair1: (sf & 2) && (cf & 2) => supplier=nation3, customer=nation4
                    uint8_t match = (sf & cf); // bits that match
                    if (!match) continue;

                    int year_idx = gendb::extract_year(sd) - year_lo;
                    double volume = l_extendedprice[i] * (1.0 - l_discount[i]);

                    if (match & 1) my_agg[0 * num_years + year_idx] += volume;
                    if (match & 2) my_agg[1 * num_years + year_idx] += volume;
                }
            }
        }
    }

    // --- Step 6: Merge thread-local aggregation ---
    std::vector<double> agg(num_buckets, 0.0);
    {
        GENDB_PHASE("aggregation");
        for (int t = 0; t < nthreads; t++) {
            for (int b = 0; b < num_buckets; b++) {
                agg[b] += tl_agg[t][b];
            }
        }
    }

    // --- Step 7: Output ---
    {
        GENDB_PHASE("output");
        struct Row {
            std::string supp_nation, cust_nation;
            int year;
            double revenue;
        };
        std::vector<Row> rows;

        // Pair 0: (nation1, nation2)
        for (int y = 0; y < num_years; y++) {
            double rev = agg[0 * num_years + y];
            if (rev != 0.0) rows.push_back({name1_str, name2_str, year_lo + y, rev});
        }
        // Pair 1: (nation3, nation4)
        for (int y = 0; y < num_years; y++) {
            double rev = agg[1 * num_years + y];
            if (rev != 0.0) rows.push_back({name3_str, name4_str, year_lo + y, rev});
        }

        std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
            if (a.supp_nation != b.supp_nation) return a.supp_nation < b.supp_nation;
            if (a.cust_nation != b.cust_nation) return a.cust_nation < b.cust_nation;
            return a.year < b.year;
        });

        std::string out_path = results_dir + "/Q7.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        fprintf(fp, "supp_nation,cust_nation,l_year,revenue\n");
        for (auto& r : rows) {
            fprintf(fp, "%s,%s,%d,%.4f\n", r.supp_nation.c_str(), r.cust_nation.c_str(), r.year, r.revenue);
        }
        fclose(fp);
    }

    } // end total timer
    return 0;
}
