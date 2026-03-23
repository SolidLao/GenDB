#include "timing_utils.h"
#include "cli_params.h"
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <algorithm>
#include <map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ── mmap helper ──────────────────────────────────────────────────────────────
struct MMapFile {
    void* data = nullptr;
    size_t size = 0;
    MMapFile() = default;
    void open(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::fprintf(stderr, "Cannot open %s\n", path.c_str()); std::exit(1); }
        struct stat st; fstat(fd, &st); size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
    }
    ~MMapFile() { if (data && data != MAP_FAILED) munmap(data, size); }
    MMapFile(MMapFile&& o) noexcept : data(o.data), size(o.size) { o.data = nullptr; o.size = 0; }
    MMapFile& operator=(MMapFile&& o) noexcept {
        if (this != &o) { if (data && data != MAP_FAILED) munmap(data, size); data = o.data; size = o.size; o.data = nullptr; o.size = 0; }
        return *this;
    }
    template<typename T> const T* as() const { return reinterpret_cast<const T*>(data); }
};

// ── Year from epoch days (Howard Hinnant) ────────────────────────────────────
static inline int days_to_year(int32_t z) {
    z += 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = (unsigned)(z - era * 146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp = (5*doy + 2) / 153;
    unsigned m = mp + (mp < 10 ? 3 : (unsigned)-9);
    int y = (int)yoe + era * 400;
    if (m <= 2) y++;
    return y;
}

// ── Read nation name by row_id from varlen columns ───────────────────────────
static std::string read_nation_name(const uint32_t* offsets, const char* ndata, int row) {
    uint32_t start = offsets[row], end = offsets[row + 1];
    return std::string(ndata + start, end - start);
}

int main(int argc, char* argv[]) {
    if (argc < 3) { std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params...]\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();

    // Parse parameters
    int32_t p_shipdate_lo = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower", 9131);
    int32_t p_shipdate_hi = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper", 9861);
    std::string p_n1 = gendb::parse_string_arg(argc, argv, "--n_name_eq",   "FRANCE");
    std::string p_n2 = gendb::parse_string_arg(argc, argv, "--n_name_eq_2", "GERMANY");
    std::string p_n3 = gendb::parse_string_arg(argc, argv, "--n_name_eq_3", "GERMANY");
    std::string p_n4 = gendb::parse_string_arg(argc, argv, "--n_name_eq_4", "FRANCE");

    // ── mmap ALL files BEFORE timer (exclude munmap TLB shootdown from timing) ──
    // Nation
    MMapFile f_n_nationkey; f_n_nationkey.open(gendb_dir + "/nation/n_nationkey.bin");
    MMapFile f_n_name_off;  f_n_name_off.open(gendb_dir + "/nation/n_name_offsets.bin");
    MMapFile f_n_name_data; f_n_name_data.open(gendb_dir + "/nation/n_name_data.bin");
    // Supplier
    MMapFile f_s_suppkey;   f_s_suppkey.open(gendb_dir + "/supplier/s_suppkey.bin");
    MMapFile f_s_nationkey; f_s_nationkey.open(gendb_dir + "/supplier/s_nationkey.bin");
    // Customer
    MMapFile f_c_custkey;   f_c_custkey.open(gendb_dir + "/customer/c_custkey.bin");
    MMapFile f_c_nationkey; f_c_nationkey.open(gendb_dir + "/customer/c_nationkey.bin");
    // Orders
    MMapFile f_o_orderkey;  f_o_orderkey.open(gendb_dir + "/orders/o_orderkey.bin");
    MMapFile f_o_custkey;   f_o_custkey.open(gendb_dir + "/orders/o_custkey.bin");
    // Lineitem
    MMapFile f_l_orderkey;  f_l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
    MMapFile f_l_suppkey;   f_l_suppkey.open(gendb_dir + "/lineitem/l_suppkey.bin");
    MMapFile f_l_shipdate;  f_l_shipdate.open(gendb_dir + "/lineitem/l_shipdate.bin");
    MMapFile f_l_extprice;  f_l_extprice.open(gendb_dir + "/lineitem/l_extendedprice.bin");
    MMapFile f_l_discount;  f_l_discount.open(gendb_dir + "/lineitem/l_discount.bin");
    // Indexes
    MMapFile f_zonemap;     f_zonemap.open(gendb_dir + "/indexes/lineitem_l_shipdate_zonemap.bin");
    MMapFile f_supp_lookup; f_supp_lookup.open(gendb_dir + "/indexes/supplier_s_suppkey_lookup.bin");
    MMapFile f_ord_lookup;  f_ord_lookup.open(gendb_dir + "/indexes/orders_o_orderkey_lookup.bin");
    MMapFile f_cust_lookup; f_cust_lookup.open(gendb_dir + "/indexes/customer_c_custkey_lookup.bin");

    { GENDB_PHASE("total");

    // ── Pointers ─────────────────────────────────────────────────────────────
    const int32_t* n_nationkey = f_n_nationkey.as<int32_t>();
    const uint32_t* n_name_off = f_n_name_off.as<uint32_t>();
    const char* n_name_data    = f_n_name_data.as<char>();
    const int32_t* s_nationkey = f_s_nationkey.as<int32_t>();
    const int32_t* c_nationkey = f_c_nationkey.as<int32_t>();
    const int32_t* o_custkey   = f_o_custkey.as<int32_t>();

    const int32_t* l_orderkey  = f_l_orderkey.as<int32_t>();
    const int32_t* l_suppkey   = f_l_suppkey.as<int32_t>();
    const int32_t* l_shipdate  = f_l_shipdate.as<int32_t>();
    const double*  l_extprice  = f_l_extprice.as<double>();
    const double*  l_discount  = f_l_discount.as<double>();

    const int64_t num_lineitem = (int64_t)(f_l_orderkey.size / sizeof(int32_t));
    const int num_nation       = (int)(f_n_nationkey.size / sizeof(int32_t));

    // Zonemap
    const uint8_t* zm_raw = f_zonemap.as<uint8_t>();
    uint64_t zm_num_blocks = *reinterpret_cast<const uint64_t*>(zm_raw);
    uint32_t zm_block_size = *reinterpret_cast<const uint32_t*>(zm_raw + 8);
    const int32_t* zm_minmax = reinterpret_cast<const int32_t*>(zm_raw + 12);

    // Lookup indexes: header = uint64_t num_entries, body = int32_t[]
    auto get_lookup = [](const MMapFile& f, uint64_t& num_entries) -> const int32_t* {
        num_entries = *reinterpret_cast<const uint64_t*>(f.data);
        return reinterpret_cast<const int32_t*>((const uint8_t*)f.data + 8);
    };
    uint64_t supp_lookup_n, ord_lookup_n, cust_lookup_n;
    const int32_t* supp_lookup = get_lookup(f_supp_lookup, supp_lookup_n);
    const int32_t* ord_lookup  = get_lookup(f_ord_lookup, ord_lookup_n);
    const int32_t* cust_lookup = get_lookup(f_cust_lookup, cust_lookup_n);

    // ── Phase 1: Nation filter ───────────────────────────────────────────────
    int32_t nk_n1 = -1, nk_n2 = -1, nk_n3 = -1, nk_n4 = -1;
    std::vector<std::string> nation_names(num_nation);
    std::vector<uint8_t> supp_nation_flag(supp_lookup_n, 0);
    std::vector<uint8_t> cust_nation_flag(cust_lookup_n, 0);
    std::vector<int32_t> orders_custkey(ord_lookup_n, -1);
    uint8_t supp_code_for_nk_n1 = 1, supp_code_for_nk_n3 = 1;
    uint8_t cust_code_for_nk_n2 = 1, cust_code_for_nk_n4 = 1;

    { GENDB_PHASE("data_loading");

    for (int i = 0; i < num_nation; i++) {
        std::string nm = read_nation_name(n_name_off, n_name_data, i);
        nation_names[n_nationkey[i]] = nm;
        if (nm == p_n1) nk_n1 = n_nationkey[i];
        if (nm == p_n2) nk_n2 = n_nationkey[i];
        if (nm == p_n3) nk_n3 = n_nationkey[i];
        if (nm == p_n4) nk_n4 = n_nationkey[i];
    }

    // Assign supplier codes: 1 for nk_n1, 2 for nk_n3 (if different)
    supp_code_for_nk_n3 = (nk_n3 == nk_n1) ? 1 : 2;
    for (uint64_t sk = 0; sk < supp_lookup_n; sk++) {
        int32_t rid = supp_lookup[sk];
        if (rid < 0) continue;
        int32_t snk = s_nationkey[rid];
        if (snk == nk_n1) supp_nation_flag[sk] = supp_code_for_nk_n1;
        else if (nk_n3 != nk_n1 && snk == nk_n3) supp_nation_flag[sk] = supp_code_for_nk_n3;
    }

    // Assign customer codes: 1 for nk_n2, 2 for nk_n4 (if different)
    cust_code_for_nk_n4 = (nk_n4 == nk_n2) ? 1 : 2;
    for (uint64_t ck = 0; ck < cust_lookup_n; ck++) {
        int32_t rid = cust_lookup[ck];
        if (rid < 0) continue;
        int32_t cnk = c_nationkey[rid];
        if (cnk == nk_n2) cust_nation_flag[ck] = cust_code_for_nk_n2;
        else if (nk_n4 != nk_n2 && cnk == nk_n4) cust_nation_flag[ck] = cust_code_for_nk_n4;
    }

    // Build orders_custkey[orderkey] for O(1) lookup
    for (uint64_t ok = 0; ok < ord_lookup_n; ok++) {
        int32_t rid = ord_lookup[ok];
        if (rid >= 0) orders_custkey[ok] = o_custkey[rid];
    }

    } // data_loading

    // ── Precompute valid pair table ──────────────────────────────────────────
    int8_t pair_table[3][3];
    std::memset(pair_table, -1, sizeof(pair_table));
    pair_table[supp_code_for_nk_n1][cust_code_for_nk_n2] = 0;
    pair_table[supp_code_for_nk_n3][cust_code_for_nk_n4] = 1;

    // pair_id → (supp_nation_name, cust_nation_name)
    std::string pair_supp_name[2], pair_cust_name[2];
    pair_supp_name[0] = p_n1; pair_cust_name[0] = p_n2;
    pair_supp_name[1] = p_n3; pair_cust_name[1] = p_n4;

    // Year range
    int min_year = days_to_year(p_shipdate_lo);
    int max_year = days_to_year(p_shipdate_hi);
    int num_years = max_year - min_year + 1;
    int num_buckets = 2 * num_years; // 2 pairs × num_years

    // madvise sequential for lineitem columns
    madvise((void*)l_orderkey, f_l_orderkey.size, MADV_SEQUENTIAL);
    madvise((void*)l_suppkey,  f_l_suppkey.size,  MADV_SEQUENTIAL);
    madvise((void*)l_shipdate, f_l_shipdate.size, MADV_SEQUENTIAL);
    madvise((void*)l_extprice, f_l_extprice.size, MADV_SEQUENTIAL);
    madvise((void*)l_discount, f_l_discount.size, MADV_SEQUENTIAL);

    // ── Phase 5: Parallel scan with zonemap ──────────────────────────────────
    int nthreads = omp_get_max_threads();
    std::vector<std::vector<double>> thread_agg(nthreads, std::vector<double>(num_buckets, 0.0));

    // Precompute year thresholds for fast year-index lookup
    // Build array of Jan-1 epoch days for each year boundary
    std::vector<int32_t> year_starts(num_years + 1);
    for (int y = 0; y <= num_years; y++) {
        // Compute epoch days for Jan 1 of (min_year + y) using civil_from_days inverse
        int cy = min_year + y;
        // Hinnant: days_from_civil(y, m, d) where m=1(Jan), d=1
        // Adjust: Jan is month 10 in March-based calendar, so y_adj = cy-1
        int y_adj = cy - 1;
        int era = (y_adj >= 0 ? y_adj : y_adj - 399) / 400;
        unsigned yoe = (unsigned)(y_adj - era * 400);
        // Jan 1 = March-year day 306 (Jan is month 10, day 1 → doy = 306)
        unsigned doy = 306; // days from March 1 to Jan 1
        unsigned doe = yoe * 365 + yoe/4 - yoe/100 + doy;
        year_starts[y] = (int32_t)(era * 146097 + (int)doe - 719468);
    }
    // For fast 2-year case
    bool fast_year = (num_years == 2);
    int32_t year_threshold = fast_year ? year_starts[1] : 0;

    const uint8_t* supp_flag_ptr = supp_nation_flag.data();
    const uint8_t* cust_flag_ptr = cust_nation_flag.data();
    const int32_t* orders_ck_ptr = orders_custkey.data();

    { GENDB_PHASE("main_scan");

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        double* local_agg = thread_agg[tid].data();

        #pragma omp for schedule(dynamic, 1)
        for (int64_t blk = 0; blk < (int64_t)zm_num_blocks; blk++) {
            // Zonemap check
            int32_t bmin = zm_minmax[blk * 2];
            int32_t bmax = zm_minmax[blk * 2 + 1];
            if (bmax < p_shipdate_lo || bmin > p_shipdate_hi) continue;

            int64_t row_start = blk * zm_block_size;
            int64_t row_end = row_start + zm_block_size;
            if (row_end > num_lineitem) row_end = num_lineitem;

            for (int64_t i = row_start; i < row_end; i++) {
                // 1. Date filter
                int32_t sd = l_shipdate[i];
                if (sd < p_shipdate_lo || sd > p_shipdate_hi) continue;

                // 2. Supplier nation check
                int32_t sk = l_suppkey[i];
                if ((uint32_t)sk >= supp_lookup_n) continue;
                uint8_t sf = supp_flag_ptr[sk];
                if (sf == 0) continue;

                // 3. Order → Customer nation check
                int32_t ok = l_orderkey[i];
                if ((uint32_t)ok >= ord_lookup_n) continue;
                int32_t ck = orders_ck_ptr[ok];
                if (ck < 0 || (uint32_t)ck >= cust_lookup_n) continue;
                uint8_t cf = cust_flag_ptr[ck];
                if (cf == 0) continue;

                // 4. Validate nation pair
                int8_t pid = pair_table[sf][cf];
                if (pid < 0) continue;

                // 5. Compute year index and volume
                int yr_idx;
                if (fast_year) {
                    yr_idx = (sd >= year_threshold) ? 1 : 0;
                } else {
                    yr_idx = days_to_year(sd) - min_year;
                }

                double volume = l_extprice[i] * (1.0 - l_discount[i]);
                local_agg[pid * num_years + yr_idx] += volume;
            }
        }
    }

    } // main_scan

    // ── Merge thread-local aggregations ──────────────────────────────────────
    std::vector<double> agg(num_buckets, 0.0);
    for (int t = 0; t < nthreads; t++) {
        for (int b = 0; b < num_buckets; b++) {
            agg[b] += thread_agg[t][b];
        }
    }

    // ── Phase 6: Output ──────────────────────────────────────────────────────
    { GENDB_PHASE("output");

    // Build output rows: (supp_nation, cust_nation, year, revenue)
    struct Row {
        std::string supp_nation, cust_nation;
        int year;
        double revenue;
    };
    std::vector<Row> rows;
    for (int p = 0; p < 2; p++) {
        for (int y = 0; y < num_years; y++) {
            double rev = agg[p * num_years + y];
            if (rev != 0.0) {
                rows.push_back({pair_supp_name[p], pair_cust_name[p], min_year + y, rev});
            }
        }
    }

    // Sort by supp_nation, cust_nation, year
    std::sort(rows.begin(), rows.end(), [](const Row& a, const Row& b) {
        if (a.supp_nation != b.supp_nation) return a.supp_nation < b.supp_nation;
        if (a.cust_nation != b.cust_nation) return a.cust_nation < b.cust_nation;
        return a.year < b.year;
    });

    // Write CSV
    std::string outpath = results_dir + "/Q7.csv";
    FILE* fp = fopen(outpath.c_str(), "w");
    fprintf(fp, "supp_nation,cust_nation,l_year,revenue\n");
    for (const auto& r : rows) {
        fprintf(fp, "%s,%s,%d,%.4f\n", r.supp_nation.c_str(), r.cust_nation.c_str(), r.year, r.revenue);
    }
    fclose(fp);

    } // output

    } // total

    return 0;
}
