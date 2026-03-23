#include <cstdint>
#include <cstdio>
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

// --- mmap helper ---
struct MappedFile {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    void open(const std::string& path, int advice = MADV_SEQUENTIAL) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::perror(path.c_str()); std::exit(1); }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
        madvise(data, size, advice);
    }

    ~MappedFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) ::close(fd);
    }

    template<typename T> const T* as() const { return reinterpret_cast<const T*>(data); }
    template<typename T> const T* as_offset(size_t byte_offset) const {
        return reinterpret_cast<const T*>(static_cast<const char*>(data) + byte_offset);
    }
};

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params...]\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();

    // Parse parameters
    int32_t date_lower = gendb::parse_date_arg(argc, argv, "--o_orderdate_lower", gendb::date_str_to_epoch_days("1994-01-01"));
    int32_t date_upper_base = gendb::parse_date_arg(argc, argv, "--o_orderdate_upper", gendb::date_str_to_epoch_days("1994-01-01"));
    int32_t date_upper = gendb::add_years(date_upper_base, 1); // + INTERVAL '1' YEAR
    std::string r_name_eq = gendb::parse_string_arg(argc, argv, "--r_name_eq", "ASIA");

    // --- mmap all files before timer to exclude TLB shootdown ---
    // Region
    MappedFile r_regionkey_f, r_name_offsets_f, r_name_data_f;
    r_regionkey_f.open(gendb_dir + "/region/r_regionkey.bin");
    r_name_offsets_f.open(gendb_dir + "/region/r_name_offsets.bin");
    r_name_data_f.open(gendb_dir + "/region/r_name_data.bin");

    // Nation
    MappedFile n_nationkey_f, n_name_offsets_f, n_name_data_f, n_regionkey_f;
    n_nationkey_f.open(gendb_dir + "/nation/n_nationkey.bin");
    n_name_offsets_f.open(gendb_dir + "/nation/n_name_offsets.bin");
    n_name_data_f.open(gendb_dir + "/nation/n_name_data.bin");
    n_regionkey_f.open(gendb_dir + "/nation/n_regionkey.bin");

    // Supplier
    MappedFile s_nationkey_f;
    s_nationkey_f.open(gendb_dir + "/supplier/s_nationkey.bin");

    // Orders
    MappedFile o_orderkey_f, o_custkey_f, o_orderdate_f;
    o_orderkey_f.open(gendb_dir + "/orders/o_orderkey.bin", MADV_SEQUENTIAL);
    o_custkey_f.open(gendb_dir + "/orders/o_custkey.bin", MADV_SEQUENTIAL);
    o_orderdate_f.open(gendb_dir + "/orders/o_orderdate.bin", MADV_SEQUENTIAL);

    // Lineitem
    MappedFile l_orderkey_f, l_suppkey_f, l_extendedprice_f, l_discount_f;
    l_orderkey_f.open(gendb_dir + "/lineitem/l_orderkey.bin", MADV_RANDOM);
    l_suppkey_f.open(gendb_dir + "/lineitem/l_suppkey.bin", MADV_RANDOM);
    l_extendedprice_f.open(gendb_dir + "/lineitem/l_extendedprice.bin", MADV_RANDOM);
    l_discount_f.open(gendb_dir + "/lineitem/l_discount.bin", MADV_RANDOM);

    // Customer nationkey
    MappedFile c_nationkey_f;
    c_nationkey_f.open(gendb_dir + "/customer/c_nationkey.bin", MADV_RANDOM);

    // Indexes
    MappedFile zonemap_f, cust_lookup_f, supp_lookup_f, li_grouped_f;
    zonemap_f.open(gendb_dir + "/indexes/orders_o_orderdate_zonemap.bin", MADV_SEQUENTIAL);
    cust_lookup_f.open(gendb_dir + "/indexes/customer_c_custkey_lookup.bin", MADV_RANDOM);
    supp_lookup_f.open(gendb_dir + "/indexes/supplier_s_suppkey_lookup.bin", MADV_RANDOM);
    li_grouped_f.open(gendb_dir + "/indexes/lineitem_l_orderkey_grouped.bin", MADV_RANDOM);

    {
    GENDB_PHASE("total");

    // Pointers
    const int32_t* r_regionkey = r_regionkey_f.as<int32_t>();
    const uint32_t* r_name_offsets = r_name_offsets_f.as<uint32_t>();
    const char* r_name_data = r_name_data_f.as<char>();

    const int32_t* n_nationkey = n_nationkey_f.as<int32_t>();
    const uint32_t* n_name_offsets = n_name_offsets_f.as<uint32_t>();
    const char* n_name_data = n_name_data_f.as<char>();
    const int32_t* n_regionkey = n_regionkey_f.as<int32_t>();

    const int32_t* s_nationkey = s_nationkey_f.as<int32_t>();
    size_t num_suppliers = s_nationkey_f.size / sizeof(int32_t);

    const int32_t* o_orderkey = o_orderkey_f.as<int32_t>();
    const int32_t* o_custkey = o_custkey_f.as<int32_t>();
    const int32_t* o_orderdate = o_orderdate_f.as<int32_t>();
    size_t num_orders = o_orderdate_f.size / sizeof(int32_t);

    const int32_t* l_suppkey = l_suppkey_f.as<int32_t>();
    const double* l_extendedprice = l_extendedprice_f.as<double>();
    const double* l_discount = l_discount_f.as<double>();

    const int32_t* c_nationkey = c_nationkey_f.as<int32_t>();

    // Zonemap: header = {uint64_t num_blocks, uint32_t block_size}, body = int32_t[num_blocks*2]
    uint64_t zm_num_blocks = *zonemap_f.as<uint64_t>();
    uint32_t zm_block_size = *zonemap_f.as_offset<uint32_t>(8);
    const int32_t* zm_minmax = zonemap_f.as_offset<int32_t>(12); // after 8+4=12 bytes

    // Customer lookup: header = uint64_t num_entries, body = int32_t[num_entries]
    uint64_t cust_num_entries = *cust_lookup_f.as<uint64_t>();
    const int32_t* cust_lookup = cust_lookup_f.as_offset<int32_t>(8);

    // Supplier lookup: header = uint64_t num_entries, body = int32_t[num_entries]
    uint64_t supp_num_entries = *supp_lookup_f.as<uint64_t>();
    const int32_t* supp_lookup = supp_lookup_f.as_offset<int32_t>(8);

    // Lineitem grouped: header = uint64_t num_entries, body = uint32_t[num_entries*2] interleaved [start, count, ...]
    uint64_t li_num_entries = *li_grouped_f.as<uint64_t>();
    const uint32_t* li_grouped = li_grouped_f.as_offset<uint32_t>(8);

    // ===================== PHASE 1: Dimension filters =====================
    bool asia_nationkeys[25] = {};
    int asia_nation_idx[25]; // nationkey -> index in output (0-4)
    std::string nation_names[5];
    int num_asia_nations = 0;

    {
        GENDB_PHASE("dim_filter");

        // Find ASIA regionkey
        int32_t asia_rk = -1;
        for (int i = 0; i < 5; i++) {
            uint32_t off = r_name_offsets[i];
            uint32_t len = r_name_offsets[i + 1] - off;
            if (len == r_name_eq.size() && memcmp(r_name_data + off, r_name_eq.c_str(), len) == 0) {
                asia_rk = r_regionkey[i];
                break;
            }
        }

        // Find ASIA nations
        memset(asia_nation_idx, -1, sizeof(asia_nation_idx));
        for (int i = 0; i < 25; i++) {
            if (n_regionkey[i] == asia_rk) {
                int nk = n_nationkey[i];
                asia_nationkeys[nk] = true;
                asia_nation_idx[nk] = num_asia_nations;
                uint32_t off = n_name_offsets[i];
                uint32_t len = n_name_offsets[i + 1] - off;
                nation_names[num_asia_nations] = std::string(n_name_data + off, len);
                num_asia_nations++;
            }
        }
    }

    // ===================== PHASE 2: Build supplier nationkey direct array =====================
    // s_suppkey is 1-based, sequential. Build direct array: supp_nk[suppkey] = s_nationkey
    // Use supplier lookup index for mapping suppkey -> row_id -> s_nationkey
    // But since supplier is sorted by s_suppkey, row i has suppkey = i+1 (1-based).
    // Direct array approach: supp_nk_arr[suppkey] = nationkey
    std::vector<int8_t> supp_nk_arr;
    {
        GENDB_PHASE("build_supplier");
        // max suppkey = num_suppliers (1-based)
        supp_nk_arr.resize(num_suppliers + 1, -1);
        for (size_t i = 0; i < num_suppliers; i++) {
            // suppkey = i+1 (sorted by s_suppkey)
            supp_nk_arr[i + 1] = (int8_t)s_nationkey[i];
        }
    }

    // ===================== PHASE 3: Scan orders + customer filter (parallel) =====================
    struct QualOrder {
        int32_t orderkey;
        int8_t c_nationkey;
    };

    std::vector<QualOrder> qualifying_orders;
    {
        GENDB_PHASE("main_scan");

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<QualOrder>> thread_local_orders(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local = thread_local_orders[tid];

            #pragma omp for schedule(dynamic, 1)
            for (uint64_t blk = 0; blk < zm_num_blocks; blk++) {
                int32_t bmin = zm_minmax[blk * 2];
                int32_t bmax = zm_minmax[blk * 2 + 1];

                // Skip block if no overlap with [date_lower, date_upper)
                if (bmax < date_lower || bmin >= date_upper) continue;

                uint64_t row_start = blk * zm_block_size;
                uint64_t row_end = row_start + zm_block_size;
                if (row_end > num_orders) row_end = num_orders;

                for (uint64_t r = row_start; r < row_end; r++) {
                    int32_t od = o_orderdate[r];
                    if (od >= date_lower && od < date_upper) {
                        int32_t ck = o_custkey[r];
                        // Lookup customer nationkey
                        if ((uint32_t)ck < cust_num_entries) {
                            int32_t row_id = cust_lookup[ck];
                            if (row_id >= 0) {
                                int32_t cnk = c_nationkey[row_id];
                                if (cnk >= 0 && cnk < 25 && asia_nationkeys[cnk]) {
                                    QualOrder qo;
                                    qo.orderkey = o_orderkey[r];
                                    qo.c_nationkey = (int8_t)cnk;
                                    local.push_back(qo);
                                }
                            }
                        }
                    }
                }
            }
        }

        // Merge
        size_t total = 0;
        for (auto& v : thread_local_orders) total += v.size();
        qualifying_orders.reserve(total);
        for (auto& v : thread_local_orders) {
            qualifying_orders.insert(qualifying_orders.end(), v.begin(), v.end());
        }
    }

    // ===================== PHASE 4: Sort qualifying orders for sequential lineitem access =====================
    {
        GENDB_PHASE("sort_orders");
        std::sort(qualifying_orders.begin(), qualifying_orders.end(),
            [](const QualOrder& a, const QualOrder& b) { return a.orderkey < b.orderkey; });
    }

    // Set MADV_SEQUENTIAL on lineitem columns now that we'll probe in sorted order
    madvise((void*)l_suppkey, l_suppkey_f.size, MADV_SEQUENTIAL);
    madvise((void*)l_extendedprice, l_extendedprice_f.size, MADV_SEQUENTIAL);
    madvise((void*)l_discount, l_discount_f.size, MADV_SEQUENTIAL);

    // ===================== PHASE 5: Probe lineitem + supplier check + aggregate =====================
    long double revenue[5] = {};
    {
        GENDB_PHASE("probe_aggregate");

        int nthreads = omp_get_max_threads();
        std::vector<std::array<long double, 5>> thread_revenue(nthreads, {0,0,0,0,0});
        size_t nq = qualifying_orders.size();

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local_rev = thread_revenue[tid];

            #pragma omp for schedule(dynamic, 4096)
            for (size_t i = 0; i < nq; i++) {
                int32_t ok = qualifying_orders[i].orderkey;
                int8_t cnk = qualifying_orders[i].c_nationkey;

                if ((uint64_t)ok >= li_num_entries) continue;
                uint32_t li_start = li_grouped[ok * 2];
                uint32_t li_count = li_grouped[ok * 2 + 1];

                for (uint32_t j = 0; j < li_count; j++) {
                    uint32_t li_row = li_start + j;
                    int32_t sk = l_suppkey[li_row];

                    // Check supplier nationkey matches customer nationkey (locality)
                    int8_t snk = supp_nk_arr[sk];
                    if (snk == cnk) {
                        int idx = asia_nation_idx[cnk];
                        double ep = l_extendedprice[li_row];
                        double disc = l_discount[li_row];
                        local_rev[idx] += ep * (1.0 - disc);
                    }
                }
            }
        }

        // Merge
        for (int t = 0; t < nthreads; t++) {
            for (int n = 0; n < 5; n++) {
                revenue[n] += thread_revenue[t][n];
            }
        }
    }

    // ===================== PHASE 6: Sort & Output =====================
    {
        GENDB_PHASE("output");

        // Sort by revenue DESC
        int order[5] = {0, 1, 2, 3, 4};
        std::sort(order, order + num_asia_nations,
            [&](int a, int b) { return revenue[a] > revenue[b]; });

        std::string outpath = results_dir + "/Q5.csv";
        FILE* fp = fopen(outpath.c_str(), "w");
        if (!fp) { perror(outpath.c_str()); return 1; }
        fprintf(fp, "n_name,revenue\n");
        for (int i = 0; i < num_asia_nations; i++) {
            int idx = order[i];
            fprintf(fp, "\"%s\",%.4Lf\n", nation_names[idx].c_str(), revenue[idx]);
        }
        fclose(fp);
    }

    } // end total timer
    return 0;
}
