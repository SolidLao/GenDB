#include <cstdio>
#include <cstdint>
#include <cstring>
#include <cstdlib>
#include <string>
#include <vector>
#include <thread>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

// Multi-word string arg parser: collects words until next --flag
static std::string parse_multiword_string_arg(int argc, char* argv[], const char* name, const std::string& default_val) {
    for (int i = 1; i < argc; i++) {
        if (std::strcmp(argv[i], name) == 0) {
            std::string result;
            for (int j = i + 1; j < argc; j++) {
                if (argv[j][0] == '-' && argv[j][1] == '-') break;
                if (!result.empty()) result += ' ';
                result += argv[j];
            }
            return result.empty() ? default_val : result;
        }
    }
    return default_val;
}

struct MmapFile {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    void open(const std::string& path, int advice = MADV_NORMAL) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::fprintf(stderr, "Cannot open %s\n", path.c_str()); std::exit(1); }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { std::fprintf(stderr, "mmap failed %s\n", path.c_str()); std::exit(1); }
        if (advice != MADV_NORMAL) madvise(data, size, advice);
    }

    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) ::close(fd);
    }

    MmapFile() = default;
    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;
    MmapFile(MmapFile&& o) noexcept : data(o.data), size(o.size), fd(o.fd) { o.data=nullptr; o.fd=-1; }
};

template<typename T>
const T* col_ptr(const MmapFile& f) { return reinterpret_cast<const T*>(f.data); }

// Dense PK array: header is uint64_t num_entries, then int32_t[num_entries]
struct DensePKArray {
    MmapFile file;
    uint64_t num_entries = 0;
    const int32_t* arr = nullptr;

    void open(const std::string& path) {
        file.open(path);
        num_entries = *reinterpret_cast<const uint64_t*>(file.data);
        arr = reinterpret_cast<const int32_t*>(static_cast<const char*>(file.data) + 8);
    }

    int32_t lookup(int32_t key) const {
        if (key < 0 || (uint64_t)key >= num_entries) return -1;
        return arr[key];
    }
};

int main(int argc, char* argv[]) {
    if (argc < 3) {
        std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params...]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();

    // Parse parameters
    std::string p_type_eq = parse_multiword_string_arg(argc, argv, "--p_type_eq", "ECONOMY ANODIZED STEEL");
    int32_t o_orderdate_lower = gendb::parse_date_arg(argc, argv, "--o_orderdate_lower", 9131);
    int32_t o_orderdate_upper = gendb::parse_date_arg(argc, argv, "--o_orderdate_upper", 9861);
    std::string r_name_eq = gendb::parse_string_arg(argc, argv, "--r_name_eq", "AMERICA");

    // Determine year boundary for the date range
    // We need to extract year from dates. For the default range [1995-01-01, 1996-12-31],
    // we use the YEAR_TABLE. For generality, collect all distinct years in range.
    // But the plan says 2 groups. We'll use YEAR_TABLE for extraction.

    // Mmap all files BEFORE timing scope to exclude munmap TLB shootdown
    // Dimension columns
    MmapFile r_regionkey_f, r_name_offsets_f, r_name_data_f;
    MmapFile n_nationkey_f, n_name_offsets_f, n_name_data_f, n_regionkey_f;
    MmapFile c_custkey_f, c_nationkey_f;
    MmapFile s_nationkey_f;
    MmapFile p_partkey_f, p_type_f, p_type_dict_f;
    MmapFile o_orderdate_f, o_custkey_f;

    // Lineitem columns
    MmapFile l_partkey_f, l_orderkey_f, l_suppkey_f, l_extprice_f, l_discount_f;

    // Indexes
    DensePKArray orders_idx, customer_idx, supplier_idx;

    {
        GENDB_PHASE("data_loading");

        // Region
        r_regionkey_f.open(gendb_dir + "/region/r_regionkey.bin");
        r_name_offsets_f.open(gendb_dir + "/region/r_name_offsets.bin");
        r_name_data_f.open(gendb_dir + "/region/r_name_data.bin");

        // Nation
        n_nationkey_f.open(gendb_dir + "/nation/n_nationkey.bin");
        n_name_offsets_f.open(gendb_dir + "/nation/n_name_offsets.bin");
        n_name_data_f.open(gendb_dir + "/nation/n_name_data.bin");
        n_regionkey_f.open(gendb_dir + "/nation/n_regionkey.bin");

        // Customer
        c_custkey_f.open(gendb_dir + "/customer/c_custkey.bin");
        c_nationkey_f.open(gendb_dir + "/customer/c_nationkey.bin");

        // Supplier
        s_nationkey_f.open(gendb_dir + "/supplier/s_nationkey.bin");

        // Part
        p_partkey_f.open(gendb_dir + "/part/p_partkey.bin");
        p_type_f.open(gendb_dir + "/part/p_type.bin");
        p_type_dict_f.open(gendb_dir + "/part/p_type_dict.bin");

        // Orders
        o_orderdate_f.open(gendb_dir + "/orders/o_orderdate.bin");
        o_custkey_f.open(gendb_dir + "/orders/o_custkey.bin");

        // Lineitem - MADV_SEQUENTIAL on l_partkey (scanned fully), demand paged on others
        l_partkey_f.open(gendb_dir + "/lineitem/l_partkey.bin", MADV_SEQUENTIAL);
        l_orderkey_f.open(gendb_dir + "/lineitem/l_orderkey.bin");
        l_suppkey_f.open(gendb_dir + "/lineitem/l_suppkey.bin");
        l_extprice_f.open(gendb_dir + "/lineitem/l_extendedprice.bin");
        l_discount_f.open(gendb_dir + "/lineitem/l_discount.bin");

        // Indexes
        orders_idx.open(gendb_dir + "/indexes/orders_o_orderkey_lookup.bin");
        customer_idx.open(gendb_dir + "/indexes/customer_c_custkey_lookup.bin");
        supplier_idx.open(gendb_dir + "/indexes/supplier_s_suppkey_lookup.bin");
    }

    // Column pointers
    const int32_t* r_regionkey = col_ptr<int32_t>(r_regionkey_f);
    const uint32_t* r_name_offsets = col_ptr<uint32_t>(r_name_offsets_f);
    const char* r_name_data = static_cast<const char*>(r_name_data_f.data);
    int num_regions = r_regionkey_f.size / sizeof(int32_t);

    const int32_t* n_nationkey = col_ptr<int32_t>(n_nationkey_f);
    const uint32_t* n_name_offsets = col_ptr<uint32_t>(n_name_offsets_f);
    const char* n_name_data = static_cast<const char*>(n_name_data_f.data);
    const int32_t* n_regionkey = col_ptr<int32_t>(n_regionkey_f);
    int num_nations = n_nationkey_f.size / sizeof(int32_t);

    const int32_t* c_custkey = col_ptr<int32_t>(c_custkey_f);
    const int32_t* c_nationkey = col_ptr<int32_t>(c_nationkey_f);
    int64_t num_customers = c_custkey_f.size / sizeof(int32_t);

    const int32_t* s_nationkey = col_ptr<int32_t>(s_nationkey_f);

    const int32_t* p_partkey = col_ptr<int32_t>(p_partkey_f);
    const int16_t* p_type = col_ptr<int16_t>(p_type_f);
    int64_t num_parts = p_partkey_f.size / sizeof(int32_t);

    const int32_t* o_orderdate = col_ptr<int32_t>(o_orderdate_f);
    const int32_t* o_custkey = col_ptr<int32_t>(o_custkey_f);

    const int32_t* l_partkey = col_ptr<int32_t>(l_partkey_f);
    const int32_t* l_orderkey = col_ptr<int32_t>(l_orderkey_f);
    const int32_t* l_suppkey = col_ptr<int32_t>(l_suppkey_f);
    const double* l_extprice = col_ptr<double>(l_extprice_f);
    const double* l_discount = col_ptr<double>(l_discount_f);
    int64_t num_lineitem = l_partkey_f.size / sizeof(int32_t);

    // Collect distinct years in the date range
    int min_year = gendb::extract_year(o_orderdate_lower);
    int max_year = gendb::extract_year(o_orderdate_upper);
    int num_years = max_year - min_year + 1;

    // Aggregation arrays per thread
    int num_threads = std::thread::hardware_concurrency();
    if (num_threads < 1) num_threads = 1;

    GENDB_PHASE("total");

    // Dimension setup
    // 1. Find AMERICA regionkey
    int32_t america_regionkey = -1;
    {
        GENDB_PHASE("dim_filter");
        for (int i = 0; i < num_regions; i++) {
            uint32_t off = r_name_offsets[i];
            uint32_t next_off = r_name_offsets[i + 1];
            int len = (int)(next_off - off);
            if (len == (int)r_name_eq.size() && std::memcmp(r_name_data + off, r_name_eq.c_str(), len) == 0) {
                america_regionkey = r_regionkey[i];
                break;
            }
        }

        // 2. Find AMERICA nation keys and BRAZIL nationkey
        // For parameterized version, we need the n2.n_name matching -- but in Q8
        // the CASE checks supplier nation = specific nation (BRAZIL by default).
        // The param is r_name_eq (region) and the nation checked in CASE is hardcoded
        // to 'BRAZIL' in the SQL. Actually looking at the SQL more carefully,
        // nation = 'BRAZIL' is in the CASE. But the parameterized SQL doesn't
        // parameterize the BRAZIL nation name. So we keep it hardcoded.
        // Actually re-reading the parameterized SQL: the CASE says "WHEN nation = 'BRAZIL'"
        // which is NOT parameterized. So BRAZIL is hardcoded.
    }

    std::vector<int32_t> america_nationkeys;
    int32_t brazil_nationkey = -1;

    for (int i = 0; i < num_nations; i++) {
        if (n_regionkey[i] == america_regionkey) {
            america_nationkeys.push_back(n_nationkey[i]);
        }
        uint32_t off = n_name_offsets[i];
        uint32_t next_off = n_name_offsets[i + 1];
        int len = (int)(next_off - off);
        if (len == 6 && std::memcmp(n_name_data + off, "BRAZIL", 6) == 0) {
            brazil_nationkey = n_nationkey[i];
        }
    }

    // Build america nationkey set (small, use bool array)
    bool america_nk_set[26] = {};
    for (int32_t nk : america_nationkeys) {
        if (nk >= 0 && nk < 26) america_nk_set[nk] = true;
    }

    // 3. Build part qualifying bitset
    // First find dictionary code for p_type_eq
    std::vector<uint64_t> part_bitset;
    {
        GENDB_PHASE("build_part_bitset");

        // Parse dictionary
        const uint8_t* dict_data = static_cast<const uint8_t*>(p_type_dict_f.data);
        uint32_t dict_count = *reinterpret_cast<const uint32_t*>(dict_data);
        const uint8_t* ptr = dict_data + 4;
        int16_t target_code = -1;

        for (uint32_t c = 0; c < dict_count; c++) {
            uint16_t slen = *reinterpret_cast<const uint16_t*>(ptr);
            ptr += 2;
            if ((int)slen == (int)p_type_eq.size() && std::memcmp(ptr, p_type_eq.c_str(), slen) == 0) {
                target_code = (int16_t)c;
            }
            ptr += slen;
        }

        // Find max partkey for bitset sizing
        int32_t max_pk = 0;
        for (int64_t i = 0; i < num_parts; i++) {
            if (p_partkey[i] > max_pk) max_pk = p_partkey[i];
        }

        size_t bitset_words = (max_pk + 64) / 64;
        part_bitset.resize(bitset_words, 0);

        if (target_code >= 0) {
            for (int64_t i = 0; i < num_parts; i++) {
                if (p_type[i] == target_code) {
                    int32_t pk = p_partkey[i];
                    part_bitset[pk >> 6] |= (1ULL << (pk & 63));
                }
            }
        }
    }

    // 4. Build america customer bitset
    std::vector<uint64_t> cust_bitset;
    {
        GENDB_PHASE("build_cust_bitset");

        // Find max custkey
        int32_t max_ck = 0;
        for (int64_t i = 0; i < num_customers; i++) {
            if (c_custkey[i] > max_ck) max_ck = c_custkey[i];
        }

        size_t bitset_words = (max_ck + 64) / 64;
        cust_bitset.resize(bitset_words, 0);

        for (int64_t i = 0; i < num_customers; i++) {
            int32_t nk = c_nationkey[i];
            if (nk >= 0 && nk < 26 && america_nk_set[nk]) {
                int32_t ck = c_custkey[i];
                cust_bitset[ck >> 6] |= (1ULL << (ck & 63));
            }
        }
    }

    // Main scan
    // Thread-local accumulators: brazil_volume[year_idx], total_volume[year_idx]
    struct ThreadAcc {
        double brazil_vol[8] = {};  // padded
        double total_vol[8] = {};
    };
    std::vector<ThreadAcc> thread_accs(num_threads);

    const uint64_t* part_bs = part_bitset.data();
    const uint64_t* cust_bs = cust_bitset.data();
    size_t part_bs_size = part_bitset.size();
    size_t cust_bs_size = cust_bitset.size();

    {
        GENDB_PHASE("main_scan");

        const int32_t* oidx_arr = orders_idx.arr;
        uint64_t oidx_num = orders_idx.num_entries;
        const int32_t* sidx_arr = supplier_idx.arr;
        uint64_t sidx_num = supplier_idx.num_entries;

        auto worker = [&](int tid) {
            const int64_t morsel = 65536;
            double local_brazil[8] = {};
            double local_total[8] = {};

            for (int64_t start = (int64_t)tid * morsel; start < num_lineitem; start += (int64_t)num_threads * morsel) {
                int64_t end = start + morsel;
                if (end > num_lineitem) end = num_lineitem;

                for (int64_t i = start; i < end; i++) {
                    // 1. Part filter (most selective ~0.07%)
                    int32_t pk = l_partkey[i];
                    uint64_t pk_word = (uint64_t)pk >> 6;
                    if (pk_word >= part_bs_size || !(part_bs[pk_word] & (1ULL << (pk & 63)))) continue;

                    // 2. Order date filter via index
                    int32_t ok = l_orderkey[i];
                    if ((uint64_t)ok >= oidx_num) continue;
                    int32_t orow = oidx_arr[ok];
                    if (orow < 0) continue;
                    int32_t odate = o_orderdate[orow];
                    if (odate < o_orderdate_lower || odate > o_orderdate_upper) continue;

                    // 3. Customer AMERICA filter
                    int32_t ck = o_custkey[orow];
                    uint64_t ck_word = (uint64_t)ck >> 6;
                    if (ck_word >= cust_bs_size || !(cust_bs[ck_word] & (1ULL << (ck & 63)))) continue;

                    // 4. Supplier nationkey lookup
                    int32_t sk = l_suppkey[i];
                    int32_t s_nk = -1;
                    if ((uint64_t)sk < sidx_num) {
                        int32_t srow = sidx_arr[sk];
                        if (srow >= 0) s_nk = s_nationkey[srow];
                    }

                    // 5. Compute volume and accumulate
                    double volume = l_extprice[i] * (1.0 - l_discount[i]);
                    int year_idx = gendb::extract_year(odate) - min_year;
                    if (year_idx >= 0 && year_idx < num_years) {
                        local_total[year_idx] += volume;
                        if (s_nk == brazil_nationkey) {
                            local_brazil[year_idx] += volume;
                        }
                    }
                }
            }

            // Store to thread accumulator
            for (int y = 0; y < num_years; y++) {
                thread_accs[tid].brazil_vol[y] = local_brazil[y];
                thread_accs[tid].total_vol[y] = local_total[y];
            }
        };

        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& t : threads) t.join();
    }

    // Merge and compute
    std::vector<double> brazil_vol(num_years, 0.0);
    std::vector<double> total_vol(num_years, 0.0);

    {
        GENDB_PHASE("aggregation");
        for (int t = 0; t < num_threads; t++) {
            for (int y = 0; y < num_years; y++) {
                brazil_vol[y] += thread_accs[t].brazil_vol[y];
                total_vol[y] += thread_accs[t].total_vol[y];
            }
        }
    }

    // Output
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q8.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) { std::fprintf(stderr, "Cannot open %s\n", outpath.c_str()); return 1; }

        std::fprintf(fp, "o_year,mkt_share\n");
        for (int y = 0; y < num_years; y++) {
            int year = min_year + y;
            double mkt_share = (total_vol[y] > 0.0) ? (brazil_vol[y] / total_vol[y]) : 0.0;
            std::fprintf(fp, "%d,%.2f\n", year, mkt_share);
        }
        std::fclose(fp);
    }

    return 0;
}
