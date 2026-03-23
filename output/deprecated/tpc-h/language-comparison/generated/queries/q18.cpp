// Q18: Large Volume Customer — GenDB iteration 1
// Key optimizations: no MAP_POPULATE, lazy mmap, MADV_SEQUENTIAL for lineitem,
// MADV_RANDOM for indexes, defer orders/customer mmap until after subquery
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include "timing_utils.h"

// ---------- lightweight mmap helper (no MAP_POPULATE) ----------
struct MMap {
    void* ptr = nullptr;
    size_t len = 0;

    void open(const std::string& path, int advice = MADV_SEQUENTIAL) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::perror(path.c_str()); std::exit(1); }
        struct stat st; fstat(fd, &st); len = st.st_size;
        ptr = mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
        ::close(fd);
        if (len > 0) madvise(ptr, len, advice);
    }

    ~MMap() { if (ptr && ptr != MAP_FAILED) munmap(ptr, len); }

    template<typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
    template<typename T> size_t count() const { return len / sizeof(T); }
};

// ---------- date helper (civil from epoch days) ----------
static void days_to_ymd(int32_t days, int& y, int& m, int& d) {
    days += 719468;
    int era = (days >= 0 ? days : days - 146096) / 146097;
    unsigned doe = static_cast<unsigned>(days - era * 146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    y = static_cast<int>(yoe) + era * 400;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp = (5*doy + 2) / 153;
    d = doy - (153*mp + 2)/5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2);
}

static void format_date(int32_t days, char* buf) {
    int y, m, d;
    days_to_ymd(days, y, m, d);
    std::sprintf(buf, "%04d-%02d-%02d", y, m, d);
}

// ---------- Result struct ----------
struct QualOrder {
    int32_t orderkey;
    double  sum_qty;
    double  o_totalprice;
    int32_t o_orderdate;
    int32_t o_custkey;
};

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gd = argv[1];
    std::string rd = argv[2];

    GENDB_PHASE("total");

    // ========== Phase 1: Subquery scan — find orderkeys with SUM(l_quantity) > 300 ==========
    // Lazy mmap only lineitem columns (sequential scan, ~685MB cached)
    MMap m_l_orderkey, m_l_quantity;
    m_l_orderkey.open(gd + "/lineitem/l_orderkey.bin", MADV_SEQUENTIAL);
    m_l_quantity.open(gd + "/lineitem/l_quantity.bin", MADV_SEQUENTIAL);

    const int32_t* l_orderkey = m_l_orderkey.as<int32_t>();
    const double*  l_quantity = m_l_quantity.as<double>();
    const size_t   li_nrows   = m_l_orderkey.count<int32_t>();

    std::vector<std::pair<int32_t, double>> qualifying;  // (orderkey, sum_qty)
    {
        GENDB_PHASE("main_scan");

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<std::pair<int32_t, double>>> thr_res(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int nt  = omp_get_num_threads();
            size_t chunk = li_nrows / nt;
            size_t start = tid * chunk;
            size_t end   = (tid == nt - 1) ? li_nrows : start + chunk;

            // Align start to orderkey group boundary (skip partial group at start)
            if (tid > 0 && start < li_nrows) {
                int32_t prev = l_orderkey[start - 1];
                while (start < end && l_orderkey[start] == prev) start++;
            }
            // Align end to orderkey group boundary (extend to include full group)
            if (tid < nt - 1 && end < li_nrows) {
                int32_t bnd = l_orderkey[end - 1];
                while (end < li_nrows && l_orderkey[end] == bnd) end++;
            }

            auto& local = thr_res[tid];
            size_t i = start;
            while (i < end) {
                int32_t ok = l_orderkey[i];
                double sq = 0.0;
                while (i < end && l_orderkey[i] == ok) {
                    sq += l_quantity[i];
                    i++;
                }
                if (sq > 300.0) {
                    local.emplace_back(ok, sq);
                }
            }
        }

        // Merge (~15K total entries)
        size_t total = 0;
        for (auto& v : thr_res) total += v.size();
        qualifying.reserve(total);
        for (auto& v : thr_res) qualifying.insert(qualifying.end(), v.begin(), v.end());
    }

    // ========== Phase 2: Index lookups — enrich with orders + customer info ==========
    // Now mmap orders columns and indexes (deferred — only needed for ~15K rows)
    MMap m_orders_lookup, m_o_custkey, m_o_orderdate, m_o_totalprice;
    m_orders_lookup.open(gd + "/indexes/orders_orderkey_lookup.bin", MADV_RANDOM);
    m_o_custkey.open(gd + "/orders/o_custkey.bin", MADV_RANDOM);
    m_o_orderdate.open(gd + "/orders/o_orderdate.bin", MADV_RANDOM);
    m_o_totalprice.open(gd + "/orders/o_totalprice.bin", MADV_RANDOM);

    uint32_t orders_max_key = *m_orders_lookup.as<uint32_t>();
    const int32_t* orders_lookup = reinterpret_cast<const int32_t*>(
        static_cast<const char*>(m_orders_lookup.ptr) + 4);
    const int32_t* o_custkey    = m_o_custkey.as<int32_t>();
    const int32_t* o_orderdate  = m_o_orderdate.as<int32_t>();
    const double*  o_totalprice = m_o_totalprice.as<double>();

    std::vector<QualOrder> results;
    results.reserve(qualifying.size());
    {
        GENDB_PHASE("build_joins");
        for (auto& [ok, sq] : qualifying) {
            if ((uint32_t)ok > orders_max_key) continue;
            int32_t orow = orders_lookup[ok];
            if (orow < 0) continue;
            results.push_back({ok, sq, o_totalprice[orow], o_orderdate[orow], o_custkey[orow]});
        }
    }

    // ========== Phase 3: Sort and limit 100 ==========
    {
        GENDB_PHASE("sort_and_limit");
        auto cmp = [](const QualOrder& a, const QualOrder& b) {
            if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
            return a.o_orderdate < b.o_orderdate;
        };
        if (results.size() > 100)
            std::partial_sort(results.begin(), results.begin() + 100, results.end(), cmp);
        else
            std::sort(results.begin(), results.end(), cmp);
    }

    // ========== Phase 4: Output ==========
    {
        GENDB_PHASE("output");

        // Customer lookup + name — only for top 100
        MMap m_cust_lookup, m_c_name_off, m_c_name_dat;
        m_cust_lookup.open(gd + "/indexes/customer_custkey_lookup.bin", MADV_RANDOM);
        m_c_name_off.open(gd + "/customer/c_name_offsets.bin", MADV_RANDOM);
        m_c_name_dat.open(gd + "/customer/c_name_data.bin", MADV_RANDOM);

        uint32_t cust_max_key = *m_cust_lookup.as<uint32_t>();
        const int32_t* cust_lookup = reinterpret_cast<const int32_t*>(
            static_cast<const char*>(m_cust_lookup.ptr) + 4);
        const int64_t* c_name_offsets = m_c_name_off.as<int64_t>();
        const char*    c_name_data    = m_c_name_dat.as<char>();

        std::string outpath = rd + "/Q18.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) { std::perror(outpath.c_str()); return 1; }

        std::fprintf(fp, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        size_t limit = std::min<size_t>(100, results.size());
        char datebuf[16];
        for (size_t i = 0; i < limit; i++) {
            auto& r = results[i];

            // Resolve customer name via custkey lookup
            int32_t crow = -1;
            if ((uint32_t)r.o_custkey <= cust_max_key)
                crow = cust_lookup[r.o_custkey];

            const char* name_ptr = "";
            size_t name_len = 0;
            if (crow >= 0) {
                int64_t s = c_name_offsets[crow];
                int64_t e = c_name_offsets[crow + 1];
                name_ptr = c_name_data + s;
                name_len = e - s;
            }

            format_date(r.o_orderdate, datebuf);
            std::fprintf(fp, "%.*s,%d,%d,%s,%.2f,%.2f\n",
                (int)name_len, name_ptr,
                r.o_custkey, r.orderkey, datebuf,
                r.o_totalprice, r.sum_qty);
        }
        std::fclose(fp);
    }

    return 0;
}
