// Q18: Large Volume Customer — Optimized C++ (no unnecessary madvise, streamlined)
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

static void* raw_mmap(const std::string& path, size_t& sz) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st); sz = st.st_size;
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    ::close(fd);
    return p;
}

static void days_to_ymd(int32_t days, int& y, int& m, int& d) {
    days += 719468;
    int era = (days >= 0 ? days : days - 146096) / 146097;
    unsigned doe = (unsigned)(days - era * 146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    y = (int)yoe + era * 400;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp = (5*doy + 2) / 153;
    d = doy - (153*mp + 2)/5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2);
}

static void format_date(int32_t days, char* buf) {
    int y, m, d;
    days_to_ymd(days, y, m, d);
    sprintf(buf, "%04d-%02d-%02d", y, m, d);
}

struct QualOrder {
    int32_t orderkey;
    double sum_qty;
    double o_totalprice;
    int32_t o_orderdate;
    int32_t o_custkey;
};

int main(int argc, char** argv) {
    if (argc < 3) return 1;
    std::string gd = argv[1], rd = argv[2];

    GENDB_PHASE("total");

    size_t sz;
    void* mm_lok = raw_mmap(gd + "/lineitem/l_orderkey.bin", sz);
    size_t li_nrows = sz / sizeof(int32_t);
    void* mm_lqty = raw_mmap(gd + "/lineitem/l_quantity.bin", sz);

    const int32_t* l_orderkey = (const int32_t*)mm_lok;
    const double* l_quantity = (const double*)mm_lqty;

    std::vector<std::pair<int32_t, double>> qualifying;

    {
        GENDB_PHASE("main_scan");

        // Bandwidth-bound scan: 32 threads optimal
        omp_set_num_threads(std::min(omp_get_max_threads(), 32));
        int nthreads = omp_get_max_threads();
        std::vector<std::vector<std::pair<int32_t, double>>> thr_res(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int nt = omp_get_num_threads();
            size_t chunk = li_nrows / nt;
            size_t start = tid * chunk;
            size_t end = (tid == nt - 1) ? li_nrows : start + chunk;

            // Align to orderkey group boundaries
            if (tid > 0 && start < li_nrows) {
                int32_t prev = l_orderkey[start - 1];
                while (start < end && l_orderkey[start] == prev) start++;
            }
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
                if (__builtin_expect(sq > 300.0, 0)) {
                    local.emplace_back(ok, sq);
                }
            }
        }

        size_t total = 0;
        for (auto& v : thr_res) total += v.size();
        qualifying.reserve(total);
        for (auto& v : thr_res) qualifying.insert(qualifying.end(), v.begin(), v.end());
    }

    // Index lookups
    void* mm_olookup = raw_mmap(gd + "/indexes/orders_orderkey_lookup.bin", sz);
    void* mm_ocust = raw_mmap(gd + "/orders/o_custkey.bin", sz);
    void* mm_odate = raw_mmap(gd + "/orders/o_orderdate.bin", sz);
    void* mm_oprice = raw_mmap(gd + "/orders/o_totalprice.bin", sz);

    uint32_t orders_max_key = *(const uint32_t*)mm_olookup;
    const int32_t* orders_lookup = (const int32_t*)((const char*)mm_olookup + 4);
    const int32_t* o_custkey = (const int32_t*)mm_ocust;
    const int32_t* o_orderdate = (const int32_t*)mm_odate;
    const double* o_totalprice = (const double*)mm_oprice;

    std::vector<QualOrder> results;
    {
        GENDB_PHASE("build_joins");
        // Pre-size results array for parallel fill
        size_t nq = qualifying.size();
        results.resize(nq);
        std::vector<int8_t> valid(nq, 0);

        #pragma omp parallel for schedule(static)
        for (size_t idx = 0; idx < nq; idx++) {
            int32_t ok = qualifying[idx].first;
            double sq = qualifying[idx].second;
            if ((uint32_t)ok > orders_max_key) continue;
            int32_t orow = orders_lookup[ok];
            if (orow < 0) continue;
            results[idx] = {ok, sq, o_totalprice[orow], o_orderdate[orow], o_custkey[orow]};
            valid[idx] = 1;
        }

        // Compact
        size_t j = 0;
        for (size_t idx = 0; idx < nq; idx++) {
            if (valid[idx]) {
                if (j != idx) results[j] = results[idx];
                j++;
            }
        }
        results.resize(j);
    }

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

    {
        GENDB_PHASE("output");

        void* mm_clookup = raw_mmap(gd + "/indexes/customer_custkey_lookup.bin", sz);
        void* mm_cnameoff = raw_mmap(gd + "/customer/c_name_offsets.bin", sz);
        void* mm_cnamedat = raw_mmap(gd + "/customer/c_name_data.bin", sz);

        uint32_t cust_max_key = *(const uint32_t*)mm_clookup;
        const int32_t* cust_lookup = (const int32_t*)((const char*)mm_clookup + 4);
        const int64_t* c_name_offsets = (const int64_t*)mm_cnameoff;
        const char* c_name_data = (const char*)mm_cnamedat;

        FILE* fp = fopen((rd + "/Q18.csv").c_str(), "w");
        fprintf(fp, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        size_t limit = std::min<size_t>(100, results.size());
        char datebuf[16];
        for (size_t i = 0; i < limit; i++) {
            auto& r = results[i];
            int32_t crow = -1;
            if ((uint32_t)r.o_custkey <= cust_max_key) crow = cust_lookup[r.o_custkey];

            const char* name_ptr = "";
            size_t name_len = 0;
            if (crow >= 0) {
                int64_t s = c_name_offsets[crow], e = c_name_offsets[crow + 1];
                name_ptr = c_name_data + s;
                name_len = e - s;
            }

            format_date(r.o_orderdate, datebuf);
            fprintf(fp, "%.*s,%d,%d,%s,%.2f,%.2f\n",
                    (int)name_len, name_ptr,
                    r.o_custkey, r.orderkey, datebuf,
                    r.o_totalprice, r.sum_qty);
        }
        fclose(fp);
    }
    return 0;
}
