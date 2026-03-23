// Q18: Large Volume Customer — GenDB Generated Code
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

// ---------- mmap helper ----------
struct MappedFile {
    void* data = nullptr;
    size_t size = 0;
    void open(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::perror(path.c_str()); std::exit(1); }
        struct stat st; fstat(fd, &st); size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (data == MAP_FAILED) { std::perror("mmap"); std::exit(1); }
        madvise(data, size, MADV_SEQUENTIAL);
        ::close(fd);
    }
    ~MappedFile() { if (data && data != MAP_FAILED) munmap(data, size); }
    template<typename T> const T* as() const { return reinterpret_cast<const T*>(data); }
};

// ---------- date helper ----------
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
struct QualResult {
    int32_t orderkey;
    double sum_qty;
    int32_t order_row_idx;
    int32_t cust_row_idx;
    double o_totalprice;
    int32_t o_orderdate;
};

int main(int argc, char** argv) {
    if (argc < 3) { std::fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // ---- mmap all needed files ----
    MappedFile f_l_orderkey, f_l_quantity;
    MappedFile f_o_custkey, f_o_orderdate, f_o_totalprice;
    MappedFile f_c_name_offsets, f_c_name_data;
    MappedFile f_orders_lookup, f_customer_lookup;

    const int32_t* l_orderkey;
    const double*  l_quantity;
    size_t li_nrows;
    const int32_t* o_custkey;
    const int32_t* o_orderdate;
    const double*  o_totalprice;
    const int64_t* c_name_offsets;
    const char*    c_name_data;
    uint32_t orders_max_key;
    const int32_t* orders_lookup;
    uint32_t cust_max_key;
    const int32_t* customer_lookup;

    {
        GENDB_PHASE("data_loading");

        f_l_orderkey.open(gendb_dir + "/lineitem/l_orderkey.bin");
        f_l_quantity.open(gendb_dir + "/lineitem/l_quantity.bin");
        f_o_custkey.open(gendb_dir + "/orders/o_custkey.bin");
        f_o_orderdate.open(gendb_dir + "/orders/o_orderdate.bin");
        f_o_totalprice.open(gendb_dir + "/orders/o_totalprice.bin");
        f_c_name_offsets.open(gendb_dir + "/customer/c_name_offsets.bin");
        f_c_name_data.open(gendb_dir + "/customer/c_name_data.bin");
        f_orders_lookup.open(gendb_dir + "/indexes/orders_orderkey_lookup.bin");
        f_customer_lookup.open(gendb_dir + "/indexes/customer_custkey_lookup.bin");

        l_orderkey = f_l_orderkey.as<int32_t>();
        l_quantity = f_l_quantity.as<double>();
        li_nrows = f_l_orderkey.size / sizeof(int32_t);

        o_custkey    = f_o_custkey.as<int32_t>();
        o_orderdate  = f_o_orderdate.as<int32_t>();
        o_totalprice = f_o_totalprice.as<double>();

        c_name_offsets = f_c_name_offsets.as<int64_t>();
        c_name_data    = f_c_name_data.as<char>();

        orders_max_key = *reinterpret_cast<const uint32_t*>(f_orders_lookup.data);
        orders_lookup = reinterpret_cast<const int32_t*>(
            static_cast<const char*>(f_orders_lookup.data) + 4);

        cust_max_key = *reinterpret_cast<const uint32_t*>(f_customer_lookup.data);
        customer_lookup = reinterpret_cast<const int32_t*>(
            static_cast<const char*>(f_customer_lookup.data) + 4);
    }

    // ---- Phase 1: Parallel scan lineitem, aggregate SUM(l_quantity) per orderkey, HAVING > 300 ----
    std::vector<QualResult> qualifying;
    {
        GENDB_PHASE("subquery_scan_aggregate");

        int nthreads = omp_get_max_threads();
        std::vector<std::vector<std::pair<int32_t, double>>> thread_results(nthreads);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int nt = omp_get_num_threads();
            size_t chunk = li_nrows / nt;
            size_t start = tid * chunk;
            size_t end = (tid == nt - 1) ? li_nrows : start + chunk;

            // Align start to orderkey boundary
            if (tid > 0 && start < li_nrows) {
                int32_t prev_key = l_orderkey[start - 1];
                while (start < end && l_orderkey[start] == prev_key) start++;
            }
            // Align end to orderkey boundary
            if (tid < nt - 1 && end < li_nrows) {
                int32_t boundary_key = l_orderkey[end - 1];
                while (end < li_nrows && l_orderkey[end] == boundary_key) end++;
            }

            auto& local = thread_results[tid];
            size_t i = start;
            while (i < end) {
                int32_t ok = l_orderkey[i];
                double sum_q = 0.0;
                while (i < end && l_orderkey[i] == ok) {
                    sum_q += l_quantity[i];
                    i++;
                }
                if (sum_q > 300.0) {
                    local.emplace_back(ok, sum_q);
                }
            }
        }

        // Merge thread results
        size_t total = 0;
        for (auto& v : thread_results) total += v.size();
        qualifying.reserve(total);
        for (auto& v : thread_results) {
            for (auto& [ok, sq] : v) {
                qualifying.push_back({ok, sq, 0, 0, 0.0, 0});
            }
        }
    }

    // ---- Phase 2: Index lookup orders ----
    {
        GENDB_PHASE("index_lookup_orders");
        for (auto& r : qualifying) {
            if (static_cast<uint32_t>(r.orderkey) <= orders_max_key) {
                int32_t row = orders_lookup[r.orderkey];
                r.order_row_idx = row;
                r.o_totalprice = o_totalprice[row];
                r.o_orderdate = o_orderdate[row];
            }
        }
    }

    // ---- Phase 3: Index lookup customer ----
    {
        GENDB_PHASE("index_lookup_customer");
        for (auto& r : qualifying) {
            int32_t ck = o_custkey[r.order_row_idx];
            if (static_cast<uint32_t>(ck) <= cust_max_key) {
                r.cust_row_idx = customer_lookup[ck];
            }
        }
    }

    // ---- Phase 4: Sort and limit 100 ----
    {
        GENDB_PHASE("sort_and_limit");
        if (qualifying.size() > 100) {
            std::partial_sort(qualifying.begin(), qualifying.begin() + 100, qualifying.end(),
                [](const QualResult& a, const QualResult& b) {
                    if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
                    return a.o_orderdate < b.o_orderdate;
                });
        } else {
            std::sort(qualifying.begin(), qualifying.end(),
                [](const QualResult& a, const QualResult& b) {
                    if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
                    return a.o_orderdate < b.o_orderdate;
                });
        }
    }

    // ---- Output ----
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q18.csv";
        FILE* fp = std::fopen(outpath.c_str(), "w");
        if (!fp) { std::perror(outpath.c_str()); return 1; }

        std::fprintf(fp, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");

        size_t limit = std::min<size_t>(100, qualifying.size());
        char datebuf[16];
        for (size_t i = 0; i < limit; i++) {
            const auto& r = qualifying[i];
            // Late materialize c_name
            int32_t crow = r.cust_row_idx;
            int64_t off0 = c_name_offsets[crow];
            int64_t off1 = c_name_offsets[crow + 1];
            std::string c_name(c_name_data + off0, off1 - off0);

            int32_t ck = o_custkey[r.order_row_idx];

            format_date(r.o_orderdate, datebuf);
            std::fprintf(fp, "%s,%d,%d,%s,%.2f,%.2f\n",
                c_name.c_str(), ck, r.orderkey, datebuf,
                r.o_totalprice, r.sum_qty);
        }
        std::fclose(fp);
    }

    return 0;
}
