// ingest.cpp - TPC-H .tbl -> binary columnar format
// Compile: g++ -O2 -std=c++17 -Wall -lpthread -o ingest ingest.cpp

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <string>
#include <vector>
#include <set>
#include <map>
#include <algorithm>
#include <stdexcept>
#include <functional>
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <queue>

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>

// ============================================================
// Constants
// ============================================================
static const int N_THREADS = 64;

// ============================================================
// Date parsing
// ============================================================
static const int32_t YEAR_DAYS[] = {
    0,365,730,1096,1461,1826,2191,2557,2922,3287,3652,4018,4383,4748,5113,
    5479,5844,6209,6574,6940,7305,7670,8035,8401,8766,9131,9496,9862,10227,10592,10957
}; // index 0=1970, index 30=2000

static const int32_t MONTH_STARTS[2][12] = {
    {0,31,59,90,120,151,181,212,243,273,304,334},  // non-leap
    {0,31,60,91,121,152,182,213,244,274,305,335}   // leap
};

inline bool is_leap_year(int y) { return y%4==0 && (y%100!=0 || y%400==0); }

inline int32_t parse_date(const char* s) {
    int yr = (s[0]-'0')*1000+(s[1]-'0')*100+(s[2]-'0')*10+(s[3]-'0');
    int mo = (s[5]-'0')*10+(s[6]-'0');
    int dy = (s[8]-'0')*10+(s[9]-'0');
    return YEAR_DAYS[yr-1970] + MONTH_STARTS[is_leap_year(yr)][mo-1] + (dy-1);
}

// ============================================================
// Fast numeric parsers
// ============================================================
inline int64_t fast_atoi(const char* s, int len) {
    bool neg = false;
    int i = 0;
    if (i < len && s[i] == '-') { neg = true; i++; }
    int64_t v = 0;
    for (; i < len; i++) {
        if (s[i] >= '0' && s[i] <= '9') v = v * 10 + (s[i] - '0');
    }
    return neg ? -v : v;
}

inline double fast_atod(const char* s, int len) {
    bool neg = false;
    int i = 0;
    if (i < len && s[i] == '-') { neg = true; i++; }
    int64_t intpart = 0;
    int64_t fracpart = 0;
    int fracdigs = 0;
    bool dot = false;
    for (; i < len; i++) {
        if (s[i] == '.') { dot = true; continue; }
        if (s[i] < '0' || s[i] > '9') continue;
        if (!dot) {
            intpart = intpart * 10 + (s[i] - '0');
        } else {
            fracpart = fracpart * 10 + (s[i] - '0');
            fracdigs++;
        }
    }
    static const double pow10[] = {1,10,100,1000,10000,100000,1000000,10000000,100000000};
    double v = (double)intpart;
    if (fracdigs > 0 && fracdigs <= 8) v += (double)fracpart / pow10[fracdigs];
    return neg ? -v : v;
}

// ============================================================
// Field parsing
// ============================================================
inline const char* get_field(const char* ls, const char* le, int fid, int* flen) {
    const char* p = ls;
    for (int i = 0; i < fid; i++) {
        while (p < le && *p != '|') p++;
        if (p < le) p++;
    }
    const char* s = p;
    while (p < le && *p != '|') p++;
    *flen = (int)(p - s);
    return s;
}

// ============================================================
// MmapFile
// ============================================================
struct MmapFile {
    const char* data;
    size_t size;
    int fd;

    MmapFile(const std::string& path) : data(nullptr), size(0), fd(-1) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);
        struct stat st;
        if (fstat(fd, &st) < 0) { close(fd); throw std::runtime_error("Cannot stat: " + path); }
        size = (size_t)st.st_size;
        if (size == 0) { data = nullptr; return; }
        void* ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { close(fd); throw std::runtime_error("mmap failed: " + path); }
        data = (const char*)ptr;
        madvise((void*)data, size, MADV_SEQUENTIAL);
    }

    ~MmapFile() {
        if (data && size > 0) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }

    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;
};

// ============================================================
// BufWriter
// ============================================================
struct BufWriter {
    int fd;
    std::vector<char> buf;
    size_t pos;

    BufWriter(const std::string& path, size_t bsz = 8*1024*1024)
        : fd(-1), buf(bsz), pos(0) {
        fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd < 0) throw std::runtime_error("Cannot create: " + path);
    }

    void write_raw(const void* d, size_t bytes) {
        const char* p = (const char*)d;
        while (bytes > 0) {
            size_t avail = buf.size() - pos;
            if (avail == 0) { flush(); avail = buf.size(); }
            size_t take = std::min(avail, bytes);
            memcpy(buf.data() + pos, p, take);
            pos += take;
            p += take;
            bytes -= take;
        }
    }

    template<typename T>
    void write_t(T val) { write_raw(&val, sizeof(T)); }

    void flush() {
        if (pos > 0) {
            ssize_t written = write(fd, buf.data(), pos);
            (void)written;
            pos = 0;
        }
    }

    ~BufWriter() {
        flush();
        if (fd >= 0) close(fd);
    }

    BufWriter(const BufWriter&) = delete;
    BufWriter& operator=(const BufWriter&) = delete;
};

// ============================================================
// Thread pool
// ============================================================
struct ThreadPool {
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex mtx;
    std::condition_variable cv_task;
    std::condition_variable cv_done;
    bool stop_flag = false;
    int pending = 0;

    explicit ThreadPool(int nthreads) {
        for (int i = 0; i < nthreads; i++) {
            workers.emplace_back([this]() {
                for (;;) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lk(mtx);
                        cv_task.wait(lk, [this]{ return stop_flag || !tasks.empty(); });
                        if (stop_flag && tasks.empty()) return;
                        task = std::move(tasks.front());
                        tasks.pop();
                    }
                    task();
                    {
                        std::unique_lock<std::mutex> lk(mtx);
                        pending--;
                    }
                    cv_done.notify_all();
                }
            });
        }
    }

    void submit(std::function<void()> f) {
        {
            std::unique_lock<std::mutex> lk(mtx);
            tasks.push(std::move(f));
            pending++;
        }
        cv_task.notify_one();
    }

    void wait_all() {
        std::unique_lock<std::mutex> lk(mtx);
        cv_done.wait(lk, [this]{ return pending == 0 && tasks.empty(); });
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lk(mtx);
            stop_flag = true;
        }
        cv_task.notify_all();
        for (auto& w : workers) w.join();
    }
};

static ThreadPool* g_pool = nullptr;

// ============================================================
// Utility: mkdir -p
// ============================================================
static void mkdir_p(const std::string& path) {
    // Create directory and any missing parents
    std::string cur;
    for (size_t i = 0; i < path.size(); i++) {
        cur += path[i];
        if (path[i] == '/' || i+1 == path.size()) {
            mkdir(cur.c_str(), 0755); // ignore errors (may already exist)
        }
    }
}

// ============================================================
// Sorting: counting sort permutation
// ============================================================
static std::vector<uint32_t> build_sort_perm(const std::vector<int32_t>& keys) {
    if (keys.empty()) return {};
    int32_t minv = *std::min_element(keys.begin(), keys.end());
    int32_t maxv = *std::max_element(keys.begin(), keys.end());
    int range = (int)(maxv - minv + 1);
    std::vector<int32_t> cnt(range, 0);
    for (auto k : keys) cnt[k - minv]++;
    std::vector<int32_t> starts(range);
    int32_t pos = 0;
    for (int i = 0; i < range; i++) { starts[i] = pos; pos += cnt[i]; }
    std::vector<uint32_t> perm(keys.size());
    std::vector<int32_t> cur = starts;
    for (uint32_t i = 0; i < (uint32_t)keys.size(); i++) perm[cur[keys[i]-minv]++] = i;
    return perm;
}

// ============================================================
// Varstring writer
// ============================================================
static void write_varstring(const std::string& base_path,
                            const std::vector<std::string>& strs,
                            const std::vector<uint32_t>& perm) {
    BufWriter ow(base_path + "_offsets.bin");
    uint32_t off = 0;
    for (size_t i = 0; i <= strs.size(); i++) {
        if (i == strs.size()) { ow.write_t(off); break; }
        ow.write_t(off);
        off += (uint32_t)strs[perm[i]].size();
    }
    BufWriter dw(base_path + "_data.bin");
    for (size_t i = 0; i < strs.size(); i++) {
        const auto& s = strs[perm[i]];
        if (!s.empty()) dw.write_raw(s.data(), s.size());
    }
}

// Identity permutation helper
static std::vector<uint32_t> identity_perm(size_t n) {
    std::vector<uint32_t> p(n);
    for (size_t i = 0; i < n; i++) p[i] = (uint32_t)i;
    return p;
}

// ============================================================
// Dictionary helper: build map from set
// ============================================================
static std::map<std::string,int8_t> build_dict_map(const std::set<std::string>& vals,
                                                     const std::string& dict_path) {
    std::vector<std::string> sorted_vals(vals.begin(), vals.end());
    // already sorted since std::set is ordered
    BufWriter dw(dict_path);
    std::map<std::string,int8_t> m;
    int8_t code = 0;
    for (const auto& v : sorted_vals) {
        m[v] = code++;
        dw.write_raw(v.data(), v.size());
        char nl = '\n';
        dw.write_raw(&nl, 1);
    }
    return m;
}

// ============================================================
// Line iterator helper
// ============================================================
struct LineIter {
    const char* cur;
    const char* end;
    LineIter(const char* d, size_t sz) : cur(d), end(d + sz) {}
    // Returns true and sets ls,le to next line [ls,le) (le points past newline or at end)
    bool next(const char*& ls, const char*& le) {
        if (cur >= end) return false;
        ls = cur;
        const char* p = cur;
        while (p < end && *p != '\n') p++;
        le = p; // le points at '\n' or end
        cur = (p < end) ? p + 1 : p;
        // skip trailing \r
        if (le > ls && *(le-1) == '\r') le--;
        return (le > ls); // skip empty lines
    }
};

// ============================================================
// Row counts (global, updated by each table task)
// ============================================================
static std::mutex g_rowcount_mtx;
static std::map<std::string,size_t> g_rowcounts;

static void set_rowcount(const std::string& tbl, size_t n) {
    std::lock_guard<std::mutex> lk(g_rowcount_mtx);
    g_rowcounts[tbl] = n;
}

// ============================================================
// TABLE: lineitem
// ============================================================
static void ingest_lineitem(const std::string& src_dir, const std::string& out_dir) {
    std::string tbl_path = src_dir + "/lineitem.tbl";
    std::string tbl_out  = out_dir + "/lineitem";

    MmapFile mf(tbl_path);

    // Pass 1: collect dict values for fields 8,9,13,14
    std::set<std::string> s8, s9, s13, s14;
    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls, le, 8, &fl);   s8.insert(std::string(f,fl));
            f = get_field(ls, le, 9, &fl);   s9.insert(std::string(f,fl));
            f = get_field(ls, le, 13, &fl);  s13.insert(std::string(f,fl));
            f = get_field(ls, le, 14, &fl);  s14.insert(std::string(f,fl));
        }
    }

    auto m8  = build_dict_map(s8,  tbl_out + "/l_returnflag_dict.txt");
    auto m9  = build_dict_map(s9,  tbl_out + "/l_linestatus_dict.txt");
    auto m13 = build_dict_map(s13, tbl_out + "/l_shipinstruct_dict.txt");
    auto m14 = build_dict_map(s14, tbl_out + "/l_shipmode_dict.txt");

    // Pass 2: full parse
    std::vector<int32_t> col0, col1, col2, col3;
    std::vector<double>  col4, col5, col6, col7;
    std::vector<int8_t>  col8, col9;
    std::vector<int32_t> col10, col11, col12;
    std::vector<int8_t>  col13, col14;
    std::vector<std::string> col15;

    // Reserve reasonable capacity (sf10 ~ 60M rows)
    const size_t RESERVE = 60000000;
    col0.reserve(RESERVE); col1.reserve(RESERVE); col2.reserve(RESERVE);
    col3.reserve(RESERVE); col4.reserve(RESERVE); col5.reserve(RESERVE);
    col6.reserve(RESERVE); col7.reserve(RESERVE); col8.reserve(RESERVE);
    col9.reserve(RESERVE); col10.reserve(RESERVE); col11.reserve(RESERVE);
    col12.reserve(RESERVE); col13.reserve(RESERVE); col14.reserve(RESERVE);
    col15.reserve(RESERVE);

    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,0,&fl);  col0.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,1,&fl);  col1.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,2,&fl);  col2.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,3,&fl);  col3.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,4,&fl);  col4.push_back(fast_atod(f,fl));
            f = get_field(ls,le,5,&fl);  col5.push_back(fast_atod(f,fl));
            f = get_field(ls,le,6,&fl);  col6.push_back(fast_atod(f,fl));
            f = get_field(ls,le,7,&fl);  col7.push_back(fast_atod(f,fl));
            f = get_field(ls,le,8,&fl);  col8.push_back(m8.at(std::string(f,fl)));
            f = get_field(ls,le,9,&fl);  col9.push_back(m9.at(std::string(f,fl)));
            f = get_field(ls,le,10,&fl); col10.push_back(parse_date(f));
            f = get_field(ls,le,11,&fl); col11.push_back(parse_date(f));
            f = get_field(ls,le,12,&fl); col12.push_back(parse_date(f));
            f = get_field(ls,le,13,&fl); col13.push_back(m13.at(std::string(f,fl)));
            f = get_field(ls,le,14,&fl); col14.push_back(m14.at(std::string(f,fl)));
            f = get_field(ls,le,15,&fl); col15.push_back(std::string(f,fl));
        }
    }

    size_t N = col0.size();
    set_rowcount("lineitem", N);

    // Build sort permutation on l_shipdate (col10)
    auto perm = build_sort_perm(col10);

    // Write binary columns
    {
        BufWriter w(tbl_out + "/l_orderkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col0[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_partkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col1[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_suppkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col2[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_linenumber.bin");
        for (size_t j=0;j<N;j++) w.write_t(col3[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_quantity.bin");
        for (size_t j=0;j<N;j++) w.write_t(col4[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_extendedprice.bin");
        for (size_t j=0;j<N;j++) w.write_t(col5[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_discount.bin");
        for (size_t j=0;j<N;j++) w.write_t(col6[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_tax.bin");
        for (size_t j=0;j<N;j++) w.write_t(col7[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_returnflag.bin");
        for (size_t j=0;j<N;j++) w.write_t(col8[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_linestatus.bin");
        for (size_t j=0;j<N;j++) w.write_t(col9[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_shipdate.bin");
        for (size_t j=0;j<N;j++) w.write_t(col10[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_commitdate.bin");
        for (size_t j=0;j<N;j++) w.write_t(col11[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_receiptdate.bin");
        for (size_t j=0;j<N;j++) w.write_t(col12[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_shipinstruct.bin");
        for (size_t j=0;j<N;j++) w.write_t(col13[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/l_shipmode.bin");
        for (size_t j=0;j<N;j++) w.write_t(col14[perm[j]]);
    }
    write_varstring(tbl_out + "/l_comment", col15, perm);
}

// ============================================================
// TABLE: orders
// ============================================================
static void ingest_orders(const std::string& src_dir, const std::string& out_dir) {
    std::string tbl_path = src_dir + "/orders.tbl";
    std::string tbl_out  = out_dir + "/orders";

    MmapFile mf(tbl_path);

    // Pass 1: dict for fields 2,5
    std::set<std::string> s2, s5;
    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,2,&fl); s2.insert(std::string(f,fl));
            f = get_field(ls,le,5,&fl); s5.insert(std::string(f,fl));
        }
    }

    auto m2 = build_dict_map(s2, tbl_out + "/o_orderstatus_dict.txt");
    auto m5 = build_dict_map(s5, tbl_out + "/o_orderpriority_dict.txt");

    // Pass 2: full parse
    std::vector<int32_t> col0, col1;
    std::vector<int8_t>  col2;
    std::vector<double>  col3;
    std::vector<int32_t> col4;
    std::vector<int8_t>  col5;
    std::vector<std::string> col6;
    std::vector<int32_t> col7;
    std::vector<std::string> col8;

    const size_t RESERVE = 16000000;
    col0.reserve(RESERVE); col1.reserve(RESERVE); col2.reserve(RESERVE);
    col3.reserve(RESERVE); col4.reserve(RESERVE); col5.reserve(RESERVE);
    col6.reserve(RESERVE); col7.reserve(RESERVE); col8.reserve(RESERVE);

    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,0,&fl); col0.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,1,&fl); col1.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,2,&fl); col2.push_back(m2.at(std::string(f,fl)));
            f = get_field(ls,le,3,&fl); col3.push_back(fast_atod(f,fl));
            f = get_field(ls,le,4,&fl); col4.push_back(parse_date(f));
            f = get_field(ls,le,5,&fl); col5.push_back(m5.at(std::string(f,fl)));
            f = get_field(ls,le,6,&fl); col6.push_back(std::string(f,fl));
            f = get_field(ls,le,7,&fl); col7.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,8,&fl); col8.push_back(std::string(f,fl));
        }
    }

    size_t N = col0.size();
    set_rowcount("orders", N);

    auto perm = build_sort_perm(col4);

    {
        BufWriter w(tbl_out + "/o_orderkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col0[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/o_custkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col1[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/o_orderstatus.bin");
        for (size_t j=0;j<N;j++) w.write_t(col2[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/o_totalprice.bin");
        for (size_t j=0;j<N;j++) w.write_t(col3[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/o_orderdate.bin");
        for (size_t j=0;j<N;j++) w.write_t(col4[perm[j]]);
    }
    {
        BufWriter w(tbl_out + "/o_orderpriority.bin");
        for (size_t j=0;j<N;j++) w.write_t(col5[perm[j]]);
    }
    write_varstring(tbl_out + "/o_clerk", col6, perm);
    {
        BufWriter w(tbl_out + "/o_shippriority.bin");
        for (size_t j=0;j<N;j++) w.write_t(col7[perm[j]]);
    }
    write_varstring(tbl_out + "/o_comment", col8, perm);
}

// ============================================================
// TABLE: customer
// ============================================================
static void ingest_customer(const std::string& src_dir, const std::string& out_dir) {
    std::string tbl_path = src_dir + "/customer.tbl";
    std::string tbl_out  = out_dir + "/customer";

    MmapFile mf(tbl_path);

    // Pass 1: dict for field 6
    std::set<std::string> s6;
    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,6,&fl); s6.insert(std::string(f,fl));
        }
    }
    auto m6 = build_dict_map(s6, tbl_out + "/c_mktsegment_dict.txt");

    std::vector<int32_t> col0;
    std::vector<std::string> col1, col2;
    std::vector<int32_t> col3;
    std::vector<std::string> col4;
    std::vector<double> col5;
    std::vector<int8_t> col6;
    std::vector<std::string> col7;

    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,0,&fl); col0.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,1,&fl); col1.push_back(std::string(f,fl));
            f = get_field(ls,le,2,&fl); col2.push_back(std::string(f,fl));
            f = get_field(ls,le,3,&fl); col3.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,4,&fl); col4.push_back(std::string(f,fl));
            f = get_field(ls,le,5,&fl); col5.push_back(fast_atod(f,fl));
            f = get_field(ls,le,6,&fl); col6.push_back(m6.at(std::string(f,fl)));
            f = get_field(ls,le,7,&fl); col7.push_back(std::string(f,fl));
        }
    }

    size_t N = col0.size();
    set_rowcount("customer", N);
    auto perm = identity_perm(N);

    {
        BufWriter w(tbl_out + "/c_custkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col0[j]);
    }
    write_varstring(tbl_out + "/c_name", col1, perm);
    write_varstring(tbl_out + "/c_address", col2, perm);
    {
        BufWriter w(tbl_out + "/c_nationkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col3[j]);
    }
    write_varstring(tbl_out + "/c_phone", col4, perm);
    {
        BufWriter w(tbl_out + "/c_acctbal.bin");
        for (size_t j=0;j<N;j++) w.write_t(col5[j]);
    }
    {
        BufWriter w(tbl_out + "/c_mktsegment.bin");
        for (size_t j=0;j<N;j++) w.write_t(col6[j]);
    }
    write_varstring(tbl_out + "/c_comment", col7, perm);
}

// ============================================================
// TABLE: part
// ============================================================
static void ingest_part(const std::string& src_dir, const std::string& out_dir) {
    std::string tbl_path = src_dir + "/part.tbl";
    std::string tbl_out  = out_dir + "/part";

    MmapFile mf(tbl_path);

    // Pass 1: dict for fields 2,3,4,6
    std::set<std::string> s2, s3, s4, s6;
    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,2,&fl); s2.insert(std::string(f,fl));
            f = get_field(ls,le,3,&fl); s3.insert(std::string(f,fl));
            f = get_field(ls,le,4,&fl); s4.insert(std::string(f,fl));
            f = get_field(ls,le,6,&fl); s6.insert(std::string(f,fl));
        }
    }
    auto m2 = build_dict_map(s2, tbl_out + "/p_mfgr_dict.txt");
    auto m3 = build_dict_map(s3, tbl_out + "/p_brand_dict.txt");
    auto m4 = build_dict_map(s4, tbl_out + "/p_type_dict.txt");
    auto m6 = build_dict_map(s6, tbl_out + "/p_container_dict.txt");

    std::vector<int32_t> col0;
    std::vector<std::string> col1;
    std::vector<int8_t> col2, col3, col4;
    std::vector<int32_t> col5;
    std::vector<int8_t> col6;
    std::vector<double> col7;
    std::vector<std::string> col8;

    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,0,&fl); col0.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,1,&fl); col1.push_back(std::string(f,fl));
            f = get_field(ls,le,2,&fl); col2.push_back(m2.at(std::string(f,fl)));
            f = get_field(ls,le,3,&fl); col3.push_back(m3.at(std::string(f,fl)));
            f = get_field(ls,le,4,&fl); col4.push_back(m4.at(std::string(f,fl)));
            f = get_field(ls,le,5,&fl); col5.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,6,&fl); col6.push_back(m6.at(std::string(f,fl)));
            f = get_field(ls,le,7,&fl); col7.push_back(fast_atod(f,fl));
            f = get_field(ls,le,8,&fl); col8.push_back(std::string(f,fl));
        }
    }

    size_t N = col0.size();
    set_rowcount("part", N);
    auto perm = identity_perm(N);

    {
        BufWriter w(tbl_out + "/p_partkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col0[j]);
    }
    write_varstring(tbl_out + "/p_name", col1, perm);
    {
        BufWriter w(tbl_out + "/p_mfgr.bin");
        for (size_t j=0;j<N;j++) w.write_t(col2[j]);
    }
    {
        BufWriter w(tbl_out + "/p_brand.bin");
        for (size_t j=0;j<N;j++) w.write_t(col3[j]);
    }
    {
        BufWriter w(tbl_out + "/p_type.bin");
        for (size_t j=0;j<N;j++) w.write_t(col4[j]);
    }
    {
        BufWriter w(tbl_out + "/p_size.bin");
        for (size_t j=0;j<N;j++) w.write_t(col5[j]);
    }
    {
        BufWriter w(tbl_out + "/p_container.bin");
        for (size_t j=0;j<N;j++) w.write_t(col6[j]);
    }
    {
        BufWriter w(tbl_out + "/p_retailprice.bin");
        for (size_t j=0;j<N;j++) w.write_t(col7[j]);
    }
    write_varstring(tbl_out + "/p_comment", col8, perm);
}

// ============================================================
// TABLE: partsupp
// ============================================================
static void ingest_partsupp(const std::string& src_dir, const std::string& out_dir) {
    std::string tbl_path = src_dir + "/partsupp.tbl";
    std::string tbl_out  = out_dir + "/partsupp";

    MmapFile mf(tbl_path);

    std::vector<int32_t> col0, col1, col2;
    std::vector<double> col3;
    std::vector<std::string> col4;

    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,0,&fl); col0.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,1,&fl); col1.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,2,&fl); col2.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,3,&fl); col3.push_back(fast_atod(f,fl));
            f = get_field(ls,le,4,&fl); col4.push_back(std::string(f,fl));
        }
    }

    size_t N = col0.size();
    set_rowcount("partsupp", N);
    auto perm = identity_perm(N);

    {
        BufWriter w(tbl_out + "/ps_partkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col0[j]);
    }
    {
        BufWriter w(tbl_out + "/ps_suppkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col1[j]);
    }
    {
        BufWriter w(tbl_out + "/ps_availqty.bin");
        for (size_t j=0;j<N;j++) w.write_t(col2[j]);
    }
    {
        BufWriter w(tbl_out + "/ps_supplycost.bin");
        for (size_t j=0;j<N;j++) w.write_t(col3[j]);
    }
    write_varstring(tbl_out + "/ps_comment", col4, perm);
}

// ============================================================
// TABLE: supplier
// ============================================================
static void ingest_supplier(const std::string& src_dir, const std::string& out_dir) {
    std::string tbl_path = src_dir + "/supplier.tbl";
    std::string tbl_out  = out_dir + "/supplier";

    MmapFile mf(tbl_path);

    std::vector<int32_t> col0;
    std::vector<std::string> col1, col2;
    std::vector<int32_t> col3;
    std::vector<std::string> col4;
    std::vector<double> col5;
    std::vector<std::string> col6;

    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,0,&fl); col0.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,1,&fl); col1.push_back(std::string(f,fl));
            f = get_field(ls,le,2,&fl); col2.push_back(std::string(f,fl));
            f = get_field(ls,le,3,&fl); col3.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,4,&fl); col4.push_back(std::string(f,fl));
            f = get_field(ls,le,5,&fl); col5.push_back(fast_atod(f,fl));
            f = get_field(ls,le,6,&fl); col6.push_back(std::string(f,fl));
        }
    }

    size_t N = col0.size();
    set_rowcount("supplier", N);
    auto perm = identity_perm(N);

    {
        BufWriter w(tbl_out + "/s_suppkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col0[j]);
    }
    write_varstring(tbl_out + "/s_name", col1, perm);
    write_varstring(tbl_out + "/s_address", col2, perm);
    {
        BufWriter w(tbl_out + "/s_nationkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col3[j]);
    }
    write_varstring(tbl_out + "/s_phone", col4, perm);
    {
        BufWriter w(tbl_out + "/s_acctbal.bin");
        for (size_t j=0;j<N;j++) w.write_t(col5[j]);
    }
    write_varstring(tbl_out + "/s_comment", col6, perm);
}

// ============================================================
// TABLE: nation
// ============================================================
static void ingest_nation(const std::string& src_dir, const std::string& out_dir) {
    std::string tbl_path = src_dir + "/nation.tbl";
    std::string tbl_out  = out_dir + "/nation";

    MmapFile mf(tbl_path);

    // Pass 1: dict for field 1
    std::set<std::string> s1;
    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,1,&fl); s1.insert(std::string(f,fl));
        }
    }
    auto m1 = build_dict_map(s1, tbl_out + "/n_name_dict.txt");

    std::vector<int32_t> col0;
    std::vector<int8_t>  col1;
    std::vector<int32_t> col2;
    std::vector<std::string> col3;

    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,0,&fl); col0.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,1,&fl); col1.push_back(m1.at(std::string(f,fl)));
            f = get_field(ls,le,2,&fl); col2.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,3,&fl); col3.push_back(std::string(f,fl));
        }
    }

    size_t N = col0.size();
    set_rowcount("nation", N);
    auto perm = identity_perm(N);

    {
        BufWriter w(tbl_out + "/n_nationkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col0[j]);
    }
    {
        BufWriter w(tbl_out + "/n_name.bin");
        for (size_t j=0;j<N;j++) w.write_t(col1[j]);
    }
    {
        BufWriter w(tbl_out + "/n_regionkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col2[j]);
    }
    write_varstring(tbl_out + "/n_comment", col3, perm);
}

// ============================================================
// TABLE: region
// ============================================================
static void ingest_region(const std::string& src_dir, const std::string& out_dir) {
    std::string tbl_path = src_dir + "/region.tbl";
    std::string tbl_out  = out_dir + "/region";

    MmapFile mf(tbl_path);

    // Pass 1: dict for field 1
    std::set<std::string> s1;
    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,1,&fl); s1.insert(std::string(f,fl));
        }
    }
    auto m1 = build_dict_map(s1, tbl_out + "/r_name_dict.txt");

    std::vector<int32_t> col0;
    std::vector<int8_t>  col1;
    std::vector<std::string> col2;

    {
        LineIter it(mf.data, mf.size);
        const char* ls; const char* le;
        while (it.next(ls, le)) {
            int fl;
            const char* f;
            f = get_field(ls,le,0,&fl); col0.push_back((int32_t)fast_atoi(f,fl));
            f = get_field(ls,le,1,&fl); col1.push_back(m1.at(std::string(f,fl)));
            f = get_field(ls,le,2,&fl); col2.push_back(std::string(f,fl));
        }
    }

    size_t N = col0.size();
    set_rowcount("region", N);
    auto perm = identity_perm(N);

    {
        BufWriter w(tbl_out + "/r_regionkey.bin");
        for (size_t j=0;j<N;j++) w.write_t(col0[j]);
    }
    {
        BufWriter w(tbl_out + "/r_name.bin");
        for (size_t j=0;j<N;j++) w.write_t(col1[j]);
    }
    write_varstring(tbl_out + "/r_comment", col2, perm);
}

// ============================================================
// Post-ingestion verification
// ============================================================
static void verify(const std::string& out_dir) {
    // Check lineitem/l_shipdate.bin: all values > 3000
    {
        std::string path = out_dir + "/lineitem/l_shipdate.bin";
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "VERIFY ERROR: cannot open %s\n", path.c_str()); abort(); }
        struct stat st;
        fstat(fd, &st);
        size_t sz = (size_t)st.st_size;
        if (sz == 0) { close(fd); fprintf(stderr, "VERIFY ERROR: l_shipdate.bin is empty\n"); abort(); }
        void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (ptr == MAP_FAILED) { fprintf(stderr, "VERIFY ERROR: mmap failed for l_shipdate.bin\n"); abort(); }
        madvise(ptr, sz, MADV_SEQUENTIAL);
        const int32_t* dates = (const int32_t*)ptr;
        size_t n = sz / sizeof(int32_t);
        // Check first and last
        if (dates[0] <= 3000) {
            fprintf(stderr, "VERIFY ERROR: l_shipdate[0] = %d <= 3000\n", dates[0]);
            munmap(ptr, sz);
            abort();
        }
        if (dates[n-1] <= 3000) {
            fprintf(stderr, "VERIFY ERROR: l_shipdate[last] = %d <= 3000\n", dates[n-1]);
            munmap(ptr, sz);
            abort();
        }
        munmap(ptr, sz);
    }

    // Check lineitem/l_extendedprice.bin: at least one value != 0.0
    {
        std::string path = out_dir + "/lineitem/l_extendedprice.bin";
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "VERIFY ERROR: cannot open %s\n", path.c_str()); abort(); }
        struct stat st;
        fstat(fd, &st);
        size_t sz = (size_t)st.st_size;
        if (sz == 0) { close(fd); fprintf(stderr, "VERIFY ERROR: l_extendedprice.bin is empty\n"); abort(); }
        void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
        close(fd);
        if (ptr == MAP_FAILED) { fprintf(stderr, "VERIFY ERROR: mmap failed for l_extendedprice.bin\n"); abort(); }
        madvise(ptr, sz, MADV_SEQUENTIAL);
        const double* prices = (const double*)ptr;
        size_t n = sz / sizeof(double);
        bool found_nonzero = false;
        for (size_t i = 0; i < n; i++) {
            if (prices[i] != 0.0) { found_nonzero = true; break; }
        }
        munmap(ptr, sz);
        if (!found_nonzero) {
            fprintf(stderr, "VERIFY ERROR: all l_extendedprice values are 0.0\n");
            abort();
        }
    }

    printf("Verification passed.\n");
}

// ============================================================
// Write metadata.json
// ============================================================
static void write_metadata(const std::string& out_dir) {
    std::string path = out_dir + "/metadata.json";
    FILE* f = fopen(path.c_str(), "w");
    if (!f) { fprintf(stderr, "Cannot write metadata.json\n"); return; }

    std::lock_guard<std::mutex> lk(g_rowcount_mtx);
    fprintf(f, "{\n");
    bool first = true;
    for (const auto& kv : g_rowcounts) {
        if (!first) fprintf(f, ",\n");
        fprintf(f, "  \"%s\":{\"rows\":%zu}", kv.first.c_str(), kv.second);
        first = false;
    }
    fprintf(f, "\n}\n");
    fclose(f);
}

// ============================================================
// main
// ============================================================
int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <src_data_dir> <output_dir>\n", argv[0]);
        return 1;
    }
    std::string src_dir = argv[1];
    std::string out_dir = argv[2];

    // Create subdirectories
    const char* tables[] = {
        "lineitem","orders","customer","part","partsupp","supplier","nation","region"
    };
    for (auto tbl : tables) {
        mkdir_p(out_dir + "/" + tbl + "/indexes");
    }

    // Create global thread pool
    int nthreads = std::min(N_THREADS, 8); // 8 tables, no point in more
    g_pool = new ThreadPool(nthreads);

    // Submit all table tasks in parallel
    std::atomic<int> errors(0);

    g_pool->submit([&]() {
        try { ingest_lineitem(src_dir, out_dir); }
        catch (const std::exception& e) {
            fprintf(stderr, "lineitem error: %s\n", e.what());
            errors++;
        }
    });
    g_pool->submit([&]() {
        try { ingest_orders(src_dir, out_dir); }
        catch (const std::exception& e) {
            fprintf(stderr, "orders error: %s\n", e.what());
            errors++;
        }
    });
    g_pool->submit([&]() {
        try { ingest_customer(src_dir, out_dir); }
        catch (const std::exception& e) {
            fprintf(stderr, "customer error: %s\n", e.what());
            errors++;
        }
    });
    g_pool->submit([&]() {
        try { ingest_part(src_dir, out_dir); }
        catch (const std::exception& e) {
            fprintf(stderr, "part error: %s\n", e.what());
            errors++;
        }
    });
    g_pool->submit([&]() {
        try { ingest_partsupp(src_dir, out_dir); }
        catch (const std::exception& e) {
            fprintf(stderr, "partsupp error: %s\n", e.what());
            errors++;
        }
    });
    g_pool->submit([&]() {
        try { ingest_supplier(src_dir, out_dir); }
        catch (const std::exception& e) {
            fprintf(stderr, "supplier error: %s\n", e.what());
            errors++;
        }
    });
    g_pool->submit([&]() {
        try { ingest_nation(src_dir, out_dir); }
        catch (const std::exception& e) {
            fprintf(stderr, "nation error: %s\n", e.what());
            errors++;
        }
    });
    g_pool->submit([&]() {
        try { ingest_region(src_dir, out_dir); }
        catch (const std::exception& e) {
            fprintf(stderr, "region error: %s\n", e.what());
            errors++;
        }
    });

    g_pool->wait_all();
    delete g_pool;
    g_pool = nullptr;

    if (errors > 0) {
        fprintf(stderr, "Ingestion failed with %d error(s).\n", (int)errors.load());
        return 1;
    }

    // Post-ingestion verification
    verify(out_dir);

    // Write metadata
    write_metadata(out_dir);

    printf("Ingestion complete.\n");
    return 0;
}
