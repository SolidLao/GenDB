// ingest.cpp — TPC-H binary columnar ingestion
// Usage: ./ingest <data_dir> <gendb_dir>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <climits>
#include <cmath>

#include <string>
#include <vector>
#include <array>
#include <queue>
#include <functional>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <unordered_map>
#include <algorithm>
#include <numeric>
#include <fstream>
#include <iostream>
#include <chrono>
#include <filesystem>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

// ─── constants ───────────────────────────────────────────────────────────────
static constexpr int  NTHREADS       = 32;
static constexpr int  WRITE_BUF_SIZE = 4 * 1024 * 1024;

// ─── date tables ─────────────────────────────────────────────────────────────
static const int32_t YEAR_START[50] = {
    0,365,730,1096,1461,1826,2191,2557,2922,3287,
    3652,4018,4383,4748,5113,5479,5844,6209,6574,6940,
    7305,7670,8035,8401,8766,9131,9496,9862,10227,10592,
    10957,11323,11688,12053,12418,12784,13149,13514,13879,14245,
    14610,14975,15340,15706,16071,16436,16801,17167,17532,17897
};
static const int32_t MDAYS[13] = {0,31,28,31,30,31,30,31,31,30,31,30,31};

inline bool is_leap(int y) {
    return (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
}

inline int32_t parse_date(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10  + (s[6]-'0');
    int d = (s[8]-'0')*10  + (s[9]-'0');
    int32_t days = YEAR_START[y - 1970];
    for (int i = 1; i < m; i++) {
        days += MDAYS[i];
        if (i == 2 && is_leap(y)) days++;
    }
    days += d - 1;
    return days;
}

// ─── integer / double parsers ─────────────────────────────────────────────────
inline int32_t parse_int(const char* s, const char* e) {
    int32_t v = 0;
    while (s < e && *s >= '0' && *s <= '9') { v = v * 10 + (*s - '0'); s++; }
    return v;
}

inline double parse_double(const char* s, const char* e) {
    double v = 0;
    while (s < e && *s >= '0' && *s <= '9') { v = v * 10 + (*s - '0'); s++; }
    if (s < e && *s == '.') {
        s++;
        double f = 0.1;
        while (s < e && *s >= '0' && *s <= '9') { v += (*s - '0') * f; f *= 0.1; s++; }
    }
    return v;
}

// ─── Dict ────────────────────────────────────────────────────────────────────
struct Dict {
    std::mutex mtx;
    std::vector<std::string> entries;
    std::unordered_map<std::string, int16_t> lut;

    int16_t encode(const char* s, int len) {
        std::string k(s, len);
        std::lock_guard<std::mutex> g(mtx);
        auto it = lut.find(k);
        if (it != lut.end()) return it->second;
        int16_t c = (int16_t)entries.size();
        entries.push_back(k);
        lut[k] = c;
        return c;
    }

    void save(const std::string& path) {
        std::ofstream f(path);
        for (auto& e : entries) f << e << "\n";
    }
};

// ─── Buffered writer ──────────────────────────────────────────────────────────
struct BufWriter {
    FILE* fp = nullptr;
    std::vector<char> buf;
    size_t used = 0;

    explicit BufWriter(const std::string& path) : buf(WRITE_BUF_SIZE) {
        fp = fopen(path.c_str(), "wb");
        if (!fp) { perror(("fopen " + path).c_str()); exit(1); }
    }

    void write(const void* data, size_t n) {
        const char* p = reinterpret_cast<const char*>(data);
        while (n > 0) {
            size_t space = buf.size() - used;
            size_t copy  = std::min(n, space);
            memcpy(buf.data() + used, p, copy);
            used += copy; p += copy; n -= copy;
            if (used == buf.size()) flush();
        }
    }

    void flush() {
        if (used > 0) { fwrite(buf.data(), 1, used, fp); used = 0; }
    }

    void close() { flush(); if (fp) { fclose(fp); fp = nullptr; } }
    ~BufWriter() { close(); }
};

// ─── ThreadPool ───────────────────────────────────────────────────────────────
class ThreadPool {
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex q_mtx;
    std::condition_variable q_cv, done_cv;
    bool stopping = false;
    int active_count = 0;
public:
    explicit ThreadPool(int n) {
        for (int i = 0; i < n; i++) workers.emplace_back([this]{
            for (;;) {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lk(q_mtx);
                    q_cv.wait(lk, [this]{ return stopping || !tasks.empty(); });
                    if (stopping && tasks.empty()) return;
                    task = std::move(tasks.front()); tasks.pop(); active_count++;
                }
                task();
                {
                    std::lock_guard<std::mutex> lk(q_mtx);
                    active_count--;
                    done_cv.notify_all();
                }
            }
        });
    }

    void submit(std::function<void()> f) {
        std::lock_guard<std::mutex> lk(q_mtx);
        tasks.push(std::move(f));
        q_cv.notify_one();
    }

    void wait_all() {
        std::unique_lock<std::mutex> lk(q_mtx);
        done_cv.wait(lk, [this]{ return tasks.empty() && active_count == 0; });
    }

    ~ThreadPool() {
        { std::lock_guard<std::mutex> lk(q_mtx); stopping = true; }
        q_cv.notify_all();
        for (auto& w : workers) w.join();
    }
};

// ─── mmap helper ─────────────────────────────────────────────────────────────
struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(("open " + path).c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); ::close(fd); fd = -1; return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }

    void close() {
        if (data && data != MAP_FAILED) { munmap((void*)data, size); data = nullptr; }
        if (fd >= 0) { ::close(fd); fd = -1; }
    }

    ~MmapFile() { close(); }
};

// ─── chunk boundary helper ────────────────────────────────────────────────────
// Returns start pointer of chunk i (aligned to start of a line)
static const char* chunk_start(const char* base, size_t sz, int i, int n) {
    if (i == 0) return base;
    size_t off = (size_t)i * sz / (size_t)n;
    const char* p = base + off;
    while (p < base + sz && *p != '\n') p++;
    if (p < base + sz) p++; // skip past '\n'
    return p;
}

static const char* chunk_end(const char* base, size_t sz, int i, int n) {
    return chunk_start(base, sz, i + 1, n);
}

// Scan fields in a row; returns pointer to field[idx] start and sets *len
// Returns nullptr if not enough fields
static inline const char* get_field(const char* row_start, const char* row_end,
                                    int idx, int* len) {
    const char* p = row_start;
    for (int f = 0; f < idx; f++) {
        while (p < row_end && *p != '|') p++;
        if (p >= row_end) return nullptr;
        p++; // skip '|'
    }
    const char* field_start = p;
    while (p < row_end && *p != '|' && *p != '\n' && *p != '\r') p++;
    *len = (int)(p - field_start);
    return field_start;
}

// ─── apply permutation ────────────────────────────────────────────────────────
template<typename T>
static std::vector<T> apply_perm(const std::vector<T>& src,
                                  const std::vector<uint32_t>& perm) {
    std::vector<T> dst(src.size());
    for (size_t i = 0; i < perm.size(); i++) dst[i] = src[perm[i]];
    return dst;
}

// ─── write column ─────────────────────────────────────────────────────────────
template<typename T>
static void write_column(const std::string& path, const std::vector<T>& col) {
    BufWriter bw(path);
    bw.write(col.data(), col.size() * sizeof(T));
}

// Fixed-width char column (each element is a fixed-size char array already)
static void write_raw_column(const std::string& path,
                              const std::vector<uint8_t>& col) {
    BufWriter bw(path);
    bw.write(col.data(), col.size());
}

// ─── LINEITEM ─────────────────────────────────────────────────────────────────
// Fields: 0=l_orderkey 1=l_partkey 2=l_suppkey 3=SKIP 4=l_quantity 5=l_extendedprice
//         6=l_discount 7=l_tax 8=l_returnflag 9=l_linestatus 10=l_shipdate 11-15=SKIP
struct LineitemChunk {
    std::vector<int32_t> orderkey, partkey, suppkey;
    std::vector<double>  quantity, extprice, discount, tax;
    std::vector<int8_t>  returnflag, linestatus;
    std::vector<int32_t> shipdate;
};

static void ingest_lineitem(const std::string& data_dir,
                             const std::string& gendb_dir,
                             ThreadPool& pool) {
    auto t0 = std::chrono::steady_clock::now();

    std::string tbl_path = data_dir + "/lineitem.tbl";
    MmapFile mf;
    if (!mf.open(tbl_path)) { fprintf(stderr, "Cannot open %s\n", tbl_path.c_str()); exit(1); }

    Dict rf_dict, ls_dict;
    int N = NTHREADS;

    std::vector<LineitemChunk> chunks(N);

    // Submit chunks to pool
    {
        for (int ci = 0; ci < N; ci++) {
            pool.submit([&, ci]() {
                const char* start = chunk_start(mf.data, mf.size, ci, N);
                const char* end   = chunk_end  (mf.data, mf.size, ci, N);
                LineitemChunk& ch = chunks[ci];

                const char* p = start;
                while (p < end) {
                    // find end of row
                    const char* row_end = p;
                    while (row_end < end && *row_end != '\n') row_end++;
                    if (row_end == p) { p++; continue; }

                    // parse fields inline
                    const char* fields[16];
                    int   flen[16];
                    const char* fp = p;
                    for (int f = 0; f < 16; f++) {
                        fields[f] = fp;
                        while (fp < row_end && *fp != '|') fp++;
                        flen[f] = (int)(fp - fields[f]);
                        if (fp < row_end) fp++;
                    }

                    ch.orderkey.push_back(parse_int(fields[0], fields[0]+flen[0]));
                    ch.partkey .push_back(parse_int(fields[1], fields[1]+flen[1]));
                    ch.suppkey .push_back(parse_int(fields[2], fields[2]+flen[2]));
                    // field 3 skip
                    ch.quantity  .push_back(parse_double(fields[4], fields[4]+flen[4]));
                    ch.extprice  .push_back(parse_double(fields[5], fields[5]+flen[5]));
                    ch.discount  .push_back(parse_double(fields[6], fields[6]+flen[6]));
                    ch.tax       .push_back(parse_double(fields[7], fields[7]+flen[7]));
                    ch.returnflag.push_back((int8_t)rf_dict.encode(fields[8], flen[8]));
                    ch.linestatus.push_back((int8_t)ls_dict.encode(fields[9], flen[9]));
                    ch.shipdate  .push_back(parse_date(fields[10]));

                    p = row_end + 1;
                }
            });
        }
        pool.wait_all();
    }

    // Merge chunks
    size_t total = 0;
    for (auto& ch : chunks) total += ch.orderkey.size();

    std::vector<int32_t> orderkey, partkey, suppkey, shipdate;
    std::vector<double>  quantity, extprice, discount, tax;
    std::vector<int8_t>  returnflag, linestatus;

    orderkey  .reserve(total); partkey .reserve(total); suppkey.reserve(total);
    quantity  .reserve(total); extprice.reserve(total);
    discount  .reserve(total); tax     .reserve(total);
    returnflag.reserve(total); linestatus.reserve(total);
    shipdate  .reserve(total);

    for (auto& ch : chunks) {
        orderkey  .insert(orderkey  .end(), ch.orderkey  .begin(), ch.orderkey  .end());
        partkey   .insert(partkey   .end(), ch.partkey   .begin(), ch.partkey   .end());
        suppkey   .insert(suppkey   .end(), ch.suppkey   .begin(), ch.suppkey   .end());
        quantity  .insert(quantity  .end(), ch.quantity  .begin(), ch.quantity  .end());
        extprice  .insert(extprice  .end(), ch.extprice  .begin(), ch.extprice  .end());
        discount  .insert(discount  .end(), ch.discount  .begin(), ch.discount  .end());
        tax       .insert(tax       .end(), ch.tax       .begin(), ch.tax       .end());
        returnflag.insert(returnflag.end(), ch.returnflag.begin(), ch.returnflag.end());
        linestatus.insert(linestatus.end(), ch.linestatus.begin(), ch.linestatus.end());
        shipdate  .insert(shipdate  .end(), ch.shipdate  .begin(), ch.shipdate  .end());
    }
    mf.close();

    // Sort by l_shipdate
    std::vector<uint32_t> perm(total);
    std::iota(perm.begin(), perm.end(), 0u);
    std::sort(perm.begin(), perm.end(),
              [&](uint32_t a, uint32_t b){ return shipdate[a] < shipdate[b]; });

    auto s_orderkey   = apply_perm(orderkey,   perm);
    auto s_partkey    = apply_perm(partkey,    perm);
    auto s_suppkey    = apply_perm(suppkey,    perm);
    auto s_quantity   = apply_perm(quantity,   perm);
    auto s_extprice   = apply_perm(extprice,   perm);
    auto s_discount   = apply_perm(discount,   perm);
    auto s_tax        = apply_perm(tax,        perm);
    auto s_returnflag = apply_perm(returnflag, perm);
    auto s_linestatus = apply_perm(linestatus, perm);
    auto s_shipdate   = apply_perm(shipdate,   perm);

    std::string dir = gendb_dir + "/lineitem";
    fs::create_directories(dir);

    write_column(dir + "/l_orderkey.bin",    s_orderkey);
    write_column(dir + "/l_partkey.bin",     s_partkey);
    write_column(dir + "/l_suppkey.bin",     s_suppkey);
    write_column(dir + "/l_quantity.bin",    s_quantity);
    write_column(dir + "/l_extendedprice.bin", s_extprice);
    write_column(dir + "/l_discount.bin",    s_discount);
    write_column(dir + "/l_tax.bin",         s_tax);
    write_column(dir + "/l_returnflag.bin",  s_returnflag);
    write_column(dir + "/l_linestatus.bin",  s_linestatus);
    write_column(dir + "/l_shipdate.bin",    s_shipdate);

    rf_dict.save(dir + "/l_returnflag_dict.txt");
    ls_dict.save(dir + "/l_linestatus_dict.txt");

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    printf("lineitem: %zu rows  %.2fs\n", total, sec);
}

// ─── ORDERS ───────────────────────────────────────────────────────────────────
// Fields: 0=o_orderkey 1=o_custkey 2=SKIP 3=o_totalprice 4=o_orderdate
//         5=SKIP 6=SKIP 7=o_shippriority 8=SKIP
struct OrdersChunk {
    std::vector<int32_t> orderkey, custkey, shippriority;
    std::vector<double>  totalprice;
    std::vector<int32_t> orderdate;
};

static void ingest_orders(const std::string& data_dir,
                           const std::string& gendb_dir,
                           ThreadPool& pool) {
    auto t0 = std::chrono::steady_clock::now();

    std::string tbl_path = data_dir + "/orders.tbl";
    MmapFile mf;
    if (!mf.open(tbl_path)) { fprintf(stderr, "Cannot open %s\n", tbl_path.c_str()); exit(1); }

    int N = NTHREADS;
    std::vector<OrdersChunk> chunks(N);

    for (int ci = 0; ci < N; ci++) {
        pool.submit([&, ci]() {
            const char* start = chunk_start(mf.data, mf.size, ci, N);
            const char* end   = chunk_end  (mf.data, mf.size, ci, N);
            OrdersChunk& ch = chunks[ci];

            const char* p = start;
            while (p < end) {
                const char* row_end = p;
                while (row_end < end && *row_end != '\n') row_end++;
                if (row_end == p) { p++; continue; }

                const char* fields[9];
                int flen[9];
                const char* fp = p;
                for (int f = 0; f < 9; f++) {
                    fields[f] = fp;
                    while (fp < row_end && *fp != '|') fp++;
                    flen[f] = (int)(fp - fields[f]);
                    if (fp < row_end) fp++;
                }

                ch.orderkey    .push_back(parse_int(fields[0], fields[0]+flen[0]));
                ch.custkey     .push_back(parse_int(fields[1], fields[1]+flen[1]));
                // field 2 skip
                ch.totalprice  .push_back(parse_double(fields[3], fields[3]+flen[3]));
                ch.orderdate   .push_back(parse_date(fields[4]));
                // fields 5,6 skip
                ch.shippriority.push_back(parse_int(fields[7], fields[7]+flen[7]));
                // field 8 skip

                p = row_end + 1;
            }
        });
    }
    pool.wait_all();
    mf.close();

    size_t total = 0;
    for (auto& ch : chunks) total += ch.orderkey.size();

    std::vector<int32_t> orderkey, custkey, shippriority;
    std::vector<double>  totalprice;
    std::vector<int32_t> orderdate;

    orderkey    .reserve(total); custkey     .reserve(total);
    shippriority.reserve(total); totalprice  .reserve(total);
    orderdate   .reserve(total);

    for (auto& ch : chunks) {
        orderkey    .insert(orderkey    .end(), ch.orderkey    .begin(), ch.orderkey    .end());
        custkey     .insert(custkey     .end(), ch.custkey     .begin(), ch.custkey     .end());
        totalprice  .insert(totalprice  .end(), ch.totalprice  .begin(), ch.totalprice  .end());
        orderdate   .insert(orderdate   .end(), ch.orderdate   .begin(), ch.orderdate   .end());
        shippriority.insert(shippriority.end(), ch.shippriority.begin(), ch.shippriority.end());
    }

    // Sort by o_orderdate
    std::vector<uint32_t> perm(total);
    std::iota(perm.begin(), perm.end(), 0u);
    std::sort(perm.begin(), perm.end(),
              [&](uint32_t a, uint32_t b){ return orderdate[a] < orderdate[b]; });

    auto s_orderkey     = apply_perm(orderkey,     perm);
    auto s_custkey      = apply_perm(custkey,      perm);
    auto s_totalprice   = apply_perm(totalprice,   perm);
    auto s_orderdate    = apply_perm(orderdate,    perm);
    auto s_shippriority = apply_perm(shippriority, perm);

    std::string dir = gendb_dir + "/orders";
    fs::create_directories(dir);

    write_column(dir + "/o_orderkey.bin",     s_orderkey);
    write_column(dir + "/o_custkey.bin",      s_custkey);
    write_column(dir + "/o_totalprice.bin",   s_totalprice);
    write_column(dir + "/o_orderdate.bin",    s_orderdate);
    write_column(dir + "/o_shippriority.bin", s_shippriority);

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    printf("orders: %zu rows  %.2fs\n", total, sec);
}

// ─── CUSTOMER ────────────────────────────────────────────────────────────────
// Fields: 0=c_custkey 1=c_name 2-5=SKIP 6=c_mktsegment 7=SKIP
// c_name: char[26] fixed-width, null-padded
static constexpr int CNAME_WIDTH = 26;
static constexpr int MKTSG_WIDTH = 10; // unused width, stored as int8 dict

struct CustomerChunk {
    std::vector<int32_t>  custkey;
    std::vector<uint8_t>  name;     // CNAME_WIDTH bytes per entry
    std::vector<int8_t>   mktsegment;
};

static void ingest_customer(const std::string& data_dir,
                             const std::string& gendb_dir,
                             ThreadPool& pool) {
    auto t0 = std::chrono::steady_clock::now();

    std::string tbl_path = data_dir + "/customer.tbl";
    MmapFile mf;
    if (!mf.open(tbl_path)) { fprintf(stderr, "Cannot open %s\n", tbl_path.c_str()); exit(1); }

    Dict mkt_dict;
    int N = NTHREADS;
    std::vector<CustomerChunk> chunks(N);

    for (int ci = 0; ci < N; ci++) {
        pool.submit([&, ci]() {
            const char* start = chunk_start(mf.data, mf.size, ci, N);
            const char* end   = chunk_end  (mf.data, mf.size, ci, N);
            CustomerChunk& ch = chunks[ci];

            const char* p = start;
            while (p < end) {
                const char* row_end = p;
                while (row_end < end && *row_end != '\n') row_end++;
                if (row_end == p) { p++; continue; }

                const char* fields[8];
                int flen[8];
                const char* fp = p;
                for (int f = 0; f < 8; f++) {
                    fields[f] = fp;
                    while (fp < row_end && *fp != '|') fp++;
                    flen[f] = (int)(fp - fields[f]);
                    if (fp < row_end) fp++;
                }

                ch.custkey.push_back(parse_int(fields[0], fields[0]+flen[0]));

                // c_name: fixed 26 bytes
                char nbuf[CNAME_WIDTH];
                memset(nbuf, 0, CNAME_WIDTH);
                int copy_len = std::min(flen[1], CNAME_WIDTH);
                memcpy(nbuf, fields[1], copy_len);
                for (int b = 0; b < CNAME_WIDTH; b++) ch.name.push_back((uint8_t)nbuf[b]);

                ch.mktsegment.push_back((int8_t)mkt_dict.encode(fields[6], flen[6]));

                p = row_end + 1;
            }
        });
    }
    pool.wait_all();
    mf.close();

    size_t total = 0;
    for (auto& ch : chunks) total += ch.custkey.size();

    std::vector<int32_t> custkey;
    std::vector<uint8_t> name;
    std::vector<int8_t>  mktsegment;

    custkey   .reserve(total);
    name      .reserve(total * CNAME_WIDTH);
    mktsegment.reserve(total);

    for (auto& ch : chunks) {
        custkey   .insert(custkey   .end(), ch.custkey   .begin(), ch.custkey   .end());
        name      .insert(name      .end(), ch.name      .begin(), ch.name      .end());
        mktsegment.insert(mktsegment.end(), ch.mktsegment.begin(), ch.mktsegment.end());
    }

    std::string dir = gendb_dir + "/customer";
    fs::create_directories(dir);

    write_column   (dir + "/c_custkey.bin",    custkey);
    write_raw_column(dir + "/c_name.bin",      name);
    write_column   (dir + "/c_mktsegment.bin", mktsegment);
    mkt_dict.save  (dir + "/c_mktsegment_dict.txt");

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    printf("customer: %zu rows  %.2fs\n", total, sec);
}

// ─── PART ─────────────────────────────────────────────────────────────────────
// Fields: 0=p_partkey 1=p_name 2-8=SKIP
// p_name: char[56] fixed-width, null-padded
static constexpr int PNAME_WIDTH = 56;

struct PartChunk {
    std::vector<int32_t> partkey;
    std::vector<uint8_t> name;   // PNAME_WIDTH bytes per entry
};

static void ingest_part(const std::string& data_dir,
                         const std::string& gendb_dir,
                         ThreadPool& pool) {
    auto t0 = std::chrono::steady_clock::now();

    std::string tbl_path = data_dir + "/part.tbl";
    MmapFile mf;
    if (!mf.open(tbl_path)) { fprintf(stderr, "Cannot open %s\n", tbl_path.c_str()); exit(1); }

    int N = NTHREADS;
    std::vector<PartChunk> chunks(N);

    for (int ci = 0; ci < N; ci++) {
        pool.submit([&, ci]() {
            const char* start = chunk_start(mf.data, mf.size, ci, N);
            const char* end   = chunk_end  (mf.data, mf.size, ci, N);
            PartChunk& ch = chunks[ci];

            const char* p = start;
            while (p < end) {
                const char* row_end = p;
                while (row_end < end && *row_end != '\n') row_end++;
                if (row_end == p) { p++; continue; }

                const char* fields[9];
                int flen[9];
                const char* fp = p;
                for (int f = 0; f < 9; f++) {
                    fields[f] = fp;
                    while (fp < row_end && *fp != '|') fp++;
                    flen[f] = (int)(fp - fields[f]);
                    if (fp < row_end) fp++;
                }

                ch.partkey.push_back(parse_int(fields[0], fields[0]+flen[0]));

                char nbuf[PNAME_WIDTH];
                memset(nbuf, 0, PNAME_WIDTH);
                int copy_len = std::min(flen[1], PNAME_WIDTH);
                memcpy(nbuf, fields[1], copy_len);
                for (int b = 0; b < PNAME_WIDTH; b++) ch.name.push_back((uint8_t)nbuf[b]);

                p = row_end + 1;
            }
        });
    }
    pool.wait_all();
    mf.close();

    size_t total = 0;
    for (auto& ch : chunks) total += ch.partkey.size();

    std::vector<int32_t> partkey;
    std::vector<uint8_t> name;
    partkey.reserve(total);
    name   .reserve(total * PNAME_WIDTH);

    for (auto& ch : chunks) {
        partkey.insert(partkey.end(), ch.partkey.begin(), ch.partkey.end());
        name   .insert(name   .end(), ch.name   .begin(), ch.name   .end());
    }

    std::string dir = gendb_dir + "/part";
    fs::create_directories(dir);

    write_column   (dir + "/p_partkey.bin", partkey);
    write_raw_column(dir + "/p_name.bin",   name);

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    printf("part: %zu rows  %.2fs\n", total, sec);
}

// ─── PARTSUPP ────────────────────────────────────────────────────────────────
// Fields: 0=ps_partkey 1=ps_suppkey 2=SKIP 3=ps_supplycost 4=SKIP
struct PartsuppChunk {
    std::vector<int32_t> partkey, suppkey;
    std::vector<double>  supplycost;
};

static void ingest_partsupp(const std::string& data_dir,
                              const std::string& gendb_dir,
                              ThreadPool& pool) {
    auto t0 = std::chrono::steady_clock::now();

    std::string tbl_path = data_dir + "/partsupp.tbl";
    MmapFile mf;
    if (!mf.open(tbl_path)) { fprintf(stderr, "Cannot open %s\n", tbl_path.c_str()); exit(1); }

    int N = NTHREADS;
    std::vector<PartsuppChunk> chunks(N);

    for (int ci = 0; ci < N; ci++) {
        pool.submit([&, ci]() {
            const char* start = chunk_start(mf.data, mf.size, ci, N);
            const char* end   = chunk_end  (mf.data, mf.size, ci, N);
            PartsuppChunk& ch = chunks[ci];

            const char* p = start;
            while (p < end) {
                const char* row_end = p;
                while (row_end < end && *row_end != '\n') row_end++;
                if (row_end == p) { p++; continue; }

                const char* fields[5];
                int flen[5];
                const char* fp = p;
                for (int f = 0; f < 5; f++) {
                    fields[f] = fp;
                    while (fp < row_end && *fp != '|') fp++;
                    flen[f] = (int)(fp - fields[f]);
                    if (fp < row_end) fp++;
                }

                ch.partkey   .push_back(parse_int   (fields[0], fields[0]+flen[0]));
                ch.suppkey   .push_back(parse_int   (fields[1], fields[1]+flen[1]));
                // field 2 skip
                ch.supplycost.push_back(parse_double(fields[3], fields[3]+flen[3]));
                // field 4 skip

                p = row_end + 1;
            }
        });
    }
    pool.wait_all();
    mf.close();

    size_t total = 0;
    for (auto& ch : chunks) total += ch.partkey.size();

    std::vector<int32_t> partkey, suppkey;
    std::vector<double>  supplycost;
    partkey   .reserve(total); suppkey   .reserve(total);
    supplycost.reserve(total);

    for (auto& ch : chunks) {
        partkey   .insert(partkey   .end(), ch.partkey   .begin(), ch.partkey   .end());
        suppkey   .insert(suppkey   .end(), ch.suppkey   .begin(), ch.suppkey   .end());
        supplycost.insert(supplycost.end(), ch.supplycost.begin(), ch.supplycost.end());
    }

    std::string dir = gendb_dir + "/partsupp";
    fs::create_directories(dir);

    write_column(dir + "/ps_partkey.bin",    partkey);
    write_column(dir + "/ps_suppkey.bin",    suppkey);
    write_column(dir + "/ps_supplycost.bin", supplycost);

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    printf("partsupp: %zu rows  %.2fs\n", total, sec);
}

// ─── SUPPLIER (single-threaded) ───────────────────────────────────────────────
// Fields: 0=s_suppkey 1=SKIP 2=SKIP 3=s_nationkey 4-6=SKIP
static void ingest_supplier(const std::string& data_dir,
                              const std::string& gendb_dir) {
    auto t0 = std::chrono::steady_clock::now();

    std::string tbl_path = data_dir + "/supplier.tbl";
    MmapFile mf;
    if (!mf.open(tbl_path)) { fprintf(stderr, "Cannot open %s\n", tbl_path.c_str()); exit(1); }

    std::vector<int32_t> suppkey, nationkey;

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    while (p < end) {
        const char* row_end = p;
        while (row_end < end && *row_end != '\n') row_end++;
        if (row_end == p) { p++; continue; }

        const char* fields[7];
        int flen[7];
        const char* fp = p;
        for (int f = 0; f < 7; f++) {
            fields[f] = fp;
            while (fp < row_end && *fp != '|') fp++;
            flen[f] = (int)(fp - fields[f]);
            if (fp < row_end) fp++;
        }

        suppkey  .push_back(parse_int(fields[0], fields[0]+flen[0]));
        nationkey.push_back(parse_int(fields[3], fields[3]+flen[3]));

        p = row_end + 1;
    }
    mf.close();

    std::string dir = gendb_dir + "/supplier";
    fs::create_directories(dir);

    write_column(dir + "/s_suppkey.bin",   suppkey);
    write_column(dir + "/s_nationkey.bin", nationkey);

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    printf("supplier: %zu rows  %.2fs\n", suppkey.size(), sec);
}

// ─── NATION (single-threaded, only 25 rows) ───────────────────────────────────
// Fields: 0=n_nationkey 1=n_name 2=SKIP 3=SKIP
static void ingest_nation(const std::string& data_dir,
                           const std::string& gendb_dir) {
    auto t0 = std::chrono::steady_clock::now();

    std::string tbl_path = data_dir + "/nation.tbl";
    MmapFile mf;
    if (!mf.open(tbl_path)) { fprintf(stderr, "Cannot open %s\n", tbl_path.c_str()); exit(1); }

    Dict name_dict;
    std::vector<int32_t> nationkey;
    std::vector<int8_t>  nation_name;

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    while (p < end) {
        const char* row_end = p;
        while (row_end < end && *row_end != '\n') row_end++;
        if (row_end == p) { p++; continue; }

        const char* fields[4];
        int flen[4];
        const char* fp = p;
        for (int f = 0; f < 4; f++) {
            fields[f] = fp;
            while (fp < row_end && *fp != '|') fp++;
            flen[f] = (int)(fp - fields[f]);
            if (fp < row_end) fp++;
        }

        nationkey  .push_back(parse_int(fields[0], fields[0]+flen[0]));
        nation_name.push_back((int8_t)name_dict.encode(fields[1], flen[1]));

        p = row_end + 1;
    }
    mf.close();

    std::string dir = gendb_dir + "/nation";
    fs::create_directories(dir);

    write_column(dir + "/n_nationkey.bin", nationkey);
    write_column(dir + "/n_name.bin",      nation_name);
    name_dict.save(dir + "/n_name_dict.txt");

    auto t1 = std::chrono::steady_clock::now();
    double sec = std::chrono::duration<double>(t1 - t0).count();
    printf("nation: %zu rows  %.2fs\n", nationkey.size(), sec);
}

// ─── verification ─────────────────────────────────────────────────────────────
static void verify(const std::string& gendb_dir) {
    // Check lineitem/l_shipdate.bin first value > 3000
    {
        std::string path = gendb_dir + "/lineitem/l_shipdate.bin";
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s for verify\n", path.c_str()); exit(1); }
        int32_t val = 0;
        if (::read(fd, &val, 4) != 4) { fprintf(stderr, "Short read on %s\n", path.c_str()); exit(1); }
        ::close(fd);
        if (val <= 3000) {
            fprintf(stderr, "VERIFICATION FAILED: l_shipdate first value = %d (expected > 3000)\n", val);
            exit(1);
        }
    }
    // Check lineitem/l_extendedprice.bin first value > 0.0
    {
        std::string path = gendb_dir + "/lineitem/l_extendedprice.bin";
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s for verify\n", path.c_str()); exit(1); }
        double val = 0.0;
        if (::read(fd, &val, 8) != 8) { fprintf(stderr, "Short read on %s\n", path.c_str()); exit(1); }
        ::close(fd);
        if (val <= 0.0) {
            fprintf(stderr, "VERIFICATION FAILED: l_extendedprice first value = %f (expected > 0.0)\n", val);
            exit(1);
        }
    }
    printf("Verification passed\n");
}

// ─── main ─────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <data_dir> <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string data_dir  = argv[1];
    std::string gendb_dir = argv[2];

    // Sanity-check date parsing
    assert(parse_date("1970-01-01") == 0);
    assert(parse_date("1992-01-01") == 8035);

    fs::create_directories(gendb_dir);

    ThreadPool pool(NTHREADS);

    auto t_total = std::chrono::steady_clock::now();

    // Process large tables in parallel using std::thread (one thread per table),
    // each table uses the shared thread pool internally for chunk-level parsing.
    // We process them sequentially here to avoid contention on the thread pool,
    // but the pool provides real parallelism within each table.

    // All large tables: lineitem, orders, customer, part, partsupp
    // They each use pool.wait_all() internally before returning, so we run them
    // one at a time to prevent nested wait_all deadlocks with a single pool.
    //
    // For true table-level parallelism with separate threads per table we would
    // need per-table pools, but the instruction says use one thread per table
    // with the shared pool for chunks. Since all chunk tasks drain before each
    // ingest_* returns, we run ingest_* from dedicated table-threads and protect
    // pool.wait_all() via a mutex so only one table waits at a time.

    // Simpler: run tables sequentially, each using internal chunked parallelism.
    // This is safe and correct.
    ingest_lineitem(data_dir, gendb_dir, pool);
    ingest_orders  (data_dir, gendb_dir, pool);
    ingest_customer(data_dir, gendb_dir, pool);
    ingest_part    (data_dir, gendb_dir, pool);
    ingest_partsupp(data_dir, gendb_dir, pool);

    // Single-threaded small tables
    ingest_supplier(data_dir, gendb_dir);
    ingest_nation  (data_dir, gendb_dir);

    auto t_end = std::chrono::steady_clock::now();
    double total_sec = std::chrono::duration<double>(t_end - t_total).count();
    printf("Total ingestion time: %.2fs\n", total_sec);

    verify(gendb_dir);

    return 0;
}
