// ingest.cpp — TPC-H SF10 binary columnar ingestion
// Compile: g++ -O2 -std=c++17 -Wall -lpthread
// Usage: ./ingest <data_dir> <out_dir>
//
// Design: tables processed sequentially (optimal for HDD — avoids seeking).
// Within each table: parsing uses N_THREADS threads (CPU parallelism).
// Within each table: column writes are parallel (after munmap of input).
#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <numeric>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include <sys/mman.h>
#include <sys/stat.h>
#include <chrono>
namespace fs = std::filesystem;

// ============================================================
// Date tables (precomputed for years 1970..2009)
// ============================================================
static int32_t g_year_epoch[40];
static int32_t g_month_start[2][13];

static void init_date_tables() {
    static const int mdays[2][13] = {
        {0,31,28,31,30,31,30,31,31,30,31,30,31},
        {0,31,29,31,30,31,30,31,31,30,31,30,31}
    };
    for (int lp = 0; lp < 2; lp++) {
        g_month_start[lp][1] = 0;
        for (int m = 2; m <= 12; m++)
            g_month_start[lp][m] = g_month_start[lp][m-1] + mdays[lp][m-1];
    }
    int days = 0;
    for (int i = 0; i < 40; i++) {
        g_year_epoch[i] = days;
        int yr = 1970 + i;
        bool lp = (yr%4==0 && yr%100!=0) || yr%400==0;
        days += lp ? 366 : 365;
    }
}

static inline int32_t date_to_epoch(int y, int m, int d) {
    bool lp = (y%4==0 && y%100!=0) || y%400==0;
    return g_year_epoch[y - 1970] + g_month_start[lp?1:0][m] + (d - 1);
}

// ============================================================
// Fast field parsers — each advances `p` past the trailing '|'
// ============================================================
static inline int32_t parse_int_field(const char*& p) {
    int32_t v = 0;
    while (*p >= '0' && *p <= '9') v = v * 10 + (*p++ - '0');
    p++; // skip '|'
    return v;
}

static inline double parse_double_field(const char*& p) {
    static const double pt[] = {1.0, 10.0, 100.0, 1000.0};
    int64_t i = 0;
    while (*p >= '0' && *p <= '9') i = i * 10 + (*p++ - '0');
    int64_t f = 0; int fd = 0;
    if (*p == '.') {
        p++;
        while (*p >= '0' && *p <= '9') { f = f*10 + (*p++ - '0'); fd++; }
    }
    p++; // skip '|'
    return (double)i + (fd > 0 ? (double)f / pt[fd] : 0.0);
}

static inline int32_t parse_date_field(const char*& p) {
    int y = (p[0]-'0')*1000 + (p[1]-'0')*100 + (p[2]-'0')*10 + (p[3]-'0');
    int m = (p[5]-'0')*10 + (p[6]-'0');
    int d = (p[8]-'0')*10 + (p[9]-'0');
    p += 10; p++; // skip '|'
    return date_to_epoch(y, m, d);
}

static inline void skip_field(const char*& p) {
    while (*p != '|') p++;
    p++;
}

static inline void parse_fixed_string(const char*& p, char* buf, int maxlen) {
    int i = 0;
    while (*p != '|' && i < maxlen - 1) buf[i++] = *p++;
    while (*p != '|') p++;
    p++;
    while (i < maxlen) buf[i++] = '\0';
}

// c_mktsegment: BUILDING=0, AUTOMOBILE=1, MACHINERY=2, HOUSEHOLD=3, FURNITURE=4
static inline uint8_t parse_mktsegment(const char*& p) {
    uint8_t code;
    switch (*p) {
        case 'B': code = 0; break; // BUILDING
        case 'A': code = 1; break; // AUTOMOBILE
        case 'M': code = 2; break; // MACHINERY
        case 'H': code = 3; break; // HOUSEHOLD
        case 'F': code = 4; break; // FURNITURE
        default:  code = 255; break;
    }
    skip_field(p);
    return code;
}

static inline void skip_to_newline(const char*& p, const char* end) {
    while (p < end && *p != '\n') p++;
    if (p < end) p++;
}

// ============================================================
// mmap helper
// ============================================================
static std::pair<const char*, size_t> mmap_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t sz = (size_t)st.st_size;
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
    madvise(ptr, sz, MADV_SEQUENTIAL);
    close(fd);
    return {(const char*)ptr, sz};
}

// Divide [0, size) into n chunks aligned to newlines
static std::vector<size_t> find_boundaries(const char* data, size_t size, int n) {
    std::vector<size_t> b; b.reserve(n + 1);
    b.push_back(0);
    for (int i = 1; i < n; i++) {
        size_t pos = (size_t)i * size / n;
        while (pos < size && data[pos] != '\n') pos++;
        if (pos < size) pos++;
        b.push_back(pos);
    }
    b.push_back(size);
    return b;
}

// ============================================================
// Write helpers
// ============================================================
template<typename T>
static void write_col(const std::vector<T>& data, const std::string& path) {
    constexpr size_t BUF = 4ULL * 1024 * 1024;
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    const char* ptr = reinterpret_cast<const char*>(data.data());
    size_t rem = data.size() * sizeof(T);
    while (rem > 0) {
        size_t ch = std::min(rem, BUF);
        ssize_t w = ::write(fd, ptr, ch); if (w <= 0) { perror("write"); exit(1); }
        ptr += w; rem -= (size_t)w;
    }
    close(fd);
}

static void write_raw(const char* data, size_t bytes, const std::string& path) {
    constexpr size_t BUF = 4ULL * 1024 * 1024;
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) { perror(path.c_str()); exit(1); }
    size_t rem = bytes; const char* ptr = data;
    while (rem > 0) {
        size_t ch = std::min(rem, BUF);
        ssize_t w = ::write(fd, ptr, ch); if (w <= 0) { perror("write"); exit(1); }
        ptr += w; rem -= (size_t)w;
    }
    close(fd);
}

// ============================================================
// Permutation application
// ============================================================
template<typename T>
static void apply_perm(std::vector<T>& arr, const std::vector<uint32_t>& perm) {
    std::vector<T> tmp(arr.size());
    for (size_t i = 0; i < perm.size(); i++) tmp[i] = arr[perm[i]];
    arr = std::move(tmp);
}

// ============================================================
// Parallel launch helper
// ============================================================
template<typename... Fns>
static void parallel_run(Fns&&... fns) {
    std::vector<std::thread> ts;
    (ts.emplace_back(std::forward<Fns>(fns)), ...);
    for (auto& t : ts) t.join();
}

// ============================================================
// Verification
// ============================================================
static void check_date(int32_t val, const char* name) {
    if (val <= 3000) {
        fprintf(stderr, "FATAL: %s value %d <= 3000 (date encoding error)\n", name, val);
        exit(1);
    }
}
static void check_decimal(double val, const char* name) {
    if (val == 0.0) {
        fprintf(stderr, "FATAL: %s value is 0.0 (decimal parsing error)\n", name);
        exit(1);
    }
}

// ============================================================
// LINEITEM — sorted by l_shipdate for zone-map effectiveness
// Fields: orderkey|partkey|suppkey|linenumber|quantity|extendedprice|
//         discount|tax|returnflag|linestatus|shipdate|commitdate|
//         receiptdate|shipinstruct|shipmode|comment
// ============================================================
static void ingest_lineitem(const std::string& data_dir,
                            const std::string& out_dir, int nthreads) {
    auto t0 = std::chrono::steady_clock::now();
    printf("[lineitem] starting with %d parse threads...\n", nthreads); fflush(stdout);

    auto [data, fsize] = mmap_file(data_dir + "/lineitem.tbl");
    auto bounds = find_boundaries(data, fsize, nthreads);

    struct Buf {
        std::vector<int32_t> ok, pk, sk, sd;
        std::vector<double>  qty, ep, disc, tax;
        std::vector<int8_t>  rf, ls;
    };
    std::vector<Buf> bufs(nthreads);
    size_t est = 60500000 / nthreads + 1000;
    for (auto& b : bufs) {
        b.ok.reserve(est); b.pk.reserve(est); b.sk.reserve(est); b.sd.reserve(est);
        b.qty.reserve(est); b.ep.reserve(est); b.disc.reserve(est); b.tax.reserve(est);
        b.rf.reserve(est); b.ls.reserve(est);
    }

    {
        std::vector<std::thread> threads; threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&bufs, &bounds, data, t]() {
                const char* p   = data + bounds[t];
                const char* end = data + bounds[t+1];
                auto& b = bufs[t];
                while (p < end) {
                    int32_t ok_  = parse_int_field(p);    // 0 orderkey
                    int32_t pk_  = parse_int_field(p);    // 1 partkey
                    int32_t sk_  = parse_int_field(p);    // 2 suppkey
                    skip_field(p);                         // 3 linenumber
                    double  qty_ = parse_double_field(p); // 4 quantity
                    double  ep_  = parse_double_field(p); // 5 extendedprice
                    double  disc_= parse_double_field(p); // 6 discount
                    double  tax_ = parse_double_field(p); // 7 tax
                    int8_t  rf_  = (int8_t)*p; skip_field(p); // 8 returnflag
                    int8_t  ls_  = (int8_t)*p; skip_field(p); // 9 linestatus
                    int32_t sd_  = parse_date_field(p);   // 10 shipdate
                    skip_to_newline(p, end);               // 11-15 skip rest
                    b.ok.push_back(ok_); b.pk.push_back(pk_); b.sk.push_back(sk_);
                    b.qty.push_back(qty_); b.ep.push_back(ep_);
                    b.disc.push_back(disc_); b.tax.push_back(tax_);
                    b.rf.push_back(rf_); b.ls.push_back(ls_); b.sd.push_back(sd_);
                }
            });
        }
        for (auto& th : threads) th.join();
    }
    munmap((void*)data, fsize);

    size_t total = 0;
    for (auto& b : bufs) total += b.ok.size();
    printf("[lineitem] parsed %zu rows\n", total); fflush(stdout);

    std::vector<int32_t> orderkey(total), partkey(total), suppkey(total), shipdate(total);
    std::vector<double>  qty(total), ep(total), disc(total), tax(total);
    std::vector<int8_t>  rf(total), ls(total);

    size_t off = 0;
    for (auto& b : bufs) {
        size_t n = b.ok.size();
        memcpy(&orderkey[off], b.ok.data(), n*4);
        memcpy(&partkey[off],  b.pk.data(), n*4);
        memcpy(&suppkey[off],  b.sk.data(), n*4);
        memcpy(&shipdate[off], b.sd.data(), n*4);
        memcpy(&qty[off],  b.qty.data(),  n*8);
        memcpy(&ep[off],   b.ep.data(),   n*8);
        memcpy(&disc[off], b.disc.data(), n*8);
        memcpy(&tax[off],  b.tax.data(),  n*8);
        memcpy(&rf[off], b.rf.data(), n);
        memcpy(&ls[off], b.ls.data(), n);
        off += n;
        Buf empty; b = std::move(empty);
    }

    if (!shipdate.empty()) check_date(shipdate[1000], "l_shipdate");
    if (!ep.empty())       check_decimal(ep[0], "l_extendedprice");

    printf("[lineitem] sorting by l_shipdate...\n"); fflush(stdout);
    std::vector<std::pair<int32_t,uint32_t>> sk_pairs(total);
    {
        int nthreads_sort = nthreads;
        size_t chunk = (total + nthreads_sort - 1) / nthreads_sort;
        std::vector<std::thread> ts; ts.reserve(nthreads_sort);
        for (int t = 0; t < nthreads_sort; t++) {
            size_t s = (size_t)t * chunk;
            size_t e = std::min(s + chunk, total);
            ts.emplace_back([&sk_pairs, &shipdate, s, e]() {
                for (size_t i = s; i < e; i++) sk_pairs[i] = {shipdate[i], (uint32_t)i};
            });
        }
        for (auto& t : ts) t.join();
    }
    std::sort(sk_pairs.begin(), sk_pairs.end());

    std::vector<uint32_t> perm(total);
    for (size_t i = 0; i < total; i++) perm[i] = sk_pairs[i].second;
    { std::vector<std::pair<int32_t,uint32_t>> tmp; sk_pairs.swap(tmp); }

    printf("[lineitem] applying sort permutation...\n"); fflush(stdout);
    parallel_run(
        [&]() { apply_perm(orderkey, perm); },
        [&]() { apply_perm(partkey,  perm); },
        [&]() { apply_perm(suppkey,  perm); },
        [&]() { apply_perm(shipdate, perm); },
        [&]() { apply_perm(qty,  perm); },
        [&]() { apply_perm(ep,   perm); },
        [&]() { apply_perm(disc, perm); },
        [&]() { apply_perm(tax,  perm); },
        [&]() { apply_perm(rf,   perm); },
        [&]() { apply_perm(ls,   perm); }
    );
    { std::vector<uint32_t> tmp; perm.swap(tmp); }

    printf("[lineitem] writing columns...\n"); fflush(stdout);
    std::string ld = out_dir + "/lineitem";
    fs::create_directories(ld);
    parallel_run(
        [&]() { write_col(orderkey, ld+"/l_orderkey.bin"); },
        [&]() { write_col(partkey,  ld+"/l_partkey.bin"); },
        [&]() { write_col(suppkey,  ld+"/l_suppkey.bin"); },
        [&]() { write_col(shipdate, ld+"/l_shipdate.bin"); },
        [&]() { write_col(qty,  ld+"/l_quantity.bin"); },
        [&]() { write_col(ep,   ld+"/l_extendedprice.bin"); },
        [&]() { write_col(disc, ld+"/l_discount.bin"); },
        [&]() { write_col(tax,  ld+"/l_tax.bin"); },
        [&]() { write_col(rf,   ld+"/l_returnflag.bin"); },
        [&]() { write_col(ls,   ld+"/l_linestatus.bin"); }
    );

    auto t1 = std::chrono::steady_clock::now();
    printf("[lineitem] done: %zu rows in %.1fs\n", total,
           std::chrono::duration<double>(t1-t0).count()); fflush(stdout);
}

// ============================================================
// ORDERS — sorted by o_orderdate for zone-map effectiveness
// Fields: orderkey|custkey|orderstatus|totalprice|orderdate|
//         orderpriority|clerk|shippriority|comment
// ============================================================
static void ingest_orders(const std::string& data_dir,
                          const std::string& out_dir, int nthreads) {
    auto t0 = std::chrono::steady_clock::now();
    printf("[orders] starting...\n"); fflush(stdout);

    auto [data, fsize] = mmap_file(data_dir + "/orders.tbl");
    auto bounds = find_boundaries(data, fsize, nthreads);

    struct Buf {
        std::vector<int32_t> ok, ck, od, sp;
        std::vector<double>  tp;
    };
    std::vector<Buf> bufs(nthreads);
    size_t est = 15100000 / nthreads + 1000;
    for (auto& b : bufs) {
        b.ok.reserve(est); b.ck.reserve(est);
        b.od.reserve(est); b.sp.reserve(est); b.tp.reserve(est);
    }

    {
        std::vector<std::thread> threads; threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&bufs, &bounds, data, t]() {
                const char* p   = data + bounds[t];
                const char* end = data + bounds[t+1];
                auto& b = bufs[t];
                while (p < end) {
                    int32_t ok_ = parse_int_field(p);    // 0 orderkey
                    int32_t ck_ = parse_int_field(p);    // 1 custkey
                    skip_field(p);                        // 2 orderstatus
                    double  tp_ = parse_double_field(p); // 3 totalprice
                    int32_t od_ = parse_date_field(p);   // 4 orderdate
                    skip_field(p);                        // 5 orderpriority
                    skip_field(p);                        // 6 clerk
                    int32_t sp_ = parse_int_field(p);    // 7 shippriority
                    skip_to_newline(p, end);              // 8 comment
                    b.ok.push_back(ok_); b.ck.push_back(ck_);
                    b.tp.push_back(tp_); b.od.push_back(od_); b.sp.push_back(sp_);
                }
            });
        }
        for (auto& th : threads) th.join();
    }
    munmap((void*)data, fsize);

    size_t total = 0;
    for (auto& b : bufs) total += b.ok.size();
    printf("[orders] parsed %zu rows\n", total); fflush(stdout);

    std::vector<int32_t> orderkey(total), custkey(total), orderdate(total), shippriority(total);
    std::vector<double>  totalprice(total);

    size_t off = 0;
    for (auto& b : bufs) {
        size_t n = b.ok.size();
        memcpy(&orderkey[off],    b.ok.data(), n*4);
        memcpy(&custkey[off],     b.ck.data(), n*4);
        memcpy(&orderdate[off],   b.od.data(), n*4);
        memcpy(&shippriority[off],b.sp.data(), n*4);
        memcpy(&totalprice[off],  b.tp.data(), n*8);
        off += n;
        Buf empty; b = std::move(empty);
    }

    if (!orderdate.empty()) check_date(orderdate[100], "o_orderdate");
    if (!totalprice.empty()) check_decimal(totalprice[0], "o_totalprice");

    printf("[orders] sorting by o_orderdate...\n"); fflush(stdout);
    std::vector<std::pair<int32_t,uint32_t>> sk_pairs(total);
    for (size_t i = 0; i < total; i++) sk_pairs[i] = {orderdate[i], (uint32_t)i};
    std::sort(sk_pairs.begin(), sk_pairs.end());
    std::vector<uint32_t> perm(total);
    for (size_t i = 0; i < total; i++) perm[i] = sk_pairs[i].second;
    { std::vector<std::pair<int32_t,uint32_t>> tmp; sk_pairs.swap(tmp); }

    parallel_run(
        [&]() { apply_perm(orderkey,    perm); },
        [&]() { apply_perm(custkey,     perm); },
        [&]() { apply_perm(orderdate,   perm); },
        [&]() { apply_perm(shippriority,perm); },
        [&]() { apply_perm(totalprice,  perm); }
    );

    std::string od = out_dir + "/orders";
    fs::create_directories(od);
    parallel_run(
        [&]() { write_col(orderkey,    od+"/o_orderkey.bin"); },
        [&]() { write_col(custkey,     od+"/o_custkey.bin"); },
        [&]() { write_col(totalprice,  od+"/o_totalprice.bin"); },
        [&]() { write_col(orderdate,   od+"/o_orderdate.bin"); },
        [&]() { write_col(shippriority,od+"/o_shippriority.bin"); }
    );

    auto t1 = std::chrono::steady_clock::now();
    printf("[orders] done: %zu rows in %.1fs\n", total,
           std::chrono::duration<double>(t1-t0).count()); fflush(stdout);
}

// ============================================================
// CUSTOMER
// Fields: custkey|name|address|nationkey|phone|acctbal|mktsegment|comment
// ============================================================
static void ingest_customer(const std::string& data_dir,
                            const std::string& out_dir, int nthreads) {
    printf("[customer] starting...\n"); fflush(stdout);
    auto [data, fsize] = mmap_file(data_dir + "/customer.tbl");
    auto bounds = find_boundaries(data, fsize, nthreads);

    struct Buf {
        std::vector<int32_t> ck;
        std::vector<char>    name; // stride 26
        std::vector<uint8_t> mkt;
    };
    std::vector<Buf> bufs(nthreads);
    size_t est = 1510000 / nthreads + 1000;
    for (auto& b : bufs) {
        b.ck.reserve(est); b.name.reserve(est*26); b.mkt.reserve(est);
    }

    {
        std::vector<std::thread> threads; threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&bufs, &bounds, data, t]() {
                const char* p   = data + bounds[t];
                const char* end = data + bounds[t+1];
                auto& b = bufs[t];
                char nbuf[26];
                while (p < end) {
                    int32_t ck_  = parse_int_field(p);      // 0 custkey
                    parse_fixed_string(p, nbuf, 26);         // 1 c_name
                    skip_field(p);                           // 2 address
                    skip_field(p);                           // 3 nationkey
                    skip_field(p);                           // 4 phone
                    skip_field(p);                           // 5 acctbal
                    uint8_t mkt_ = parse_mktsegment(p);      // 6 mktsegment
                    skip_to_newline(p, end);                 // 7 comment
                    b.ck.push_back(ck_);
                    for (int i = 0; i < 26; i++) b.name.push_back(nbuf[i]);
                    b.mkt.push_back(mkt_);
                }
            });
        }
        for (auto& th : threads) th.join();
    }
    munmap((void*)data, fsize);

    size_t total = 0;
    for (auto& b : bufs) total += b.ck.size();
    printf("[customer] parsed %zu rows\n", total); fflush(stdout);

    std::vector<int32_t> custkey(total);
    std::vector<char>    name(total * 26);
    std::vector<uint8_t> mktseg(total);

    size_t off = 0;
    for (auto& b : bufs) {
        size_t n = b.ck.size();
        memcpy(&custkey[off],  b.ck.data(),   n*4);
        memcpy(&name[off*26],  b.name.data(), n*26);
        memcpy(&mktseg[off],   b.mkt.data(),  n);
        off += n;
        Buf empty; b = std::move(empty);
    }

    std::string cd = out_dir + "/customer";
    fs::create_directories(cd);
    parallel_run(
        [&]() { write_col(custkey, cd+"/c_custkey.bin"); },
        [&, tot=total]() { write_raw(name.data(), tot*26, cd+"/c_name.bin"); },
        [&]() { write_col(mktseg,  cd+"/c_mktsegment.bin"); }
    );
    // Write dictionary: line index = code value
    FILE* f = fopen((cd+"/c_mktsegment_dict.txt").c_str(), "w");
    fprintf(f, "BUILDING\nAUTOMOBILE\nMACHINERY\nHOUSEHOLD\nFURNITURE\n"); fclose(f);
    printf("[customer] done: %zu rows\n", total); fflush(stdout);
}

// ============================================================
// PART
// Fields: partkey|name|mfgr|brand|type|size|container|retailprice|comment
// ============================================================
static void ingest_part(const std::string& data_dir,
                        const std::string& out_dir, int nthreads) {
    printf("[part] starting...\n"); fflush(stdout);
    auto [data, fsize] = mmap_file(data_dir + "/part.tbl");
    auto bounds = find_boundaries(data, fsize, nthreads);

    struct Buf {
        std::vector<int32_t> pk;
        std::vector<char>    name; // stride 56
    };
    std::vector<Buf> bufs(nthreads);
    size_t est = 2010000 / nthreads + 1000;
    for (auto& b : bufs) { b.pk.reserve(est); b.name.reserve(est*56); }

    {
        std::vector<std::thread> threads; threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&bufs, &bounds, data, t]() {
                const char* p   = data + bounds[t];
                const char* end = data + bounds[t+1];
                auto& b = bufs[t];
                char nbuf[56];
                while (p < end) {
                    int32_t pk_ = parse_int_field(p);  // 0 partkey
                    parse_fixed_string(p, nbuf, 56);   // 1 p_name
                    skip_to_newline(p, end);            // 2-8 skip rest
                    b.pk.push_back(pk_);
                    for (int i = 0; i < 56; i++) b.name.push_back(nbuf[i]);
                }
            });
        }
        for (auto& th : threads) th.join();
    }
    munmap((void*)data, fsize);

    size_t total = 0;
    for (auto& b : bufs) total += b.pk.size();
    printf("[part] parsed %zu rows\n", total); fflush(stdout);

    std::vector<int32_t> partkey(total);
    std::vector<char>    name(total * 56);

    size_t off = 0;
    for (auto& b : bufs) {
        size_t n = b.pk.size();
        memcpy(&partkey[off],  b.pk.data(),   n*4);
        memcpy(&name[off*56],  b.name.data(), n*56);
        off += n;
        Buf empty; b = std::move(empty);
    }

    std::string pd = out_dir + "/part";
    fs::create_directories(pd);
    parallel_run(
        [&]() { write_col(partkey, pd+"/p_partkey.bin"); },
        [&, tot=total]() { write_raw(name.data(), tot*56, pd+"/p_name.bin"); }
    );
    printf("[part] done: %zu rows\n", total); fflush(stdout);
}

// ============================================================
// PARTSUPP
// Fields: partkey|suppkey|availqty|supplycost|comment
// ============================================================
static void ingest_partsupp(const std::string& data_dir,
                            const std::string& out_dir, int nthreads) {
    printf("[partsupp] starting...\n"); fflush(stdout);
    auto [data, fsize] = mmap_file(data_dir + "/partsupp.tbl");
    auto bounds = find_boundaries(data, fsize, nthreads);

    struct Buf {
        std::vector<int32_t> pk, sk;
        std::vector<double>  sc;
    };
    std::vector<Buf> bufs(nthreads);
    size_t est = 8010000 / nthreads + 1000;
    for (auto& b : bufs) { b.pk.reserve(est); b.sk.reserve(est); b.sc.reserve(est); }

    {
        std::vector<std::thread> threads; threads.reserve(nthreads);
        for (int t = 0; t < nthreads; t++) {
            threads.emplace_back([&bufs, &bounds, data, t]() {
                const char* p   = data + bounds[t];
                const char* end = data + bounds[t+1];
                auto& b = bufs[t];
                while (p < end) {
                    int32_t pk_ = parse_int_field(p);    // 0 partkey
                    int32_t sk_ = parse_int_field(p);    // 1 suppkey
                    skip_field(p);                        // 2 availqty
                    double  sc_ = parse_double_field(p); // 3 supplycost
                    skip_to_newline(p, end);              // 4 comment
                    b.pk.push_back(pk_); b.sk.push_back(sk_); b.sc.push_back(sc_);
                }
            });
        }
        for (auto& th : threads) th.join();
    }
    munmap((void*)data, fsize);

    size_t total = 0;
    for (auto& b : bufs) total += b.pk.size();
    printf("[partsupp] parsed %zu rows\n", total); fflush(stdout);

    std::vector<int32_t> partkey(total), suppkey(total);
    std::vector<double>  supplycost(total);

    size_t off = 0;
    for (auto& b : bufs) {
        size_t n = b.pk.size();
        memcpy(&partkey[off],   b.pk.data(), n*4);
        memcpy(&suppkey[off],   b.sk.data(), n*4);
        memcpy(&supplycost[off],b.sc.data(), n*8);
        off += n;
        Buf empty; b = std::move(empty);
    }

    if (!supplycost.empty()) check_decimal(supplycost[0], "ps_supplycost");

    std::string pd = out_dir + "/partsupp";
    fs::create_directories(pd);
    parallel_run(
        [&]() { write_col(partkey,    pd+"/ps_partkey.bin"); },
        [&]() { write_col(suppkey,    pd+"/ps_suppkey.bin"); },
        [&]() { write_col(supplycost, pd+"/ps_supplycost.bin"); }
    );
    printf("[partsupp] done: %zu rows\n", total); fflush(stdout);
}

// ============================================================
// SUPPLIER
// Fields: suppkey|name|address|nationkey|phone|acctbal|comment
// ============================================================
static void ingest_supplier(const std::string& data_dir,
                            const std::string& out_dir) {
    printf("[supplier] starting...\n"); fflush(stdout);
    auto [data, fsize] = mmap_file(data_dir + "/supplier.tbl");

    std::vector<int32_t> suppkey, nationkey;
    suppkey.reserve(100000); nationkey.reserve(100000);

    const char* p   = data;
    const char* end = data + fsize;
    while (p < end) {
        int32_t sk = parse_int_field(p); // 0 suppkey
        skip_field(p);                   // 1 name
        skip_field(p);                   // 2 address
        int32_t nk = parse_int_field(p); // 3 nationkey
        skip_to_newline(p, end);         // 4-6 skip rest
        suppkey.push_back(sk); nationkey.push_back(nk);
    }
    munmap((void*)data, fsize);

    std::string sd = out_dir + "/supplier";
    fs::create_directories(sd);
    parallel_run(
        [&]() { write_col(suppkey,   sd+"/s_suppkey.bin"); },
        [&]() { write_col(nationkey, sd+"/s_nationkey.bin"); }
    );
    printf("[supplier] done: %zu rows\n", suppkey.size()); fflush(stdout);
}

// ============================================================
// NATION
// Fields: nationkey|name|regionkey|comment
// ============================================================
static void ingest_nation(const std::string& data_dir,
                          const std::string& out_dir) {
    printf("[nation] starting...\n"); fflush(stdout);
    auto [data, fsize] = mmap_file(data_dir + "/nation.tbl");

    std::vector<int32_t> nationkey;
    std::vector<char>    name;
    nationkey.reserve(30); name.reserve(30*26);

    const char* p   = data;
    const char* end = data + fsize;
    char nbuf[26];
    while (p < end) {
        int32_t nk = parse_int_field(p);  // 0 nationkey
        parse_fixed_string(p, nbuf, 26);  // 1 n_name (up to 25 chars)
        skip_to_newline(p, end);          // 2-3 skip rest
        nationkey.push_back(nk);
        for (int i = 0; i < 26; i++) name.push_back(nbuf[i]);
    }
    munmap((void*)data, fsize);

    std::string nd = out_dir + "/nation";
    fs::create_directories(nd);
    size_t nr = nationkey.size();
    parallel_run(
        [&]() { write_col(nationkey, nd+"/n_nationkey.bin"); },
        [&, nr]() { write_raw(name.data(), nr*26, nd+"/n_name.bin"); }
    );
    printf("[nation] done: %zu rows\n", nr); fflush(stdout);
}

// ============================================================
// MAIN — sequential table processing (optimal for HDD)
// ============================================================
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <data_dir> <out_dir>\n", argv[0]);
        return 1;
    }
    init_date_tables();
    // Self-test: 1970-01-01 must be epoch day 0
    assert(date_to_epoch(1970, 1, 1) == 0 && "Date epoch self-test failed!");
    assert(date_to_epoch(1970, 1, 2) == 1);
    assert(date_to_epoch(1970, 2, 1) == 31);
    assert(date_to_epoch(1992, 1, 1) > 3000 && "TPC-H date range self-test failed!");

    std::string data_dir = argv[1];
    std::string out_dir  = argv[2];

    int nthreads = (int)std::thread::hardware_concurrency();
    if (nthreads <= 0) nthreads = 8;
    printf("Using %d threads per table\n", nthreads); fflush(stdout);

    fs::create_directories(out_dir);
    fs::create_directories(out_dir + "/indexes");

    // Process tables sequentially (HDD-optimal: avoids random seeks between files).
    // Each table fully processed (parse + sort + write) before the next begins.
    ingest_nation(data_dir, out_dir);
    ingest_supplier(data_dir, out_dir);
    ingest_customer(data_dir, out_dir, std::min(nthreads, 16));
    ingest_part(data_dir, out_dir, std::min(nthreads, 16));
    ingest_partsupp(data_dir, out_dir, std::min(nthreads, 32));
    ingest_orders(data_dir, out_dir, std::min(nthreads, 32));
    ingest_lineitem(data_dir, out_dir, nthreads);

    printf("\nAll tables ingested successfully.\n");
    return 0;
}
