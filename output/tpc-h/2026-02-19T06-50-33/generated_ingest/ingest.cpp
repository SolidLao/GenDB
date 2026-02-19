// ingest.cpp — TPC-H Binary Columnar Ingestion
// Compiles with: g++ -O2 -std=c++17 -Wall -lpthread
// Usage: ./ingest <src_dir> <dst_dir>
//
// Layout: <dst_dir>/<table>/<col>.bin
// Sorted: lineitem by l_shipdate, orders by o_orderdate
// Dictionary-encoded: l_returnflag, l_linestatus, c_mktsegment,
//   o_orderstatus, p_mfgr, p_brand, l_shipinstruct, l_shipmode

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <numeric>

namespace fs = std::filesystem;

// ─────────────────────────────────────────────────────────────────────────────
// Date parsing: days since 1970-01-01
// Self-test: parse_date("1970-01-01") == 0
// ─────────────────────────────────────────────────────────────────────────────
static inline int32_t parse_date(const char* s) {
    int year  = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int month = (s[5]-'0')*10 + (s[6]-'0');
    int day   = (s[8]-'0')*10 + (s[9]-'0');
    static const int dm[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = (year%4==0 && year%100!=0) || (year%400==0);
    int y1 = year - 1;
    // leap years from 1970 to year-1: formula using 1969 reference values
    int leaps = (y1/4 - 492) - (y1/100 - 19) + (y1/400 - 4);
    int32_t d = 365*(year-1970) + leaps;
    for (int m = 1; m < month; m++) d += dm[m] + (m==2 && leap ? 1 : 0);
    d += day - 1;
    return d;
}

// ─────────────────────────────────────────────────────────────────────────────
// Field parsers (advance pointer past delimiter '|' or '\n')
// ─────────────────────────────────────────────────────────────────────────────
static inline int32_t parse_int(const char*& p) {
    bool neg = (*p == '-'); if (neg) p++;
    int32_t v = 0;
    while ((unsigned)(*p - '0') < 10u) v = v*10 + (*p++ - '0');
    if (*p == '|' || *p == '\n') p++;
    return neg ? -v : v;
}

static inline double parse_double(const char*& p) {
    bool neg = (*p == '-'); if (neg) p++;
    int64_t whole = 0;
    while ((unsigned)(*p - '0') < 10u) whole = whole*10 + (*p++ - '0');
    double v = (double)whole;
    if (*p == '.') {
        p++;
        double frac = 0.1;
        while ((unsigned)(*p - '0') < 10u) { v += (*p++ - '0') * frac; frac *= 0.1; }
    }
    if (*p == '|' || *p == '\n') p++;
    return neg ? -v : v;
}

static inline int32_t parse_date_field(const char*& p) {
    int32_t v = parse_date(p);
    p += 10; // YYYY-MM-DD
    if (*p == '|' || *p == '\n') p++;
    return v;
}

static inline void skip_field(const char*& p) {
    while (*p != '|' && *p != '\n' && *p != '\0') p++;
    if (*p == '|') p++;
}

// Copy fixed-width string, null-pad to N bytes
template<int N>
static inline void copy_str_field(const char*& p, char* dst) {
    int i = 0;
    while (*p != '|' && *p != '\n' && *p != '\0' && i < N-1) dst[i++] = *p++;
    while (i < N) dst[i++] = '\0';
    if (*p == '|') p++;
}

// ─────────────────────────────────────────────────────────────────────────────
// Dictionary encoders (hardcoded TPC-H values)
// ─────────────────────────────────────────────────────────────────────────────
static inline int8_t encode_returnflag(const char*& p) {
    // A=0, N=1, R=2
    char c = *p++;
    if (*p == '|' || *p == '\n') p++;
    return (c == 'A') ? 0 : (c == 'N') ? 1 : 2;
}

static inline int8_t encode_linestatus(const char*& p) {
    // F=0, O=1
    char c = *p++;
    if (*p == '|' || *p == '\n') p++;
    return (c == 'F') ? 0 : 1;
}

static inline int8_t encode_orderstatus(const char*& p) {
    // F=0, O=1, P=2
    char c = *p++;
    if (*p == '|' || *p == '\n') p++;
    return (c == 'F') ? 0 : (c == 'O') ? 1 : 2;
}

static inline int8_t encode_mktsegment(const char*& p) {
    // AUTOMOBILE=0, BUILDING=1, FURNITURE=2, HOUSEHOLD=3, MACHINERY=4
    int8_t code = 0;
    char buf[16]; int len = 0;
    while (*p != '|' && *p != '\n' && *p != '\0' && len < 15) buf[len++] = *p++;
    buf[len] = '\0';
    if (*p == '|') p++;
    if (buf[0] == 'A') code = 0;
    else if (buf[0] == 'B') code = 1;
    else if (buf[0] == 'F') code = 2;
    else if (buf[0] == 'H') code = 3;
    else code = 4; // MACHINERY
    return code;
}

static inline int8_t encode_shipinstruct(const char*& p) {
    // COLLECT COD=0, DELIVER IN PERSON=1, NONE=2, TAKE BACK RETURN=3
    int8_t code = 2;
    if (*p == 'C') code = 0;
    else if (*p == 'D') code = 1;
    else if (*p == 'N') code = 2;
    else if (*p == 'T') code = 3;
    while (*p != '|' && *p != '\n' && *p != '\0') p++;
    if (*p == '|') p++;
    return code;
}

static inline int8_t encode_shipmode(const char*& p) {
    // AIR=0, FOB=1, MAIL=2, RAIL=3, REG AIR=4, SHIP=5, TRUCK=6
    int8_t code = 0;
    char buf[12]; int len = 0;
    while (*p != '|' && *p != '\n' && *p != '\0' && len < 11) buf[len++] = *p++;
    buf[len] = '\0';
    if (*p == '|') p++;
    if (len >= 2) {
        if (buf[0]=='A') code = 0;           // AIR
        else if (buf[0]=='F') code = 1;      // FOB
        else if (buf[0]=='M') code = 2;      // MAIL
        else if (buf[0]=='R' && buf[1]=='A') code = 3; // RAIL
        else if (buf[0]=='R' && buf[1]=='E') code = 4; // REG AIR
        else if (buf[0]=='S') code = 5;      // SHIP
        else if (buf[0]=='T') code = 6;      // TRUCK
    }
    return code;
}

static inline int8_t encode_mfgr(const char*& p) {
    // Manufacturer#1=0 .. Manufacturer#5=4
    // Skip to last char before '|'
    while (*p != '#' && *p != '|' && *p != '\n') p++;
    int8_t code = 0;
    if (*p == '#') { p++; code = (*p - '1'); p++; }
    if (*p == '|' || *p == '\n') p++;
    return code;
}

static inline int8_t encode_brand(const char*& p) {
    // Brand#XY -> code = (X-1)*5 + (Y-1), range 0..24
    while (*p != '#' && *p != '|' && *p != '\n') p++;
    int8_t code = 0;
    if (*p == '#') {
        p++;
        int x = *p++ - '1';
        int y = *p++ - '1';
        code = (int8_t)(x * 5 + y);
    }
    if (*p == '|' || *p == '\n') p++;
    return code;
}

// ─────────────────────────────────────────────────────────────────────────────
// mmap helper
// ─────────────────────────────────────────────────────────────────────────────
struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;
    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); return false; }
        struct stat st; fstat(fd, &st); size = st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { ::close(fd); fd=-1; return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) ::close(fd);
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// Buffered column writer
// ─────────────────────────────────────────────────────────────────────────────
static void write_col(const std::string& path, const void* data, size_t elem_sz, size_t n) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); exit(1); }
    const size_t BUF = 4*1024*1024;
    setvbuf(f, nullptr, _IOFBF, BUF);
    fwrite(data, elem_sz, n, f);
    fclose(f);
}

template<typename T>
static void write_col_perm(const std::string& path, const T* data, const uint32_t* perm, size_t n) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); exit(1); }
    const size_t BUF = 1*1024*1024 / sizeof(T);
    std::vector<T> buf; buf.reserve(BUF);
    for (size_t i = 0; i < n; i++) {
        buf.push_back(data[perm[i]]);
        if (buf.size() == BUF) { fwrite(buf.data(), sizeof(T), BUF, f); buf.clear(); }
    }
    if (!buf.empty()) fwrite(buf.data(), sizeof(T), buf.size(), f);
    fclose(f);
}

static void write_str_perm(const std::string& path, const char* data, int str_len,
                            const uint32_t* perm, size_t n) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot write %s\n", path.c_str()); exit(1); }
    const size_t BUF_ROWS = 65536;
    std::vector<char> buf(BUF_ROWS * str_len);
    size_t brows = 0;
    for (size_t i = 0; i < n; i++) {
        memcpy(buf.data() + brows * str_len, data + (size_t)perm[i] * str_len, str_len);
        if (++brows == BUF_ROWS) { fwrite(buf.data(), str_len, BUF_ROWS, f); brows = 0; }
    }
    if (brows > 0) fwrite(buf.data(), str_len, brows, f);
    fclose(f);
}

// Write dictionary file
static void write_dict(const std::string& path, const std::vector<std::string>& dict) {
    FILE* f = fopen(path.c_str(), "w");
    if (!f) { fprintf(stderr, "Cannot write dict %s\n", path.c_str()); exit(1); }
    for (auto& s : dict) fprintf(f, "%s\n", s.c_str());
    fclose(f);
}

// ─────────────────────────────────────────────────────────────────────────────
// Chunk boundary finder for parallel parsing
// ─────────────────────────────────────────────────────────────────────────────
static std::vector<size_t> find_chunk_starts(const char* data, size_t size, int nchunks) {
    std::vector<size_t> starts(nchunks + 1);
    starts[0] = 0;
    for (int i = 1; i < nchunks; i++) {
        size_t pos = (size_t)i * (size / nchunks);
        while (pos < size && data[pos] != '\n') pos++;
        starts[i] = (pos < size) ? pos + 1 : size;
    }
    starts[nchunks] = size;
    return starts;
}

static size_t count_newlines(const char* data, size_t from, size_t to) {
    size_t n = 0;
    for (size_t i = from; i < to; i++) if (data[i] == '\n') n++;
    return n;
}

// Returns (chunk_byte_starts, chunk_row_starts, total_rows)
static std::tuple<std::vector<size_t>, std::vector<size_t>, size_t>
partition_chunks(const char* data, size_t size, int nchunks) {
    auto bstarts = find_chunk_starts(data, size, nchunks);
    std::vector<size_t> row_counts(nchunks);
    std::vector<std::thread> threads;
    for (int t = 0; t < nchunks; t++) {
        threads.emplace_back([&, t]() {
            row_counts[t] = count_newlines(data, bstarts[t], bstarts[t+1]);
        });
    }
    for (auto& th : threads) th.join();
    std::vector<size_t> rstarts(nchunks + 1, 0);
    for (int t = 0; t < nchunks; t++) rstarts[t+1] = rstarts[t] + row_counts[t];
    return {bstarts, rstarts, rstarts[nchunks]};
}

// ─────────────────────────────────────────────────────────────────────────────
// LINEITEM ingestion
// ─────────────────────────────────────────────────────────────────────────────
void ingest_lineitem(const std::string& src, const std::string& dst) {
    printf("[lineitem] starting...\n"); fflush(stdout);
    MmapFile mf; mf.open(src + "/lineitem.tbl");

    const int NP = 32;
    auto [bstarts, rstarts, N] = partition_chunks(mf.data, mf.size, NP);
    printf("[lineitem] %zu rows\n", N); fflush(stdout);

    // Allocate columns
    std::vector<int32_t> orderkey(N), partkey(N), suppkey(N), linenumber(N);
    std::vector<double>  quantity(N), extprice(N), discount(N), tax(N);
    std::vector<int8_t>  returnflag(N), linestatus(N), shipinstruct(N), shipmode(N);
    std::vector<int32_t> shipdate(N), commitdate(N), receiptdate(N);
    std::vector<char>    comment(N * 45);

    // Parallel parse
    std::vector<std::thread> threads;
    for (int t = 0; t < NP; t++) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + bstarts[t];
            const char* end = mf.data + bstarts[t+1];
            size_t row = rstarts[t];
            while (p < end && *p != '\0') {
                if (*p == '\n') { p++; continue; }
                orderkey[row]     = parse_int(p);
                partkey[row]      = parse_int(p);
                suppkey[row]      = parse_int(p);
                linenumber[row]   = parse_int(p);
                quantity[row]     = parse_double(p);
                extprice[row]     = parse_double(p);
                discount[row]     = parse_double(p);
                tax[row]          = parse_double(p);
                returnflag[row]   = encode_returnflag(p);
                linestatus[row]   = encode_linestatus(p);
                shipdate[row]     = parse_date_field(p);
                commitdate[row]   = parse_date_field(p);
                receiptdate[row]  = parse_date_field(p);
                shipinstruct[row] = encode_shipinstruct(p);
                shipmode[row]     = encode_shipmode(p);
                copy_str_field<45>(p, comment.data() + row * 45);
                // skip trailing newline
                while (*p == '\n') p++;
                row++;
            }
        });
    }
    for (auto& th : threads) th.join();
    printf("[lineitem] parsed, sorting by l_shipdate...\n"); fflush(stdout);

    // Sort by shipdate
    std::vector<uint32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b) {
        return shipdate[a] < shipdate[b];
    });
    printf("[lineitem] sorted, writing columns...\n"); fflush(stdout);

    // Write columns in parallel
    std::string tdir = dst + "/lineitem/";
    std::vector<std::thread> wthreads;
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_orderkey.bin", orderkey.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_partkey.bin", partkey.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_suppkey.bin", suppkey.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_linenumber.bin", linenumber.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_quantity.bin", quantity.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_extendedprice.bin", extprice.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_discount.bin", discount.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_tax.bin", tax.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_returnflag.bin", returnflag.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_linestatus.bin", linestatus.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_shipdate.bin", shipdate.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_commitdate.bin", commitdate.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_receiptdate.bin", receiptdate.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_shipinstruct.bin", shipinstruct.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"l_shipmode.bin", shipmode.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_str_perm(tdir+"l_comment.bin", comment.data(), 45, perm.data(), N); });
    for (auto& th : wthreads) th.join();

    // Write dictionaries
    write_dict(tdir+"l_returnflag_dict.txt", {"A","N","R"});
    write_dict(tdir+"l_linestatus_dict.txt", {"F","O"});
    write_dict(tdir+"l_shipinstruct_dict.txt", {"COLLECT COD","DELIVER IN PERSON","NONE","TAKE BACK RETURN"});
    write_dict(tdir+"l_shipmode_dict.txt", {"AIR","FOB","MAIL","RAIL","REG AIR","SHIP","TRUCK"});

    printf("[lineitem] done. %zu rows written.\n", N); fflush(stdout);
}

// ─────────────────────────────────────────────────────────────────────────────
// ORDERS ingestion
// ─────────────────────────────────────────────────────────────────────────────
void ingest_orders(const std::string& src, const std::string& dst) {
    printf("[orders] starting...\n"); fflush(stdout);
    MmapFile mf; mf.open(src + "/orders.tbl");

    const int NP = 16;
    auto [bstarts, rstarts, N] = partition_chunks(mf.data, mf.size, NP);
    printf("[orders] %zu rows\n", N); fflush(stdout);

    std::vector<int32_t> orderkey(N), custkey(N), shippriority(N), orderdate(N);
    std::vector<double>  totalprice(N);
    std::vector<int8_t>  orderstatus(N);
    std::vector<char>    orderpriority(N * 16), clerk(N * 16), comment(N * 80);

    std::vector<std::thread> threads;
    for (int t = 0; t < NP; t++) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + bstarts[t];
            const char* end = mf.data + bstarts[t+1];
            size_t row = rstarts[t];
            while (p < end && *p != '\0') {
                if (*p == '\n') { p++; continue; }
                orderkey[row]      = parse_int(p);
                custkey[row]       = parse_int(p);
                orderstatus[row]   = encode_orderstatus(p);
                totalprice[row]    = parse_double(p);
                orderdate[row]     = parse_date_field(p);
                copy_str_field<16>(p, orderpriority.data() + row*16);
                copy_str_field<16>(p, clerk.data() + row*16);
                shippriority[row]  = parse_int(p);
                copy_str_field<80>(p, comment.data() + row*80);
                while (*p == '\n') p++;
                row++;
            }
        });
    }
    for (auto& th : threads) th.join();
    printf("[orders] parsed, sorting by o_orderdate...\n"); fflush(stdout);

    std::vector<uint32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b) {
        return orderdate[a] < orderdate[b];
    });
    printf("[orders] sorted, writing...\n"); fflush(stdout);

    std::string tdir = dst + "/orders/";
    std::vector<std::thread> wthreads;
    wthreads.emplace_back([&](){ write_col_perm(tdir+"o_orderkey.bin", orderkey.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"o_custkey.bin", custkey.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"o_orderstatus.bin", orderstatus.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"o_totalprice.bin", totalprice.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"o_orderdate.bin", orderdate.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_str_perm(tdir+"o_orderpriority.bin", orderpriority.data(), 16, perm.data(), N); });
    wthreads.emplace_back([&](){ write_str_perm(tdir+"o_clerk.bin", clerk.data(), 16, perm.data(), N); });
    wthreads.emplace_back([&](){ write_col_perm(tdir+"o_shippriority.bin", shippriority.data(), perm.data(), N); });
    wthreads.emplace_back([&](){ write_str_perm(tdir+"o_comment.bin", comment.data(), 80, perm.data(), N); });
    for (auto& th : wthreads) th.join();

    write_dict(tdir+"o_orderstatus_dict.txt", {"F","O","P"});
    printf("[orders] done. %zu rows.\n", N); fflush(stdout);
}

// ─────────────────────────────────────────────────────────────────────────────
// CUSTOMER ingestion
// ─────────────────────────────────────────────────────────────────────────────
void ingest_customer(const std::string& src, const std::string& dst) {
    printf("[customer] starting...\n"); fflush(stdout);
    MmapFile mf; mf.open(src + "/customer.tbl");

    const int NP = 8;
    auto [bstarts, rstarts, N] = partition_chunks(mf.data, mf.size, NP);
    printf("[customer] %zu rows\n", N); fflush(stdout);

    std::vector<int32_t> custkey(N), nationkey(N);
    std::vector<double>  acctbal(N);
    std::vector<int8_t>  mktsegment(N);
    std::vector<char>    name(N*26), address(N*41), phone(N*16), comment(N*118);

    std::vector<std::thread> threads;
    for (int t = 0; t < NP; t++) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + bstarts[t];
            const char* end = mf.data + bstarts[t+1];
            size_t row = rstarts[t];
            while (p < end && *p != '\0') {
                if (*p == '\n') { p++; continue; }
                custkey[row]    = parse_int(p);
                copy_str_field<26>(p, name.data() + row*26);
                copy_str_field<41>(p, address.data() + row*41);
                nationkey[row]  = parse_int(p);
                copy_str_field<16>(p, phone.data() + row*16);
                acctbal[row]    = parse_double(p);
                mktsegment[row] = encode_mktsegment(p);
                copy_str_field<118>(p, comment.data() + row*118);
                while (*p == '\n') p++;
                row++;
            }
        });
    }
    for (auto& th : threads) th.join();

    std::string tdir = dst + "/customer/";
    std::vector<std::thread> wthreads;
    wthreads.emplace_back([&](){ write_col(tdir+"c_custkey.bin", custkey.data(), 4, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"c_name.bin", name.data(), 26, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"c_address.bin", address.data(), 41, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"c_nationkey.bin", nationkey.data(), 4, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"c_phone.bin", phone.data(), 16, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"c_acctbal.bin", acctbal.data(), 8, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"c_mktsegment.bin", mktsegment.data(), 1, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"c_comment.bin", comment.data(), 118, N); });
    for (auto& th : wthreads) th.join();

    write_dict(tdir+"c_mktsegment_dict.txt", {"AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"});
    printf("[customer] done. %zu rows.\n", N); fflush(stdout);
}

// ─────────────────────────────────────────────────────────────────────────────
// PART ingestion
// ─────────────────────────────────────────────────────────────────────────────
void ingest_part(const std::string& src, const std::string& dst) {
    printf("[part] starting...\n"); fflush(stdout);
    MmapFile mf; mf.open(src + "/part.tbl");

    const int NP = 8;
    auto [bstarts, rstarts, N] = partition_chunks(mf.data, mf.size, NP);
    printf("[part] %zu rows\n", N); fflush(stdout);

    std::vector<int32_t> partkey(N), size_col(N);
    std::vector<double>  retailprice(N);
    std::vector<int8_t>  mfgr(N), brand(N);
    std::vector<char>    name(N*56), type(N*26), container(N*11), comment(N*24);

    std::vector<std::thread> threads;
    for (int t = 0; t < NP; t++) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + bstarts[t];
            const char* end = mf.data + bstarts[t+1];
            size_t row = rstarts[t];
            while (p < end && *p != '\0') {
                if (*p == '\n') { p++; continue; }
                partkey[row]    = parse_int(p);
                copy_str_field<56>(p, name.data() + row*56);
                mfgr[row]       = encode_mfgr(p);
                brand[row]      = encode_brand(p);
                copy_str_field<26>(p, type.data() + row*26);
                size_col[row]   = parse_int(p);
                copy_str_field<11>(p, container.data() + row*11);
                retailprice[row]= parse_double(p);
                copy_str_field<24>(p, comment.data() + row*24);
                while (*p == '\n') p++;
                row++;
            }
        });
    }
    for (auto& th : threads) th.join();

    std::string tdir = dst + "/part/";
    std::vector<std::thread> wthreads;
    wthreads.emplace_back([&](){ write_col(tdir+"p_partkey.bin", partkey.data(), 4, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"p_name.bin", name.data(), 56, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"p_mfgr.bin", mfgr.data(), 1, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"p_brand.bin", brand.data(), 1, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"p_type.bin", type.data(), 26, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"p_size.bin", size_col.data(), 4, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"p_container.bin", container.data(), 11, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"p_retailprice.bin", retailprice.data(), 8, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"p_comment.bin", comment.data(), 24, N); });
    for (auto& th : wthreads) th.join();

    // Write p_mfgr dict
    write_dict(tdir+"p_mfgr_dict.txt",
        {"Manufacturer#1","Manufacturer#2","Manufacturer#3","Manufacturer#4","Manufacturer#5"});
    std::vector<std::string> brand_dict;
    for (int x = 1; x <= 5; x++)
        for (int y = 1; y <= 5; y++)
            brand_dict.push_back("Brand#" + std::to_string(x) + std::to_string(y));
    write_dict(tdir+"p_brand_dict.txt", brand_dict);

    printf("[part] done. %zu rows.\n", N); fflush(stdout);
}

// ─────────────────────────────────────────────────────────────────────────────
// PARTSUPP ingestion
// ─────────────────────────────────────────────────────────────────────────────
void ingest_partsupp(const std::string& src, const std::string& dst) {
    printf("[partsupp] starting...\n"); fflush(stdout);
    MmapFile mf; mf.open(src + "/partsupp.tbl");

    const int NP = 16;
    auto [bstarts, rstarts, N] = partition_chunks(mf.data, mf.size, NP);
    printf("[partsupp] %zu rows\n", N); fflush(stdout);

    std::vector<int32_t> partkey(N), suppkey(N), availqty(N);
    std::vector<double>  supplycost(N);
    std::vector<char>    comment(N * 200);

    std::vector<std::thread> threads;
    for (int t = 0; t < NP; t++) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + bstarts[t];
            const char* end = mf.data + bstarts[t+1];
            size_t row = rstarts[t];
            while (p < end && *p != '\0') {
                if (*p == '\n') { p++; continue; }
                partkey[row]    = parse_int(p);
                suppkey[row]    = parse_int(p);
                availqty[row]   = parse_int(p);
                supplycost[row] = parse_double(p);
                copy_str_field<200>(p, comment.data() + row*200);
                while (*p == '\n') p++;
                row++;
            }
        });
    }
    for (auto& th : threads) th.join();

    std::string tdir = dst + "/partsupp/";
    std::vector<std::thread> wthreads;
    wthreads.emplace_back([&](){ write_col(tdir+"ps_partkey.bin", partkey.data(), 4, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"ps_suppkey.bin", suppkey.data(), 4, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"ps_availqty.bin", availqty.data(), 4, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"ps_supplycost.bin", supplycost.data(), 8, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"ps_comment.bin", comment.data(), 200, N); });
    for (auto& th : wthreads) th.join();

    printf("[partsupp] done. %zu rows.\n", N); fflush(stdout);
}

// ─────────────────────────────────────────────────────────────────────────────
// SUPPLIER ingestion
// ─────────────────────────────────────────────────────────────────────────────
void ingest_supplier(const std::string& src, const std::string& dst) {
    printf("[supplier] starting...\n"); fflush(stdout);
    MmapFile mf; mf.open(src + "/supplier.tbl");

    const char* p = mf.data; const char* end = mf.data + mf.size;
    std::vector<int32_t> suppkey(200000), nationkey(200000);
    std::vector<double>  acctbal(200000);
    std::vector<char>    name(200000*26), address(200000*41), phone(200000*16), comment(200000*102);
    size_t N = 0;
    while (p < end && *p != '\0') {
        if (*p == '\n') { p++; continue; }
        suppkey[N]    = parse_int(p);
        copy_str_field<26>(p, name.data() + N*26);
        copy_str_field<41>(p, address.data() + N*41);
        nationkey[N]  = parse_int(p);
        copy_str_field<16>(p, phone.data() + N*16);
        acctbal[N]    = parse_double(p);
        copy_str_field<102>(p, comment.data() + N*102);
        while (*p == '\n') p++;
        N++;
    }
    std::string tdir = dst + "/supplier/";
    std::vector<std::thread> wthreads;
    wthreads.emplace_back([&](){ write_col(tdir+"s_suppkey.bin", suppkey.data(), 4, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"s_name.bin", name.data(), 26, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"s_address.bin", address.data(), 41, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"s_nationkey.bin", nationkey.data(), 4, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"s_phone.bin", phone.data(), 16, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"s_acctbal.bin", acctbal.data(), 8, N); });
    wthreads.emplace_back([&](){ write_col(tdir+"s_comment.bin", comment.data(), 102, N); });
    for (auto& th : wthreads) th.join();
    printf("[supplier] done. %zu rows.\n", N); fflush(stdout);
}

// ─────────────────────────────────────────────────────────────────────────────
// NATION ingestion
// ─────────────────────────────────────────────────────────────────────────────
void ingest_nation(const std::string& src, const std::string& dst) {
    MmapFile mf; mf.open(src + "/nation.tbl");
    const char* p = mf.data; const char* end = mf.data + mf.size;
    std::vector<int32_t> nationkey(30), regionkey(30);
    std::vector<char>    name(30*26), comment(30*153);
    size_t N = 0;
    while (p < end && *p != '\0') {
        if (*p == '\n') { p++; continue; }
        nationkey[N] = parse_int(p);
        copy_str_field<26>(p, name.data() + N*26);
        regionkey[N] = parse_int(p);
        copy_str_field<153>(p, comment.data() + N*153);
        while (*p == '\n') p++;
        N++;
    }
    std::string tdir = dst + "/nation/";
    write_col(tdir+"n_nationkey.bin", nationkey.data(), 4, N);
    write_col(tdir+"n_name.bin", name.data(), 26, N);
    write_col(tdir+"n_regionkey.bin", regionkey.data(), 4, N);
    write_col(tdir+"n_comment.bin", comment.data(), 153, N);
    printf("[nation] done. %zu rows.\n", N); fflush(stdout);
}

// ─────────────────────────────────────────────────────────────────────────────
// REGION ingestion
// ─────────────────────────────────────────────────────────────────────────────
void ingest_region(const std::string& src, const std::string& dst) {
    MmapFile mf; mf.open(src + "/region.tbl");
    const char* p = mf.data; const char* end = mf.data + mf.size;
    std::vector<int32_t> regionkey(10);
    std::vector<char>    name(10*26), comment(10*153);
    size_t N = 0;
    while (p < end && *p != '\0') {
        if (*p == '\n') { p++; continue; }
        regionkey[N] = parse_int(p);
        copy_str_field<26>(p, name.data() + N*26);
        copy_str_field<153>(p, comment.data() + N*153);
        while (*p == '\n') p++;
        N++;
    }
    std::string tdir = dst + "/region/";
    write_col(tdir+"r_regionkey.bin", regionkey.data(), 4, N);
    write_col(tdir+"r_name.bin", name.data(), 26, N);
    write_col(tdir+"r_comment.bin", comment.data(), 153, N);
    printf("[region] done. %zu rows.\n", N); fflush(stdout);
}

// ─────────────────────────────────────────────────────────────────────────────
// Post-ingestion verification
// ─────────────────────────────────────────────────────────────────────────────
static void verify_date_col(const std::string& path, int32_t min_expected) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { fprintf(stderr, "VERIFY FAIL: cannot open %s\n", path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t n = st.st_size / 4;
    const int32_t* data = (const int32_t*)mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    ::close(fd);
    int32_t sample = data[n/2]; // sample middle row
    munmap((void*)data, st.st_size);
    if (sample <= min_expected) {
        fprintf(stderr, "VERIFY FAIL: %s sample=%d <= threshold=%d\n", path.c_str(), sample, min_expected);
        exit(1);
    }
    printf("  VERIFY OK: %s sample=%d\n", path.c_str(), sample);
}

static void verify_double_col(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) { fprintf(stderr, "VERIFY FAIL: cannot open %s\n", path.c_str()); exit(1); }
    struct stat st; fstat(fd, &st);
    size_t n = st.st_size / 8;
    const double* data = (const double*)mmap(nullptr, st.st_size, PROT_READ, MAP_SHARED, fd, 0);
    ::close(fd);
    double sample = data[n/2];
    munmap((void*)data, st.st_size);
    if (sample == 0.0) {
        fprintf(stderr, "VERIFY FAIL: %s sample=0.0\n", path.c_str());
        exit(1);
    }
    printf("  VERIFY OK: %s sample=%.4f\n", path.c_str(), sample);
}

// ─────────────────────────────────────────────────────────────────────────────
// Main
// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <src_dir> <dst_dir>\n", argv[0]);
        return 1;
    }
    std::string src = argv[1];
    std::string dst = argv[2];

    // Self-test date parsing
    assert(parse_date("1970-01-01") == 0 && "Date epoch parse FAILED");
    assert(parse_date("1970-01-02") == 1 && "Date epoch parse FAILED");
    assert(parse_date("1970-02-01") == 31 && "Date epoch parse FAILED");
    printf("Date parse self-test PASSED\n");

    // Create directories
    for (auto& t : {"lineitem","orders","customer","part","partsupp","supplier","nation","region"}) {
        fs::create_directories(dst + "/" + t);
    }

    // Ingest all tables in parallel
    printf("=== Starting parallel ingestion ===\n"); fflush(stdout);
    auto t0 = std::chrono::steady_clock::now();

    // Large tables run concurrently
    std::thread tli([&](){ ingest_lineitem(src, dst); });
    std::thread tor([&](){ ingest_orders(src, dst); });
    std::thread tps([&](){ ingest_partsupp(src, dst); });
    std::thread tpa([&](){ ingest_part(src, dst); });
    std::thread tcu([&](){ ingest_customer(src, dst); });
    std::thread tsu([&](){ ingest_supplier(src, dst); });
    std::thread tna([&](){ ingest_nation(src, dst); });
    std::thread tre([&](){ ingest_region(src, dst); });

    tli.join(); tor.join(); tps.join(); tpa.join();
    tcu.join(); tsu.join(); tna.join(); tre.join();

    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    printf("=== Ingestion complete in %.1fs ===\n", elapsed);

    // Post-ingestion verification
    printf("\n=== Post-ingestion verification ===\n");
    verify_date_col(dst + "/lineitem/l_shipdate.bin", 3000);
    verify_date_col(dst + "/orders/o_orderdate.bin", 3000);
    verify_double_col(dst + "/lineitem/l_extendedprice.bin");
    verify_double_col(dst + "/orders/o_totalprice.bin");
    verify_double_col(dst + "/partsupp/ps_supplycost.bin");
    printf("=== All checks PASSED ===\n");

    return 0;
}
