// ingest.cpp
// TPC-H binary columnar ingestion
// Reads .tbl (pipe-delimited, trailing pipe) files and writes binary columns.
// Tables are ingested in parallel (one thread per table).
// lineitem is sorted by l_shipdate after parsing for zone-map support.

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fcntl.h>
#include <numeric>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <parallel/algorithm>

namespace fs = std::filesystem;

// ---------------------------------------------------------------------------
// Date conversion: YYYY-MM-DD -> days since 1970-01-01
// ---------------------------------------------------------------------------
static inline int32_t date_to_days(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10  + (s[6]-'0');
    int d = (s[8]-'0')*10  + (s[9]-'0');
    if (m <= 2) { y--; m += 12; }
    int era = (y >= 0 ? y : y - 399) / 400;
    int yoe = y - era * 400;
    int doy = (153*(m - 3) + 2) / 5 + d - 1;
    int doe = yoe*365 + yoe/4 - yoe/100 + doy;
    return (int32_t)(era * 146097 + doe - 719468);
}

// ---------------------------------------------------------------------------
// Fast field parsers operating on a pointer into a mmap'd buffer.
// After each call, p points to the '|' separator (caller advances past it).
// ---------------------------------------------------------------------------

static inline int32_t parse_int32(const char*& p) {
    int32_t x = 0;
    while (*p >= '0' && *p <= '9') { x = x * 10 + (*p - '0'); ++p; }
    return x;
}

// Parse decimal as double, stopping at '|'
static inline double parse_double(const char*& p) {
    double v = 0.0;
    bool neg = (*p == '-') ? (++p, true) : false;
    while (*p >= '0' && *p <= '9') { v = v * 10.0 + (*p - '0'); ++p; }
    if (*p == '.') {
        ++p;
        double f = 0.1;
        while (*p >= '0' && *p <= '9') { v += (*p - '0') * f; f *= 0.1; ++p; }
    }
    return neg ? -v : v;
}

// Parse integer DECIMAL (e.g. "17" or "17.00") -> int8 (values 1-50)
static inline int8_t parse_int8_integer(const char*& p) {
    int8_t x = 0;
    while (*p >= '0' && *p <= '9') { x = (int8_t)(x * 10 + (*p - '0')); ++p; }
    while (*p != '|' && *p != '\n' && *p != '\r') ++p; // skip .00 if present
    return x;
}

// Parse "0.XX" decimal -> int8 hundredths (discount 0-10, tax 0-8)
static inline int8_t parse_hundredths(const char*& p) {
    // skip integer part (always 0)
    while (*p != '.' && *p != '|' && *p != '\n') ++p;
    int v = 0;
    if (*p == '.') {
        ++p;
        if (*p >= '0') { v  = (*p - '0') * 10; ++p; }
        if (*p >= '0') { v += (*p - '0');       ++p; }
    }
    return (int8_t)v;
}

// Skip to next '|' and consume it; or stop at \n/\r
static inline void skip_field(const char*& p) {
    while (*p != '|' && *p != '\n' && *p != '\r') ++p;
    if (*p == '|') ++p;
}

// Skip to end of line (consuming the \n)
static inline void skip_line(const char*& p) {
    while (*p != '\n' && *p != '\0') ++p;
    if (*p == '\n') ++p;
}

// ---------------------------------------------------------------------------
// MappedFile: RAII wrapper for read-only mmap of a file
// ---------------------------------------------------------------------------
struct MappedFile {
    const char* data = nullptr;
    size_t      size = 0;
    int         fd   = -1;

    explicit MappedFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(("open: " + path).c_str()); return; }
        struct stat sb;
        fstat(fd, &sb);
        size = (size_t)sb.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); data = nullptr; }
        // Hint sequential access
        if (data) madvise((void*)data, size, MADV_SEQUENTIAL);
    }
    ~MappedFile() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
};

// ---------------------------------------------------------------------------
// write_column: write a contiguous vector to a binary file
// ---------------------------------------------------------------------------
template<typename T>
static void write_column(const std::string& path, const std::vector<T>& v) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(("fopen: " + path).c_str()); return; }
    const size_t CHUNK = 1 << 22; // 4 MB chunks
    size_t written = 0;
    while (written < v.size()) {
        size_t n = std::min(CHUNK, v.size() - written);
        fwrite(v.data() + written, sizeof(T), n, f);
        written += n;
    }
    fclose(f);
}

// Write raw fixed-size string array (each element is SZ bytes)
template<size_t SZ>
static void write_fixed_strings(const std::string& path,
                                 const std::vector<std::array<char, SZ>>& v) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(("fopen: " + path).c_str()); return; }
    fwrite(v.data(), SZ, v.size(), f);
    fclose(f);
}

// ---------------------------------------------------------------------------
// Apply permutation perm[] to a vector in-place using a temp buffer
// ---------------------------------------------------------------------------
template<typename T>
static void apply_permutation(std::vector<T>& v, const std::vector<int32_t>& perm) {
    std::vector<T> tmp(v.size());
    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < v.size(); i++)
        tmp[i] = v[perm[i]];
    v = std::move(tmp);
}

template<size_t SZ>
static void apply_permutation_fixed(std::vector<std::array<char, SZ>>& v,
                                     const std::vector<int32_t>& perm) {
    std::vector<std::array<char, SZ>> tmp(v.size());
    #pragma omp parallel for schedule(static)
    for (size_t i = 0; i < v.size(); i++)
        tmp[i] = v[perm[i]];
    v = std::move(tmp);
}

// ---------------------------------------------------------------------------
// LINEITEM ingestion
// ---------------------------------------------------------------------------
static void ingest_lineitem(const std::string& src_dir, const std::string& dst_dir) {
    std::string tbl_path = src_dir + "/lineitem.tbl";
    std::string out_dir  = dst_dir + "/lineitem";
    fs::create_directories(out_dir);

    printf("[lineitem] opening %s\n", tbl_path.c_str()); fflush(stdout);
    MappedFile mf(tbl_path);
    if (!mf.data) return;

    const size_t RESERVE = 61000000;
    std::vector<int32_t>              orderkey;    orderkey.reserve(RESERVE);
    std::vector<int32_t>              partkey;     partkey.reserve(RESERVE);
    std::vector<int32_t>              suppkey;     suppkey.reserve(RESERVE);
    std::vector<int32_t>              linenumber;  linenumber.reserve(RESERVE);
    std::vector<int8_t>               quantity;    quantity.reserve(RESERVE);
    std::vector<double>               extprice;    extprice.reserve(RESERVE);
    std::vector<int8_t>               discount;    discount.reserve(RESERVE);
    std::vector<int8_t>               tax;         tax.reserve(RESERVE);
    std::vector<int8_t>               returnflag;  returnflag.reserve(RESERVE);
    std::vector<int8_t>               linestatus;  linestatus.reserve(RESERVE);
    std::vector<int32_t>              shipdate;    shipdate.reserve(RESERVE);
    std::vector<int32_t>              commitdate;  commitdate.reserve(RESERVE);
    std::vector<int32_t>              receiptdate; receiptdate.reserve(RESERVE);

    // Dict for returnflag: A=0, N=1, R=2
    // Dict for linestatus: F=0, O=1

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;

    printf("[lineitem] parsing...\n"); fflush(stdout);
    while (p < end && *p != '\0') {
        if (*p == '\n' || *p == '\r') { ++p; continue; }

        // col1: l_orderkey
        orderkey.push_back(parse_int32(p));
        if (*p == '|') ++p;

        // col2: l_partkey
        partkey.push_back(parse_int32(p));
        if (*p == '|') ++p;

        // col3: l_suppkey
        suppkey.push_back(parse_int32(p));
        if (*p == '|') ++p;

        // col4: l_linenumber
        linenumber.push_back(parse_int32(p));
        if (*p == '|') ++p;

        // col5: l_quantity (integer 1-50)
        quantity.push_back(parse_int8_integer(p));
        if (*p == '|') ++p;

        // col6: l_extendedprice
        extprice.push_back(parse_double(p));
        if (*p == '|') ++p;

        // col7: l_discount (0.00-0.10 -> int8 0-10)
        discount.push_back(parse_hundredths(p));
        if (*p == '|') ++p;

        // col8: l_tax (0.00-0.08 -> int8 0-8)
        tax.push_back(parse_hundredths(p));
        if (*p == '|') ++p;

        // col9: l_returnflag (A=0, N=1, R=2)
        {
            char c = *p; ++p;
            int8_t code = (c == 'A') ? 0 : (c == 'N') ? 1 : 2;
            returnflag.push_back(code);
        }
        if (*p == '|') ++p;

        // col10: l_linestatus (F=0, O=1)
        {
            char c = *p; ++p;
            linestatus.push_back((c == 'F') ? (int8_t)0 : (int8_t)1);
        }
        if (*p == '|') ++p;

        // col11: l_shipdate
        shipdate.push_back(date_to_days(p));
        p += 10;
        if (*p == '|') ++p;

        // col12: l_commitdate
        commitdate.push_back(date_to_days(p));
        p += 10;
        if (*p == '|') ++p;

        // col13: l_receiptdate
        receiptdate.push_back(date_to_days(p));
        p += 10;
        if (*p == '|') ++p;

        // col14-16: skip shipinstruct, shipmode, comment + trailing |
        skip_field(p); // shipinstruct (already consumed |)
        skip_field(p); // shipmode
        skip_field(p); // comment (includes trailing |)

        // skip newline
        while (p < end && (*p == '\r' || *p == '\n')) ++p;
    }

    size_t N = orderkey.size();
    printf("[lineitem] parsed %zu rows. Sorting by l_shipdate...\n", N); fflush(stdout);

    // Sort by l_shipdate: generate permutation index
    std::vector<int32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0);
    __gnu_parallel::sort(perm.begin(), perm.end(),
        [&](int32_t a, int32_t b) { return shipdate[a] < shipdate[b]; });

    printf("[lineitem] applying permutation to all columns...\n"); fflush(stdout);
    apply_permutation(orderkey,    perm);
    apply_permutation(partkey,     perm);
    apply_permutation(suppkey,     perm);
    apply_permutation(linenumber,  perm);
    apply_permutation(quantity,    perm);
    apply_permutation(extprice,    perm);
    apply_permutation(discount,    perm);
    apply_permutation(tax,         perm);
    apply_permutation(returnflag,  perm);
    apply_permutation(linestatus,  perm);
    apply_permutation(shipdate,    perm);
    apply_permutation(commitdate,  perm);
    apply_permutation(receiptdate, perm);

    printf("[lineitem] writing columns...\n"); fflush(stdout);
    write_column(out_dir + "/l_orderkey.bin",      orderkey);
    write_column(out_dir + "/l_partkey.bin",       partkey);
    write_column(out_dir + "/l_suppkey.bin",       suppkey);
    write_column(out_dir + "/l_linenumber.bin",    linenumber);
    write_column(out_dir + "/l_quantity.bin",      quantity);
    write_column(out_dir + "/l_extendedprice.bin", extprice);
    write_column(out_dir + "/l_discount.bin",      discount);
    write_column(out_dir + "/l_tax.bin",           tax);
    write_column(out_dir + "/l_returnflag.bin",    returnflag);
    write_column(out_dir + "/l_linestatus.bin",    linestatus);
    write_column(out_dir + "/l_shipdate.bin",      shipdate);
    write_column(out_dir + "/l_commitdate.bin",    commitdate);
    write_column(out_dir + "/l_receiptdate.bin",   receiptdate);

    // Write dict sidecar files
    // l_returnflag: code -> char
    {
        FILE* f = fopen((out_dir + "/l_returnflag_dict.bin").c_str(), "wb");
        char buf[3] = {'A', 'N', 'R'};
        fwrite(buf, 1, 3, f); fclose(f);
    }
    {
        FILE* f = fopen((out_dir + "/l_linestatus_dict.bin").c_str(), "wb");
        char buf[2] = {'F', 'O'};
        fwrite(buf, 1, 2, f); fclose(f);
    }

    printf("[lineitem] done. %zu rows written.\n", N); fflush(stdout);
}

// ---------------------------------------------------------------------------
// ORDERS ingestion
// ---------------------------------------------------------------------------
static void ingest_orders(const std::string& src_dir, const std::string& dst_dir) {
    std::string tbl_path = src_dir + "/orders.tbl";
    std::string out_dir  = dst_dir + "/orders";
    fs::create_directories(out_dir);

    printf("[orders] opening %s\n", tbl_path.c_str()); fflush(stdout);
    MappedFile mf(tbl_path);
    if (!mf.data) return;

    const size_t RESERVE = 15100000;
    std::vector<int32_t> orderkey;     orderkey.reserve(RESERVE);
    std::vector<int32_t> custkey;      custkey.reserve(RESERVE);
    std::vector<double>  totalprice;   totalprice.reserve(RESERVE);
    std::vector<int32_t> orderdate;    orderdate.reserve(RESERVE);
    std::vector<int32_t> shippriority; shippriority.reserve(RESERVE);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;

    while (p < end && *p != '\0') {
        if (*p == '\n' || *p == '\r') { ++p; continue; }

        // col1: o_orderkey
        orderkey.push_back(parse_int32(p));
        if (*p == '|') ++p;
        // col2: o_custkey
        custkey.push_back(parse_int32(p));
        if (*p == '|') ++p;
        // col3: o_orderstatus (skip)
        skip_field(p);
        // col4: o_totalprice
        totalprice.push_back(parse_double(p));
        if (*p == '|') ++p;
        // col5: o_orderdate
        orderdate.push_back(date_to_days(p));
        p += 10;
        if (*p == '|') ++p;
        // col6: o_orderpriority (skip)
        skip_field(p);
        // col7: o_clerk (skip)
        skip_field(p);
        // col8: o_shippriority
        shippriority.push_back(parse_int32(p));
        if (*p == '|') ++p;
        // col9: o_comment + trailing |
        skip_field(p);

        while (p < end && (*p == '\r' || *p == '\n')) ++p;
    }

    size_t N = orderkey.size();
    write_column(out_dir + "/o_orderkey.bin",     orderkey);
    write_column(out_dir + "/o_custkey.bin",      custkey);
    write_column(out_dir + "/o_totalprice.bin",   totalprice);
    write_column(out_dir + "/o_orderdate.bin",    orderdate);
    write_column(out_dir + "/o_shippriority.bin", shippriority);

    printf("[orders] done. %zu rows written.\n", N); fflush(stdout);
}

// ---------------------------------------------------------------------------
// CUSTOMER ingestion
// ---------------------------------------------------------------------------
static void ingest_customer(const std::string& src_dir, const std::string& dst_dir) {
    std::string tbl_path = src_dir + "/customer.tbl";
    std::string out_dir  = dst_dir + "/customer";
    fs::create_directories(out_dir);

    printf("[customer] opening %s\n", tbl_path.c_str()); fflush(stdout);
    MappedFile mf(tbl_path);
    if (!mf.data) return;

    // mktsegment dict: AUTOMOBILE=0, BUILDING=1, FURNITURE=2, HOUSEHOLD=3, MACHINERY=4
    auto encode_mktsegment = [](const char* s) -> int8_t {
        if (s[0] == 'A') return 0; // AUTOMOBILE
        if (s[0] == 'B') return 1; // BUILDING
        if (s[0] == 'F') return 2; // FURNITURE
        if (s[0] == 'H') return 3; // HOUSEHOLD
        return 4;                  // MACHINERY
    };

    const size_t RESERVE = 1510000;
    std::vector<int32_t>            custkey;    custkey.reserve(RESERVE);
    std::vector<std::array<char,26>> name;       name.reserve(RESERVE);
    std::vector<int32_t>            nationkey;  nationkey.reserve(RESERVE);
    std::vector<int8_t>             mktsegment; mktsegment.reserve(RESERVE);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;

    while (p < end && *p != '\0') {
        if (*p == '\n' || *p == '\r') { ++p; continue; }

        // col1: c_custkey
        custkey.push_back(parse_int32(p));
        if (*p == '|') ++p;

        // col2: c_name (VARCHAR 25)
        {
            std::array<char,26> buf{}; // zero-initialized
            size_t i = 0;
            while (*p != '|' && *p != '\n' && *p != '\r' && i < 25)
                buf[i++] = *p++;
            while (*p != '|' && *p != '\n') ++p; // overflow guard
            name.push_back(buf);
        }
        if (*p == '|') ++p;

        // col3: c_address (skip)
        skip_field(p);

        // col4: c_nationkey
        nationkey.push_back(parse_int32(p));
        if (*p == '|') ++p;

        // col5: c_phone (skip)
        skip_field(p);
        // col6: c_acctbal (skip)
        skip_field(p);

        // col7: c_mktsegment
        {
            const char* start = p;
            while (*p != '|' && *p != '\n') ++p;
            mktsegment.push_back(encode_mktsegment(start));
        }
        if (*p == '|') ++p;

        // col8: c_comment + trailing |
        skip_field(p);

        while (p < end && (*p == '\r' || *p == '\n')) ++p;
    }

    size_t N = custkey.size();
    write_column(out_dir + "/c_custkey.bin",    custkey);
    write_fixed_strings<26>(out_dir + "/c_name.bin", name);
    write_column(out_dir + "/c_nationkey.bin",  nationkey);
    write_column(out_dir + "/c_mktsegment.bin", mktsegment);

    // Write mktsegment dict sidecar: 5 entries of 16 bytes each (null-padded)
    {
        FILE* f = fopen((out_dir + "/c_mktsegment_dict.bin").c_str(), "wb");
        const char* entries[5] = {"AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"};
        char buf[16];
        for (int i = 0; i < 5; i++) {
            memset(buf, 0, 16);
            strncpy(buf, entries[i], 15);
            fwrite(buf, 1, 16, f);
        }
        fclose(f);
    }

    printf("[customer] done. %zu rows written.\n", N); fflush(stdout);
}

// ---------------------------------------------------------------------------
// PART ingestion
// ---------------------------------------------------------------------------
static void ingest_part(const std::string& src_dir, const std::string& dst_dir) {
    std::string tbl_path = src_dir + "/part.tbl";
    std::string out_dir  = dst_dir + "/part";
    fs::create_directories(out_dir);

    printf("[part] opening %s\n", tbl_path.c_str()); fflush(stdout);
    MappedFile mf(tbl_path);
    if (!mf.data) return;

    const size_t RESERVE = 2010000;
    std::vector<int32_t>             partkey; partkey.reserve(RESERVE);
    std::vector<std::array<char,56>> pname;   pname.reserve(RESERVE);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;

    while (p < end && *p != '\0') {
        if (*p == '\n' || *p == '\r') { ++p; continue; }

        // col1: p_partkey
        partkey.push_back(parse_int32(p));
        if (*p == '|') ++p;

        // col2: p_name (VARCHAR 55)
        {
            std::array<char,56> buf{}; // zero-initialized = null-padded
            size_t i = 0;
            while (*p != '|' && *p != '\n' && *p != '\r' && i < 55)
                buf[i++] = *p++;
            while (*p != '|' && *p != '\n') ++p;
            pname.push_back(buf);
        }
        if (*p == '|') ++p;

        // cols 3-9: skip (mfgr, brand, type, size, container, retailprice, comment + trailing |)
        for (int i = 0; i < 7; i++) skip_field(p);

        while (p < end && (*p == '\r' || *p == '\n')) ++p;
    }

    size_t N = partkey.size();
    write_column(out_dir + "/p_partkey.bin", partkey);
    write_fixed_strings<56>(out_dir + "/p_name.bin", pname);

    printf("[part] done. %zu rows written.\n", N); fflush(stdout);
}

// ---------------------------------------------------------------------------
// PARTSUPP ingestion
// ---------------------------------------------------------------------------
static void ingest_partsupp(const std::string& src_dir, const std::string& dst_dir) {
    std::string tbl_path = src_dir + "/partsupp.tbl";
    std::string out_dir  = dst_dir + "/partsupp";
    fs::create_directories(out_dir);

    printf("[partsupp] opening %s\n", tbl_path.c_str()); fflush(stdout);
    MappedFile mf(tbl_path);
    if (!mf.data) return;

    const size_t RESERVE = 8010000;
    std::vector<int32_t> pspartkey;   pspartkey.reserve(RESERVE);
    std::vector<int32_t> pssuppkey;   pssuppkey.reserve(RESERVE);
    std::vector<double>  supplycost;  supplycost.reserve(RESERVE);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;

    while (p < end && *p != '\0') {
        if (*p == '\n' || *p == '\r') { ++p; continue; }

        // col1: ps_partkey
        pspartkey.push_back(parse_int32(p));
        if (*p == '|') ++p;
        // col2: ps_suppkey
        pssuppkey.push_back(parse_int32(p));
        if (*p == '|') ++p;
        // col3: ps_availqty (skip)
        skip_field(p);
        // col4: ps_supplycost
        supplycost.push_back(parse_double(p));
        if (*p == '|') ++p;
        // col5: ps_comment + trailing |
        skip_field(p);

        while (p < end && (*p == '\r' || *p == '\n')) ++p;
    }

    size_t N = pspartkey.size();
    write_column(out_dir + "/ps_partkey.bin",    pspartkey);
    write_column(out_dir + "/ps_suppkey.bin",    pssuppkey);
    write_column(out_dir + "/ps_supplycost.bin", supplycost);

    printf("[partsupp] done. %zu rows written.\n", N); fflush(stdout);
}

// ---------------------------------------------------------------------------
// SUPPLIER ingestion
// ---------------------------------------------------------------------------
static void ingest_supplier(const std::string& src_dir, const std::string& dst_dir) {
    std::string tbl_path = src_dir + "/supplier.tbl";
    std::string out_dir  = dst_dir + "/supplier";
    fs::create_directories(out_dir);

    printf("[supplier] opening %s\n", tbl_path.c_str()); fflush(stdout);
    MappedFile mf(tbl_path);
    if (!mf.data) return;

    std::vector<int32_t> suppkey;   suppkey.reserve(102000);
    std::vector<int32_t> nationkey; nationkey.reserve(102000);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;

    while (p < end && *p != '\0') {
        if (*p == '\n' || *p == '\r') { ++p; continue; }

        // col1: s_suppkey
        suppkey.push_back(parse_int32(p));
        if (*p == '|') ++p;
        // col2: s_name (skip)
        skip_field(p);
        // col3: s_address (skip)
        skip_field(p);
        // col4: s_nationkey
        nationkey.push_back(parse_int32(p));
        if (*p == '|') ++p;
        // col5-7: phone, acctbal, comment + trailing |
        skip_field(p); skip_field(p); skip_field(p);

        while (p < end && (*p == '\r' || *p == '\n')) ++p;
    }

    size_t N = suppkey.size();
    write_column(out_dir + "/s_suppkey.bin",   suppkey);
    write_column(out_dir + "/s_nationkey.bin", nationkey);

    printf("[supplier] done. %zu rows written.\n", N); fflush(stdout);
}

// ---------------------------------------------------------------------------
// NATION ingestion
// ---------------------------------------------------------------------------
static void ingest_nation(const std::string& src_dir, const std::string& dst_dir) {
    std::string tbl_path = src_dir + "/nation.tbl";
    std::string out_dir  = dst_dir + "/nation";
    fs::create_directories(out_dir);

    MappedFile mf(tbl_path);
    if (!mf.data) return;

    std::vector<int32_t>             nkey;    nkey.reserve(30);
    std::vector<std::array<char,26>> nname;   nname.reserve(30);
    std::vector<int32_t>             nregkey; nregkey.reserve(30);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;

    while (p < end && *p != '\0') {
        if (*p == '\n' || *p == '\r') { ++p; continue; }

        nkey.push_back(parse_int32(p));
        if (*p == '|') ++p;

        {
            std::array<char,26> buf{};
            size_t i = 0;
            while (*p != '|' && *p != '\n' && i < 25) buf[i++] = *p++;
            while (*p != '|' && *p != '\n') ++p;
            // trim trailing spaces
            while (i > 0 && buf[i-1] == ' ') buf[--i] = '\0';
            nname.push_back(buf);
        }
        if (*p == '|') ++p;

        nregkey.push_back(parse_int32(p));
        if (*p == '|') ++p;

        skip_field(p); // comment + trailing |

        while (p < end && (*p == '\r' || *p == '\n')) ++p;
    }

    size_t N = nkey.size();
    write_column(out_dir + "/n_nationkey.bin", nkey);
    write_fixed_strings<26>(out_dir + "/n_name.bin", nname);
    write_column(out_dir + "/n_regionkey.bin", nregkey);

    printf("[nation] done. %zu rows written.\n", N); fflush(stdout);
}

// ---------------------------------------------------------------------------
// REGION ingestion
// ---------------------------------------------------------------------------
static void ingest_region(const std::string& src_dir, const std::string& dst_dir) {
    std::string tbl_path = src_dir + "/region.tbl";
    std::string out_dir  = dst_dir + "/region";
    fs::create_directories(out_dir);

    MappedFile mf(tbl_path);
    if (!mf.data) return;

    std::vector<int32_t>             rkey;  rkey.reserve(10);
    std::vector<std::array<char,26>> rname; rname.reserve(10);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;

    while (p < end && *p != '\0') {
        if (*p == '\n' || *p == '\r') { ++p; continue; }

        rkey.push_back(parse_int32(p));
        if (*p == '|') ++p;

        {
            std::array<char,26> buf{};
            size_t i = 0;
            while (*p != '|' && *p != '\n' && i < 25) buf[i++] = *p++;
            while (*p != '|' && *p != '\n') ++p;
            while (i > 0 && buf[i-1] == ' ') buf[--i] = '\0';
            rname.push_back(buf);
        }
        if (*p == '|') ++p;

        skip_field(p); // comment + trailing |

        while (p < end && (*p == '\r' || *p == '\n')) ++p;
    }

    write_column(out_dir + "/r_regionkey.bin", rkey);
    write_fixed_strings<26>(out_dir + "/r_name.bin", rname);

    printf("[region] done. %zu rows written.\n", rkey.size()); fflush(stdout);
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <src_data_dir> <dst_gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string src = argv[1];
    std::string dst = argv[2];

    printf("Ingesting TPC-H data from %s -> %s\n", src.c_str(), dst.c_str());
    fflush(stdout);

    // Launch all table ingestions in parallel threads
    // lineitem and orders are largest; run them concurrently with others
    std::vector<std::thread> threads;
    threads.emplace_back(ingest_lineitem, src, dst);
    threads.emplace_back(ingest_orders,   src, dst);
    threads.emplace_back(ingest_customer, src, dst);
    threads.emplace_back(ingest_part,     src, dst);
    threads.emplace_back(ingest_partsupp, src, dst);
    threads.emplace_back(ingest_supplier, src, dst);
    threads.emplace_back(ingest_nation,   src, dst);
    threads.emplace_back(ingest_region,   src, dst);

    for (auto& t : threads) t.join();

    printf("All tables ingested successfully.\n");
    return 0;
}
