// ingest.cpp — TPC-H binary columnar ingestion
// Reads .tbl (pipe-delimited) files in parallel, converts types, sorts key tables,
// writes binary column files to the gendb directory.
//
// Usage: ./ingest <data_dir> <gendb_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <cassert>
#include <vector>
#include <string>
#include <algorithm>
#include <numeric>
#include <thread>
#include <stdexcept>
#include <sys/stat.h>
#include <sys/types.h>
#include <errno.h>

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

static void mkdirp(const std::string& path) {
    if (mkdir(path.c_str(), 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "mkdir failed: %s\n", path.c_str());
        exit(1);
    }
}

// Split a pipe-delimited line into tokens (modifies line in-place).
// Returns number of tokens found.
static int split_pipe(char* line, const char** tok, int max_tok) {
    int n = 0;
    tok[n++] = line;
    for (char* p = line; ; ++p) {
        if (*p == '|') {
            *p = '\0';
            if (n < max_tok) tok[n++] = p + 1;
        } else if (*p == '\n' || *p == '\r' || *p == '\0') {
            *p = '\0';
            break;
        }
    }
    return n;
}

// Convert "YYYY-MM-DD" to days since 1970-01-01 (Julian Day arithmetic).
static int32_t parse_date(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10 + (s[6]-'0');
    int d = (s[8]-'0')*10 + (s[9]-'0');
    // Convert to Julian Day Number, then subtract JDN of 1970-01-01 (2440588)
    if (m <= 2) { y--; m += 12; }
    int A = y / 100;
    int B = 2 - A + A / 4;
    int jdn = (int)(365.25 * (y + 4716)) + (int)(30.6001 * (m + 1)) + d + B - 1524;
    return jdn - 2440588;
}

// Write a binary column file.
template<typename T>
static void write_col(const std::string& path, const std::vector<T>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot open for write: %s\n", path.c_str()); exit(1); }
    size_t written = fwrite(data.data(), sizeof(T), data.size(), f);
    if (written != data.size()) { fprintf(stderr, "Short write: %s\n", path.c_str()); exit(1); }
    fclose(f);
}

// Write a flat char buffer (fixed-width strings).
static void write_strbuf(const std::string& path, const std::vector<char>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { fprintf(stderr, "Cannot open for write: %s\n", path.c_str()); exit(1); }
    fwrite(data.data(), 1, data.size(), f);
    fclose(f);
}

// Apply permutation to a vector: result[i] = v[perm[i]]
template<typename T>
static void apply_perm(std::vector<T>& v, const std::vector<int32_t>& perm) {
    std::vector<T> tmp(perm.size());
    for (size_t i = 0; i < perm.size(); i++) tmp[i] = v[perm[i]];
    v = std::move(tmp);
}

// Apply permutation to a flat char buffer with fixed stride.
static void apply_perm_str(std::vector<char>& buf, const std::vector<int32_t>& perm, int stride) {
    std::vector<char> tmp(perm.size() * stride);
    for (size_t i = 0; i < perm.size(); i++)
        memcpy(tmp.data() + i * stride, buf.data() + perm[i] * stride, stride);
    buf = std::move(tmp);
}

// Dict encode l_returnflag: A=0, N=1, R=2
static inline int8_t encode_returnflag(char c) {
    return (c == 'A') ? 0 : (c == 'N') ? 1 : 2;
}

// Dict encode l_linestatus: F=0, O=1
static inline int8_t encode_linestatus(char c) {
    return (c == 'F') ? 0 : 1;
}

// Dict encode c_mktsegment by first char: A=0,B=1,F=2,H=3,M=4
static inline int8_t encode_mktsegment(const char* s) {
    switch (s[0]) {
        case 'A': return 0; // AUTOMOBILE
        case 'B': return 1; // BUILDING
        case 'F': return 2; // FURNITURE
        case 'H': return 3; // HOUSEHOLD
        case 'M': return 4; // MACHINERY
        default:  return -1;
    }
}

// ---------------------------------------------------------------------------
// Table ingestion functions
// ---------------------------------------------------------------------------

// lineitem — sort by l_shipdate ascending
static void ingest_lineitem(const std::string& data_dir, const std::string& gendb_dir) {
    std::string inpath = data_dir + "/lineitem.tbl";
    FILE* f = fopen(inpath.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", inpath.c_str()); exit(1); }
    // Use large I/O buffer
    setvbuf(f, nullptr, _IOFBF, 64 * 1024 * 1024);

    const int64_t EST = 60000000;
    std::vector<int32_t> l_orderkey;      l_orderkey.reserve(EST);
    std::vector<int32_t> l_partkey;       l_partkey.reserve(EST);
    std::vector<int32_t> l_suppkey;       l_suppkey.reserve(EST);
    std::vector<double>  l_quantity;      l_quantity.reserve(EST);
    std::vector<double>  l_extendedprice; l_extendedprice.reserve(EST);
    std::vector<double>  l_discount;      l_discount.reserve(EST);
    std::vector<double>  l_tax;           l_tax.reserve(EST);
    std::vector<int8_t>  l_returnflag;    l_returnflag.reserve(EST);
    std::vector<int8_t>  l_linestatus;    l_linestatus.reserve(EST);
    std::vector<int32_t> l_shipdate;      l_shipdate.reserve(EST);

    char line[1024];
    const char* tok[20];
    // cols: 0=orderkey,1=partkey,2=suppkey,3=linenumber(skip),4=qty,5=extprice,
    //       6=disc,7=tax,8=returnflag,9=linestatus,10=shipdate,...rest skip
    while (fgets(line, sizeof(line), f)) {
        int n = split_pipe(line, tok, 16);
        if (n < 11) continue;
        l_orderkey.push_back((int32_t)atoi(tok[0]));
        l_partkey.push_back((int32_t)atoi(tok[1]));
        l_suppkey.push_back((int32_t)atoi(tok[2]));
        // tok[3] = linenumber (skip)
        l_quantity.push_back(atof(tok[4]));
        l_extendedprice.push_back(atof(tok[5]));
        l_discount.push_back(atof(tok[6]));
        l_tax.push_back(atof(tok[7]));
        l_returnflag.push_back(encode_returnflag(tok[8][0]));
        l_linestatus.push_back(encode_linestatus(tok[9][0]));
        l_shipdate.push_back(parse_date(tok[10]));
    }
    fclose(f);

    int64_t N = (int64_t)l_shipdate.size();
    fprintf(stdout, "[lineitem] parsed %ld rows\n", (long)N);

    // Sort by l_shipdate ascending
    std::vector<int32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](int32_t a, int32_t b){
        return l_shipdate[a] < l_shipdate[b];
    });

    apply_perm(l_orderkey,      perm);
    apply_perm(l_partkey,       perm);
    apply_perm(l_suppkey,       perm);
    apply_perm(l_quantity,      perm);
    apply_perm(l_extendedprice, perm);
    apply_perm(l_discount,      perm);
    apply_perm(l_tax,           perm);
    apply_perm(l_returnflag,    perm);
    apply_perm(l_linestatus,    perm);
    apply_perm(l_shipdate,      perm);

    std::string dir = gendb_dir + "/lineitem";
    mkdirp(dir);
    write_col(dir + "/l_orderkey.bin",      l_orderkey);
    write_col(dir + "/l_partkey.bin",       l_partkey);
    write_col(dir + "/l_suppkey.bin",       l_suppkey);
    write_col(dir + "/l_quantity.bin",      l_quantity);
    write_col(dir + "/l_extendedprice.bin", l_extendedprice);
    write_col(dir + "/l_discount.bin",      l_discount);
    write_col(dir + "/l_tax.bin",           l_tax);
    write_col(dir + "/l_returnflag.bin",    l_returnflag);
    write_col(dir + "/l_linestatus.bin",    l_linestatus);
    write_col(dir + "/l_shipdate.bin",      l_shipdate);

    fprintf(stdout, "[lineitem] done, sorted by l_shipdate\n");
}

// orders — sort by o_orderdate ascending
static void ingest_orders(const std::string& data_dir, const std::string& gendb_dir) {
    std::string inpath = data_dir + "/orders.tbl";
    FILE* f = fopen(inpath.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", inpath.c_str()); exit(1); }
    setvbuf(f, nullptr, _IOFBF, 32 * 1024 * 1024);

    const int64_t EST = 15100000;
    std::vector<int32_t> o_orderkey;     o_orderkey.reserve(EST);
    std::vector<int32_t> o_custkey;      o_custkey.reserve(EST);
    std::vector<double>  o_totalprice;   o_totalprice.reserve(EST);
    std::vector<int32_t> o_orderdate;    o_orderdate.reserve(EST);
    std::vector<int32_t> o_shippriority; o_shippriority.reserve(EST);

    char line[1024];
    const char* tok[12];
    // cols: 0=orderkey,1=custkey,2=orderstatus(skip),3=totalprice,4=orderdate,
    //       5=orderpriority(skip),6=clerk(skip),7=shippriority,8=comment(skip)
    while (fgets(line, sizeof(line), f)) {
        int n = split_pipe(line, tok, 9);
        if (n < 8) continue;
        o_orderkey.push_back((int32_t)atoi(tok[0]));
        o_custkey.push_back((int32_t)atoi(tok[1]));
        // tok[2] = orderstatus (skip)
        o_totalprice.push_back(atof(tok[3]));
        o_orderdate.push_back(parse_date(tok[4]));
        // tok[5] = orderpriority (skip), tok[6] = clerk (skip)
        o_shippriority.push_back((int32_t)atoi(tok[7]));
    }
    fclose(f);

    int64_t N = (int64_t)o_orderdate.size();
    fprintf(stdout, "[orders] parsed %ld rows\n", (long)N);

    // Sort by o_orderdate ascending
    std::vector<int32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0);
    std::sort(perm.begin(), perm.end(), [&](int32_t a, int32_t b){
        return o_orderdate[a] < o_orderdate[b];
    });

    apply_perm(o_orderkey,     perm);
    apply_perm(o_custkey,      perm);
    apply_perm(o_totalprice,   perm);
    apply_perm(o_orderdate,    perm);
    apply_perm(o_shippriority, perm);

    std::string dir = gendb_dir + "/orders";
    mkdirp(dir);
    write_col(dir + "/o_orderkey.bin",     o_orderkey);
    write_col(dir + "/o_custkey.bin",      o_custkey);
    write_col(dir + "/o_totalprice.bin",   o_totalprice);
    write_col(dir + "/o_orderdate.bin",    o_orderdate);
    write_col(dir + "/o_shippriority.bin", o_shippriority);

    fprintf(stdout, "[orders] done, sorted by o_orderdate\n");
}

// customer
static void ingest_customer(const std::string& data_dir, const std::string& gendb_dir) {
    std::string inpath = data_dir + "/customer.tbl";
    FILE* f = fopen(inpath.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", inpath.c_str()); exit(1); }
    setvbuf(f, nullptr, _IOFBF, 8 * 1024 * 1024);

    const int64_t EST = 1510000;
    const int CNAME_STRIDE = 26;
    std::vector<int32_t> c_custkey;    c_custkey.reserve(EST);
    std::vector<char>    c_name;       c_name.reserve(EST * CNAME_STRIDE);
    std::vector<int32_t> c_nationkey;  c_nationkey.reserve(EST);
    std::vector<int8_t>  c_mktsegment; c_mktsegment.reserve(EST);

    char line[512];
    const char* tok[10];
    // cols: 0=custkey,1=name,2=address(skip),3=nationkey,4=phone(skip),
    //       5=acctbal(skip),6=mktsegment,7=comment(skip)
    while (fgets(line, sizeof(line), f)) {
        int n = split_pipe(line, tok, 8);
        if (n < 7) continue;
        c_custkey.push_back((int32_t)atoi(tok[0]));
        // Store c_name as fixed char[26]
        char name_buf[CNAME_STRIDE] = {};
        strncpy(name_buf, tok[1], CNAME_STRIDE - 1);
        c_name.insert(c_name.end(), name_buf, name_buf + CNAME_STRIDE);
        c_nationkey.push_back((int32_t)atoi(tok[3]));
        c_mktsegment.push_back(encode_mktsegment(tok[6]));
    }
    fclose(f);

    fprintf(stdout, "[customer] parsed %ld rows\n", (long)c_custkey.size());

    std::string dir = gendb_dir + "/customer";
    mkdirp(dir);
    write_col(dir + "/c_custkey.bin",    c_custkey);
    write_strbuf(dir + "/c_name.bin",    c_name);
    write_col(dir + "/c_nationkey.bin",  c_nationkey);
    write_col(dir + "/c_mktsegment.bin", c_mktsegment);

    fprintf(stdout, "[customer] done\n");
}

// part
static void ingest_part(const std::string& data_dir, const std::string& gendb_dir) {
    std::string inpath = data_dir + "/part.tbl";
    FILE* f = fopen(inpath.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", inpath.c_str()); exit(1); }
    setvbuf(f, nullptr, _IOFBF, 8 * 1024 * 1024);

    const int64_t EST = 2010000;
    const int PNAME_STRIDE = 56;
    std::vector<int32_t> p_partkey; p_partkey.reserve(EST);
    std::vector<char>    p_name;    p_name.reserve(EST * PNAME_STRIDE);

    char line[512];
    const char* tok[10];
    // cols: 0=partkey,1=name,...rest skip
    while (fgets(line, sizeof(line), f)) {
        int n = split_pipe(line, tok, 3);
        if (n < 2) continue;
        p_partkey.push_back((int32_t)atoi(tok[0]));
        char name_buf[PNAME_STRIDE] = {};
        strncpy(name_buf, tok[1], PNAME_STRIDE - 1);
        p_name.insert(p_name.end(), name_buf, name_buf + PNAME_STRIDE);
    }
    fclose(f);

    fprintf(stdout, "[part] parsed %ld rows\n", (long)p_partkey.size());

    std::string dir = gendb_dir + "/part";
    mkdirp(dir);
    write_col(dir + "/p_partkey.bin", p_partkey);
    write_strbuf(dir + "/p_name.bin", p_name);

    fprintf(stdout, "[part] done\n");
}

// partsupp
static void ingest_partsupp(const std::string& data_dir, const std::string& gendb_dir) {
    std::string inpath = data_dir + "/partsupp.tbl";
    FILE* f = fopen(inpath.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", inpath.c_str()); exit(1); }
    setvbuf(f, nullptr, _IOFBF, 32 * 1024 * 1024);

    const int64_t EST = 8010000;
    std::vector<int32_t> ps_partkey;    ps_partkey.reserve(EST);
    std::vector<int32_t> ps_suppkey;    ps_suppkey.reserve(EST);
    std::vector<double>  ps_supplycost; ps_supplycost.reserve(EST);

    char line[512];
    const char* tok[6];
    // cols: 0=partkey,1=suppkey,2=availqty(skip),3=supplycost,4=comment(skip)
    while (fgets(line, sizeof(line), f)) {
        int n = split_pipe(line, tok, 5);
        if (n < 4) continue;
        ps_partkey.push_back((int32_t)atoi(tok[0]));
        ps_suppkey.push_back((int32_t)atoi(tok[1]));
        ps_supplycost.push_back(atof(tok[3]));
    }
    fclose(f);

    fprintf(stdout, "[partsupp] parsed %ld rows\n", (long)ps_partkey.size());

    std::string dir = gendb_dir + "/partsupp";
    mkdirp(dir);
    write_col(dir + "/ps_partkey.bin",    ps_partkey);
    write_col(dir + "/ps_suppkey.bin",    ps_suppkey);
    write_col(dir + "/ps_supplycost.bin", ps_supplycost);

    fprintf(stdout, "[partsupp] done\n");
}

// supplier
static void ingest_supplier(const std::string& data_dir, const std::string& gendb_dir) {
    std::string inpath = data_dir + "/supplier.tbl";
    FILE* f = fopen(inpath.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", inpath.c_str()); exit(1); }
    setvbuf(f, nullptr, _IOFBF, 4 * 1024 * 1024);

    const int64_t EST = 101000;
    std::vector<int32_t> s_suppkey;   s_suppkey.reserve(EST);
    std::vector<int32_t> s_nationkey; s_nationkey.reserve(EST);

    char line[512];
    const char* tok[8];
    // cols: 0=suppkey,1=name(skip),2=address(skip),3=nationkey,4=phone(skip),
    //       5=acctbal(skip),6=comment(skip)
    while (fgets(line, sizeof(line), f)) {
        int n = split_pipe(line, tok, 5);
        if (n < 4) continue;
        s_suppkey.push_back((int32_t)atoi(tok[0]));
        s_nationkey.push_back((int32_t)atoi(tok[3]));
    }
    fclose(f);

    fprintf(stdout, "[supplier] parsed %ld rows\n", (long)s_suppkey.size());

    std::string dir = gendb_dir + "/supplier";
    mkdirp(dir);
    write_col(dir + "/s_suppkey.bin",   s_suppkey);
    write_col(dir + "/s_nationkey.bin", s_nationkey);

    fprintf(stdout, "[supplier] done\n");
}

// nation
static void ingest_nation(const std::string& data_dir, const std::string& gendb_dir) {
    std::string inpath = data_dir + "/nation.tbl";
    FILE* f = fopen(inpath.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", inpath.c_str()); exit(1); }

    const int NNAME_STRIDE = 26;
    std::vector<int32_t> n_nationkey; n_nationkey.reserve(30);
    std::vector<char>    n_name;      n_name.reserve(30 * NNAME_STRIDE);

    char line[512];
    const char* tok[5];
    // cols: 0=nationkey,1=name,2=regionkey(skip),3=comment(skip)
    while (fgets(line, sizeof(line), f)) {
        int n = split_pipe(line, tok, 3);
        if (n < 2) continue;
        n_nationkey.push_back((int32_t)atoi(tok[0]));
        char name_buf[NNAME_STRIDE] = {};
        strncpy(name_buf, tok[1], NNAME_STRIDE - 1);
        n_name.insert(n_name.end(), name_buf, name_buf + NNAME_STRIDE);
    }
    fclose(f);

    fprintf(stdout, "[nation] parsed %ld rows\n", (long)n_nationkey.size());

    std::string dir = gendb_dir + "/nation";
    mkdirp(dir);
    write_col(dir + "/n_nationkey.bin", n_nationkey);
    write_strbuf(dir + "/n_name.bin",   n_name);

    fprintf(stdout, "[nation] done\n");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

int main(int argc, char** argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <data_dir> <gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string data_dir  = argv[1];
    std::string gendb_dir = argv[2];

    mkdirp(gendb_dir);

    fprintf(stdout, "Starting parallel ingestion...\n");

    // Launch one thread per table
    std::thread t_lineitem ([&]{ ingest_lineitem (data_dir, gendb_dir); });
    std::thread t_orders   ([&]{ ingest_orders   (data_dir, gendb_dir); });
    std::thread t_customer ([&]{ ingest_customer (data_dir, gendb_dir); });
    std::thread t_part     ([&]{ ingest_part     (data_dir, gendb_dir); });
    std::thread t_partsupp ([&]{ ingest_partsupp (data_dir, gendb_dir); });
    std::thread t_supplier ([&]{ ingest_supplier (data_dir, gendb_dir); });
    std::thread t_nation   ([&]{ ingest_nation   (data_dir, gendb_dir); });

    t_lineitem.join();
    t_orders.join();
    t_customer.join();
    t_part.join();
    t_partsupp.join();
    t_supplier.join();
    t_nation.join();

    fprintf(stdout, "All tables ingested successfully.\n");
    return 0;
}
