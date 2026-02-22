// ingest.cpp — TPC-H binary columnar ingestion
// Compile: g++ -O2 -std=c++17 -Wall -lpthread -o ingest ingest.cpp

#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <thread>
#include <algorithm>
#include <numeric>
#include <cstring>
#include <cstdint>
#include <cassert>
#include <map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cerrno>

static const int    BLOCK_SIZE  = 100000;
static const size_t WRITE_BUF   = 4 * 1024 * 1024; // 4 MB

// ── Utilities ────────────────────────────────────────────────────────────────

void mkdir_p(const std::string& path) {
    for (size_t i = 1; i < path.size(); i++) {
        if (path[i] == '/') {
            std::string sub = path.substr(0, i);
            mkdir(sub.c_str(), 0755);
        }
    }
    mkdir(path.c_str(), 0755);
}

// Parse "YYYY-MM-DD" → int32_t days since 1970-01-01
// Self-test: parse_date("1970-01-01") == 0
int32_t parse_date(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10 + (s[6]-'0');
    int d = (s[8]-'0')*10 + (s[9]-'0');
    auto is_leap = [](int yr){ return (yr%4==0 && yr%100!=0) || yr%400==0; };
    static const int mdays[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    int days = 0;
    for (int yr = 1970; yr < y; yr++) days += is_leap(yr) ? 366 : 365;
    for (int mo = 1; mo < m; mo++) { days += mdays[mo]; if (mo==2 && is_leap(y)) days++; }
    days += d - 1;
    return (int32_t)days;
}

int32_t parse_int(const char* s, const char* e) {
    int32_t v = 0; bool neg = false;
    if (s < e && *s == '-') { neg = true; ++s; }
    while (s < e && *s >= '0') v = v*10 + (*s++ - '0');
    return neg ? -v : v;
}

double parse_double(const char* s, const char* e) {
    char buf[32]; int len = (int)(e - s);
    if (len <= 0) return 0.0;
    if (len > 31) len = 31;
    memcpy(buf, s, len); buf[len] = '\0';
    return strtod(buf, nullptr);
}

// Write binary column with buffered I/O
template<typename T>
void write_col(const std::string& path, const T* data, size_t n) {
    int fd = open(path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { perror(("open: "+path).c_str()); return; }
    const char* ptr = reinterpret_cast<const char*>(data);
    size_t total = n * sizeof(T), written = 0;
    while (written < total) {
        size_t chunk = std::min(WRITE_BUF, total - written);
        ssize_t r = write(fd, ptr + written, chunk);
        if (r <= 0) break;
        written += r;
    }
    close(fd);
}

// Write text lookup file (one entry per line)
void write_lookup(const std::string& path, const std::vector<std::string>& vals) {
    std::ofstream f(path);
    for (auto& s : vals) f << s << '\n';
}

// Zone map for int32_t column: num_blocks then (min,max) pairs
void write_zonemap(const std::string& path, const std::vector<int32_t>& col) {
    uint32_t nb = (uint32_t)((col.size() + BLOCK_SIZE - 1) / BLOCK_SIZE);
    int fd = open(path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { perror(("open:"+path).c_str()); return; }
    if (write(fd, &nb, 4) != 4) { close(fd); return; }
    for (uint32_t b = 0; b < nb; b++) {
        size_t start = (size_t)b * BLOCK_SIZE;
        size_t end   = std::min(start + (size_t)BLOCK_SIZE, col.size());
        int32_t mn = col[start], mx = col[start];
        for (size_t i = start+1; i < end; i++) {
            if (col[i] < mn) mn = col[i];
            if (col[i] > mx) mx = col[i];
        }
        write(fd, &mn, 4); write(fd, &mx, 4);
    }
    close(fd);
}

// Apply sort permutation to a vector
template<typename T>
std::vector<T> apply_perm(const std::vector<T>& v, const std::vector<uint32_t>& perm) {
    std::vector<T> out(v.size());
    for (size_t i = 0; i < perm.size(); i++) out[i] = v[perm[i]];
    return out;
}

// BytePack: collect unique string values, assign sorted codes
struct BytePack {
    std::vector<std::string>           values; // first-seen order during parse
    std::map<std::string, uint8_t>     code_map;

    uint8_t encode_lazy(const std::string& s) {
        auto it = code_map.find(s);
        if (it != code_map.end()) return it->second;
        uint8_t c = (uint8_t)values.size();
        values.push_back(s); code_map[s] = c;
        return c;
    }

    // Sort values alphabetically for stable codes; remap a raw vector
    void finalize_and_remap(std::vector<uint8_t>& raw) {
        std::vector<std::string> sorted_vals = values;
        std::sort(sorted_vals.begin(), sorted_vals.end());
        std::map<std::string, uint8_t> new_map;
        for (size_t i = 0; i < sorted_vals.size(); i++) new_map[sorted_vals[i]] = (uint8_t)i;
        std::vector<uint8_t> remap(values.size());
        for (size_t i = 0; i < values.size(); i++) remap[i] = new_map[values[i]];
        for (auto& c : raw) c = remap[c];
        code_map = new_map;
        values   = sorted_vals;
    }
};

// ── mmap helper ──────────────────────────────────────────────────────────────
struct MmapFile {
    const char* data = nullptr;
    size_t      size = 0;
    int         fd   = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(("open:"+path).c_str()); return false; }
        struct stat st; fstat(fd, &st); size = (size_t)st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) { perror("mmap"); close(fd); return false; }
        madvise((void*)data, size, MADV_SEQUENTIAL);
        return true;
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }
};

// Field iterator for pipe-delimited TPC-H lines (handles trailing '|')
struct FieldIter {
    const char* p;
    const char* end;

    inline std::pair<const char*, const char*> next() {
        const char* s = p;
        while (p < end && *p != '|' && *p != '\n' && *p != '\r') p++;
        const char* e = p;
        if (p < end && *p == '|') p++;
        return {s, e};
    }
    inline void skip_line() {
        while (p < end && *p != '\n') p++;
        if (p < end) p++;
    }
    inline bool at_end() const { return p >= end; }
};

// ── lineitem ingestion ────────────────────────────────────────────────────────
// Stores 10 columns; source has 16 pipe-delimited fields.
// Sorted by l_shipdate.
void ingest_lineitem(const std::string& src_file, const std::string& dst_dir) {
    std::cout << "[lineitem] opening " << src_file << "\n" << std::flush;
    MmapFile mf;
    if (!mf.open(src_file)) return;

    // Estimate rows to pre-reserve
    size_t approx = mf.size / 120;
    std::vector<int32_t> l_orderkey;     l_orderkey.reserve(approx);
    std::vector<int32_t> l_partkey;      l_partkey.reserve(approx);
    std::vector<int32_t> l_suppkey;      l_suppkey.reserve(approx);
    std::vector<double>  l_quantity;     l_quantity.reserve(approx);
    std::vector<double>  l_extendedprice;l_extendedprice.reserve(approx);
    std::vector<double>  l_discount;     l_discount.reserve(approx);
    std::vector<double>  l_tax;          l_tax.reserve(approx);
    std::vector<uint8_t> l_returnflag;   l_returnflag.reserve(approx);
    std::vector<uint8_t> l_linestatus;   l_linestatus.reserve(approx);
    std::vector<int32_t> l_shipdate;     l_shipdate.reserve(approx);

    BytePack rf_pack, ls_pack;

    FieldIter fi{mf.data, mf.data + mf.size};
    while (!fi.at_end()) {
        if (*fi.p == '\n' || *fi.p == '\r') { fi.p++; continue; }
        auto [s0,e0] = fi.next(); // l_orderkey
        auto [s1,e1] = fi.next(); // l_partkey
        auto [s2,e2] = fi.next(); // l_suppkey
        auto [s3,e3] = fi.next(); // l_linenumber (skip)
        auto [s4,e4] = fi.next(); // l_quantity
        auto [s5,e5] = fi.next(); // l_extendedprice
        auto [s6,e6] = fi.next(); // l_discount
        auto [s7,e7] = fi.next(); // l_tax
        auto [s8,e8] = fi.next(); // l_returnflag
        auto [s9,e9] = fi.next(); // l_linestatus
        auto [s10,e10] = fi.next(); // l_shipdate
        fi.next(); fi.next(); fi.next(); fi.next(); fi.next(); // skip remaining 5 fields
        fi.skip_line();

        if (s0 == e0) continue; // empty line guard
        l_orderkey.push_back(parse_int(s0,e0));
        l_partkey.push_back(parse_int(s1,e1));
        l_suppkey.push_back(parse_int(s2,e2));
        l_quantity.push_back(parse_double(s4,e4));
        l_extendedprice.push_back(parse_double(s5,e5));
        l_discount.push_back(parse_double(s6,e6));
        l_tax.push_back(parse_double(s7,e7));
        l_returnflag.push_back(rf_pack.encode_lazy(std::string(s8,e8)));
        l_linestatus.push_back(ls_pack.encode_lazy(std::string(s9,e9)));
        l_shipdate.push_back(parse_date(s10));
    }

    size_t N = l_orderkey.size();
    std::cout << "[lineitem] parsed " << N << " rows; sorting by l_shipdate...\n" << std::flush;

    // Sort by l_shipdate (permutation-based)
    std::vector<uint32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0u);
    std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b){
        return l_shipdate[a] < l_shipdate[b];
    });

    l_orderkey     = apply_perm(l_orderkey,      perm);
    l_partkey      = apply_perm(l_partkey,       perm);
    l_suppkey      = apply_perm(l_suppkey,       perm);
    l_quantity     = apply_perm(l_quantity,      perm);
    l_extendedprice= apply_perm(l_extendedprice, perm);
    l_discount     = apply_perm(l_discount,      perm);
    l_tax          = apply_perm(l_tax,           perm);
    l_returnflag   = apply_perm(l_returnflag,    perm);
    l_linestatus   = apply_perm(l_linestatus,    perm);
    l_shipdate     = apply_perm(l_shipdate,      perm);

    // Finalize byte-pack encodings (sort for stable codes)
    rf_pack.finalize_and_remap(l_returnflag);
    ls_pack.finalize_and_remap(l_linestatus);

    std::cout << "[lineitem] writing columns...\n" << std::flush;
    // Write columns in parallel threads
    std::vector<std::thread> threads;
    threads.emplace_back([&]{ write_col(dst_dir+"/l_orderkey.bin",      l_orderkey.data(),      N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/l_partkey.bin",       l_partkey.data(),       N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/l_suppkey.bin",       l_suppkey.data(),       N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/l_quantity.bin",      l_quantity.data(),      N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/l_extendedprice.bin", l_extendedprice.data(), N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/l_discount.bin",      l_discount.data(),      N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/l_tax.bin",           l_tax.data(),           N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/l_returnflag.bin",    l_returnflag.data(),    N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/l_linestatus.bin",    l_linestatus.data(),    N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/l_shipdate.bin",      l_shipdate.data(),      N); });
    for (auto& t : threads) t.join();

    // Lookup files and zone map
    write_lookup(dst_dir+"/l_returnflag_lookup.txt", rf_pack.values);
    write_lookup(dst_dir+"/l_linestatus_lookup.txt", ls_pack.values);
    write_zonemap(dst_dir+"/../indexes/lineitem_shipdate_zonemap.bin", l_shipdate);

    // Post-ingest checks
    bool date_ok = false, dec_ok = false;
    for (size_t i = 0; i < N && (!date_ok || !dec_ok); i++) {
        if (l_shipdate[i] > 3000) date_ok = true;
        if (l_extendedprice[i] != 0.0) dec_ok = true;
    }
    if (!date_ok) { std::cerr << "FATAL: lineitem l_shipdate values not > 3000\n"; exit(1); }
    if (!dec_ok)  { std::cerr << "FATAL: lineitem l_extendedprice all zero\n";      exit(1); }

    std::cout << "[lineitem] done. rows=" << N << "\n";
}

// ── orders ingestion ──────────────────────────────────────────────────────────
// 5 columns stored; source has 9 fields. Sorted by o_orderdate.
void ingest_orders(const std::string& src_file, const std::string& dst_dir) {
    std::cout << "[orders] opening " << src_file << "\n" << std::flush;
    MmapFile mf;
    if (!mf.open(src_file)) return;

    size_t approx = mf.size / 60;
    std::vector<int32_t> o_orderkey;    o_orderkey.reserve(approx);
    std::vector<int32_t> o_custkey;     o_custkey.reserve(approx);
    std::vector<double>  o_totalprice;  o_totalprice.reserve(approx);
    std::vector<int32_t> o_orderdate;   o_orderdate.reserve(approx);
    std::vector<int32_t> o_shippriority;o_shippriority.reserve(approx);

    FieldIter fi{mf.data, mf.data + mf.size};
    while (!fi.at_end()) {
        if (*fi.p == '\n' || *fi.p == '\r') { fi.p++; continue; }
        auto [s0,e0] = fi.next(); // o_orderkey
        auto [s1,e1] = fi.next(); // o_custkey
        fi.next();                // o_orderstatus (skip)
        auto [s3,e3] = fi.next(); // o_totalprice
        auto [s4,e4] = fi.next(); // o_orderdate
        fi.next();                // o_orderpriority (skip)
        fi.next();                // o_clerk (skip)
        auto [s7,e7] = fi.next(); // o_shippriority
        fi.next();                // o_comment (skip)
        fi.skip_line();

        if (s0 == e0) continue;
        o_orderkey.push_back(parse_int(s0,e0));
        o_custkey.push_back(parse_int(s1,e1));
        o_totalprice.push_back(parse_double(s3,e3));
        o_orderdate.push_back(parse_date(s4));
        o_shippriority.push_back(parse_int(s7,e7));
    }

    size_t N = o_orderkey.size();
    std::cout << "[orders] parsed " << N << " rows; sorting by o_orderdate...\n" << std::flush;

    std::vector<uint32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0u);
    std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b){
        return o_orderdate[a] < o_orderdate[b];
    });
    o_orderkey     = apply_perm(o_orderkey,     perm);
    o_custkey      = apply_perm(o_custkey,      perm);
    o_totalprice   = apply_perm(o_totalprice,   perm);
    o_orderdate    = apply_perm(o_orderdate,    perm);
    o_shippriority = apply_perm(o_shippriority, perm);

    std::cout << "[orders] writing columns...\n" << std::flush;
    std::vector<std::thread> threads;
    threads.emplace_back([&]{ write_col(dst_dir+"/o_orderkey.bin",     o_orderkey.data(),     N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/o_custkey.bin",      o_custkey.data(),      N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/o_totalprice.bin",   o_totalprice.data(),   N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/o_orderdate.bin",    o_orderdate.data(),    N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/o_shippriority.bin", o_shippriority.data(), N); });
    for (auto& t : threads) t.join();

    write_zonemap(dst_dir+"/../indexes/orders_orderdate_zonemap.bin", o_orderdate);

    bool date_ok = false, dec_ok = false;
    for (size_t i = 0; i < N && (!date_ok || !dec_ok); i++) {
        if (o_orderdate[i] > 3000) date_ok = true;
        if (o_totalprice[i] != 0.0) dec_ok = true;
    }
    if (!date_ok) { std::cerr << "FATAL: orders o_orderdate not > 3000\n"; exit(1); }
    if (!dec_ok)  { std::cerr << "FATAL: orders o_totalprice all zero\n";  exit(1); }

    std::cout << "[orders] done. rows=" << N << "\n";
}

// ── customer ingestion ────────────────────────────────────────────────────────
// 3 columns: c_custkey, c_name (fixed 25B), c_mktsegment (byte_pack)
void ingest_customer(const std::string& src_file, const std::string& dst_dir) {
    std::cout << "[customer] opening " << src_file << "\n" << std::flush;
    MmapFile mf;
    if (!mf.open(src_file)) return;

    size_t approx = mf.size / 100;
    std::vector<int32_t> c_custkey;    c_custkey.reserve(approx);
    std::vector<char>    c_name;       c_name.reserve(approx * 25);
    std::vector<uint8_t> c_mktsegment; c_mktsegment.reserve(approx);
    BytePack mkt_pack;

    FieldIter fi{mf.data, mf.data + mf.size};
    while (!fi.at_end()) {
        if (*fi.p == '\n' || *fi.p == '\r') { fi.p++; continue; }
        auto [s0,e0] = fi.next(); // c_custkey
        auto [s1,e1] = fi.next(); // c_name
        fi.next();                // c_address (skip)
        fi.next();                // c_nationkey (skip)
        fi.next();                // c_phone (skip)
        fi.next();                // c_acctbal (skip)
        auto [s6,e6] = fi.next(); // c_mktsegment
        fi.next();                // c_comment (skip)
        fi.skip_line();

        if (s0 == e0) continue;
        c_custkey.push_back(parse_int(s0,e0));
        // Fixed 25-byte c_name (null-padded)
        char buf[25]; memset(buf, 0, 25);
        size_t len = std::min((size_t)(e1-s1), (size_t)25);
        memcpy(buf, s1, len);
        c_name.insert(c_name.end(), buf, buf+25);
        c_mktsegment.push_back(mkt_pack.encode_lazy(std::string(s6,e6)));
    }

    size_t N = c_custkey.size();
    mkt_pack.finalize_and_remap(c_mktsegment);

    std::cout << "[customer] writing " << N << " rows...\n" << std::flush;
    std::vector<std::thread> threads;
    threads.emplace_back([&]{ write_col(dst_dir+"/c_custkey.bin",    c_custkey.data(),    N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/c_name.bin",       c_name.data(),       N*25); });
    threads.emplace_back([&]{ write_col(dst_dir+"/c_mktsegment.bin", c_mktsegment.data(), N); });
    for (auto& t : threads) t.join();
    write_lookup(dst_dir+"/c_mktsegment_lookup.txt", mkt_pack.values);
    std::cout << "[customer] done. rows=" << N << "\n";
}

// ── part ingestion ────────────────────────────────────────────────────────────
// 2 columns: p_partkey, p_name (fixed 55B)
void ingest_part(const std::string& src_file, const std::string& dst_dir) {
    std::cout << "[part] opening " << src_file << "\n" << std::flush;
    MmapFile mf;
    if (!mf.open(src_file)) return;

    size_t approx = mf.size / 80;
    std::vector<int32_t> p_partkey; p_partkey.reserve(approx);
    std::vector<char>    p_name;    p_name.reserve(approx * 55);

    FieldIter fi{mf.data, mf.data + mf.size};
    while (!fi.at_end()) {
        if (*fi.p == '\n' || *fi.p == '\r') { fi.p++; continue; }
        auto [s0,e0] = fi.next(); // p_partkey
        auto [s1,e1] = fi.next(); // p_name
        fi.next(); fi.next(); fi.next(); fi.next(); fi.next(); fi.next(); fi.next();
        fi.skip_line();

        if (s0 == e0) continue;
        p_partkey.push_back(parse_int(s0,e0));
        char buf[55]; memset(buf, 0, 55);
        size_t len = std::min((size_t)(e1-s1), (size_t)55);
        memcpy(buf, s1, len);
        p_name.insert(p_name.end(), buf, buf+55);
    }

    size_t N = p_partkey.size();
    std::cout << "[part] writing " << N << " rows...\n" << std::flush;
    std::vector<std::thread> threads;
    threads.emplace_back([&]{ write_col(dst_dir+"/p_partkey.bin", p_partkey.data(), N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/p_name.bin",    p_name.data(),    N*55); });
    for (auto& t : threads) t.join();
    std::cout << "[part] done. rows=" << N << "\n";
}

// ── partsupp ingestion ────────────────────────────────────────────────────────
// 3 columns: ps_partkey, ps_suppkey, ps_supplycost
void ingest_partsupp(const std::string& src_file, const std::string& dst_dir) {
    std::cout << "[partsupp] opening " << src_file << "\n" << std::flush;
    MmapFile mf;
    if (!mf.open(src_file)) return;

    size_t approx = mf.size / 100;
    std::vector<int32_t> ps_partkey;    ps_partkey.reserve(approx);
    std::vector<int32_t> ps_suppkey;    ps_suppkey.reserve(approx);
    std::vector<double>  ps_supplycost; ps_supplycost.reserve(approx);

    FieldIter fi{mf.data, mf.data + mf.size};
    while (!fi.at_end()) {
        if (*fi.p == '\n' || *fi.p == '\r') { fi.p++; continue; }
        auto [s0,e0] = fi.next(); // ps_partkey
        auto [s1,e1] = fi.next(); // ps_suppkey
        fi.next();                // ps_availqty (skip)
        auto [s3,e3] = fi.next(); // ps_supplycost
        fi.next();                // ps_comment (skip)
        fi.skip_line();

        if (s0 == e0) continue;
        ps_partkey.push_back(parse_int(s0,e0));
        ps_suppkey.push_back(parse_int(s1,e1));
        ps_supplycost.push_back(parse_double(s3,e3));
    }

    size_t N = ps_partkey.size();
    std::cout << "[partsupp] writing " << N << " rows...\n" << std::flush;
    std::vector<std::thread> threads;
    threads.emplace_back([&]{ write_col(dst_dir+"/ps_partkey.bin",    ps_partkey.data(),    N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/ps_suppkey.bin",    ps_suppkey.data(),    N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/ps_supplycost.bin", ps_supplycost.data(), N); });
    for (auto& t : threads) t.join();

    bool dec_ok = false;
    for (size_t i = 0; i < N && !dec_ok; i++) if (ps_supplycost[i] != 0.0) dec_ok = true;
    if (!dec_ok) { std::cerr << "FATAL: partsupp ps_supplycost all zero\n"; exit(1); }

    std::cout << "[partsupp] done. rows=" << N << "\n";
}

// ── supplier ingestion ────────────────────────────────────────────────────────
// 2 columns: s_suppkey, s_nationkey
void ingest_supplier(const std::string& src_file, const std::string& dst_dir) {
    std::cout << "[supplier] opening " << src_file << "\n" << std::flush;
    MmapFile mf;
    if (!mf.open(src_file)) return;

    std::vector<int32_t> s_suppkey, s_nationkey;
    FieldIter fi{mf.data, mf.data + mf.size};
    while (!fi.at_end()) {
        if (*fi.p == '\n' || *fi.p == '\r') { fi.p++; continue; }
        auto [s0,e0] = fi.next(); // s_suppkey
        fi.next();                // s_name (skip)
        fi.next();                // s_address (skip)
        auto [s3,e3] = fi.next(); // s_nationkey
        fi.next(); fi.next(); fi.next();
        fi.skip_line();

        if (s0 == e0) continue;
        s_suppkey.push_back(parse_int(s0,e0));
        s_nationkey.push_back(parse_int(s3,e3));
    }
    size_t N = s_suppkey.size();
    std::cout << "[supplier] writing " << N << " rows...\n" << std::flush;
    std::vector<std::thread> threads;
    threads.emplace_back([&]{ write_col(dst_dir+"/s_suppkey.bin",   s_suppkey.data(),   N); });
    threads.emplace_back([&]{ write_col(dst_dir+"/s_nationkey.bin", s_nationkey.data(), N); });
    for (auto& t : threads) t.join();
    std::cout << "[supplier] done. rows=" << N << "\n";
}

// ── nation ingestion ──────────────────────────────────────────────────────────
// 2 columns: n_nationkey, n_name (byte_pack, 25 nations)
void ingest_nation(const std::string& src_file, const std::string& dst_dir) {
    std::cout << "[nation] opening " << src_file << "\n" << std::flush;
    MmapFile mf;
    if (!mf.open(src_file)) return;

    std::vector<int32_t> n_nationkey;
    std::vector<uint8_t> n_name;
    BytePack nm_pack;

    FieldIter fi{mf.data, mf.data + mf.size};
    while (!fi.at_end()) {
        if (*fi.p == '\n' || *fi.p == '\r') { fi.p++; continue; }
        auto [s0,e0] = fi.next(); // n_nationkey
        auto [s1,e1] = fi.next(); // n_name
        fi.next(); fi.next();
        fi.skip_line();

        if (s0 == e0) continue;
        n_nationkey.push_back(parse_int(s0,e0));
        n_name.push_back(nm_pack.encode_lazy(std::string(s1,e1)));
    }
    size_t N = n_nationkey.size();
    nm_pack.finalize_and_remap(n_name);
    write_col(dst_dir+"/n_nationkey.bin", n_nationkey.data(), N);
    write_col(dst_dir+"/n_name.bin",      n_name.data(),      N);
    write_lookup(dst_dir+"/n_name_lookup.txt", nm_pack.values);
    std::cout << "[nation] done. rows=" << N << "\n";
}

// ── main ──────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: ingest <src_dir> <dst_dir>\n";
        return 1;
    }
    std::string src = argv[1];
    std::string dst = argv[2];

    // Verify date parser: 1970-01-01 must be 0
    assert(parse_date("1970-01-01") == 0 && "Date parser self-test failed");

    // Create directory structure
    for (auto& sub : {"lineitem","orders","customer","part","partsupp","supplier","nation","indexes"}) {
        mkdir_p(dst + "/" + sub);
    }

    // Launch table ingestion in parallel (all 7 tables simultaneously)
    std::vector<std::thread> threads;
    threads.emplace_back(ingest_lineitem, src+"/lineitem.tbl",  dst+"/lineitem");
    threads.emplace_back(ingest_orders,   src+"/orders.tbl",    dst+"/orders");
    threads.emplace_back(ingest_customer, src+"/customer.tbl",  dst+"/customer");
    threads.emplace_back(ingest_part,     src+"/part.tbl",      dst+"/part");
    threads.emplace_back(ingest_partsupp, src+"/partsupp.tbl",  dst+"/partsupp");
    threads.emplace_back(ingest_supplier, src+"/supplier.tbl",  dst+"/supplier");
    threads.emplace_back(ingest_nation,   src+"/nation.tbl",    dst+"/nation");
    for (auto& t : threads) t.join();

    std::cout << "\n=== Ingestion complete ===\n";
    return 0;
}
