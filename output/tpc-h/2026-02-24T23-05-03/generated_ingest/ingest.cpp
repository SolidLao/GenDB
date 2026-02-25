// ingest.cpp — TPC-H sf10 binary columnar ingestion
// Parses pipe-delimited .tbl files, writes binary column files to <db_dir>/<table>/<col>.bin
// DATE → int32_t days-since-epoch (Howard Hinnant algorithm; self-test: 1970-01-01 → 0)
// DECIMAL → double (max individual value ~105K, well below 10^13; double precision sufficient)
// Low-cardinality strings → int16_t dict code + _dict.txt
// p_name (2M distinct) → int32_t dict code (exceeds int16_t range)
// All tables ingested in parallel via OpenMP sections

#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ── Utilities ─────────────────────────────────────────────────────────────

static void mkdirp(const std::string& path) {
    std::string cmd = "mkdir -p \"" + path + "\"";
    (void)system(cmd.c_str());
}

static void write_bin(const std::string& path, const void* data, size_t bytes) {
    int fd = open(path.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644);
    if (fd < 0) { perror(("open: " + path).c_str()); return; }
    size_t done = 0;
    while (done < bytes) {
        ssize_t n = write(fd, (const char*)data + done, bytes - done);
        if (n <= 0) { perror("write"); break; }
        done += (size_t)n;
    }
    close(fd);
}

static void write_dict(const std::string& path, const std::vector<std::string>& dict) {
    FILE* f = fopen(path.c_str(), "w");
    if (!f) { perror(("fopen: " + path).c_str()); return; }
    for (const auto& s : dict) { fputs(s.c_str(), f); fputc('\n', f); }
    fclose(f);
}

static const char* mmap_file(const std::string& path, size_t& out_size) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) { perror(("open: " + path).c_str()); out_size = 0; return nullptr; }
    struct stat st; fstat(fd, &st);
    out_size = (size_t)st.st_size;
    void* p = mmap(nullptr, out_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (p == MAP_FAILED) { perror("mmap"); out_size = 0; return nullptr; }
    madvise(p, out_size, MADV_SEQUENTIAL);
    return (const char*)p;
}

// ── Fast parsers ──────────────────────────────────────────────────────────

inline int32_t parse_int32(const char* s, const char* e) {
    int32_t v = 0;
    while (s < e) v = v * 10 + (*s++ - '0');
    return v;
}

inline double parse_double(const char* s, const char* e) {
    double v = 0;
    bool neg = (s < e && *s == '-') ? (++s, true) : false;
    while (s < e && *s != '.') v = v * 10.0 + (*s++ - '0');
    if (s < e && *s == '.') {
        ++s; double f = 0.1;
        while (s < e) { v += (*s++ - '0') * f; f *= 0.1; }
    }
    return neg ? -v : v;
}

// Howard Hinnant algorithm — days since 1970-01-01 (self-test: "1970-01-01" → 0)
inline int32_t parse_date(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10 + (s[6]-'0');
    int d = (s[8]-'0')*10 + (s[9]-'0');
    y -= (m <= 2) ? 1 : 0;
    int era = (y >= 0 ? y : y - 399) / 400;
    int yoe = y - era * 400;
    int doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    int doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return (int32_t)(era * 146097 + doe - 719468);
}

inline int16_t dict16_get(std::vector<std::string>& dict,
                          std::unordered_map<std::string,int16_t>& map,
                          const char* s, size_t len) {
    std::string key(s, len);
    auto it = map.find(key);
    if (it != map.end()) return it->second;
    int16_t code = (int16_t)dict.size();
    dict.push_back(key); map[key] = code; return code;
}

inline int32_t dict32_get(std::vector<std::string>& dict,
                          std::unordered_map<std::string,int32_t>& map,
                          const char* s, size_t len) {
    std::string key(s, len);
    auto it = map.find(key);
    if (it != map.end()) return it->second;
    int32_t code = (int32_t)dict.size();
    dict.push_back(key); map[key] = code; return code;
}

// ── Table ingestors ───────────────────────────────────────────────────────

// lineitem.tbl column order (0-indexed):
//  0:l_orderkey 1:l_partkey 2:l_suppkey 3:l_linenumber 4:l_quantity
//  5:l_extendedprice 6:l_discount 7:l_tax 8:l_returnflag 9:l_linestatus
//  10:l_shipdate 11:l_commitdate 12:l_receiptdate 13:l_shipinstruct 14:l_shipmode 15:l_comment
// Store: 0,1,2,4,5,6,7,8,9,10
static void ingest_lineitem(const std::string& data_dir, const std::string& db_dir) {
    mkdirp(db_dir + "/lineitem");
    mkdirp(db_dir + "/lineitem/indexes");
    size_t sz; const char* buf = mmap_file(data_dir + "/lineitem.tbl", sz);
    if (!buf) return;

    const size_t R = 62000000;
    std::vector<int32_t> v_ok;  v_ok.reserve(R);
    std::vector<int32_t> v_pk;  v_pk.reserve(R);
    std::vector<int32_t> v_sk;  v_sk.reserve(R);
    std::vector<double>  v_qty; v_qty.reserve(R);
    std::vector<double>  v_ep;  v_ep.reserve(R);
    std::vector<double>  v_dis; v_dis.reserve(R);
    std::vector<double>  v_tax; v_tax.reserve(R);
    std::vector<int16_t> v_rf;  v_rf.reserve(R);
    std::vector<int16_t> v_ls;  v_ls.reserve(R);
    std::vector<int32_t> v_sd;  v_sd.reserve(R);

    std::vector<std::string> d_rf, d_ls;
    std::unordered_map<std::string,int16_t> m_rf, m_ls;

    const char* p = buf, *end = buf + sz;
    while (p < end) {
        if (*p == '\r' || *p == '\n') { ++p; continue; }
        // col 0: l_orderkey
        const char* f = p; while (*p != '|') ++p;
        v_ok.push_back(parse_int32(f, p)); ++p;
        // col 1: l_partkey
        f = p; while (*p != '|') ++p;
        v_pk.push_back(parse_int32(f, p)); ++p;
        // col 2: l_suppkey
        f = p; while (*p != '|') ++p;
        v_sk.push_back(parse_int32(f, p)); ++p;
        // col 3: l_linenumber (skip)
        while (*p != '|') ++p; ++p;
        // col 4: l_quantity
        f = p; while (*p != '|') ++p;
        v_qty.push_back(parse_double(f, p)); ++p;
        // col 5: l_extendedprice
        f = p; while (*p != '|') ++p;
        v_ep.push_back(parse_double(f, p)); ++p;
        // col 6: l_discount
        f = p; while (*p != '|') ++p;
        v_dis.push_back(parse_double(f, p)); ++p;
        // col 7: l_tax
        f = p; while (*p != '|') ++p;
        v_tax.push_back(parse_double(f, p)); ++p;
        // col 8: l_returnflag (dict int16)
        f = p; while (*p != '|') ++p;
        v_rf.push_back(dict16_get(d_rf, m_rf, f, p - f)); ++p;
        // col 9: l_linestatus (dict int16)
        f = p; while (*p != '|') ++p;
        v_ls.push_back(dict16_get(d_ls, m_ls, f, p - f)); ++p;
        // col 10: l_shipdate (DATE → int32_t epoch days)
        v_sd.push_back(parse_date(p)); p += 10;
        // skip rest of line (cols 11-15)
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }
    munmap((void*)buf, sz);

    write_bin(db_dir+"/lineitem/l_orderkey.bin",      v_ok.data(),  v_ok.size()*4);
    write_bin(db_dir+"/lineitem/l_partkey.bin",        v_pk.data(),  v_pk.size()*4);
    write_bin(db_dir+"/lineitem/l_suppkey.bin",        v_sk.data(),  v_sk.size()*4);
    write_bin(db_dir+"/lineitem/l_quantity.bin",       v_qty.data(), v_qty.size()*8);
    write_bin(db_dir+"/lineitem/l_extendedprice.bin",  v_ep.data(),  v_ep.size()*8);
    write_bin(db_dir+"/lineitem/l_discount.bin",       v_dis.data(), v_dis.size()*8);
    write_bin(db_dir+"/lineitem/l_tax.bin",            v_tax.data(), v_tax.size()*8);
    write_bin(db_dir+"/lineitem/l_returnflag.bin",     v_rf.data(),  v_rf.size()*2);
    write_bin(db_dir+"/lineitem/l_linestatus.bin",     v_ls.data(),  v_ls.size()*2);
    write_bin(db_dir+"/lineitem/l_shipdate.bin",       v_sd.data(),  v_sd.size()*4);
    write_dict(db_dir+"/lineitem/l_returnflag_dict.txt", d_rf);
    write_dict(db_dir+"/lineitem/l_linestatus_dict.txt", d_ls);
    printf("[lineitem] %zu rows written\n", v_ok.size());
}

// orders.tbl column order:
//  0:o_orderkey 1:o_custkey 2:o_orderstatus 3:o_totalprice 4:o_orderdate
//  5:o_orderpriority 6:o_clerk 7:o_shippriority 8:o_comment
// Store: 0,1,3,4,7
static void ingest_orders(const std::string& data_dir, const std::string& db_dir) {
    mkdirp(db_dir + "/orders");
    mkdirp(db_dir + "/orders/indexes");
    size_t sz; const char* buf = mmap_file(data_dir + "/orders.tbl", sz);
    if (!buf) return;

    const size_t R = 15200000;
    std::vector<int32_t> v_ok;  v_ok.reserve(R);
    std::vector<int32_t> v_ck;  v_ck.reserve(R);
    std::vector<double>  v_tp;  v_tp.reserve(R);
    std::vector<int32_t> v_od;  v_od.reserve(R);
    std::vector<int32_t> v_sp;  v_sp.reserve(R);

    const char* p = buf, *end = buf + sz;
    while (p < end) {
        if (*p == '\r' || *p == '\n') { ++p; continue; }
        // col 0: o_orderkey
        const char* f = p; while (*p != '|') ++p;
        v_ok.push_back(parse_int32(f, p)); ++p;
        // col 1: o_custkey
        f = p; while (*p != '|') ++p;
        v_ck.push_back(parse_int32(f, p)); ++p;
        // col 2: o_orderstatus (skip)
        while (*p != '|') ++p; ++p;
        // col 3: o_totalprice
        f = p; while (*p != '|') ++p;
        v_tp.push_back(parse_double(f, p)); ++p;
        // col 4: o_orderdate (DATE → int32_t epoch days)
        v_od.push_back(parse_date(p)); p += 10; ++p; // skip trailing '|'
        // col 5: o_orderpriority (skip)
        while (*p != '|') ++p; ++p;
        // col 6: o_clerk (skip)
        while (*p != '|') ++p; ++p;
        // col 7: o_shippriority
        f = p; while (*p != '|') ++p;
        v_sp.push_back(parse_int32(f, p)); ++p;
        // skip rest
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }
    munmap((void*)buf, sz);

    write_bin(db_dir+"/orders/o_orderkey.bin",     v_ok.data(), v_ok.size()*4);
    write_bin(db_dir+"/orders/o_custkey.bin",      v_ck.data(), v_ck.size()*4);
    write_bin(db_dir+"/orders/o_totalprice.bin",   v_tp.data(), v_tp.size()*8);
    write_bin(db_dir+"/orders/o_orderdate.bin",    v_od.data(), v_od.size()*4);
    write_bin(db_dir+"/orders/o_shippriority.bin", v_sp.data(), v_sp.size()*4);
    printf("[orders] %zu rows written\n", v_ok.size());
}

// customer.tbl column order:
//  0:c_custkey 1:c_name 2:c_address 3:c_nationkey 4:c_phone 5:c_acctbal 6:c_mktsegment 7:c_comment
// Store: 0,3,6  (c_name NOT stored — Q18 reconstructs as "Customer#%09d" from c_custkey)
static void ingest_customer(const std::string& data_dir, const std::string& db_dir) {
    mkdirp(db_dir + "/customer");
    mkdirp(db_dir + "/customer/indexes");
    size_t sz; const char* buf = mmap_file(data_dir + "/customer.tbl", sz);
    if (!buf) return;

    const size_t R = 1520000;
    std::vector<int32_t> v_ck;  v_ck.reserve(R);
    std::vector<int32_t> v_nk;  v_nk.reserve(R);
    std::vector<int16_t> v_ms;  v_ms.reserve(R);
    std::vector<std::string> d_ms;
    std::unordered_map<std::string,int16_t> m_ms;

    const char* p = buf, *end = buf + sz;
    while (p < end) {
        if (*p == '\r' || *p == '\n') { ++p; continue; }
        // col 0: c_custkey
        const char* f = p; while (*p != '|') ++p;
        v_ck.push_back(parse_int32(f, p)); ++p;
        // col 1: c_name (skip)
        while (*p != '|') ++p; ++p;
        // col 2: c_address (skip)
        while (*p != '|') ++p; ++p;
        // col 3: c_nationkey
        f = p; while (*p != '|') ++p;
        v_nk.push_back(parse_int32(f, p)); ++p;
        // col 4: c_phone (skip)
        while (*p != '|') ++p; ++p;
        // col 5: c_acctbal (skip)
        while (*p != '|') ++p; ++p;
        // col 6: c_mktsegment (dict int16)
        f = p; while (*p != '|') ++p;
        v_ms.push_back(dict16_get(d_ms, m_ms, f, p - f)); ++p;
        // skip rest
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }
    munmap((void*)buf, sz);

    write_bin(db_dir+"/customer/c_custkey.bin",    v_ck.data(), v_ck.size()*4);
    write_bin(db_dir+"/customer/c_nationkey.bin",  v_nk.data(), v_nk.size()*4);
    write_bin(db_dir+"/customer/c_mktsegment.bin", v_ms.data(), v_ms.size()*2);
    write_dict(db_dir+"/customer/c_mktsegment_dict.txt", d_ms);
    printf("[customer] %zu rows written\n", v_ck.size());
}

// part.tbl column order:
//  0:p_partkey 1:p_name 2:p_mfgr 3:p_brand 4:p_type 5:p_size 6:p_container 7:p_retailprice 8:p_comment
// Store: 0,1 (p_name as int32_t dict — 2M distinct values exceed int16_t range)
static void ingest_part(const std::string& data_dir, const std::string& db_dir) {
    mkdirp(db_dir + "/part");
    size_t sz; const char* buf = mmap_file(data_dir + "/part.tbl", sz);
    if (!buf) return;

    const size_t R = 2020000;
    std::vector<int32_t> v_pk;  v_pk.reserve(R);
    std::vector<int32_t> v_pn;  v_pn.reserve(R);
    std::vector<std::string> d_pn; d_pn.reserve(R);
    std::unordered_map<std::string,int32_t> m_pn; m_pn.reserve(R * 2);

    const char* p = buf, *end = buf + sz;
    while (p < end) {
        if (*p == '\r' || *p == '\n') { ++p; continue; }
        // col 0: p_partkey
        const char* f = p; while (*p != '|') ++p;
        v_pk.push_back(parse_int32(f, p)); ++p;
        // col 1: p_name (dict int32 — 2M distinct, exceeds int16_t range)
        f = p; while (*p != '|') ++p;
        v_pn.push_back(dict32_get(d_pn, m_pn, f, p - f)); ++p;
        // skip rest of line
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }
    munmap((void*)buf, sz);

    write_bin(db_dir+"/part/p_partkey.bin", v_pk.data(), v_pk.size()*4);
    write_bin(db_dir+"/part/p_name.bin",    v_pn.data(), v_pn.size()*4);
    write_dict(db_dir+"/part/p_name_dict.txt", d_pn);
    printf("[part] %zu rows written\n", v_pk.size());
}

// partsupp.tbl column order:
//  0:ps_partkey 1:ps_suppkey 2:ps_availqty 3:ps_supplycost 4:ps_comment
// Store: 0,1,3
static void ingest_partsupp(const std::string& data_dir, const std::string& db_dir) {
    mkdirp(db_dir + "/partsupp");
    mkdirp(db_dir + "/partsupp/indexes");
    size_t sz; const char* buf = mmap_file(data_dir + "/partsupp.tbl", sz);
    if (!buf) return;

    const size_t R = 8100000;
    std::vector<int32_t> v_pk;  v_pk.reserve(R);
    std::vector<int32_t> v_sk;  v_sk.reserve(R);
    std::vector<double>  v_sc;  v_sc.reserve(R);

    const char* p = buf, *end = buf + sz;
    while (p < end) {
        if (*p == '\r' || *p == '\n') { ++p; continue; }
        // col 0: ps_partkey
        const char* f = p; while (*p != '|') ++p;
        v_pk.push_back(parse_int32(f, p)); ++p;
        // col 1: ps_suppkey
        f = p; while (*p != '|') ++p;
        v_sk.push_back(parse_int32(f, p)); ++p;
        // col 2: ps_availqty (skip)
        while (*p != '|') ++p; ++p;
        // col 3: ps_supplycost
        f = p; while (*p != '|') ++p;
        v_sc.push_back(parse_double(f, p)); ++p;
        // skip rest
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }
    munmap((void*)buf, sz);

    write_bin(db_dir+"/partsupp/ps_partkey.bin",    v_pk.data(), v_pk.size()*4);
    write_bin(db_dir+"/partsupp/ps_suppkey.bin",    v_sk.data(), v_sk.size()*4);
    write_bin(db_dir+"/partsupp/ps_supplycost.bin", v_sc.data(), v_sc.size()*8);
    printf("[partsupp] %zu rows written\n", v_pk.size());
}

// supplier.tbl column order:
//  0:s_suppkey 1:s_name 2:s_address 3:s_nationkey 4:s_phone 5:s_acctbal 6:s_comment
// Store: 0,3
static void ingest_supplier(const std::string& data_dir, const std::string& db_dir) {
    mkdirp(db_dir + "/supplier");
    mkdirp(db_dir + "/supplier/indexes");
    size_t sz; const char* buf = mmap_file(data_dir + "/supplier.tbl", sz);
    if (!buf) return;

    std::vector<int32_t> v_sk;  v_sk.reserve(110000);
    std::vector<int32_t> v_nk;  v_nk.reserve(110000);

    const char* p = buf, *end = buf + sz;
    while (p < end) {
        if (*p == '\r' || *p == '\n') { ++p; continue; }
        // col 0: s_suppkey
        const char* f = p; while (*p != '|') ++p;
        v_sk.push_back(parse_int32(f, p)); ++p;
        // col 1: s_name (skip)
        while (*p != '|') ++p; ++p;
        // col 2: s_address (skip)
        while (*p != '|') ++p; ++p;
        // col 3: s_nationkey
        f = p; while (*p != '|') ++p;
        v_nk.push_back(parse_int32(f, p)); ++p;
        // skip rest
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }
    munmap((void*)buf, sz);

    write_bin(db_dir+"/supplier/s_suppkey.bin",   v_sk.data(), v_sk.size()*4);
    write_bin(db_dir+"/supplier/s_nationkey.bin", v_nk.data(), v_nk.size()*4);
    printf("[supplier] %zu rows written\n", v_sk.size());
}

// nation.tbl column order:
//  0:n_nationkey 1:n_name 2:n_regionkey 3:n_comment
// Store: 0,1 (n_name as int16_t dict — 25 values)
static void ingest_nation(const std::string& data_dir, const std::string& db_dir) {
    mkdirp(db_dir + "/nation");
    size_t sz; const char* buf = mmap_file(data_dir + "/nation.tbl", sz);
    if (!buf) return;

    std::vector<int32_t> v_nk;
    std::vector<int16_t> v_nm;
    std::vector<std::string> d_nm;
    std::unordered_map<std::string,int16_t> m_nm;

    const char* p = buf, *end = buf + sz;
    while (p < end) {
        if (*p == '\r' || *p == '\n') { ++p; continue; }
        // col 0: n_nationkey
        const char* f = p; while (*p != '|') ++p;
        v_nk.push_back(parse_int32(f, p)); ++p;
        // col 1: n_name (dict int16)
        f = p; while (*p != '|') ++p;
        v_nm.push_back(dict16_get(d_nm, m_nm, f, p - f)); ++p;
        // skip rest
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }
    munmap((void*)buf, sz);

    write_bin(db_dir+"/nation/n_nationkey.bin", v_nk.data(), v_nk.size()*4);
    write_bin(db_dir+"/nation/n_name.bin",      v_nm.data(), v_nm.size()*2);
    write_dict(db_dir+"/nation/n_name_dict.txt", d_nm);
    printf("[nation] %zu rows written\n", v_nk.size());
}

// region.tbl column order:
//  0:r_regionkey 1:r_name 2:r_comment
// Store: 0,1 (r_name as int16_t dict — 5 values)
static void ingest_region(const std::string& data_dir, const std::string& db_dir) {
    mkdirp(db_dir + "/region");
    size_t sz; const char* buf = mmap_file(data_dir + "/region.tbl", sz);
    if (!buf) return;

    std::vector<int32_t> v_rk;
    std::vector<int16_t> v_rn;
    std::vector<std::string> d_rn;
    std::unordered_map<std::string,int16_t> m_rn;

    const char* p = buf, *end = buf + sz;
    while (p < end) {
        if (*p == '\r' || *p == '\n') { ++p; continue; }
        // col 0: r_regionkey
        const char* f = p; while (*p != '|') ++p;
        v_rk.push_back(parse_int32(f, p)); ++p;
        // col 1: r_name (dict int16)
        f = p; while (*p != '|') ++p;
        v_rn.push_back(dict16_get(d_rn, m_rn, f, p - f)); ++p;
        // skip rest
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
    }
    munmap((void*)buf, sz);

    write_bin(db_dir+"/region/r_regionkey.bin", v_rk.data(), v_rk.size()*4);
    write_bin(db_dir+"/region/r_name.bin",      v_rn.data(), v_rn.size()*2);
    write_dict(db_dir+"/region/r_name_dict.txt", d_rn);
    printf("[region] %zu rows written\n", v_rk.size());
}

// ── Main ──────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: ingest <data_dir> <db_dir>\n"); return 1; }
    std::string data_dir = argv[1];
    std::string db_dir   = argv[2];
    mkdirp(db_dir);
    printf("Ingesting TPC-H sf10: %s → %s\n", data_dir.c_str(), db_dir.c_str());
    double t0 = omp_get_wtime();

    // Parallel ingestion of all 8 tables
    #pragma omp parallel sections num_threads(8)
    {
        #pragma omp section
        ingest_lineitem(data_dir, db_dir);

        #pragma omp section
        ingest_orders(data_dir, db_dir);

        #pragma omp section
        ingest_customer(data_dir, db_dir);

        #pragma omp section
        ingest_part(data_dir, db_dir);

        #pragma omp section
        ingest_partsupp(data_dir, db_dir);

        #pragma omp section
        ingest_supplier(data_dir, db_dir);

        #pragma omp section
        ingest_nation(data_dir, db_dir);

        #pragma omp section
        ingest_region(data_dir, db_dir);
    }

    printf("Ingestion done in %.1f s\n", omp_get_wtime() - t0);
    return 0;
}
