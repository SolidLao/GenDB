// ingest.cpp — TPC-H binary columnar ingestion
// Reads pipe-delimited .tbl files; writes binary columnar format to gendb directory.
// lineitem sorted by l_shipdate; orders sorted by o_orderdate (counting sort, O(N)).
// All 8 tables processed in parallel via std::thread.

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cassert>
#include <climits>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <algorithm>
#include <thread>
#include <stdexcept>

// ─── Utilities ─────────────────────────────────────────────────────────────

static void mkdirp(const std::string& path) {
    mkdir(path.c_str(), 0755);
}

// Howard Hinnant days_from_civil: returns days since 1970-01-01
static int32_t date_to_days(int y, int m, int d) {
    y -= (m <= 2);
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153u * (unsigned)(m + (m > 2 ? -3 : 9)) + 2u) / 5u + (unsigned)d - 1u;
    unsigned doe = yoe * 365u + yoe / 4u - yoe / 100u + doy;
    return (int32_t)(era * 146097 + (int)doe - 719468);
}

// Parse "YYYY-MM-DD" from mmap'd (non-null-terminated) pointer
static int32_t parse_date(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10 + (s[6]-'0');
    int d = (s[8]-'0')*10 + (s[9]-'0');
    return date_to_days(y, m, d);
}

static int32_t parse_int(const char* s) {
    int32_t r = 0;
    bool neg = (*s == '-');
    if (neg) s++;
    while (*s >= '0' && *s <= '9') r = r * 10 + (*s++ - '0');
    return neg ? -r : r;
}

static double parse_dbl(const char* s) {
    return strtod(s, nullptr);
}

// ─── MmapFile ──────────────────────────────────────────────────────────────

struct MmapFile {
    const char* data = nullptr;
    size_t size = 0;
    int fd = -1;

    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);
        struct stat st;
        fstat(fd, &st);
        size = (size_t)st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);
        madvise((void*)data, size, MADV_SEQUENTIAL);
    }
    ~MmapFile() {
        if (data && data != MAP_FAILED) munmap((void*)data, size);
        if (fd >= 0) close(fd);
    }
    MmapFile(const MmapFile&) = delete;
};

// ─── Column writers ────────────────────────────────────────────────────────

template<typename T>
static void write_col(const std::string& path, const std::vector<T>& col) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot create: " + path);
    if (!col.empty()) fwrite(col.data(), sizeof(T), col.size(), f);
    fclose(f);
}

template<typename T>
static void write_col_sorted(const std::string& path, const std::vector<T>& col,
                              const std::vector<int32_t>& sidx) {
    size_t n = sidx.size();
    std::vector<T> tmp(n);
    for (size_t i = 0; i < n; i++) tmp[i] = col[(size_t)sidx[i]];
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot create: " + path);
    fwrite(tmp.data(), sizeof(T), n, f);
    fclose(f);
}

static void write_dict(const std::string& path, const std::vector<const char*>& entries) {
    FILE* f = fopen(path.c_str(), "w");
    if (!f) return;
    for (auto e : entries) fprintf(f, "%s\n", e);
    fclose(f);
}

// ─── VarlenBuf ─────────────────────────────────────────────────────────────
// Accumulates variable-length strings into offset+data arrays.

struct VarlenBuf {
    std::vector<int32_t> offsets;
    std::vector<char>    data;

    void reserve_rows(size_t n_rows, size_t avg_len) {
        offsets.reserve(n_rows + 1);
        data.reserve(n_rows * avg_len);
    }

    void push(const char* s, int len) {
        offsets.push_back((int32_t)data.size());
        if (len > 0) data.insert(data.end(), s, s + len);
    }

    void finalize() {
        offsets.push_back((int32_t)data.size());
    }

    void write_direct(const std::string& dir, const std::string& col) const {
        std::string op = dir + "/" + col + ".offsets";
        FILE* f = fopen(op.c_str(), "wb");
        fwrite(offsets.data(), 4, offsets.size(), f);
        fclose(f);
        std::string dp = dir + "/" + col + ".data";
        f = fopen(dp.c_str(), "wb");
        if (!data.empty()) fwrite(data.data(), 1, data.size(), f);
        fclose(f);
    }

    void write_sorted(const std::string& dir, const std::string& col,
                      const std::vector<int32_t>& sidx) const {
        size_t n = sidx.size();
        std::vector<int32_t> new_off(n + 1);
        std::vector<char> new_data;
        new_data.reserve(data.size());
        new_off[0] = 0;
        for (size_t i = 0; i < n; i++) {
            int32_t idx   = sidx[i];
            int32_t start = offsets[(size_t)idx];
            int32_t len   = offsets[(size_t)idx + 1] - start;
            if (len > 0) new_data.insert(new_data.end(),
                                          data.begin() + start,
                                          data.begin() + start + len);
            new_off[i + 1] = (int32_t)new_data.size();
        }
        std::string op = dir + "/" + col + ".offsets";
        FILE* f = fopen(op.c_str(), "wb");
        fwrite(new_off.data(), 4, n + 1, f);
        fclose(f);
        std::string dp = dir + "/" + col + ".data";
        f = fopen(dp.c_str(), "wb");
        if (!new_data.empty()) fwrite(new_data.data(), 1, new_data.size(), f);
        fclose(f);
    }
};

// ─── Counting sort (O(N), good for low-cardinality keys like dates) ────────

static std::vector<int32_t> count_sort_idx(const std::vector<int32_t>& key) {
    if (key.empty()) return {};
    int32_t mn = key[0], mx = key[0];
    for (auto v : key) { if (v < mn) mn = v; if (v > mx) mx = v; }
    size_t range = (size_t)(mx - mn) + 1;
    std::vector<int64_t> cnt(range, 0);
    for (auto v : key) cnt[(size_t)(v - mn)]++;
    std::vector<int64_t> pos(range);
    pos[0] = 0;
    for (size_t i = 1; i < range; i++) pos[i] = pos[i-1] + cnt[i-1];
    size_t n = key.size();
    std::vector<int32_t> sidx(n);
    std::vector<int64_t> cur = pos;
    for (size_t i = 0; i < n; i++) {
        size_t b = (size_t)(key[i] - mn);
        sidx[(size_t)cur[b]++] = (int32_t)i;
    }
    return sidx;
}

// ─── Dict encoding helpers ─────────────────────────────────────────────────

static int8_t enc_returnflag(char c) {
    if (c == 'A') return 0;
    if (c == 'N') return 1;
    return 2; // R
}

static int8_t enc_linestatus(char c) {
    return c == 'O' ? 1 : 0; // F=0, O=1
}

static int8_t enc_mktsegment(const char* s) {
    switch (s[0]) {
        case 'A': return 0; // AUTOMOBILE
        case 'B': return 1; // BUILDING
        case 'F': return 2; // FURNITURE
        case 'H': return 3; // HOUSEHOLD
        default:  return 4; // MACHINERY
    }
}

// ─── Pipe-field splitter ───────────────────────────────────────────────────

struct PipeFields {
    const char* f[20];
    int         l[20];
    int         n;

    void parse(const char* line, int len) {
        n = 0;
        const char* start = line;
        for (int i = 0; i <= len && n < 20; i++) {
            if (i == len || line[i] == '|') {
                f[n] = start;
                l[n] = (int)(line + i - start);
                n++;
                start = line + i + 1;
            }
        }
    }
};

// ─── Table ingestors ───────────────────────────────────────────────────────

void ingest_lineitem(const std::string& src_dir, const std::string& dst_dir) {
    std::string dst = dst_dir + "/lineitem";
    mkdirp(dst);
    MmapFile mf(src_dir + "/lineitem.tbl");

    const size_t RESERVE = 60500000;
    std::vector<int32_t> orderkey, partkey, suppkey, linenumber;
    std::vector<double>  quantity, extprice, discount, tax;
    std::vector<int8_t>  returnflag, linestatus;
    std::vector<int32_t> shipdate, commitdate, receiptdate;
    VarlenBuf            si_buf, sm_buf, cm_buf;

    orderkey.reserve(RESERVE);  partkey.reserve(RESERVE);
    suppkey.reserve(RESERVE);   linenumber.reserve(RESERVE);
    quantity.reserve(RESERVE);  extprice.reserve(RESERVE);
    discount.reserve(RESERVE);  tax.reserve(RESERVE);
    returnflag.reserve(RESERVE); linestatus.reserve(RESERVE);
    shipdate.reserve(RESERVE);  commitdate.reserve(RESERVE);
    receiptdate.reserve(RESERVE);
    si_buf.reserve_rows(RESERVE, 20);
    sm_buf.reserve_rows(RESERVE, 8);
    cm_buf.reserve_rows(RESERVE, 22);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    PipeFields pf;

    while (p < end) {
        const char* line = p;
        while (p < end && *p != '\n') p++;
        int llen = (int)(p - line);
        if (p < end) p++;
        if (llen == 0) continue;
        if (llen > 0 && line[llen-1] == '\r') llen--;
        pf.parse(line, llen);
        if (pf.n < 16) continue;

        // 0:orderkey 1:partkey 2:suppkey 3:linenumber 4:quantity 5:extendedprice
        // 6:discount 7:tax 8:returnflag 9:linestatus 10:shipdate 11:commitdate
        // 12:receiptdate 13:shipinstruct 14:shipmode 15:comment
        orderkey.push_back(parse_int(pf.f[0]));
        partkey.push_back(parse_int(pf.f[1]));
        suppkey.push_back(parse_int(pf.f[2]));
        linenumber.push_back(parse_int(pf.f[3]));
        quantity.push_back(parse_dbl(pf.f[4]));
        extprice.push_back(parse_dbl(pf.f[5]));
        discount.push_back(parse_dbl(pf.f[6]));
        tax.push_back(parse_dbl(pf.f[7]));
        returnflag.push_back(enc_returnflag(pf.f[8][0]));
        linestatus.push_back(enc_linestatus(pf.f[9][0]));
        shipdate.push_back(parse_date(pf.f[10]));
        commitdate.push_back(parse_date(pf.f[11]));
        receiptdate.push_back(parse_date(pf.f[12]));
        si_buf.push(pf.f[13], pf.l[13]);
        sm_buf.push(pf.f[14], pf.l[14]);
        cm_buf.push(pf.f[15], pf.l[15]);
    }
    si_buf.finalize(); sm_buf.finalize(); cm_buf.finalize();

    size_t n = orderkey.size();
    printf("[lineitem] parsed %zu rows, sorting by l_shipdate...\n", n); fflush(stdout);

    auto sidx = count_sort_idx(shipdate);

    write_col_sorted(dst + "/l_orderkey.bin",      orderkey,    sidx);
    write_col_sorted(dst + "/l_partkey.bin",        partkey,     sidx);
    write_col_sorted(dst + "/l_suppkey.bin",        suppkey,     sidx);
    write_col_sorted(dst + "/l_linenumber.bin",     linenumber,  sidx);
    write_col_sorted(dst + "/l_quantity.bin",       quantity,    sidx);
    write_col_sorted(dst + "/l_extendedprice.bin",  extprice,    sidx);
    write_col_sorted(dst + "/l_discount.bin",       discount,    sidx);
    write_col_sorted(dst + "/l_tax.bin",            tax,         sidx);
    write_col_sorted(dst + "/l_returnflag.bin",     returnflag,  sidx);
    write_col_sorted(dst + "/l_linestatus.bin",     linestatus,  sidx);
    write_col_sorted(dst + "/l_shipdate.bin",       shipdate,    sidx);
    write_col_sorted(dst + "/l_commitdate.bin",     commitdate,  sidx);
    write_col_sorted(dst + "/l_receiptdate.bin",    receiptdate, sidx);
    si_buf.write_sorted(dst, "l_shipinstruct", sidx);
    sm_buf.write_sorted(dst, "l_shipmode",     sidx);
    cm_buf.write_sorted(dst, "l_comment",      sidx);

    write_dict(dst + "/l_returnflag.dict", {"A", "N", "R"});
    write_dict(dst + "/l_linestatus.dict", {"F", "O"});

    printf("[lineitem] done: %zu rows written, sorted by l_shipdate\n", n); fflush(stdout);
}

void ingest_orders(const std::string& src_dir, const std::string& dst_dir) {
    std::string dst = dst_dir + "/orders";
    mkdirp(dst);
    MmapFile mf(src_dir + "/orders.tbl");

    const size_t RESERVE = 15100000;
    std::vector<int32_t> orderkey, custkey, shippriority;
    std::vector<int8_t>  orderstatus;
    std::vector<double>  totalprice;
    std::vector<int32_t> orderdate;
    VarlenBuf            opri_buf, clerk_buf, comment_buf;

    orderkey.reserve(RESERVE);    custkey.reserve(RESERVE);
    shippriority.reserve(RESERVE); orderstatus.reserve(RESERVE);
    totalprice.reserve(RESERVE);   orderdate.reserve(RESERVE);
    opri_buf.reserve_rows(RESERVE, 10);
    clerk_buf.reserve_rows(RESERVE, 14);
    comment_buf.reserve_rows(RESERVE, 30);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    PipeFields pf;

    while (p < end) {
        const char* line = p;
        while (p < end && *p != '\n') p++;
        int llen = (int)(p - line);
        if (p < end) p++;
        if (llen == 0) continue;
        if (line[llen-1] == '\r') llen--;
        pf.parse(line, llen);
        if (pf.n < 9) continue;

        // 0:orderkey 1:custkey 2:orderstatus 3:totalprice 4:orderdate
        // 5:orderpriority 6:clerk 7:shippriority 8:comment
        orderkey.push_back(parse_int(pf.f[0]));
        custkey.push_back(parse_int(pf.f[1]));
        orderstatus.push_back((int8_t)pf.f[2][0]);
        totalprice.push_back(parse_dbl(pf.f[3]));
        orderdate.push_back(parse_date(pf.f[4]));
        opri_buf.push(pf.f[5], pf.l[5]);
        clerk_buf.push(pf.f[6], pf.l[6]);
        shippriority.push_back(parse_int(pf.f[7]));
        comment_buf.push(pf.f[8], pf.l[8]);
    }
    opri_buf.finalize(); clerk_buf.finalize(); comment_buf.finalize();

    size_t n = orderkey.size();
    printf("[orders] parsed %zu rows, sorting by o_orderdate...\n", n); fflush(stdout);

    auto sidx = count_sort_idx(orderdate);

    write_col_sorted(dst + "/o_orderkey.bin",     orderkey,     sidx);
    write_col_sorted(dst + "/o_custkey.bin",       custkey,      sidx);
    write_col_sorted(dst + "/o_orderstatus.bin",   orderstatus,  sidx);
    write_col_sorted(dst + "/o_totalprice.bin",    totalprice,   sidx);
    write_col_sorted(dst + "/o_orderdate.bin",     orderdate,    sidx);
    opri_buf.write_sorted(dst, "o_orderpriority", sidx);
    clerk_buf.write_sorted(dst, "o_clerk",         sidx);
    write_col_sorted(dst + "/o_shippriority.bin",  shippriority, sidx);
    comment_buf.write_sorted(dst, "o_comment",     sidx);

    printf("[orders] done: %zu rows written, sorted by o_orderdate\n", n); fflush(stdout);
}

void ingest_customer(const std::string& src_dir, const std::string& dst_dir) {
    std::string dst = dst_dir + "/customer";
    mkdirp(dst);
    MmapFile mf(src_dir + "/customer.tbl");

    const size_t RESERVE = 1510000;
    std::vector<int32_t> custkey, nationkey;
    std::vector<double>  acctbal;
    std::vector<int8_t>  mktsegment;
    VarlenBuf            name_buf, addr_buf, phone_buf, comment_buf;

    custkey.reserve(RESERVE);  nationkey.reserve(RESERVE);
    acctbal.reserve(RESERVE);  mktsegment.reserve(RESERVE);
    name_buf.reserve_rows(RESERVE, 18);
    addr_buf.reserve_rows(RESERVE, 25);
    phone_buf.reserve_rows(RESERVE, 15);
    comment_buf.reserve_rows(RESERVE, 60);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    PipeFields pf;

    while (p < end) {
        const char* line = p;
        while (p < end && *p != '\n') p++;
        int llen = (int)(p - line);
        if (p < end) p++;
        if (llen == 0) continue;
        if (line[llen-1] == '\r') llen--;
        pf.parse(line, llen);
        if (pf.n < 8) continue;

        // 0:custkey 1:name 2:address 3:nationkey 4:phone 5:acctbal 6:mktsegment 7:comment
        custkey.push_back(parse_int(pf.f[0]));
        name_buf.push(pf.f[1], pf.l[1]);
        addr_buf.push(pf.f[2], pf.l[2]);
        nationkey.push_back(parse_int(pf.f[3]));
        phone_buf.push(pf.f[4], pf.l[4]);
        acctbal.push_back(parse_dbl(pf.f[5]));
        mktsegment.push_back(enc_mktsegment(pf.f[6]));
        comment_buf.push(pf.f[7], pf.l[7]);
    }
    name_buf.finalize(); addr_buf.finalize();
    phone_buf.finalize(); comment_buf.finalize();

    size_t n = custkey.size();
    write_col(dst + "/c_custkey.bin",    custkey);
    write_col(dst + "/c_nationkey.bin",  nationkey);
    write_col(dst + "/c_acctbal.bin",    acctbal);
    write_col(dst + "/c_mktsegment.bin", mktsegment);
    name_buf.write_direct(dst, "c_name");
    addr_buf.write_direct(dst, "c_address");
    phone_buf.write_direct(dst, "c_phone");
    comment_buf.write_direct(dst, "c_comment");
    write_dict(dst + "/c_mktsegment.dict",
               {"AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"});

    printf("[customer] done: %zu rows\n", n); fflush(stdout);
}

void ingest_part(const std::string& src_dir, const std::string& dst_dir) {
    std::string dst = dst_dir + "/part";
    mkdirp(dst);
    MmapFile mf(src_dir + "/part.tbl");

    const size_t RESERVE = 2010000;
    std::vector<int32_t> partkey, size_col;
    std::vector<double>  retailprice;
    VarlenBuf            name_buf, mfgr_buf, brand_buf, type_buf,
                         container_buf, comment_buf;

    partkey.reserve(RESERVE);    size_col.reserve(RESERVE);
    retailprice.reserve(RESERVE);
    name_buf.reserve_rows(RESERVE, 30);
    mfgr_buf.reserve_rows(RESERVE, 15);
    brand_buf.reserve_rows(RESERVE, 9);
    type_buf.reserve_rows(RESERVE, 18);
    container_buf.reserve_rows(RESERVE, 8);
    comment_buf.reserve_rows(RESERVE, 12);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    PipeFields pf;

    while (p < end) {
        const char* line = p;
        while (p < end && *p != '\n') p++;
        int llen = (int)(p - line);
        if (p < end) p++;
        if (llen == 0) continue;
        if (line[llen-1] == '\r') llen--;
        pf.parse(line, llen);
        if (pf.n < 9) continue;

        // 0:partkey 1:name 2:mfgr 3:brand 4:type 5:size 6:container 7:retailprice 8:comment
        partkey.push_back(parse_int(pf.f[0]));
        name_buf.push(pf.f[1], pf.l[1]);
        mfgr_buf.push(pf.f[2], pf.l[2]);
        brand_buf.push(pf.f[3], pf.l[3]);
        type_buf.push(pf.f[4], pf.l[4]);
        size_col.push_back(parse_int(pf.f[5]));
        container_buf.push(pf.f[6], pf.l[6]);
        retailprice.push_back(parse_dbl(pf.f[7]));
        comment_buf.push(pf.f[8], pf.l[8]);
    }
    name_buf.finalize();   mfgr_buf.finalize();
    brand_buf.finalize();  type_buf.finalize();
    container_buf.finalize(); comment_buf.finalize();

    size_t n = partkey.size();
    write_col(dst + "/p_partkey.bin",    partkey);
    write_col(dst + "/p_size.bin",        size_col);
    write_col(dst + "/p_retailprice.bin", retailprice);
    name_buf.write_direct(dst, "p_name");
    mfgr_buf.write_direct(dst, "p_mfgr");
    brand_buf.write_direct(dst, "p_brand");
    type_buf.write_direct(dst, "p_type");
    container_buf.write_direct(dst, "p_container");
    comment_buf.write_direct(dst, "p_comment");

    printf("[part] done: %zu rows\n", n); fflush(stdout);
}

void ingest_partsupp(const std::string& src_dir, const std::string& dst_dir) {
    std::string dst = dst_dir + "/partsupp";
    mkdirp(dst);
    MmapFile mf(src_dir + "/partsupp.tbl");

    const size_t RESERVE = 8100000;
    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty;
    std::vector<double>  ps_supplycost;
    VarlenBuf            comment_buf;

    ps_partkey.reserve(RESERVE);  ps_suppkey.reserve(RESERVE);
    ps_availqty.reserve(RESERVE); ps_supplycost.reserve(RESERVE);
    comment_buf.reserve_rows(RESERVE, 80);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    PipeFields pf;

    while (p < end) {
        const char* line = p;
        while (p < end && *p != '\n') p++;
        int llen = (int)(p - line);
        if (p < end) p++;
        if (llen == 0) continue;
        if (line[llen-1] == '\r') llen--;
        pf.parse(line, llen);
        if (pf.n < 5) continue;

        // 0:ps_partkey 1:ps_suppkey 2:ps_availqty 3:ps_supplycost 4:ps_comment
        ps_partkey.push_back(parse_int(pf.f[0]));
        ps_suppkey.push_back(parse_int(pf.f[1]));
        ps_availqty.push_back(parse_int(pf.f[2]));
        ps_supplycost.push_back(parse_dbl(pf.f[3]));
        comment_buf.push(pf.f[4], pf.l[4]);
    }
    comment_buf.finalize();

    size_t n = ps_partkey.size();
    write_col(dst + "/ps_partkey.bin",    ps_partkey);
    write_col(dst + "/ps_suppkey.bin",    ps_suppkey);
    write_col(dst + "/ps_availqty.bin",   ps_availqty);
    write_col(dst + "/ps_supplycost.bin", ps_supplycost);
    comment_buf.write_direct(dst, "ps_comment");

    printf("[partsupp] done: %zu rows\n", n); fflush(stdout);
}

void ingest_supplier(const std::string& src_dir, const std::string& dst_dir) {
    std::string dst = dst_dir + "/supplier";
    mkdirp(dst);
    MmapFile mf(src_dir + "/supplier.tbl");

    const size_t RESERVE = 101000;
    std::vector<int32_t> suppkey, nationkey;
    std::vector<double>  acctbal;
    VarlenBuf            name_buf, addr_buf, phone_buf, comment_buf;

    suppkey.reserve(RESERVE);  nationkey.reserve(RESERVE);
    acctbal.reserve(RESERVE);
    name_buf.reserve_rows(RESERVE, 18);
    addr_buf.reserve_rows(RESERVE, 25);
    phone_buf.reserve_rows(RESERVE, 15);
    comment_buf.reserve_rows(RESERVE, 60);

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    PipeFields pf;

    while (p < end) {
        const char* line = p;
        while (p < end && *p != '\n') p++;
        int llen = (int)(p - line);
        if (p < end) p++;
        if (llen == 0) continue;
        if (line[llen-1] == '\r') llen--;
        pf.parse(line, llen);
        if (pf.n < 7) continue;

        // 0:suppkey 1:name 2:address 3:nationkey 4:phone 5:acctbal 6:comment
        suppkey.push_back(parse_int(pf.f[0]));
        name_buf.push(pf.f[1], pf.l[1]);
        addr_buf.push(pf.f[2], pf.l[2]);
        nationkey.push_back(parse_int(pf.f[3]));
        phone_buf.push(pf.f[4], pf.l[4]);
        acctbal.push_back(parse_dbl(pf.f[5]));
        comment_buf.push(pf.f[6], pf.l[6]);
    }
    name_buf.finalize(); addr_buf.finalize();
    phone_buf.finalize(); comment_buf.finalize();

    size_t n = suppkey.size();
    write_col(dst + "/s_suppkey.bin",   suppkey);
    write_col(dst + "/s_nationkey.bin", nationkey);
    write_col(dst + "/s_acctbal.bin",   acctbal);
    name_buf.write_direct(dst, "s_name");
    addr_buf.write_direct(dst, "s_address");
    phone_buf.write_direct(dst, "s_phone");
    comment_buf.write_direct(dst, "s_comment");

    printf("[supplier] done: %zu rows\n", n); fflush(stdout);
}

void ingest_nation(const std::string& src_dir, const std::string& dst_dir) {
    std::string dst = dst_dir + "/nation";
    mkdirp(dst);
    MmapFile mf(src_dir + "/nation.tbl");

    std::vector<int32_t> nationkey, regionkey;
    VarlenBuf            name_buf, comment_buf;

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    PipeFields pf;

    while (p < end) {
        const char* line = p;
        while (p < end && *p != '\n') p++;
        int llen = (int)(p - line);
        if (p < end) p++;
        if (llen == 0) continue;
        if (line[llen-1] == '\r') llen--;
        pf.parse(line, llen);
        if (pf.n < 4) continue;

        // 0:nationkey 1:name 2:regionkey 3:comment
        nationkey.push_back(parse_int(pf.f[0]));
        name_buf.push(pf.f[1], pf.l[1]);
        regionkey.push_back(parse_int(pf.f[2]));
        comment_buf.push(pf.f[3], pf.l[3]);
    }
    name_buf.finalize(); comment_buf.finalize();

    size_t n = nationkey.size();
    write_col(dst + "/n_nationkey.bin", nationkey);
    write_col(dst + "/n_regionkey.bin", regionkey);
    name_buf.write_direct(dst, "n_name");
    comment_buf.write_direct(dst, "n_comment");

    printf("[nation] done: %zu rows\n", n); fflush(stdout);
}

void ingest_region(const std::string& src_dir, const std::string& dst_dir) {
    std::string dst = dst_dir + "/region";
    mkdirp(dst);
    MmapFile mf(src_dir + "/region.tbl");

    std::vector<int32_t> regionkey;
    VarlenBuf            name_buf, comment_buf;

    const char* p   = mf.data;
    const char* end = mf.data + mf.size;
    PipeFields pf;

    while (p < end) {
        const char* line = p;
        while (p < end && *p != '\n') p++;
        int llen = (int)(p - line);
        if (p < end) p++;
        if (llen == 0) continue;
        if (line[llen-1] == '\r') llen--;
        pf.parse(line, llen);
        if (pf.n < 3) continue;

        // 0:regionkey 1:name 2:comment
        regionkey.push_back(parse_int(pf.f[0]));
        name_buf.push(pf.f[1], pf.l[1]);
        comment_buf.push(pf.f[2], pf.l[2]);
    }
    name_buf.finalize(); comment_buf.finalize();

    size_t n = regionkey.size();
    write_col(dst + "/r_regionkey.bin", regionkey);
    name_buf.write_direct(dst, "r_name");
    comment_buf.write_direct(dst, "r_comment");

    printf("[region] done: %zu rows\n", n); fflush(stdout);
}

// ─── Main ──────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <src_tbl_dir> <dst_gendb_dir>\n", argv[0]);
        return 1;
    }
    std::string src(argv[1]);
    std::string dst(argv[2]);
    mkdirp(dst);

    // Thread wrappers with error reporting
    auto run = [](const char* name, auto fn) {
        return std::thread([=]() {
            try { fn(); }
            catch (std::exception& e) {
                fprintf(stderr, "[%s] ERROR: %s\n", name, e.what());
            }
        });
    };

    std::vector<std::thread> threads;
    threads.push_back(run("lineitem", [&]{ ingest_lineitem(src, dst); }));
    threads.push_back(run("orders",   [&]{ ingest_orders(src, dst);   }));
    threads.push_back(run("customer", [&]{ ingest_customer(src, dst); }));
    threads.push_back(run("part",     [&]{ ingest_part(src, dst);     }));
    threads.push_back(run("partsupp", [&]{ ingest_partsupp(src, dst); }));
    threads.push_back(run("supplier", [&]{ ingest_supplier(src, dst); }));
    threads.push_back(run("nation",   [&]{ ingest_nation(src, dst);   }));
    threads.push_back(run("region",   [&]{ ingest_region(src, dst);   }));

    for (auto& t : threads) t.join();
    printf("[ingest] All tables done.\n");
    return 0;
}
