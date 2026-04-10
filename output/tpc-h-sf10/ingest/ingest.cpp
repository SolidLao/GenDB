#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <filesystem>
#include <algorithm>
#include <numeric>
#include <chrono>

namespace fs = std::filesystem;

// ============================================================
// Utility: date encoding (days since 1970-01-01)
// ============================================================
static int32_t days_from_civil(int y, int m, int d) {
    y -= (m <= 2);
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int)doe - 719468;
}

static int32_t parse_date(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10 + (s[6]-'0');
    int d = (s[8]-'0')*10 + (s[9]-'0');
    return days_from_civil(y, m, d);
}

// ============================================================
// Fast parsers
// ============================================================
static int32_t fast_atoi(const char* p, int len) {
    int32_t v = 0;
    bool neg = false;
    int i = 0;
    if (len > 0 && p[0] == '-') { neg = true; i = 1; }
    for (; i < len; i++) v = v * 10 + (p[i] - '0');
    return neg ? -v : v;
}

static double fast_atof(const char* p, int len) {
    char buf[64];
    memcpy(buf, p, len);
    buf[len] = '\0';
    return strtod(buf, nullptr);
}

// ============================================================
// Field splitter for pipe-delimited TPC-H format
// ============================================================
struct Fields {
    const char* s[20]; // start pointers
    int         l[20]; // lengths
    int         n;     // count
};

static void split_pipe(const char* line, Fields& f) {
    f.n = 0;
    const char* p = line;
    while (*p && *p != '\n' && *p != '\r') {
        f.s[f.n] = p;
        while (*p && *p != '|' && *p != '\n' && *p != '\r') p++;
        f.l[f.n] = (int)(p - f.s[f.n]);
        f.n++;
        if (*p == '|') p++;
    }
}

// ============================================================
// Dictionary encoder
// ============================================================
struct DictEncoder {
    std::unordered_map<std::string, uint8_t> map;
    std::vector<std::string> strs;

    uint8_t encode(const char* s, int len) {
        std::string key(s, len);
        auto it = map.find(key);
        if (it != map.end()) return it->second;
        uint8_t code = (uint8_t)strs.size();
        map[key] = code;
        strs.push_back(key);
        return code;
    }
};

// ============================================================
// Binary writers
// ============================================================
template<typename T>
static void write_bin(const std::string& path, const std::vector<T>& v) {
    FILE* fp = fopen(path.c_str(), "wb");
    if (!fp) { perror(path.c_str()); exit(1); }
    if (!v.empty()) fwrite(v.data(), sizeof(T), v.size(), fp);
    fclose(fp);
}

static void write_varlen(const std::string& dir, const std::string& name,
                         const std::vector<uint32_t>& offsets,
                         const std::vector<char>& data) {
    write_bin(dir + "/" + name + ".bin", offsets);
    FILE* fp = fopen((dir + "/" + name + "_data.bin").c_str(), "wb");
    if (!fp) { perror((dir + "/" + name + "_data.bin").c_str()); exit(1); }
    if (!data.empty()) fwrite(data.data(), 1, data.size(), fp);
    fclose(fp);
}

static void write_dict(const std::string& dir, const std::string& name,
                       const DictEncoder& enc, const std::vector<uint8_t>& codes) {
    write_bin(dir + "/" + name + ".bin", codes);
    FILE* fp = fopen((dir + "/" + name + "_dict.bin").c_str(), "wb");
    if (!fp) { perror(name.c_str()); exit(1); }
    uint32_t n = (uint32_t)enc.strs.size();
    fwrite(&n, 4, 1, fp);
    for (auto& s : enc.strs) {
        uint16_t len = (uint16_t)s.size();
        fwrite(&len, 2, 1, fp);
        fwrite(s.data(), 1, len, fp);
    }
    fclose(fp);
}

static void write_meta(const std::string& dir, uint64_t row_count) {
    FILE* fp = fopen((dir + "/meta.txt").c_str(), "w");
    fprintf(fp, "row_count=%lu\n", (unsigned long)row_count);
    fclose(fp);
}

// ============================================================
// Varlen column accumulator
// ============================================================
struct VarlenCol {
    std::vector<uint32_t> offsets;
    std::vector<char> data;

    void reserve(size_t n, size_t avg_len) {
        offsets.reserve(n + 1);
        data.reserve(n * avg_len);
        offsets.push_back(0);
    }
    void add(const char* s, int len) {
        data.insert(data.end(), s, s + len);
        offsets.push_back((uint32_t)data.size());
    }
};

// ============================================================
// Progress reporting
// ============================================================
static std::mutex g_print_mtx;
static void report(const char* table, const char* msg, size_t rows = 0) {
    std::lock_guard<std::mutex> lk(g_print_mtx);
    if (rows > 0)
        printf("[%-10s] %s (%zu rows)\n", table, msg, rows);
    else
        printf("[%-10s] %s\n", table, msg);
    fflush(stdout);
}

// ============================================================
// Table ingestion functions
// ============================================================
static void ingest_lineitem(const std::string& src_dir, const std::string& dst_dir) {
    std::string out = dst_dir + "/lineitem";
    fs::create_directories(out);

    std::string path = src_dir + "/lineitem.tbl";
    FILE* fp = fopen(path.c_str(), "r");
    if (!fp) { perror(path.c_str()); exit(1); }

    const size_t EST = 60000000;
    std::vector<int32_t> orderkey, partkey, suppkey, linenumber;
    std::vector<double> quantity, extprice, discount, tax;
    std::vector<int8_t> returnflag, linestatus;
    std::vector<int32_t> shipdate, commitdate, receiptdate;
    DictEncoder shipinstruct_enc, shipmode_enc;
    std::vector<uint8_t> shipinstruct_codes, shipmode_codes;
    VarlenCol comment;

    orderkey.reserve(EST); partkey.reserve(EST); suppkey.reserve(EST); linenumber.reserve(EST);
    quantity.reserve(EST); extprice.reserve(EST); discount.reserve(EST); tax.reserve(EST);
    returnflag.reserve(EST); linestatus.reserve(EST);
    shipdate.reserve(EST); commitdate.reserve(EST); receiptdate.reserve(EST);
    shipinstruct_codes.reserve(EST); shipmode_codes.reserve(EST);
    comment.reserve(EST, 20);

    report("lineitem", "parsing...");
    char line[2048];
    size_t cnt = 0;
    while (fgets(line, sizeof(line), fp)) {
        Fields f;
        split_pipe(line, f);
        if (f.n < 16) continue;

        orderkey.push_back(fast_atoi(f.s[0], f.l[0]));
        partkey.push_back(fast_atoi(f.s[1], f.l[1]));
        suppkey.push_back(fast_atoi(f.s[2], f.l[2]));
        linenumber.push_back(fast_atoi(f.s[3], f.l[3]));
        quantity.push_back(fast_atof(f.s[4], f.l[4]));
        extprice.push_back(fast_atof(f.s[5], f.l[5]));
        discount.push_back(fast_atof(f.s[6], f.l[6]));
        tax.push_back(fast_atof(f.s[7], f.l[7]));
        returnflag.push_back((int8_t)f.s[8][0]);
        linestatus.push_back((int8_t)f.s[9][0]);
        shipdate.push_back(parse_date(f.s[10]));
        commitdate.push_back(parse_date(f.s[11]));
        receiptdate.push_back(parse_date(f.s[12]));
        shipinstruct_codes.push_back(shipinstruct_enc.encode(f.s[13], f.l[13]));
        shipmode_codes.push_back(shipmode_enc.encode(f.s[14], f.l[14]));
        comment.add(f.s[15], f.l[15]);

        if (++cnt % 10000000 == 0) report("lineitem", "parsed rows", cnt);
    }
    fclose(fp);
    report("lineitem", "parsed total", cnt);

    // Verify sort by l_orderkey
    bool sorted = true;
    for (size_t i = 1; i < orderkey.size(); i++) {
        if (orderkey[i] < orderkey[i-1]) { sorted = false; break; }
    }
    if (!sorted) {
        report("lineitem", "sorting by l_orderkey...");
        size_t N = orderkey.size();
        std::vector<uint32_t> idx(N);
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](uint32_t a, uint32_t b) {
            return orderkey[a] < orderkey[b];
        });

        // Permute helper
        auto permute_i32 = [&](std::vector<int32_t>& v) {
            std::vector<int32_t> tmp(N);
            for (size_t i = 0; i < N; i++) tmp[i] = v[idx[i]];
            v.swap(tmp);
        };
        auto permute_dbl = [&](std::vector<double>& v) {
            std::vector<double> tmp(N);
            for (size_t i = 0; i < N; i++) tmp[i] = v[idx[i]];
            v.swap(tmp);
        };
        auto permute_i8 = [&](std::vector<int8_t>& v) {
            std::vector<int8_t> tmp(N);
            for (size_t i = 0; i < N; i++) tmp[i] = v[idx[i]];
            v.swap(tmp);
        };
        auto permute_u8 = [&](std::vector<uint8_t>& v) {
            std::vector<uint8_t> tmp(N);
            for (size_t i = 0; i < N; i++) tmp[i] = v[idx[i]];
            v.swap(tmp);
        };

        permute_i32(orderkey); permute_i32(partkey); permute_i32(suppkey); permute_i32(linenumber);
        permute_dbl(quantity); permute_dbl(extprice); permute_dbl(discount); permute_dbl(tax);
        permute_i8(returnflag); permute_i8(linestatus);
        permute_i32(shipdate); permute_i32(commitdate); permute_i32(receiptdate);
        permute_u8(shipinstruct_codes); permute_u8(shipmode_codes);

        // Permute varlen comment
        VarlenCol new_comment;
        new_comment.reserve(N, 20);
        for (size_t i = 0; i < N; i++) {
            uint32_t os = comment.offsets[idx[i]];
            uint32_t oe = comment.offsets[idx[i]+1];
            new_comment.add(comment.data.data() + os, oe - os);
        }
        comment = std::move(new_comment);
        report("lineitem", "sort complete");
    }

    // Write binary files
    report("lineitem", "writing binary files...");
    write_bin(out + "/l_orderkey.bin", orderkey);
    write_bin(out + "/l_partkey.bin", partkey);
    write_bin(out + "/l_suppkey.bin", suppkey);
    write_bin(out + "/l_linenumber.bin", linenumber);
    write_bin(out + "/l_quantity.bin", quantity);
    write_bin(out + "/l_extendedprice.bin", extprice);
    write_bin(out + "/l_discount.bin", discount);
    write_bin(out + "/l_tax.bin", tax);
    write_bin(out + "/l_returnflag.bin", returnflag);
    write_bin(out + "/l_linestatus.bin", linestatus);
    write_bin(out + "/l_shipdate.bin", shipdate);
    write_bin(out + "/l_commitdate.bin", commitdate);
    write_bin(out + "/l_receiptdate.bin", receiptdate);
    write_dict(out, "l_shipinstruct", shipinstruct_enc, shipinstruct_codes);
    write_dict(out, "l_shipmode", shipmode_enc, shipmode_codes);
    write_varlen(out, "l_comment", comment.offsets, comment.data);
    write_meta(out, cnt);
    report("lineitem", "done", cnt);
}

static void ingest_orders(const std::string& src_dir, const std::string& dst_dir) {
    std::string out = dst_dir + "/orders";
    fs::create_directories(out);

    FILE* fp = fopen((src_dir + "/orders.tbl").c_str(), "r");
    if (!fp) { perror("orders.tbl"); exit(1); }

    const size_t EST = 15000000;
    std::vector<int32_t> orderkey, custkey, shippriority;
    std::vector<int8_t> orderstatus;
    std::vector<double> totalprice;
    std::vector<int32_t> orderdate;
    DictEncoder orderpriority_enc;
    std::vector<uint8_t> orderpriority_codes;
    VarlenCol clerk, comment;

    orderkey.reserve(EST); custkey.reserve(EST); shippriority.reserve(EST);
    orderstatus.reserve(EST); totalprice.reserve(EST); orderdate.reserve(EST);
    orderpriority_codes.reserve(EST);
    clerk.reserve(EST, 15); comment.reserve(EST, 40);

    report("orders", "parsing...");
    char line[2048];
    size_t cnt = 0;
    while (fgets(line, sizeof(line), fp)) {
        Fields f;
        split_pipe(line, f);
        if (f.n < 9) continue;

        orderkey.push_back(fast_atoi(f.s[0], f.l[0]));
        custkey.push_back(fast_atoi(f.s[1], f.l[1]));
        orderstatus.push_back((int8_t)f.s[2][0]);
        totalprice.push_back(fast_atof(f.s[3], f.l[3]));
        orderdate.push_back(parse_date(f.s[4]));
        orderpriority_codes.push_back(orderpriority_enc.encode(f.s[5], f.l[5]));
        clerk.add(f.s[6], f.l[6]);
        shippriority.push_back(fast_atoi(f.s[7], f.l[7]));
        comment.add(f.s[8], f.l[8]);
        cnt++;
    }
    fclose(fp);
    report("orders", "parsed", cnt);

    // Verify sort
    bool sorted = true;
    for (size_t i = 1; i < orderkey.size(); i++) {
        if (orderkey[i] < orderkey[i-1]) { sorted = false; break; }
    }
    if (!sorted) {
        report("orders", "sorting by o_orderkey...");
        size_t N = orderkey.size();
        std::vector<uint32_t> idx(N);
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](uint32_t a, uint32_t b) {
            return orderkey[a] < orderkey[b];
        });
        auto perm_i32 = [&](std::vector<int32_t>& v) {
            std::vector<int32_t> t(N); for (size_t i=0;i<N;i++) t[i]=v[idx[i]]; v.swap(t);
        };
        auto perm_dbl = [&](std::vector<double>& v) {
            std::vector<double> t(N); for (size_t i=0;i<N;i++) t[i]=v[idx[i]]; v.swap(t);
        };
        auto perm_i8 = [&](std::vector<int8_t>& v) {
            std::vector<int8_t> t(N); for (size_t i=0;i<N;i++) t[i]=v[idx[i]]; v.swap(t);
        };
        auto perm_u8 = [&](std::vector<uint8_t>& v) {
            std::vector<uint8_t> t(N); for (size_t i=0;i<N;i++) t[i]=v[idx[i]]; v.swap(t);
        };
        auto perm_vl = [&](VarlenCol& vc) {
            VarlenCol nv; nv.reserve(N, 30);
            for (size_t i=0;i<N;i++) {
                uint32_t os=vc.offsets[idx[i]], oe=vc.offsets[idx[i]+1];
                nv.add(vc.data.data()+os, oe-os);
            }
            vc = std::move(nv);
        };
        perm_i32(orderkey); perm_i32(custkey); perm_i32(shippriority);
        perm_i8(orderstatus); perm_dbl(totalprice); perm_i32(orderdate);
        perm_u8(orderpriority_codes);
        perm_vl(clerk); perm_vl(comment);
    }

    report("orders", "writing binary files...");
    write_bin(out + "/o_orderkey.bin", orderkey);
    write_bin(out + "/o_custkey.bin", custkey);
    write_bin(out + "/o_orderstatus.bin", orderstatus);
    write_bin(out + "/o_totalprice.bin", totalprice);
    write_bin(out + "/o_orderdate.bin", orderdate);
    write_dict(out, "o_orderpriority", orderpriority_enc, orderpriority_codes);
    write_varlen(out, "o_clerk", clerk.offsets, clerk.data);
    write_bin(out + "/o_shippriority.bin", shippriority);
    write_varlen(out, "o_comment", comment.offsets, comment.data);
    write_meta(out, cnt);
    report("orders", "done", cnt);
}

static void ingest_customer(const std::string& src_dir, const std::string& dst_dir) {
    std::string out = dst_dir + "/customer";
    fs::create_directories(out);

    FILE* fp = fopen((src_dir + "/customer.tbl").c_str(), "r");
    if (!fp) { perror("customer.tbl"); exit(1); }

    const size_t EST = 1500000;
    std::vector<int32_t> custkey, nationkey;
    std::vector<double> acctbal;
    DictEncoder mktsegment_enc;
    std::vector<uint8_t> mktsegment_codes;
    VarlenCol name, address, phone, comment;

    custkey.reserve(EST); nationkey.reserve(EST); acctbal.reserve(EST);
    mktsegment_codes.reserve(EST);
    name.reserve(EST, 20); address.reserve(EST, 25); phone.reserve(EST, 15); comment.reserve(EST, 60);

    report("customer", "parsing...");
    char line[2048];
    size_t cnt = 0;
    while (fgets(line, sizeof(line), fp)) {
        Fields f;
        split_pipe(line, f);
        if (f.n < 8) continue;

        custkey.push_back(fast_atoi(f.s[0], f.l[0]));
        name.add(f.s[1], f.l[1]);
        address.add(f.s[2], f.l[2]);
        nationkey.push_back(fast_atoi(f.s[3], f.l[3]));
        phone.add(f.s[4], f.l[4]);
        acctbal.push_back(fast_atof(f.s[5], f.l[5]));
        mktsegment_codes.push_back(mktsegment_enc.encode(f.s[6], f.l[6]));
        comment.add(f.s[7], f.l[7]);
        cnt++;
    }
    fclose(fp);

    report("customer", "writing binary files...");
    write_bin(out + "/c_custkey.bin", custkey);
    write_varlen(out, "c_name", name.offsets, name.data);
    write_varlen(out, "c_address", address.offsets, address.data);
    write_bin(out + "/c_nationkey.bin", nationkey);
    write_varlen(out, "c_phone", phone.offsets, phone.data);
    write_bin(out + "/c_acctbal.bin", acctbal);
    write_dict(out, "c_mktsegment", mktsegment_enc, mktsegment_codes);
    write_varlen(out, "c_comment", comment.offsets, comment.data);
    write_meta(out, cnt);
    report("customer", "done", cnt);
}

static void ingest_part(const std::string& src_dir, const std::string& dst_dir) {
    std::string out = dst_dir + "/part";
    fs::create_directories(out);

    FILE* fp = fopen((src_dir + "/part.tbl").c_str(), "r");
    if (!fp) { perror("part.tbl"); exit(1); }

    const size_t EST = 2000000;
    std::vector<int32_t> partkey, size_col;
    std::vector<double> retailprice;
    DictEncoder mfgr_enc, brand_enc, type_enc, container_enc;
    std::vector<uint8_t> mfgr_codes, brand_codes, type_codes, container_codes;
    VarlenCol name, comment;

    partkey.reserve(EST); size_col.reserve(EST); retailprice.reserve(EST);
    mfgr_codes.reserve(EST); brand_codes.reserve(EST);
    type_codes.reserve(EST); container_codes.reserve(EST);
    name.reserve(EST, 30); comment.reserve(EST, 15);

    report("part", "parsing...");
    char line[2048];
    size_t cnt = 0;
    while (fgets(line, sizeof(line), fp)) {
        Fields f;
        split_pipe(line, f);
        if (f.n < 9) continue;

        partkey.push_back(fast_atoi(f.s[0], f.l[0]));
        name.add(f.s[1], f.l[1]);
        mfgr_codes.push_back(mfgr_enc.encode(f.s[2], f.l[2]));
        brand_codes.push_back(brand_enc.encode(f.s[3], f.l[3]));
        type_codes.push_back(type_enc.encode(f.s[4], f.l[4]));
        size_col.push_back(fast_atoi(f.s[5], f.l[5]));
        container_codes.push_back(container_enc.encode(f.s[6], f.l[6]));
        retailprice.push_back(fast_atof(f.s[7], f.l[7]));
        comment.add(f.s[8], f.l[8]);
        cnt++;
    }
    fclose(fp);

    report("part", "writing binary files...");
    write_bin(out + "/p_partkey.bin", partkey);
    write_varlen(out, "p_name", name.offsets, name.data);
    write_dict(out, "p_mfgr", mfgr_enc, mfgr_codes);
    write_dict(out, "p_brand", brand_enc, brand_codes);
    write_dict(out, "p_type", type_enc, type_codes);
    write_bin(out + "/p_size.bin", size_col);
    write_dict(out, "p_container", container_enc, container_codes);
    write_bin(out + "/p_retailprice.bin", retailprice);
    write_varlen(out, "p_comment", comment.offsets, comment.data);
    write_meta(out, cnt);
    report("part", "done", cnt);
}

static void ingest_partsupp(const std::string& src_dir, const std::string& dst_dir) {
    std::string out = dst_dir + "/partsupp";
    fs::create_directories(out);

    FILE* fp = fopen((src_dir + "/partsupp.tbl").c_str(), "r");
    if (!fp) { perror("partsupp.tbl"); exit(1); }

    const size_t EST = 8000000;
    std::vector<int32_t> partkey, suppkey, availqty;
    std::vector<double> supplycost;
    VarlenCol comment;

    partkey.reserve(EST); suppkey.reserve(EST); availqty.reserve(EST);
    supplycost.reserve(EST);
    comment.reserve(EST, 100);

    report("partsupp", "parsing...");
    char line[2048];
    size_t cnt = 0;
    while (fgets(line, sizeof(line), fp)) {
        Fields f;
        split_pipe(line, f);
        if (f.n < 5) continue;

        partkey.push_back(fast_atoi(f.s[0], f.l[0]));
        suppkey.push_back(fast_atoi(f.s[1], f.l[1]));
        availqty.push_back(fast_atoi(f.s[2], f.l[2]));
        supplycost.push_back(fast_atof(f.s[3], f.l[3]));
        comment.add(f.s[4], f.l[4]);
        cnt++;
    }
    fclose(fp);

    // Verify sort by (ps_partkey, ps_suppkey)
    bool sorted = true;
    for (size_t i = 1; i < partkey.size(); i++) {
        if (partkey[i] < partkey[i-1] ||
            (partkey[i] == partkey[i-1] && suppkey[i] < suppkey[i-1])) {
            sorted = false; break;
        }
    }
    if (!sorted) {
        report("partsupp", "sorting by (ps_partkey, ps_suppkey)...");
        size_t N = partkey.size();
        std::vector<uint32_t> idx(N);
        std::iota(idx.begin(), idx.end(), 0);
        std::sort(idx.begin(), idx.end(), [&](uint32_t a, uint32_t b) {
            if (partkey[a] != partkey[b]) return partkey[a] < partkey[b];
            return suppkey[a] < suppkey[b];
        });
        auto perm_i32 = [&](std::vector<int32_t>& v) {
            std::vector<int32_t> t(N); for (size_t i=0;i<N;i++) t[i]=v[idx[i]]; v.swap(t);
        };
        auto perm_dbl = [&](std::vector<double>& v) {
            std::vector<double> t(N); for (size_t i=0;i<N;i++) t[i]=v[idx[i]]; v.swap(t);
        };
        perm_i32(partkey); perm_i32(suppkey); perm_i32(availqty); perm_dbl(supplycost);
        VarlenCol nc; nc.reserve(N, 100);
        for (size_t i=0;i<N;i++) {
            uint32_t os=comment.offsets[idx[i]], oe=comment.offsets[idx[i]+1];
            nc.add(comment.data.data()+os, oe-os);
        }
        comment = std::move(nc);
    }

    report("partsupp", "writing binary files...");
    write_bin(out + "/ps_partkey.bin", partkey);
    write_bin(out + "/ps_suppkey.bin", suppkey);
    write_bin(out + "/ps_availqty.bin", availqty);
    write_bin(out + "/ps_supplycost.bin", supplycost);
    write_varlen(out, "ps_comment", comment.offsets, comment.data);
    write_meta(out, cnt);
    report("partsupp", "done", cnt);
}

static void ingest_supplier(const std::string& src_dir, const std::string& dst_dir) {
    std::string out = dst_dir + "/supplier";
    fs::create_directories(out);

    FILE* fp = fopen((src_dir + "/supplier.tbl").c_str(), "r");
    if (!fp) { perror("supplier.tbl"); exit(1); }

    const size_t EST = 100000;
    std::vector<int32_t> suppkey, nationkey;
    std::vector<double> acctbal;
    VarlenCol name, address, phone, comment;

    suppkey.reserve(EST); nationkey.reserve(EST); acctbal.reserve(EST);
    name.reserve(EST, 20); address.reserve(EST, 25); phone.reserve(EST, 15); comment.reserve(EST, 60);

    report("supplier", "parsing...");
    char line[2048];
    size_t cnt = 0;
    while (fgets(line, sizeof(line), fp)) {
        Fields f;
        split_pipe(line, f);
        if (f.n < 7) continue;

        suppkey.push_back(fast_atoi(f.s[0], f.l[0]));
        name.add(f.s[1], f.l[1]);
        address.add(f.s[2], f.l[2]);
        nationkey.push_back(fast_atoi(f.s[3], f.l[3]));
        phone.add(f.s[4], f.l[4]);
        acctbal.push_back(fast_atof(f.s[5], f.l[5]));
        comment.add(f.s[6], f.l[6]);
        cnt++;
    }
    fclose(fp);

    report("supplier", "writing binary files...");
    write_bin(out + "/s_suppkey.bin", suppkey);
    write_varlen(out, "s_name", name.offsets, name.data);
    write_varlen(out, "s_address", address.offsets, address.data);
    write_bin(out + "/s_nationkey.bin", nationkey);
    write_varlen(out, "s_phone", phone.offsets, phone.data);
    write_bin(out + "/s_acctbal.bin", acctbal);
    write_varlen(out, "s_comment", comment.offsets, comment.data);
    write_meta(out, cnt);
    report("supplier", "done", cnt);
}

static void ingest_nation(const std::string& src_dir, const std::string& dst_dir) {
    std::string out = dst_dir + "/nation";
    fs::create_directories(out);

    FILE* fp = fopen((src_dir + "/nation.tbl").c_str(), "r");
    if (!fp) { perror("nation.tbl"); exit(1); }

    std::vector<int32_t> nationkey, regionkey;
    DictEncoder name_enc;
    std::vector<uint8_t> name_codes;
    VarlenCol comment;

    nationkey.reserve(25); regionkey.reserve(25); name_codes.reserve(25);
    comment.reserve(25, 80);

    char line[2048];
    size_t cnt = 0;
    while (fgets(line, sizeof(line), fp)) {
        Fields f;
        split_pipe(line, f);
        if (f.n < 4) continue;

        nationkey.push_back(fast_atoi(f.s[0], f.l[0]));
        name_codes.push_back(name_enc.encode(f.s[1], f.l[1]));
        regionkey.push_back(fast_atoi(f.s[2], f.l[2]));
        comment.add(f.s[3], f.l[3]);
        cnt++;
    }
    fclose(fp);

    write_bin(out + "/n_nationkey.bin", nationkey);
    write_dict(out, "n_name", name_enc, name_codes);
    write_bin(out + "/n_regionkey.bin", regionkey);
    write_varlen(out, "n_comment", comment.offsets, comment.data);
    write_meta(out, cnt);
    report("nation", "done", cnt);
}

static void ingest_region(const std::string& src_dir, const std::string& dst_dir) {
    std::string out = dst_dir + "/region";
    fs::create_directories(out);

    FILE* fp = fopen((src_dir + "/region.tbl").c_str(), "r");
    if (!fp) { perror("region.tbl"); exit(1); }

    std::vector<int32_t> regionkey;
    DictEncoder name_enc;
    std::vector<uint8_t> name_codes;
    VarlenCol comment;

    regionkey.reserve(5); name_codes.reserve(5);
    comment.reserve(5, 80);

    char line[2048];
    size_t cnt = 0;
    while (fgets(line, sizeof(line), fp)) {
        Fields f;
        split_pipe(line, f);
        if (f.n < 3) continue;

        regionkey.push_back(fast_atoi(f.s[0], f.l[0]));
        name_codes.push_back(name_enc.encode(f.s[1], f.l[1]));
        comment.add(f.s[2], f.l[2]);
        cnt++;
    }
    fclose(fp);

    write_bin(out + "/r_regionkey.bin", regionkey);
    write_dict(out, "r_name", name_enc, name_codes);
    write_varlen(out, "r_comment", comment.offsets, comment.data);
    write_meta(out, cnt);
    report("region", "done", cnt);
}

// ============================================================
// Main
// ============================================================
int main(int argc, char** argv) {
    if (argc != 3) {
        fprintf(stderr, "Usage: %s <src_data_dir> <dst_storage_dir>\n", argv[0]);
        return 1;
    }
    std::string src = argv[1];
    std::string dst = argv[2];
    fs::create_directories(dst);

    auto t0 = std::chrono::high_resolution_clock::now();

    // Launch all table ingestions in parallel
    std::thread t1(ingest_lineitem, src, dst);
    std::thread t2(ingest_orders, src, dst);
    std::thread t3(ingest_customer, src, dst);
    std::thread t4(ingest_part, src, dst);
    std::thread t5(ingest_partsupp, src, dst);
    std::thread t6(ingest_supplier, src, dst);
    std::thread t7(ingest_nation, src, dst);
    std::thread t8(ingest_region, src, dst);

    t1.join(); t2.join(); t3.join(); t4.join();
    t5.join(); t6.join(); t7.join(); t8.join();

    auto t1_end = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(t1_end - t0).count();
    printf("\nIngestion complete in %.1f seconds\n", elapsed);
    return 0;
}
