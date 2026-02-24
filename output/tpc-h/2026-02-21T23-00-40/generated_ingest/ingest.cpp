// ingest.cpp — TPC-H binary columnar ingestion
// Compile: g++ -O2 -std=c++17 -Wall -lpthread -o ingest ingest.cpp

#include <algorithm>
#include <cassert>
#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <numeric>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// ── helpers ──────────────────────────────────────────────────────────────────

static void die(const char* msg) {
    perror(msg);
    std::exit(1);
}

static void mkdir_p(const std::string& path) {
    std::string cmd = "mkdir -p " + path;
    if (std::system(cmd.c_str()) != 0)
        die(("mkdir_p: " + path).c_str());
}

// days since 1970-01-01  (civil calendar, proleptic Gregorian)
// Self-test: parse_date("1970-01-01") == 0
static int32_t parse_date(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10  + (s[6]-'0');
    int d = (s[8]-'0')*10  + (s[9]-'0');
    if (m <= 2) { y--; m += 12; }
    int era = (y >= 0 ? y : y-399) / 400;
    int yoe = y - era * 400;
    int doy = (153*(m-3)+2)/5 + d - 1;
    int doe = yoe*365 + yoe/4 - yoe/100 + doy;
    return (int32_t)(era*146097 + doe - 719468);
}

// advance past next '|'; return new pointer
static inline const char* skip_field(const char* p) {
    while (*p != '|' && *p != '\n' && *p != '\0') ++p;
    if (*p == '|') ++p;
    return p;
}

static inline const char* parse_int32(const char* p, int32_t& v) {
    v = 0;
    bool neg = (*p == '-'); if (neg) ++p;
    while (*p >= '0' && *p <= '9') v = v*10 + (*p++ - '0');
    if (neg) v = -v;
    if (*p == '|') ++p;
    return p;
}

static inline const char* parse_double(const char* p, double& v) {
    char* e; v = strtod(p, &e); p = e;
    if (*p == '|') ++p;
    return p;
}

static inline const char* parse_char1(const char* p, int8_t& v) {
    v = (int8_t)*p; p++;
    if (*p == '|') ++p;
    return p;
}

// copy up to (width-1) chars into buf (null-padded), advance past '|'
static inline const char* parse_fixed(const char* p, char* buf, int width) {
    int i = 0;
    while (*p != '|' && *p != '\n' && *p != '\0' && i < width-1)
        buf[i++] = *p++;
    while (i < width) buf[i++] = '\0';
    while (*p != '|' && *p != '\n' && *p != '\0') ++p;
    if (*p == '|') ++p;
    return p;
}

// skip to end of line, return ptr past '\n'
static inline const char* next_line(const char* p) {
    while (*p != '\n' && *p != '\0') ++p;
    if (*p == '\n') ++p;
    return p;
}

// write a vector<T> to file
template<typename T>
static void write_col(const std::string& path, const std::vector<T>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) die(("fopen write: " + path).c_str());
    if (!data.empty())
        fwrite(data.data(), sizeof(T), data.size(), f);
    fclose(f);
}

// write fixed-width string column
static void write_fixed_col(const std::string& path, const std::vector<char>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) die(("fopen write: " + path).c_str());
    if (!data.empty())
        fwrite(data.data(), 1, data.size(), f);
    fclose(f);
}

// write 256-entry double lookup table
static void write_lookup(const std::string& path, const double lut[256]) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) die(("fopen lookup write: " + path).c_str());
    fwrite(lut, sizeof(double), 256, f);
    fclose(f);
}

// write dictionary file (one entry per line)
static void write_dict(const std::string& path,
                       const std::vector<std::string>& dict) {
    FILE* f = fopen(path.c_str(), "w");
    if (!f) die(("fopen dict: " + path).c_str());
    for (auto& s : dict) { fputs(s.c_str(), f); fputc('\n', f); }
    fclose(f);
}

// mmap a file for reading; returns {ptr, size}
static std::pair<const char*, size_t> mmap_file(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) die(("open: " + path).c_str());
    struct stat st; fstat(fd, &st);
    size_t sz = (size_t)st.st_size;
    if (sz == 0) { close(fd); return {nullptr, 0}; }
    void* p = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    if (p == MAP_FAILED) die(("mmap: " + path).c_str());
    madvise(p, sz, MADV_SEQUENTIAL);
    close(fd);
    return {(const char*)p, sz};
}

// ── lineitem ─────────────────────────────────────────────────────────────────

static void ingest_lineitem(const std::string& src, const std::string& dst) {
    auto [data, sz] = mmap_file(src + "/lineitem.tbl");
    const char* p   = data;
    const char* end = data + sz;

    const size_t RESERVE = 60000000UL;
    std::vector<int32_t> orderkey, partkey, suppkey;
    std::vector<double>  extprice;
    std::vector<uint8_t> quantity_b, discount_b, tax_b, returnflag_b, linestatus_b;
    std::vector<int32_t> shipdate;

    orderkey.reserve(RESERVE);   partkey.reserve(RESERVE);
    suppkey.reserve(RESERVE);    extprice.reserve(RESERVE);
    quantity_b.reserve(RESERVE); discount_b.reserve(RESERVE);
    tax_b.reserve(RESERVE);      returnflag_b.reserve(RESERVE);
    linestatus_b.reserve(RESERVE);
    shipdate.reserve(RESERVE);

    // lookup tables (256 doubles, indexed by uint8_t code)
    double qty_lut[256] = {}, disc_lut[256] = {}, tax_lut[256] = {};

    while (p < end) {
        if (*p == '\n') { ++p; continue; }
        int32_t ok, pk, sk, lnum;
        double qty_d, ep, disc_d, tax_d;
        int8_t rf, ls;

        p = parse_int32(p, ok);
        p = parse_int32(p, pk);
        p = parse_int32(p, sk);
        p = parse_int32(p, lnum);     // linenumber – skip
        p = parse_double(p, qty_d);
        p = parse_double(p, ep);
        p = parse_double(p, disc_d);
        p = parse_double(p, tax_d);
        p = parse_char1(p, rf);
        p = parse_char1(p, ls);
        // shipdate
        int32_t sd = parse_date(p); p += 10; if (*p=='|') ++p;
        // commitdate, receiptdate, shipinstruct, shipmode, comment – skip
        p = skip_field(p); p = skip_field(p);
        p = skip_field(p); p = skip_field(p); p = skip_field(p);
        p = next_line(p);

        uint8_t qb  = (uint8_t)std::lround(qty_d);
        uint8_t db  = (uint8_t)std::lround(disc_d * 100.0);
        uint8_t tb  = (uint8_t)std::lround(tax_d  * 100.0);

        qty_lut[qb]  = qty_d;
        disc_lut[db] = disc_d;
        tax_lut[tb]  = tax_d;

        orderkey.push_back(ok);
        partkey.push_back(pk);
        suppkey.push_back(sk);
        extprice.push_back(ep);
        quantity_b.push_back(qb);
        discount_b.push_back(db);
        tax_b.push_back(tb);
        returnflag_b.push_back((uint8_t)rf);
        linestatus_b.push_back((uint8_t)ls);
        shipdate.push_back(sd);
    }
    munmap((void*)data, sz);

    size_t N = orderkey.size();
    printf("[lineitem] parsed %zu rows\n", N); fflush(stdout);

    // Permutation sort by l_shipdate
    std::vector<uint32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0u);
    std::sort(perm.begin(), perm.end(),
              [&](uint32_t a, uint32_t b){ return shipdate[a] < shipdate[b]; });

    // apply permutation to all columns (reuse temp buffers)
    auto apply_perm_i32 = [&](std::vector<int32_t>& col) {
        std::vector<int32_t> tmp(N);
        for (size_t i=0;i<N;i++) tmp[i]=col[perm[i]];
        col.swap(tmp);
    };
    auto apply_perm_u8 = [&](std::vector<uint8_t>& col) {
        std::vector<uint8_t> tmp(N);
        for (size_t i=0;i<N;i++) tmp[i]=col[perm[i]];
        col.swap(tmp);
    };
    auto apply_perm_f64 = [&](std::vector<double>& col) {
        std::vector<double> tmp(N);
        for (size_t i=0;i<N;i++) tmp[i]=col[perm[i]];
        col.swap(tmp);
    };

    apply_perm_i32(orderkey); apply_perm_i32(partkey); apply_perm_i32(suppkey);
    apply_perm_i32(shipdate); apply_perm_f64(extprice);
    apply_perm_u8(quantity_b); apply_perm_u8(discount_b); apply_perm_u8(tax_b);
    apply_perm_u8(returnflag_b); apply_perm_u8(linestatus_b);

    // write columns in parallel
    std::string d = dst + "/lineitem/";
    std::vector<std::thread> tw;
    tw.emplace_back([&]{ write_col(d+"l_orderkey.bin",    orderkey);    });
    tw.emplace_back([&]{ write_col(d+"l_partkey.bin",     partkey);     });
    tw.emplace_back([&]{ write_col(d+"l_suppkey.bin",     suppkey);     });
    tw.emplace_back([&]{ write_col(d+"l_shipdate.bin",    shipdate);    });
    tw.emplace_back([&]{ write_col(d+"l_extendedprice.bin", extprice);  });
    tw.emplace_back([&]{ write_col(d+"l_quantity.bin",    quantity_b);  });
    tw.emplace_back([&]{ write_col(d+"l_discount.bin",    discount_b);  });
    tw.emplace_back([&]{ write_col(d+"l_tax.bin",         tax_b);       });
    tw.emplace_back([&]{ write_col(d+"l_returnflag.bin",  returnflag_b);});
    tw.emplace_back([&]{ write_col(d+"l_linestatus.bin",  linestatus_b);});
    tw.emplace_back([&]{ write_lookup(d+"l_quantity_lookup.bin", qty_lut);  });
    tw.emplace_back([&]{ write_lookup(d+"l_discount_lookup.bin", disc_lut); });
    tw.emplace_back([&]{ write_lookup(d+"l_tax_lookup.bin",      tax_lut);  });
    for (auto& t : tw) t.join();

    // Verification
    if (shipdate[0] <= 3000)
        { fprintf(stderr,"ERROR: lineitem shipdate[0]=%d not > 3000\n", shipdate[0]); std::exit(1); }
    if (extprice[0] == 0.0)
        { fprintf(stderr,"ERROR: lineitem extprice[0] is zero\n"); std::exit(1); }
    printf("[lineitem] done. shipdate[0]=%d extprice[0]=%.2f\n",
           shipdate[0], extprice[0]);
    fflush(stdout);
}

// ── orders ───────────────────────────────────────────────────────────────────

static void ingest_orders(const std::string& src, const std::string& dst) {
    auto [data, sz] = mmap_file(src + "/orders.tbl");
    const char* p   = data;
    const char* end = data + sz;

    const size_t RESERVE = 15000000UL;
    std::vector<int32_t> orderkey, custkey, shippriority, orderdate;
    std::vector<double>  totalprice;
    orderkey.reserve(RESERVE);   custkey.reserve(RESERVE);
    shippriority.reserve(RESERVE); orderdate.reserve(RESERVE);
    totalprice.reserve(RESERVE);

    while (p < end) {
        if (*p == '\n') { ++p; continue; }
        int32_t ok, ck, sp;
        double  tp;
        p = parse_int32(p, ok);
        p = parse_int32(p, ck);
        p = skip_field(p);           // o_orderstatus
        p = parse_double(p, tp);
        int32_t od = parse_date(p); p += 10; if (*p=='|') ++p;
        p = skip_field(p);           // o_orderpriority
        p = skip_field(p);           // o_clerk
        p = parse_int32(p, sp);
        p = skip_field(p);           // o_comment
        p = next_line(p);

        orderkey.push_back(ok);
        custkey.push_back(ck);
        totalprice.push_back(tp);
        orderdate.push_back(od);
        shippriority.push_back(sp);
    }
    munmap((void*)data, sz);

    size_t N = orderkey.size();
    printf("[orders] parsed %zu rows\n", N); fflush(stdout);

    // sort by o_orderdate
    std::vector<uint32_t> perm(N);
    std::iota(perm.begin(), perm.end(), 0u);
    std::sort(perm.begin(), perm.end(),
              [&](uint32_t a, uint32_t b){ return orderdate[a] < orderdate[b]; });

    auto apply_i32 = [&](std::vector<int32_t>& col) {
        std::vector<int32_t> tmp(N);
        for (size_t i=0;i<N;i++) tmp[i]=col[perm[i]];
        col.swap(tmp);
    };
    auto apply_f64 = [&](std::vector<double>& col) {
        std::vector<double> tmp(N);
        for (size_t i=0;i<N;i++) tmp[i]=col[perm[i]];
        col.swap(tmp);
    };
    apply_i32(orderkey); apply_i32(custkey); apply_i32(orderdate);
    apply_i32(shippriority); apply_f64(totalprice);

    std::string d = dst + "/orders/";
    std::vector<std::thread> tw;
    tw.emplace_back([&]{ write_col(d+"o_orderkey.bin",     orderkey);    });
    tw.emplace_back([&]{ write_col(d+"o_custkey.bin",      custkey);     });
    tw.emplace_back([&]{ write_col(d+"o_orderdate.bin",    orderdate);   });
    tw.emplace_back([&]{ write_col(d+"o_shippriority.bin", shippriority);});
    tw.emplace_back([&]{ write_col(d+"o_totalprice.bin",   totalprice);  });
    for (auto& t : tw) t.join();

    if (orderdate[0] <= 3000)
        { fprintf(stderr,"ERROR: orders orderdate[0]=%d not > 3000\n", orderdate[0]); std::exit(1); }
    if (totalprice[0] == 0.0)
        { fprintf(stderr,"ERROR: orders totalprice[0] is zero\n"); std::exit(1); }
    printf("[orders] done. orderdate[0]=%d totalprice[0]=%.2f\n",
           orderdate[0], totalprice[0]);
    fflush(stdout);
}

// ── customer ─────────────────────────────────────────────────────────────────

static void ingest_customer(const std::string& src, const std::string& dst) {
    auto [data, sz] = mmap_file(src + "/customer.tbl");
    const char* p   = data;
    const char* end = data + sz;

    const int NAME_W = 26;
    std::vector<int32_t> custkey, nationkey;
    std::vector<double>  acctbal;
    std::vector<char>    name_buf;     // NAME_W bytes per row
    std::vector<uint8_t> mktseg;

    // build dict dynamically
    std::vector<std::string>          dict;
    std::unordered_map<std::string,uint8_t> dict_map;

    custkey.reserve(1500000); nationkey.reserve(1500000);
    acctbal.reserve(1500000); name_buf.reserve(1500000*NAME_W);
    mktseg.reserve(1500000);

    char nbuf[NAME_W];
    while (p < end) {
        if (*p == '\n') { ++p; continue; }
        int32_t ck, nk;
        double  ab;
        p = parse_int32(p, ck);
        p = parse_fixed(p, nbuf, NAME_W);
        p = skip_field(p);          // c_address
        p = parse_int32(p, nk);
        p = skip_field(p);          // c_phone
        p = parse_double(p, ab);
        // c_mktsegment
        const char* ms_start = p;
        while (*p != '|' && *p != '\n') ++p;
        std::string ms(ms_start, p);
        if (*p=='|') ++p;
        p = skip_field(p);          // c_comment
        p = next_line(p);

        auto it = dict_map.find(ms);
        uint8_t code;
        if (it == dict_map.end()) {
            code = (uint8_t)dict.size();
            dict_map[ms] = code;
            dict.push_back(ms);
        } else {
            code = it->second;
        }

        custkey.push_back(ck);
        nationkey.push_back(nk);
        acctbal.push_back(ab);
        name_buf.insert(name_buf.end(), nbuf, nbuf+NAME_W);
        mktseg.push_back(code);
    }
    munmap((void*)data, sz);

    size_t N = custkey.size();
    printf("[customer] parsed %zu rows\n", N); fflush(stdout);

    std::string d = dst + "/customer/";
    std::vector<std::thread> tw;
    tw.emplace_back([&]{ write_col(d+"c_custkey.bin",    custkey);   });
    tw.emplace_back([&]{ write_col(d+"c_nationkey.bin",  nationkey); });
    tw.emplace_back([&]{ write_col(d+"c_acctbal.bin",    acctbal);   });
    tw.emplace_back([&]{ write_fixed_col(d+"c_name.bin", name_buf);  });
    tw.emplace_back([&]{ write_col(d+"c_mktsegment.bin", mktseg);    });
    tw.emplace_back([&]{ write_dict(d+"c_mktsegment_dict.txt", dict);});
    for (auto& t : tw) t.join();

    printf("[customer] done. dict size=%zu\n", dict.size()); fflush(stdout);
}

// ── part ─────────────────────────────────────────────────────────────────────

static void ingest_part(const std::string& src, const std::string& dst) {
    auto [data, sz] = mmap_file(src + "/part.tbl");
    const char* p   = data;
    const char* end = data + sz;

    const int NAME_W = 56;
    std::vector<int32_t> partkey;
    std::vector<char>    name_buf;
    partkey.reserve(2000000);
    name_buf.reserve(2000000*NAME_W);

    char nbuf[NAME_W];
    while (p < end) {
        if (*p == '\n') { ++p; continue; }
        int32_t pk;
        p = parse_int32(p, pk);
        p = parse_fixed(p, nbuf, NAME_W);
        // skip remaining 7 fields
        for (int i=0;i<7;i++) p = skip_field(p);
        p = next_line(p);

        partkey.push_back(pk);
        name_buf.insert(name_buf.end(), nbuf, nbuf+NAME_W);
    }
    munmap((void*)data, sz);

    size_t N = partkey.size();
    printf("[part] parsed %zu rows\n", N); fflush(stdout);

    std::string d = dst + "/part/";
    std::vector<std::thread> tw;
    tw.emplace_back([&]{ write_col(d+"p_partkey.bin", partkey);       });
    tw.emplace_back([&]{ write_fixed_col(d+"p_name.bin", name_buf);   });
    for (auto& t : tw) t.join();
    printf("[part] done.\n"); fflush(stdout);
}

// ── partsupp ─────────────────────────────────────────────────────────────────

static void ingest_partsupp(const std::string& src, const std::string& dst) {
    auto [data, sz] = mmap_file(src + "/partsupp.tbl");
    const char* p   = data;
    const char* end = data + sz;

    std::vector<int32_t> pspartkey, pssuppkey;
    std::vector<double>  supplycost;
    pspartkey.reserve(8000000); pssuppkey.reserve(8000000);
    supplycost.reserve(8000000);

    while (p < end) {
        if (*p == '\n') { ++p; continue; }
        int32_t pk, sk;
        double  sc;
        p = parse_int32(p, pk);
        p = parse_int32(p, sk);
        p = skip_field(p);          // ps_availqty
        p = parse_double(p, sc);
        p = skip_field(p);          // ps_comment
        p = next_line(p);

        pspartkey.push_back(pk);
        pssuppkey.push_back(sk);
        supplycost.push_back(sc);
    }
    munmap((void*)data, sz);

    size_t N = pspartkey.size();
    printf("[partsupp] parsed %zu rows\n", N); fflush(stdout);

    std::string d = dst + "/partsupp/";
    std::vector<std::thread> tw;
    tw.emplace_back([&]{ write_col(d+"ps_partkey.bin",    pspartkey);  });
    tw.emplace_back([&]{ write_col(d+"ps_suppkey.bin",    pssuppkey);  });
    tw.emplace_back([&]{ write_col(d+"ps_supplycost.bin", supplycost); });
    for (auto& t : tw) t.join();

    if (supplycost[0] == 0.0)
        { fprintf(stderr,"ERROR: partsupp supplycost[0] is zero\n"); std::exit(1); }
    printf("[partsupp] done. supplycost[0]=%.4f\n", supplycost[0]); fflush(stdout);
}

// ── supplier ─────────────────────────────────────────────────────────────────

static void ingest_supplier(const std::string& src, const std::string& dst) {
    auto [data, sz] = mmap_file(src + "/supplier.tbl");
    const char* p   = data;
    const char* end = data + sz;

    std::vector<int32_t> suppkey, nationkey;
    suppkey.reserve(100000); nationkey.reserve(100000);

    while (p < end) {
        if (*p == '\n') { ++p; continue; }
        int32_t sk, nk;
        p = parse_int32(p, sk);
        p = skip_field(p);   // s_name
        p = skip_field(p);   // s_address
        p = parse_int32(p, nk);
        p = skip_field(p);   // s_phone
        p = skip_field(p);   // s_acctbal
        p = skip_field(p);   // s_comment
        p = next_line(p);

        suppkey.push_back(sk);
        nationkey.push_back(nk);
    }
    munmap((void*)data, sz);
    printf("[supplier] parsed %zu rows\n", suppkey.size()); fflush(stdout);

    std::string d = dst + "/supplier/";
    std::vector<std::thread> tw;
    tw.emplace_back([&]{ write_col(d+"s_suppkey.bin",   suppkey);   });
    tw.emplace_back([&]{ write_col(d+"s_nationkey.bin", nationkey); });
    for (auto& t : tw) t.join();
    printf("[supplier] done.\n"); fflush(stdout);
}

// ── nation ───────────────────────────────────────────────────────────────────

static void ingest_nation(const std::string& src, const std::string& dst) {
    auto [data, sz] = mmap_file(src + "/nation.tbl");
    const char* p   = data;
    const char* end = data + sz;

    const int NAME_W = 26;
    std::vector<int32_t> nationkey;
    std::vector<char>    name_buf;

    char nbuf[NAME_W];
    while (p < end) {
        if (*p == '\n') { ++p; continue; }
        int32_t nk;
        p = parse_int32(p, nk);
        p = parse_fixed(p, nbuf, NAME_W);
        p = skip_field(p);   // n_regionkey
        p = skip_field(p);   // n_comment
        p = next_line(p);

        nationkey.push_back(nk);
        name_buf.insert(name_buf.end(), nbuf, nbuf+NAME_W);
    }
    munmap((void*)data, sz);
    printf("[nation] parsed %zu rows\n", nationkey.size()); fflush(stdout);

    std::string d = dst + "/nation/";
    write_col(d+"n_nationkey.bin", nationkey);
    write_fixed_col(d+"n_name.bin", name_buf);
    printf("[nation] done.\n"); fflush(stdout);
}

// ── region ───────────────────────────────────────────────────────────────────

static void ingest_region(const std::string& src, const std::string& dst) {
    auto [data, sz] = mmap_file(src + "/region.tbl");
    const char* p   = data;
    const char* end = data + sz;

    const int NAME_W = 26;
    std::vector<int32_t> regionkey;
    std::vector<char>    name_buf;

    char nbuf[NAME_W];
    while (p < end) {
        if (*p == '\n') { ++p; continue; }
        int32_t rk;
        p = parse_int32(p, rk);
        p = parse_fixed(p, nbuf, NAME_W);
        p = skip_field(p);   // r_comment
        p = next_line(p);

        regionkey.push_back(rk);
        name_buf.insert(name_buf.end(), nbuf, nbuf+NAME_W);
    }
    munmap((void*)data, sz);

    std::string d = dst + "/region/";
    write_col(d+"r_regionkey.bin", regionkey);
    write_fixed_col(d+"r_name.bin", name_buf);
    printf("[region] done.\n"); fflush(stdout);
}

// ── main ─────────────────────────────────────────────────────────────────────

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <src_dir> <dst_dir>\n", argv[0]);
        return 1;
    }
    std::string src(argv[1]), dst(argv[2]);

    // verify date formula
    {
        int32_t epoch = parse_date("1970-01-01");
        if (epoch != 0) {
            fprintf(stderr, "FATAL: parse_date('1970-01-01')=%d, expected 0\n", epoch);
            return 1;
        }
    }

    // create table subdirectories
    for (auto t : {"lineitem","orders","customer","part","partsupp",
                   "supplier","nation","region"})
        mkdir_p(dst + "/" + t);

    // run all table ingestion in parallel threads
    // (HDD sequential reads per table; parallel mainly for CPU work)
    printf("Starting ingestion from %s -> %s\n", src.c_str(), dst.c_str());
    fflush(stdout);

    // lineitem and orders are large — start them first sequentially for I/O
    // then do smaller tables
    std::thread t_lineitem([&]{ ingest_lineitem(src, dst); });
    std::thread t_orders  ([&]{ ingest_orders(src, dst);   });
    // smaller tables concurrently
    std::thread t_customer([&]{ ingest_customer(src, dst); });
    std::thread t_part    ([&]{ ingest_part(src, dst);     });
    std::thread t_partsupp([&]{ ingest_partsupp(src, dst); });
    std::thread t_supplier([&]{ ingest_supplier(src, dst); });
    std::thread t_nation  ([&]{ ingest_nation(src, dst);   });
    std::thread t_region  ([&]{ ingest_region(src, dst);   });

    t_lineitem.join(); t_orders.join();
    t_customer.join(); t_part.join();
    t_partsupp.join(); t_supplier.join();
    t_nation.join();   t_region.join();

    printf("\nAll tables ingested successfully.\n");
    return 0;
}
