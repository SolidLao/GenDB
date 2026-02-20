// ingest.cpp  — TPC-H SF10 binary columnar ingestion
// Compile: g++ -O2 -std=c++17 -Wall -lpthread -o ingest ingest.cpp

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <cmath>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <algorithm>
#include <numeric>
#include <map>
#include <stdexcept>
#include <filesystem>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

static const int N_THREADS = 32;
static const int BLOCK_SIZE = 100000;
static const size_t WRITE_BUF = 4 * 1024 * 1024; // 4MB

// ─── Date parsing ────────────────────────────────────────────────────────────
// Returns days since 1970-01-01. Self-test: 1970-01-01 → 0.
static int32_t parse_date(const char* s) {
    int y = (s[0]-'0')*1000 + (s[1]-'0')*100 + (s[2]-'0')*10 + (s[3]-'0');
    int m = (s[5]-'0')*10  + (s[6]-'0');
    int d = (s[8]-'0')*10  + (s[9]-'0');
    // cumulative days before each month (non-leap)
    static const int md[12] = {0,31,59,90,120,151,181,212,243,273,304,334};
    int year_days = (y-1970)*365 + (y-1969)/4 - (y-1901)/100 + (y-1601)/400;
    bool leap = (y%4==0 && (y%100!=0 || y%400==0));
    return (int32_t)(year_days + md[m-1] + (m>2 && leap ? 1 : 0) + (d-1));
}

// ─── Fast field parsers ──────────────────────────────────────────────────────
static inline int32_t parse_int(const char* p, const char** end) {
    int32_t v = 0; bool neg = (*p=='-'); if (neg) ++p;
    while (*p >= '0' && *p <= '9') v = v*10 + (*p++ - '0');
    *end = p; return neg ? -v : v;
}
static inline double parse_double(const char* p, const char** end) {
    char* e; double v = strtod(p, &e); *end = e; return v;
}
static inline const char* skip_field(const char* p) {
    while (*p != '|' && *p != '\n' && *p != '\0') ++p;
    if (*p == '|') ++p;
    return p;
}
static inline const char* next_field(const char* p) {
    while (*p != '|' && *p != '\n' && *p != '\0') ++p;
    if (*p == '|') ++p; return p;
}
static inline size_t field_len(const char* p) {
    const char* s = p; while (*p != '|' && *p != '\n' && *p != '\0') ++p;
    return (size_t)(p - s);
}

// ─── mmap helper ─────────────────────────────────────────────────────────────
struct MmapFile {
    const char* data = nullptr; size_t size = 0; int fd = -1;
    MmapFile(const std::string& path) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) throw std::runtime_error("Cannot open: " + path);
        struct stat st; fstat(fd, &st); size = (size_t)st.st_size;
        data = (const char*)mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) throw std::runtime_error("mmap failed: " + path);
        madvise((void*)data, size, MADV_SEQUENTIAL);
    }
    ~MmapFile() { if (data && data!=MAP_FAILED) munmap((void*)data,size); if(fd>=0) close(fd); }
};

// ─── Buffered column writer ───────────────────────────────────────────────────
static void write_col_file(const std::string& path, const void* data, size_t bytes) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot write: " + path);
    setvbuf(f, nullptr, _IOFBF, WRITE_BUF);
    const char* p = (const char*)data; size_t rem = bytes;
    while (rem > 0) {
        size_t w = fwrite(p, 1, rem, f);
        if (w == 0) { fclose(f); throw std::runtime_error("Write error: " + path); }
        p += w; rem -= w;
    }
    fclose(f);
}

template<typename T>
static void write_col(const std::string& path, const std::vector<T>& v) {
    write_col_file(path, v.data(), v.size() * sizeof(T));
}

static void write_dict(const std::string& path, const std::vector<std::string>& dict) {
    FILE* f = fopen(path.c_str(), "w");
    if (!f) throw std::runtime_error("Cannot write dict: " + path);
    for (size_t i = 0; i < dict.size(); ++i) fprintf(f, "%zu=%s\n", i, dict[i].c_str());
    fclose(f);
}

// Build dict from a set of string values; returns sorted dict and lookup map
static std::vector<std::string> build_dict(const std::vector<std::string>& vals,
                                            std::map<std::string,uint8_t>& lookup) {
    std::vector<std::string> dict = vals;
    std::sort(dict.begin(), dict.end());
    dict.erase(std::unique(dict.begin(), dict.end()), dict.end());
    for (uint8_t i = 0; i < (uint8_t)dict.size(); ++i) lookup[dict[i]] = i;
    return dict;
}

// ─── Find chunk boundaries at newlines ───────────────────────────────────────
static std::vector<size_t> chunk_offsets(const char* data, size_t size, int nthreads) {
    std::vector<size_t> offs;
    offs.push_back(0);
    for (int t = 1; t < nthreads; ++t) {
        size_t pos = (size_t)t * size / (size_t)nthreads;
        while (pos < size && data[pos] != '\n') ++pos;
        if (pos < size) ++pos; // skip past newline
        offs.push_back(pos);
    }
    offs.push_back(size);
    return offs;
}

// ─── LINEITEM ingestion ───────────────────────────────────────────────────────
// Fields: orderkey|partkey|suppkey|linenumber|quantity|extendedprice|discount|tax|
//         returnflag|linestatus|shipdate|commitdate|receiptdate|shipinstruct|shipmode|comment|
struct LineitemRow {
    int32_t orderkey, partkey, suppkey;
    double   quantity, extprice, discount, tax;
    char     returnflag, linestatus;
    int32_t  shipdate;
};

static void ingest_lineitem(const std::string& in_dir, const std::string& out_dir) {
    fprintf(stderr, "[lineitem] start\n");
    MmapFile mf(in_dir + "/lineitem.tbl");
    auto chunks = chunk_offsets(mf.data, mf.size, N_THREADS);

    // Per-thread row vectors
    std::vector<std::vector<LineitemRow>> thread_rows(N_THREADS);
    // Pre-size estimate per thread
    size_t est_per = 59986052 / N_THREADS + 1024;
    for (auto& v : thread_rows) v.reserve(est_per);

    std::vector<std::thread> threads;
    for (int t = 0; t < N_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + chunks[t];
            const char* end = mf.data + chunks[t+1];
            auto& rows = thread_rows[t];
            while (p < end) {
                if (*p == '\n' || *p == '\0') { ++p; continue; }
                LineitemRow r;
                const char* fe;
                r.orderkey = parse_int(p, &fe); p = next_field(fe);
                r.partkey  = parse_int(p, &fe); p = next_field(fe);
                r.suppkey  = parse_int(p, &fe); p = next_field(fe);
                p = skip_field(p); // linenumber skip
                r.quantity = parse_double(p, &fe); p = next_field(fe);
                r.extprice = parse_double(p, &fe); p = next_field(fe);
                r.discount = parse_double(p, &fe); p = next_field(fe);
                r.tax      = parse_double(p, &fe); p = next_field(fe);
                r.returnflag = *p; p = skip_field(p);
                r.linestatus = *p; p = skip_field(p);
                r.shipdate = parse_date(p); p = skip_field(p);
                // skip remaining fields
                p = skip_field(p); // commitdate
                p = skip_field(p); // receiptdate
                p = skip_field(p); // shipinstruct
                p = skip_field(p); // shipmode
                p = skip_field(p); // comment
                while (p < end && *p != '\n') ++p;
                if (p < end) ++p;
                rows.push_back(r);
            }
        });
    }
    for (auto& th : threads) th.join();

    // Concatenate
    size_t total = 0;
    for (auto& v : thread_rows) total += v.size();
    fprintf(stderr, "[lineitem] parsed %zu rows, sorting by l_shipdate...\n", total);

    std::vector<LineitemRow> all_rows;
    all_rows.reserve(total);
    for (auto& v : thread_rows) {
        all_rows.insert(all_rows.end(), v.begin(), v.end());
        std::vector<LineitemRow>().swap(v);
    }

    // Sort by shipdate
    std::vector<uint32_t> perm(total);
    std::iota(perm.begin(), perm.end(), 0u);
    std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b){
        return all_rows[a].shipdate < all_rows[b].shipdate;
    });
    fprintf(stderr, "[lineitem] sort done, writing columns...\n");

    // Scatter columns
    std::vector<int32_t> c_orderkey(total), c_partkey(total), c_suppkey(total), c_shipdate(total);
    std::vector<double>  c_qty(total), c_price(total), c_disc(total), c_tax(total);
    std::vector<uint8_t> c_rflag(total), c_lstatus(total);

    // Build returnflag dict: A→0, N→1, R→2; linestatus: F→0, O→1
    // Apply permutation (parallel)
    std::vector<std::thread> wthreads;
    int wt = std::min(N_THREADS, 16);
    size_t chunk = (total + wt - 1) / wt;
    for (int t = 0; t < wt; ++t) {
        wthreads.emplace_back([&, t]() {
            size_t s = (size_t)t * chunk, e = std::min(s + chunk, total);
            for (size_t i = s; i < e; ++i) {
                const auto& r = all_rows[perm[i]];
                c_orderkey[i] = r.orderkey; c_partkey[i] = r.partkey;
                c_suppkey[i]  = r.suppkey;  c_qty[i]     = r.quantity;
                c_price[i]    = r.extprice; c_disc[i]    = r.discount;
                c_tax[i]      = r.tax;       c_shipdate[i]= r.shipdate;
                // returnflag dict: A=0, N=1, R=2
                c_rflag[i] = (r.returnflag == 'A') ? 0 : (r.returnflag == 'N') ? 1 : 2;
                // linestatus dict: F=0, O=1
                c_lstatus[i] = (r.linestatus == 'F') ? 0 : 1;
            }
        });
    }
    for (auto& th : wthreads) th.join();

    // Free all_rows and perm
    { std::vector<LineitemRow>().swap(all_rows); std::vector<uint32_t>().swap(perm); }

    std::string tdir = out_dir + "/lineitem";
    fs::create_directories(tdir);

    // Write columns in parallel
    std::vector<std::thread> wts2;
    auto launch = [&](auto fn) { wts2.emplace_back(fn); };
    launch([&](){ write_col(tdir+"/l_orderkey.bin", c_orderkey); });
    launch([&](){ write_col(tdir+"/l_partkey.bin",  c_partkey);  });
    launch([&](){ write_col(tdir+"/l_suppkey.bin",  c_suppkey);  });
    launch([&](){ write_col(tdir+"/l_quantity.bin", c_qty);      });
    launch([&](){ write_col(tdir+"/l_extendedprice.bin", c_price); });
    launch([&](){ write_col(tdir+"/l_discount.bin", c_disc);     });
    launch([&](){ write_col(tdir+"/l_tax.bin",      c_tax);      });
    launch([&](){ write_col(tdir+"/l_returnflag.bin",  c_rflag);   });
    launch([&](){ write_col(tdir+"/l_linestatus.bin",  c_lstatus); });
    launch([&](){ write_col(tdir+"/l_shipdate.bin", c_shipdate); });
    for (auto& th : wts2) th.join();

    // Write dict files
    write_dict(tdir+"/l_returnflag_dict.txt", {"A","N","R"});
    write_dict(tdir+"/l_linestatus_dict.txt", {"F","O"});

    fprintf(stderr, "[lineitem] done (%zu rows)\n", total);
}

// ─── ORDERS ingestion ─────────────────────────────────────────────────────────
// Fields: orderkey|custkey|orderstatus|totalprice|orderdate|orderpriority|clerk|shippriority|comment|
static void ingest_orders(const std::string& in_dir, const std::string& out_dir) {
    fprintf(stderr, "[orders] start\n");
    MmapFile mf(in_dir + "/orders.tbl");
    auto chunks = chunk_offsets(mf.data, mf.size, N_THREADS);

    std::vector<std::vector<int32_t>> t_okey(N_THREADS), t_ckey(N_THREADS),
                                      t_odate(N_THREADS), t_ship(N_THREADS);
    std::vector<std::vector<double>>  t_price(N_THREADS);
    size_t est = 15000000 / N_THREADS + 1024;
    for (int t = 0; t < N_THREADS; ++t) {
        t_okey[t].reserve(est); t_ckey[t].reserve(est); t_odate[t].reserve(est);
        t_ship[t].reserve(est); t_price[t].reserve(est);
    }

    std::vector<std::thread> threads;
    for (int t = 0; t < N_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + chunks[t];
            const char* end = mf.data + chunks[t+1];
            while (p < end) {
                if (*p == '\n' || *p == '\0') { ++p; continue; }
                const char* fe;
                int32_t okey = parse_int(p, &fe);    p = next_field(fe);
                int32_t ckey = parse_int(p, &fe);    p = next_field(fe);
                p = skip_field(p); // orderstatus
                double price = parse_double(p, &fe); p = next_field(fe);
                int32_t odate = parse_date(p);       p = skip_field(p);
                p = skip_field(p); // orderpriority
                p = skip_field(p); // clerk
                int32_t ship = parse_int(p, &fe);   p = next_field(fe);
                p = skip_field(p); // comment
                while (p < end && *p != '\n') ++p;
                if (p < end) ++p;
                t_okey[t].push_back(okey); t_ckey[t].push_back(ckey);
                t_price[t].push_back(price); t_odate[t].push_back(odate);
                t_ship[t].push_back(ship);
            }
        });
    }
    for (auto& th : threads) th.join();

    size_t total = 0; for (auto& v : t_okey) total += v.size();
    std::vector<int32_t> c_okey(total), c_ckey(total), c_odate(total), c_ship(total);
    std::vector<double>  c_price(total);
    size_t off = 0;
    for (int t = 0; t < N_THREADS; ++t) {
        size_t n = t_okey[t].size();
        memcpy(&c_okey[off],  t_okey[t].data(),  n*4);
        memcpy(&c_ckey[off],  t_ckey[t].data(),  n*4);
        memcpy(&c_price[off], t_price[t].data(),  n*8);
        memcpy(&c_odate[off], t_odate[t].data(),  n*4);
        memcpy(&c_ship[off],  t_ship[t].data(),   n*4);
        off += n;
    }

    std::string tdir = out_dir + "/orders";
    fs::create_directories(tdir);
    std::vector<std::thread> wts;
    wts.emplace_back([&](){ write_col(tdir+"/o_orderkey.bin",    c_okey);  });
    wts.emplace_back([&](){ write_col(tdir+"/o_custkey.bin",     c_ckey);  });
    wts.emplace_back([&](){ write_col(tdir+"/o_totalprice.bin",  c_price); });
    wts.emplace_back([&](){ write_col(tdir+"/o_orderdate.bin",   c_odate); });
    wts.emplace_back([&](){ write_col(tdir+"/o_shippriority.bin",c_ship);  });
    for (auto& th : wts) th.join();
    fprintf(stderr, "[orders] done (%zu rows)\n", total);
}

// ─── CUSTOMER ingestion ───────────────────────────────────────────────────────
// Fields: custkey|name|address|nationkey|phone|acctbal|mktsegment|comment|
static void ingest_customer(const std::string& in_dir, const std::string& out_dir) {
    fprintf(stderr, "[customer] start\n");
    MmapFile mf(in_dir + "/customer.tbl");
    // c_mktsegment known values: AUTOMOBILE=0, BUILDING=1, FURNITURE=2, HOUSEHOLD=3, MACHINERY=4
    std::map<std::string,uint8_t> seg_map = {
        {"AUTOMOBILE",0},{"BUILDING",1},{"FURNITURE",2},{"HOUSEHOLD",3},{"MACHINERY",4}
    };
    auto chunks = chunk_offsets(mf.data, mf.size, N_THREADS);

    std::vector<std::vector<int32_t>>  t_ckey(N_THREADS);
    std::vector<std::vector<uint8_t>>  t_seg(N_THREADS);
    std::vector<std::vector<uint32_t>> t_name_off(N_THREADS); // offsets within thread buffer
    std::vector<std::string>           t_name_buf(N_THREADS);
    size_t est = 1500000 / N_THREADS + 1024;
    for (int t = 0; t < N_THREADS; ++t) {
        t_ckey[t].reserve(est); t_seg[t].reserve(est);
        t_name_off[t].reserve(est + 1); t_name_off[t].push_back(0);
        t_name_buf[t].reserve(est * 20);
    }

    std::vector<std::thread> threads;
    for (int t = 0; t < N_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + chunks[t];
            const char* end = mf.data + chunks[t+1];
            while (p < end) {
                if (*p == '\n' || *p == '\0') { ++p; continue; }
                const char* fe;
                int32_t ckey = parse_int(p, &fe); p = next_field(fe);
                // name
                size_t nlen = field_len(p);
                t_name_buf[t].append(p, nlen);
                t_name_off[t].push_back((uint32_t)t_name_buf[t].size());
                p = skip_field(p);
                p = skip_field(p); // address
                p = skip_field(p); // nationkey
                p = skip_field(p); // phone
                p = skip_field(p); // acctbal
                // mktsegment
                size_t slen = field_len(p);
                std::string seg(p, slen); p = skip_field(p);
                p = skip_field(p); // comment
                while (p < end && *p != '\n') ++p;
                if (p < end) ++p;
                t_ckey[t].push_back(ckey);
                t_seg[t].push_back(seg_map.count(seg) ? seg_map[seg] : 0);
            }
        });
    }
    for (auto& th : threads) th.join();

    size_t total = 0; for (auto& v : t_ckey) total += v.size();
    std::vector<int32_t>  c_ckey(total);
    std::vector<uint8_t>  c_seg(total);
    std::vector<uint32_t> c_name_off; c_name_off.reserve(total + 1); c_name_off.push_back(0);
    std::string c_name_data; c_name_data.reserve(total * 20);

    size_t off = 0;
    for (int t = 0; t < N_THREADS; ++t) {
        size_t n = t_ckey[t].size();
        memcpy(&c_ckey[off], t_ckey[t].data(), n*4);
        memcpy(&c_seg[off],  t_seg[t].data(),  n*1);
        uint32_t base = (uint32_t)c_name_data.size();
        c_name_data += t_name_buf[t];
        for (size_t i = 1; i <= n; ++i)
            c_name_off.push_back(base + t_name_off[t][i]);
        off += n;
    }

    std::string tdir = out_dir + "/customer";
    fs::create_directories(tdir);
    std::vector<std::thread> wts;
    wts.emplace_back([&](){ write_col(tdir+"/c_custkey.bin",        c_ckey);   });
    wts.emplace_back([&](){ write_col(tdir+"/c_mktsegment.bin",     c_seg);    });
    wts.emplace_back([&](){ write_col(tdir+"/c_name_offsets.bin",   c_name_off); });
    wts.emplace_back([&](){ write_col_file(tdir+"/c_name_data.bin",
                                           c_name_data.data(), c_name_data.size()); });
    for (auto& th : wts) th.join();
    write_dict(tdir+"/c_mktsegment_dict.txt", {"AUTOMOBILE","BUILDING","FURNITURE","HOUSEHOLD","MACHINERY"});
    fprintf(stderr, "[customer] done (%zu rows)\n", total);
}

// ─── PART ingestion ───────────────────────────────────────────────────────────
// Fields: partkey|name|mfgr|brand|type|size|container|retailprice|comment|
static void ingest_part(const std::string& in_dir, const std::string& out_dir) {
    fprintf(stderr, "[part] start\n");
    MmapFile mf(in_dir + "/part.tbl");
    auto chunks = chunk_offsets(mf.data, mf.size, N_THREADS);

    std::vector<std::vector<int32_t>>  t_pkey(N_THREADS);
    std::vector<std::vector<uint32_t>> t_name_off(N_THREADS);
    std::vector<std::string>           t_name_buf(N_THREADS);
    size_t est = 2000000 / N_THREADS + 1024;
    for (int t = 0; t < N_THREADS; ++t) {
        t_pkey[t].reserve(est);
        t_name_off[t].reserve(est + 1); t_name_off[t].push_back(0);
        t_name_buf[t].reserve(est * 40);
    }

    std::vector<std::thread> threads;
    for (int t = 0; t < N_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + chunks[t];
            const char* end = mf.data + chunks[t+1];
            while (p < end) {
                if (*p == '\n' || *p == '\0') { ++p; continue; }
                const char* fe;
                int32_t pkey = parse_int(p, &fe); p = next_field(fe);
                size_t nlen = field_len(p);
                t_name_buf[t].append(p, nlen);
                t_name_off[t].push_back((uint32_t)t_name_buf[t].size());
                p = skip_field(p);
                // skip remaining fields
                for (int f = 0; f < 7; ++f) p = skip_field(p);
                while (p < end && *p != '\n') ++p;
                if (p < end) ++p;
                t_pkey[t].push_back(pkey);
            }
        });
    }
    for (auto& th : threads) th.join();

    size_t total = 0; for (auto& v : t_pkey) total += v.size();
    std::vector<int32_t>  c_pkey(total);
    std::vector<uint32_t> c_name_off; c_name_off.reserve(total + 1); c_name_off.push_back(0);
    std::string c_name_data; c_name_data.reserve(total * 40);

    size_t off = 0;
    for (int t = 0; t < N_THREADS; ++t) {
        size_t n = t_pkey[t].size();
        memcpy(&c_pkey[off], t_pkey[t].data(), n*4);
        uint32_t base = (uint32_t)c_name_data.size();
        c_name_data += t_name_buf[t];
        for (size_t i = 1; i <= n; ++i)
            c_name_off.push_back(base + t_name_off[t][i]);
        off += n;
    }

    std::string tdir = out_dir + "/part";
    fs::create_directories(tdir);
    std::vector<std::thread> wts;
    wts.emplace_back([&](){ write_col(tdir+"/p_partkey.bin",      c_pkey);    });
    wts.emplace_back([&](){ write_col(tdir+"/p_name_offsets.bin", c_name_off); });
    wts.emplace_back([&](){ write_col_file(tdir+"/p_name_data.bin",
                                           c_name_data.data(), c_name_data.size()); });
    for (auto& th : wts) th.join();
    fprintf(stderr, "[part] done (%zu rows)\n", total);
}

// ─── PARTSUPP ingestion ───────────────────────────────────────────────────────
// Fields: partkey|suppkey|availqty|supplycost|comment|
static void ingest_partsupp(const std::string& in_dir, const std::string& out_dir) {
    fprintf(stderr, "[partsupp] start\n");
    MmapFile mf(in_dir + "/partsupp.tbl");
    auto chunks = chunk_offsets(mf.data, mf.size, N_THREADS);

    std::vector<std::vector<int32_t>> t_pkey(N_THREADS), t_skey(N_THREADS);
    std::vector<std::vector<double>>  t_cost(N_THREADS);
    size_t est = 8000000 / N_THREADS + 1024;
    for (int t = 0; t < N_THREADS; ++t) {
        t_pkey[t].reserve(est); t_skey[t].reserve(est); t_cost[t].reserve(est);
    }

    std::vector<std::thread> threads;
    for (int t = 0; t < N_THREADS; ++t) {
        threads.emplace_back([&, t]() {
            const char* p = mf.data + chunks[t];
            const char* end = mf.data + chunks[t+1];
            while (p < end) {
                if (*p == '\n' || *p == '\0') { ++p; continue; }
                const char* fe;
                int32_t pk = parse_int(p, &fe);    p = next_field(fe);
                int32_t sk = parse_int(p, &fe);    p = next_field(fe);
                p = skip_field(p); // availqty
                double cost = parse_double(p, &fe); p = next_field(fe);
                p = skip_field(p); // comment
                while (p < end && *p != '\n') ++p;
                if (p < end) ++p;
                t_pkey[t].push_back(pk); t_skey[t].push_back(sk); t_cost[t].push_back(cost);
            }
        });
    }
    for (auto& th : threads) th.join();

    size_t total = 0; for (auto& v : t_pkey) total += v.size();
    std::vector<int32_t> c_pkey(total), c_skey(total);
    std::vector<double>  c_cost(total);
    size_t off = 0;
    for (int t = 0; t < N_THREADS; ++t) {
        size_t n = t_pkey[t].size();
        memcpy(&c_pkey[off], t_pkey[t].data(), n*4);
        memcpy(&c_skey[off], t_skey[t].data(), n*4);
        memcpy(&c_cost[off], t_cost[t].data(), n*8);
        off += n;
    }

    std::string tdir = out_dir + "/partsupp";
    fs::create_directories(tdir);
    std::vector<std::thread> wts;
    wts.emplace_back([&](){ write_col(tdir+"/ps_partkey.bin",    c_pkey); });
    wts.emplace_back([&](){ write_col(tdir+"/ps_suppkey.bin",    c_skey); });
    wts.emplace_back([&](){ write_col(tdir+"/ps_supplycost.bin", c_cost); });
    for (auto& th : wts) th.join();
    fprintf(stderr, "[partsupp] done (%zu rows)\n", total);
}

// ─── SUPPLIER ingestion ───────────────────────────────────────────────────────
// Fields: suppkey|name|address|nationkey|phone|acctbal|comment|
static void ingest_supplier(const std::string& in_dir, const std::string& out_dir) {
    fprintf(stderr, "[supplier] start\n");
    MmapFile mf(in_dir + "/supplier.tbl");
    std::vector<int32_t> c_skey, c_nkey;
    const char* p = mf.data; const char* end = p + mf.size;
    while (p < end) {
        if (*p == '\n' || *p == '\0') { ++p; continue; }
        const char* fe;
        int32_t sk = parse_int(p, &fe); p = next_field(fe);
        p = skip_field(p); // name
        p = skip_field(p); // address
        int32_t nk = parse_int(p, &fe); p = next_field(fe);
        p = skip_field(p); // phone
        p = skip_field(p); // acctbal
        p = skip_field(p); // comment
        while (p < end && *p != '\n') ++p; if (p < end) ++p;
        c_skey.push_back(sk); c_nkey.push_back(nk);
    }
    std::string tdir = out_dir + "/supplier";
    fs::create_directories(tdir);
    write_col(tdir+"/s_suppkey.bin",   c_skey);
    write_col(tdir+"/s_nationkey.bin", c_nkey);
    fprintf(stderr, "[supplier] done (%zu rows)\n", c_skey.size());
}

// ─── NATION ingestion ─────────────────────────────────────────────────────────
// Fields: nationkey|name|regionkey|comment|
static void ingest_nation(const std::string& in_dir, const std::string& out_dir) {
    fprintf(stderr, "[nation] start\n");
    MmapFile mf(in_dir + "/nation.tbl");
    std::vector<int32_t> c_nkey;
    std::vector<std::string> names_in_order;
    std::vector<std::string> unique_names;
    const char* p = mf.data; const char* end = p + mf.size;
    while (p < end) {
        if (*p == '\n' || *p == '\0') { ++p; continue; }
        const char* fe;
        int32_t nk = parse_int(p, &fe); p = next_field(fe);
        size_t nlen = field_len(p);
        std::string name(p, nlen); p = skip_field(p);
        p = skip_field(p); p = skip_field(p); // regionkey, comment
        while (p < end && *p != '\n') ++p; if (p < end) ++p;
        c_nkey.push_back(nk);
        names_in_order.push_back(name);
        unique_names.push_back(name);
    }
    std::map<std::string,uint8_t> dict_map;
    auto dict = build_dict(unique_names, dict_map);
    std::vector<uint8_t> c_name;
    for (auto& n : names_in_order) c_name.push_back(dict_map[n]);
    std::string tdir = out_dir + "/nation";
    fs::create_directories(tdir);
    write_col(tdir+"/n_nationkey.bin", c_nkey);
    write_col(tdir+"/n_name.bin",      c_name);
    write_dict(tdir+"/n_name_dict.txt", dict);
    fprintf(stderr, "[nation] done (%zu rows)\n", c_nkey.size());
}

// ─── Post-ingestion verification ─────────────────────────────────────────────
static void verify(const std::string& out_dir) {
    fprintf(stderr, "[verify] checking date values and decimal values...\n");

    // Check lineitem l_shipdate > 3000 (1970 + ~8 years = ~3000+ epoch days)
    {
        FILE* f = fopen((out_dir+"/lineitem/l_shipdate.bin").c_str(), "rb");
        if (!f) { fprintf(stderr, "VERIFY FAIL: cannot open l_shipdate.bin\n"); exit(1); }
        int32_t v; fread(&v, 4, 1, f); fclose(f);
        if (v <= 3000) {
            fprintf(stderr, "VERIFY FAIL: l_shipdate[0]=%d, expected >3000\n", v); exit(1);
        }
        fprintf(stderr, "[verify] l_shipdate[0]=%d OK\n", v);
    }
    // Check o_orderdate > 3000
    {
        FILE* f = fopen((out_dir+"/orders/o_orderdate.bin").c_str(), "rb");
        if (!f) { fprintf(stderr, "VERIFY FAIL: cannot open o_orderdate.bin\n"); exit(1); }
        int32_t v; fread(&v, 4, 1, f); fclose(f);
        if (v <= 3000) {
            fprintf(stderr, "VERIFY FAIL: o_orderdate[0]=%d, expected >3000\n", v); exit(1);
        }
        fprintf(stderr, "[verify] o_orderdate[0]=%d OK\n", v);
    }
    // Check l_extendedprice non-zero
    {
        FILE* f = fopen((out_dir+"/lineitem/l_extendedprice.bin").c_str(), "rb");
        if (!f) { fprintf(stderr, "VERIFY FAIL: cannot open l_extendedprice.bin\n"); exit(1); }
        double v; fread(&v, 8, 1, f); fclose(f);
        if (v == 0.0) {
            fprintf(stderr, "VERIFY FAIL: l_extendedprice[0]=0\n"); exit(1);
        }
        fprintf(stderr, "[verify] l_extendedprice[0]=%.2f OK\n", v);
    }
    // Check ps_supplycost non-zero
    {
        FILE* f = fopen((out_dir+"/partsupp/ps_supplycost.bin").c_str(), "rb");
        if (!f) { fprintf(stderr, "VERIFY FAIL: cannot open ps_supplycost.bin\n"); exit(1); }
        double v; fread(&v, 8, 1, f); fclose(f);
        if (v == 0.0) {
            fprintf(stderr, "VERIFY FAIL: ps_supplycost[0]=0\n"); exit(1);
        }
        fprintf(stderr, "[verify] ps_supplycost[0]=%.2f OK\n", v);
    }
    fprintf(stderr, "[verify] all checks PASSED\n");
}

// ─── main ─────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <input_dir> <output_dir>\n", argv[0]); return 1;
    }
    std::string in_dir = argv[1], out_dir = argv[2];
    fs::create_directories(out_dir);
    fs::create_directories(out_dir + "/indexes");

    // Self-test date parser
    if (parse_date("1970-01-01") != 0) {
        fprintf(stderr, "DATE SELF-TEST FAIL: 1970-01-01 should be 0\n"); return 1;
    }
    fprintf(stderr, "Date self-test: 1970-01-01=%d OK\n", parse_date("1970-01-01"));
    fprintf(stderr, "Date self-test: 1998-09-02=%d\n", parse_date("1998-09-02"));
    fprintf(stderr, "Date self-test: 1994-01-01=%d\n", parse_date("1994-01-01"));
    fprintf(stderr, "Date self-test: 1995-03-15=%d\n", parse_date("1995-03-15"));

    // Run smaller tables in parallel, then lineitem (big sort)
    std::vector<std::thread> par_threads;
    par_threads.emplace_back([&](){ ingest_nation(in_dir, out_dir);   });
    par_threads.emplace_back([&](){ ingest_supplier(in_dir, out_dir); });
    par_threads.emplace_back([&](){ ingest_customer(in_dir, out_dir); });
    par_threads.emplace_back([&](){ ingest_part(in_dir, out_dir);     });
    par_threads.emplace_back([&](){ ingest_orders(in_dir, out_dir);   });
    par_threads.emplace_back([&](){ ingest_partsupp(in_dir, out_dir); });
    for (auto& th : par_threads) th.join();

    // lineitem last (largest, needs sort)
    ingest_lineitem(in_dir, out_dir);

    verify(out_dir);
    fprintf(stderr, "Ingestion complete.\n");
    return 0;
}
