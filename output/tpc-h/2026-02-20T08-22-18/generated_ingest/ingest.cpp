// TPC-H Binary Columnar Data Ingestion
// Sorted: lineitem by l_shipdate, orders by o_orderdate
// Parallel: all tables concurrently, each table uses N_PARSE_THREADS parse threads

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <numeric>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std;

// ============ Constants ============
static constexpr int  BLOCK_SIZE      = 100000;
static constexpr int  N_PARSE_THREADS = 16;

// ============ Dict ============
struct Dict {
    unordered_map<string,int16_t> map;
    vector<string> vals;
    mutex mu;

    int16_t encode(const char* s, size_t len) {
        string key(s, len);
        lock_guard<mutex> lk(mu);
        auto it = map.find(key);
        if (it != map.end()) return it->second;
        int16_t code = (int16_t)vals.size();
        map[key] = code;
        vals.push_back(key);
        return code;
    }
    void save(const string& path) {
        ofstream f(path);
        for (size_t i = 0; i < vals.size(); i++)
            f << i << "=" << vals[i] << "\n";
    }
};

// ============ MmapFile ============
struct MmapFile {
    void*  ptr  = MAP_FAILED;
    size_t sz   = 0;
    MmapFile(const string& path) {
        int fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { perror(path.c_str()); exit(1); }
        struct stat st;
        if (fstat(fd, &st) < 0) { perror("fstat"); exit(1); }
        sz  = (size_t)st.st_size;
        ptr = mmap(nullptr, sz, PROT_READ, MAP_SHARED, fd, 0);
        if (ptr == MAP_FAILED) { perror("mmap"); exit(1); }
        madvise(ptr, sz, MADV_SEQUENTIAL);
        close(fd);
    }
    ~MmapFile() { if (ptr != MAP_FAILED) munmap(ptr, sz); }
    const char* data() const { return (const char*)ptr; }
};

// ============ Parse Helpers ============
inline int32_t pi(const char*& p) {   // parse int, skip trailing '|'
    bool neg = (*p == '-'); if (neg) ++p;
    int32_t v = 0;
    while (*p >= '0' && *p <= '9') v = v * 10 + (*p++ - '0');
    if (*p == '|') ++p;
    return neg ? -v : v;
}
inline double pd(const char*& p) {    // parse double, skip trailing '|'
    char* e; double v = strtod(p, &e); p = e;
    if (*p == '|') ++p;
    return v;
}
inline int32_t pdate(const char*& p) { // parse YYYY-MM-DD → epoch days, skip '|'
    int yr  = (p[0]-'0')*1000+(p[1]-'0')*100+(p[2]-'0')*10+(p[3]-'0');
    int mo  = (p[5]-'0')*10 + (p[6]-'0');
    int day = (p[8]-'0')*10 + (p[9]-'0');
    p += 10;
    if (*p == '|') ++p;
    // Self-test formula: parse_date("1970-01-01") = 0
    int y1 = yr - 1;
    int leaps = y1/4 - y1/100 + y1/400 - 477; // 477 = leaps up to 1969
    int days  = 365*(yr-1970) + leaps;
    static const int md[] = {0,31,28,31,30,31,30,31,31,30,31,30,31};
    bool leap = (yr%4==0 && yr%100!=0) || yr%400==0;
    for (int m=1; m<mo; ++m) { days += md[m]; if (m==2 && leap) ++days; }
    days += day - 1;
    return (int32_t)days;
}
inline int16_t pdict(const char*& p, Dict& d) {
    const char* s = p;
    while (*p != '|' && *p != '\n') ++p;
    int16_t code = d.encode(s, (size_t)(p - s));
    if (*p == '|') ++p;
    return code;
}
inline void sf(const char*& p) {      // skip field (past '|')
    while (*p != '|' && *p != '\n') ++p;
    if (*p == '|') ++p;
}
inline void seol(const char*& p) {    // skip to end-of-line (past '\n')
    while (*p != '\n') ++p;
    ++p;
}

// ============ Write Helpers ============
template<typename T>
static void wc(const string& path, const vector<T>& col) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(path.c_str()); exit(1); }
    static constexpr size_t BLEN = (1<<20) / sizeof(T);
    size_t N = col.size();
    for (size_t off = 0; off < N; off += BLEN) {
        size_t cnt = min(BLEN, N-off);
        fwrite(col.data()+off, sizeof(T), cnt, f);
    }
    fclose(f);
}
template<typename T>
static void wcp(const string& path, const vector<T>& col, const vector<uint32_t>& perm) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(path.c_str()); exit(1); }
    static constexpr size_t BLEN = (1<<20) / sizeof(T);
    vector<T> buf(BLEN);
    size_t N = perm.size();
    for (size_t off = 0; off < N; off += BLEN) {
        size_t cnt = min(BLEN, N-off);
        for (size_t i=0; i<cnt; ++i) buf[i] = col[perm[off+i]];
        fwrite(buf.data(), sizeof(T), cnt, f);
    }
    fclose(f);
}
static void wcharNp(const string& path, const vector<char>& data, int stride,
                    const vector<uint32_t>& perm) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(path.c_str()); exit(1); }
    size_t pb = (size_t)((1<<20)/stride);
    vector<char> buf(pb * stride);
    size_t N = perm.size();
    for (size_t off = 0; off < N; off += pb) {
        size_t cnt = min(pb, N-off);
        for (size_t i=0; i<cnt; ++i)
            memcpy(buf.data()+i*stride, data.data()+(size_t)perm[off+i]*stride, stride);
        fwrite(buf.data(), stride, cnt, f);
    }
    fclose(f);
}
static void wcharN(const string& path, const vector<char>& data, int stride) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(path.c_str()); exit(1); }
    size_t N = data.size() / stride;
    size_t pb = (size_t)((1<<20)/stride);
    for (size_t off=0; off<N; off+=pb) {
        size_t cnt = min(pb, N-off);
        fwrite(data.data()+off*stride, stride, cnt, f);
    }
    fclose(f);
}
static void wzonemap(const string& path, const vector<int32_t>& sc) {
    // sc is sorted; min per block = sc[start], max = sc[end-1]
    uint32_t N = (uint32_t)sc.size();
    uint32_t nb = (N + BLOCK_SIZE - 1) / BLOCK_SIZE;
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) { perror(path.c_str()); exit(1); }
    fwrite(&nb, 4, 1, f);
    for (uint32_t b=0; b<nb; ++b) {
        uint32_t s = b*BLOCK_SIZE, e = min(N, s+(uint32_t)BLOCK_SIZE);
        int32_t mn = sc[s], mx = sc[e-1];
        uint32_t nr = e - s;
        fwrite(&mn,4,1,f); fwrite(&mx,4,1,f); fwrite(&nr,4,1,f);
    }
    fclose(f);
}

// ============ Chunk Split ============
static vector<pair<const char*,const char*>>
split_chunks(const char* data, size_t sz, int n) {
    vector<pair<const char*,const char*>> res;
    const char* start = data;
    for (int i=0; i<n; ++i) {
        const char* end;
        if (i == n-1) {
            end = data + sz;
        } else {
            end = data + (size_t)(i+1) * (sz/n);
            while (end < data+sz && *end != '\n') ++end;
            if (end < data+sz) ++end; // include newline
        }
        if (start < end) res.push_back({start, end});
        else res.push_back({end, end}); // empty chunk
        start = end;
    }
    return res;
}

// ============ Merge Helper ============
template<typename T>
static void merge_vecs(vector<T>& dst, const vector<vector<T>>& srcs, const vector<size_t>& offs) {
    for (size_t t=0; t<srcs.size(); ++t)
        memcpy(dst.data()+offs[t], srcs[t].data(), srcs[t].size()*sizeof(T));
}
static void merge_chars(vector<char>& dst, const vector<vector<char>>& srcs,
                        const vector<size_t>& offs, int stride) {
    for (size_t t=0; t<srcs.size(); ++t)
        memcpy(dst.data()+offs[t]*stride, srcs[t].data(), srcs[t].size());
}

// ============ LINEITEM ============
static void ingest_lineitem(const string& data_dir, const string& out_dir) {
    auto t0 = chrono::steady_clock::now();
    fs::create_directories(out_dir+"/lineitem");

    Dict rfdict, lsdict, sidict, smdict;

    MmapFile mm(data_dir+"/lineitem.tbl");
    auto chunks = split_chunks(mm.data(), mm.sz, N_PARSE_THREADS);
    int NC = (int)chunks.size();

    struct LIChunk {
        vector<int32_t> ok,pk,sk,ln,sd,cd,rd;
        vector<double>  qty,ep,disc,tax;
        vector<int16_t> rf,ls,si,sm;
    };
    vector<LIChunk> tdata(NC);

    vector<thread> threads;
    threads.reserve(NC);
    for (int t=0; t<NC; ++t) {
        threads.emplace_back([&,t](){
            const char* ptr = chunks[t].first;
            const char* end = chunks[t].second;
            auto& D = tdata[t];
            while (ptr < end) {
                D.ok.push_back(pi(ptr));
                D.pk.push_back(pi(ptr));
                D.sk.push_back(pi(ptr));
                D.ln.push_back(pi(ptr));
                D.qty.push_back(pd(ptr));
                D.ep.push_back(pd(ptr));
                D.disc.push_back(pd(ptr));
                D.tax.push_back(pd(ptr));
                D.rf.push_back(pdict(ptr,rfdict));
                D.ls.push_back(pdict(ptr,lsdict));
                D.sd.push_back(pdate(ptr));
                D.cd.push_back(pdate(ptr));
                D.rd.push_back(pdate(ptr));
                D.si.push_back(pdict(ptr,sidict));
                D.sm.push_back(pdict(ptr,smdict));
                seol(ptr); // skip comment
            }
        });
    }
    for (auto& th : threads) th.join();

    // Compute offsets and total
    vector<size_t> offs(NC+1,0);
    for (int t=0; t<NC; ++t) offs[t+1] = offs[t] + tdata[t].ok.size();
    size_t N = offs[NC];
    cout << "lineitem: parsed " << N << " rows\n";

    // Allocate merged arrays
    vector<int32_t> ok(N),pk(N),sk(N),ln(N),sd(N),cd(N),rd(N);
    vector<double>  qty(N),ep(N),disc(N),tax(N);
    vector<int16_t> rf(N),ls(N),si(N),sm(N);
    for (int t=0; t<NC; ++t) {
        auto& D = tdata[t]; size_t o = offs[t];
        memcpy(ok.data()+o,   D.ok.data(),  D.ok.size()*4);
        memcpy(pk.data()+o,   D.pk.data(),  D.pk.size()*4);
        memcpy(sk.data()+o,   D.sk.data(),  D.sk.size()*4);
        memcpy(ln.data()+o,   D.ln.data(),  D.ln.size()*4);
        memcpy(sd.data()+o,   D.sd.data(),  D.sd.size()*4);
        memcpy(cd.data()+o,   D.cd.data(),  D.cd.size()*4);
        memcpy(rd.data()+o,   D.rd.data(),  D.rd.size()*4);
        memcpy(qty.data()+o,  D.qty.data(), D.qty.size()*8);
        memcpy(ep.data()+o,   D.ep.data(),  D.ep.size()*8);
        memcpy(disc.data()+o, D.disc.data(),D.disc.size()*8);
        memcpy(tax.data()+o,  D.tax.data(), D.tax.size()*8);
        memcpy(rf.data()+o,   D.rf.data(),  D.rf.size()*2);
        memcpy(ls.data()+o,   D.ls.data(),  D.ls.size()*2);
        memcpy(si.data()+o,   D.si.data(),  D.si.size()*2);
        memcpy(sm.data()+o,   D.sm.data(),  D.sm.size()*2);
        D = LIChunk{}; // free memory
    }
    tdata.clear();

    // Sort by l_shipdate
    vector<uint32_t> perm(N);
    iota(perm.begin(), perm.end(), 0u);
    sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b){ return sd[a] < sd[b]; });

    // Write columns
    string d = out_dir + "/lineitem/";
    auto t1 = chrono::steady_clock::now();
    cout << "lineitem: sort done in "
         << chrono::duration_cast<chrono::seconds>(t1-t0).count() << "s, writing...\n";

    vector<thread> writers;
    writers.emplace_back([&](){ wcp(d+"l_orderkey.bin",      ok,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_partkey.bin",       pk,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_suppkey.bin",       sk,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_linenumber.bin",    ln,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_quantity.bin",      qty, perm); });
    writers.emplace_back([&](){ wcp(d+"l_extendedprice.bin", ep,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_discount.bin",      disc,perm); });
    writers.emplace_back([&](){ wcp(d+"l_tax.bin",           tax, perm); });
    writers.emplace_back([&](){ wcp(d+"l_returnflag.bin",    rf,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_linestatus.bin",    ls,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_shipdate.bin",      sd,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_commitdate.bin",    cd,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_receiptdate.bin",   rd,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_shipinstruct.bin",  si,  perm); });
    writers.emplace_back([&](){ wcp(d+"l_shipmode.bin",      sm,  perm); });
    for (auto& w : writers) w.join();

    // Write dicts
    rfdict.save(d+"l_returnflag_dict.txt");
    lsdict.save(d+"l_linestatus_dict.txt");
    sidict.save(d+"l_shipinstruct_dict.txt");
    smdict.save(d+"l_shipmode_dict.txt");

    // Build sorted shipdate for zone map
    vector<int32_t> ssd(N);
    for (size_t i=0; i<N; ++i) ssd[i] = sd[perm[i]];
    wzonemap(out_dir+"/indexes/lineitem_shipdate_zonemap.bin", ssd);

    auto t2 = chrono::steady_clock::now();
    cout << "lineitem: done in "
         << chrono::duration_cast<chrono::seconds>(t2-t0).count() << "s\n";
}

// ============ ORDERS ============
static void ingest_orders(const string& data_dir, const string& out_dir) {
    auto t0 = chrono::steady_clock::now();
    fs::create_directories(out_dir+"/orders");

    MmapFile mm(data_dir+"/orders.tbl");
    auto chunks = split_chunks(mm.data(), mm.sz, N_PARSE_THREADS);
    int NC = (int)chunks.size();

    struct OChunk {
        vector<int32_t> ok, ck, sp, od;
        vector<double>  tp;
    };
    vector<OChunk> tdata(NC);

    vector<thread> threads;
    for (int t=0; t<NC; ++t) {
        threads.emplace_back([&,t](){
            const char* ptr = chunks[t].first;
            const char* end = chunks[t].second;
            auto& D = tdata[t];
            while (ptr < end) {
                D.ok.push_back(pi(ptr));   // o_orderkey
                D.ck.push_back(pi(ptr));   // o_custkey
                sf(ptr);                   // o_orderstatus (skip)
                D.tp.push_back(pd(ptr));   // o_totalprice
                D.od.push_back(pdate(ptr));// o_orderdate
                sf(ptr);                   // o_orderpriority (skip)
                sf(ptr);                   // o_clerk (skip)
                D.sp.push_back(pi(ptr));   // o_shippriority
                seol(ptr);                 // o_comment (skip)
            }
        });
    }
    for (auto& th : threads) th.join();

    vector<size_t> offs(NC+1,0);
    for (int t=0; t<NC; ++t) offs[t+1] = offs[t] + tdata[t].ok.size();
    size_t N = offs[NC];
    cout << "orders: parsed " << N << " rows\n";

    vector<int32_t> ok(N),ck(N),sp(N),od(N);
    vector<double>  tp(N);
    for (int t=0; t<NC; ++t) {
        auto& D = tdata[t]; size_t o = offs[t];
        memcpy(ok.data()+o, D.ok.data(), D.ok.size()*4);
        memcpy(ck.data()+o, D.ck.data(), D.ck.size()*4);
        memcpy(sp.data()+o, D.sp.data(), D.sp.size()*4);
        memcpy(od.data()+o, D.od.data(), D.od.size()*4);
        memcpy(tp.data()+o, D.tp.data(), D.tp.size()*8);
        D = OChunk{};
    }
    tdata.clear();

    // Sort by o_orderdate
    vector<uint32_t> perm(N);
    iota(perm.begin(), perm.end(), 0u);
    sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b){ return od[a] < od[b]; });

    string d = out_dir + "/orders/";
    vector<thread> writers;
    writers.emplace_back([&](){ wcp(d+"o_orderkey.bin",    ok, perm); });
    writers.emplace_back([&](){ wcp(d+"o_custkey.bin",     ck, perm); });
    writers.emplace_back([&](){ wcp(d+"o_totalprice.bin",  tp, perm); });
    writers.emplace_back([&](){ wcp(d+"o_orderdate.bin",   od, perm); });
    writers.emplace_back([&](){ wcp(d+"o_shippriority.bin",sp, perm); });
    for (auto& w : writers) w.join();

    vector<int32_t> sod(N);
    for (size_t i=0; i<N; ++i) sod[i] = od[perm[i]];
    wzonemap(out_dir+"/indexes/orders_orderdate_zonemap.bin", sod);

    auto t1 = chrono::steady_clock::now();
    cout << "orders: done in "
         << chrono::duration_cast<chrono::seconds>(t1-t0).count() << "s\n";
}

// ============ CUSTOMER ============
static void ingest_customer(const string& data_dir, const string& out_dir) {
    fs::create_directories(out_dir+"/customer");
    Dict mktseg;
    MmapFile mm(data_dir+"/customer.tbl");
    auto chunks = split_chunks(mm.data(), mm.sz, N_PARSE_THREADS);
    int NC = (int)chunks.size();

    struct CChunk {
        vector<int32_t> ck;
        vector<char>    cn; // char[26] flat
        vector<int16_t> ms;
    };
    vector<CChunk> tdata(NC);

    vector<thread> threads;
    for (int t=0; t<NC; ++t) {
        threads.emplace_back([&,t](){
            const char* ptr = chunks[t].first;
            const char* end = chunks[t].second;
            auto& D = tdata[t];
            while (ptr < end) {
                D.ck.push_back(pi(ptr));       // c_custkey
                // c_name: fixed 26 chars
                char nm[26] = {};
                const char* ns = ptr;
                while (*ptr != '|') ++ptr;
                size_t len = min((size_t)(ptr-ns), (size_t)25);
                memcpy(nm, ns, len);
                D.cn.insert(D.cn.end(), nm, nm+26);
                if (*ptr == '|') ++ptr;
                sf(ptr);                       // c_address (skip)
                sf(ptr);                       // c_nationkey (skip)
                sf(ptr);                       // c_phone (skip)
                sf(ptr);                       // c_acctbal (skip)
                D.ms.push_back(pdict(ptr,mktseg)); // c_mktsegment
                seol(ptr);                     // c_comment (skip)
            }
        });
    }
    for (auto& th : threads) th.join();

    vector<size_t> offs(NC+1,0);
    for (int t=0; t<NC; ++t) offs[t+1] = offs[t] + tdata[t].ck.size();
    size_t N = offs[NC];

    vector<int32_t> ck(N);
    vector<char>    cn(N*26);
    vector<int16_t> ms(N);
    for (int t=0; t<NC; ++t) {
        auto& D = tdata[t]; size_t o = offs[t];
        memcpy(ck.data()+o,     D.ck.data(), D.ck.size()*4);
        memcpy(cn.data()+o*26,  D.cn.data(), D.cn.size());
        memcpy(ms.data()+o,     D.ms.data(), D.ms.size()*2);
    }

    string d = out_dir+"/customer/";
    wc(d+"c_custkey.bin",    ck);
    wcharN(d+"c_name.bin",   cn, 26);
    wc(d+"c_mktsegment.bin", ms);
    mktseg.save(d+"c_mktsegment_dict.txt");
    cout << "customer: " << N << " rows\n";
}

// ============ PART ============
static void ingest_part(const string& data_dir, const string& out_dir) {
    fs::create_directories(out_dir+"/part");
    MmapFile mm(data_dir+"/part.tbl");
    auto chunks = split_chunks(mm.data(), mm.sz, N_PARSE_THREADS);
    int NC = (int)chunks.size();

    struct PChunk {
        vector<int32_t> pk;
        vector<char>    pn; // char[56] flat
    };
    vector<PChunk> tdata(NC);

    vector<thread> threads;
    for (int t=0; t<NC; ++t) {
        threads.emplace_back([&,t](){
            const char* ptr = chunks[t].first;
            const char* end = chunks[t].second;
            auto& D = tdata[t];
            while (ptr < end) {
                D.pk.push_back(pi(ptr));    // p_partkey
                // p_name: char[56]
                char nm[56] = {};
                const char* ns = ptr;
                while (*ptr != '|') ++ptr;
                size_t len = min((size_t)(ptr-ns),(size_t)55);
                memcpy(nm, ns, len);
                D.pn.insert(D.pn.end(), nm, nm+56);
                if (*ptr=='|') ++ptr;
                sf(ptr); sf(ptr); sf(ptr);  // p_mfgr, p_brand, p_type (skip)
                sf(ptr); sf(ptr); sf(ptr);  // p_size, p_container, p_retailprice (skip)
                seol(ptr);                  // p_comment (skip)
            }
        });
    }
    for (auto& th : threads) th.join();

    vector<size_t> offs(NC+1,0);
    for (int t=0; t<NC; ++t) offs[t+1] = offs[t] + tdata[t].pk.size();
    size_t N = offs[NC];

    vector<int32_t> pk(N);
    vector<char>    pn(N*56);
    for (int t=0; t<NC; ++t) {
        auto& D = tdata[t]; size_t o = offs[t];
        memcpy(pk.data()+o,    D.pk.data(), D.pk.size()*4);
        memcpy(pn.data()+o*56, D.pn.data(), D.pn.size());
    }

    string d = out_dir+"/part/";
    wc(d+"p_partkey.bin", pk);
    wcharN(d+"p_name.bin", pn, 56);
    cout << "part: " << N << " rows\n";
}

// ============ PARTSUPP ============
static void ingest_partsupp(const string& data_dir, const string& out_dir) {
    fs::create_directories(out_dir+"/partsupp");
    MmapFile mm(data_dir+"/partsupp.tbl");
    auto chunks = split_chunks(mm.data(), mm.sz, N_PARSE_THREADS);
    int NC = (int)chunks.size();

    struct PSChunk {
        vector<int32_t> ppk, psk;
        vector<double>  sc;
    };
    vector<PSChunk> tdata(NC);

    vector<thread> threads;
    for (int t=0; t<NC; ++t) {
        threads.emplace_back([&,t](){
            const char* ptr = chunks[t].first;
            const char* end = chunks[t].second;
            auto& D = tdata[t];
            while (ptr < end) {
                D.ppk.push_back(pi(ptr)); // ps_partkey
                D.psk.push_back(pi(ptr)); // ps_suppkey
                sf(ptr);                  // ps_availqty (skip)
                D.sc.push_back(pd(ptr));  // ps_supplycost
                seol(ptr);                // ps_comment (skip)
            }
        });
    }
    for (auto& th : threads) th.join();

    vector<size_t> offs(NC+1,0);
    for (int t=0; t<NC; ++t) offs[t+1] = offs[t] + tdata[t].ppk.size();
    size_t N = offs[NC];

    vector<int32_t> ppk(N),psk(N);
    vector<double>  sc(N);
    for (int t=0; t<NC; ++t) {
        auto& D = tdata[t]; size_t o = offs[t];
        memcpy(ppk.data()+o, D.ppk.data(), D.ppk.size()*4);
        memcpy(psk.data()+o, D.psk.data(), D.psk.size()*4);
        memcpy(sc.data()+o,  D.sc.data(),  D.sc.size()*8);
    }

    string d = out_dir+"/partsupp/";
    wc(d+"ps_partkey.bin",    ppk);
    wc(d+"ps_suppkey.bin",    psk);
    wc(d+"ps_supplycost.bin", sc);
    cout << "partsupp: " << N << " rows\n";
}

// ============ SUPPLIER ============
static void ingest_supplier(const string& data_dir, const string& out_dir) {
    fs::create_directories(out_dir+"/supplier");
    MmapFile mm(data_dir+"/supplier.tbl");
    auto chunks = split_chunks(mm.data(), mm.sz, 4);
    int NC = (int)chunks.size();

    struct SChunk { vector<int32_t> sk, nk; };
    vector<SChunk> tdata(NC);

    vector<thread> threads;
    for (int t=0; t<NC; ++t) {
        threads.emplace_back([&,t](){
            const char* ptr = chunks[t].first;
            const char* end = chunks[t].second;
            auto& D = tdata[t];
            while (ptr < end) {
                D.sk.push_back(pi(ptr)); // s_suppkey
                sf(ptr);                 // s_name (skip)
                sf(ptr);                 // s_address (skip)
                D.nk.push_back(pi(ptr)); // s_nationkey
                sf(ptr); sf(ptr);        // s_phone, s_acctbal (skip)
                seol(ptr);               // s_comment (skip)
            }
        });
    }
    for (auto& th : threads) th.join();

    vector<size_t> offs(NC+1,0);
    for (int t=0; t<NC; ++t) offs[t+1] = offs[t] + tdata[t].sk.size();
    size_t N = offs[NC];

    vector<int32_t> sk(N),nk(N);
    for (int t=0; t<NC; ++t) {
        auto& D = tdata[t]; size_t o = offs[t];
        memcpy(sk.data()+o, D.sk.data(), D.sk.size()*4);
        memcpy(nk.data()+o, D.nk.data(), D.nk.size()*4);
    }
    string d = out_dir+"/supplier/";
    wc(d+"s_suppkey.bin",   sk);
    wc(d+"s_nationkey.bin", nk);
    cout << "supplier: " << N << " rows\n";
}

// ============ NATION ============
static void ingest_nation(const string& data_dir, const string& out_dir) {
    fs::create_directories(out_dir+"/nation");
    Dict ndict;
    MmapFile mm(data_dir+"/nation.tbl");
    const char* ptr = mm.data();
    const char* end = mm.data() + mm.sz;

    vector<int32_t> nk;
    vector<int16_t> nn;
    while (ptr < end) {
        nk.push_back(pi(ptr));       // n_nationkey
        nn.push_back(pdict(ptr,ndict)); // n_name
        sf(ptr);                     // n_regionkey (skip)
        seol(ptr);                   // n_comment (skip)
    }
    string d = out_dir+"/nation/";
    wc(d+"n_nationkey.bin", nk);
    wc(d+"n_name.bin",      nn);
    ndict.save(d+"n_name_dict.txt");
    cout << "nation: " << nk.size() << " rows\n";
}

// ============ Verification ============
static void verify(const string& out_dir) {
    // Check l_shipdate > 3000 (lineitem sorted by shipdate: first = smallest date)
    {
        FILE* f = fopen((out_dir+"/lineitem/l_shipdate.bin").c_str(), "rb");
        if (!f) { fprintf(stderr, "ABORT: cannot open l_shipdate.bin\n"); exit(1); }
        int32_t v; fread(&v, 4, 1, f); fclose(f);
        if (v <= 3000) { fprintf(stderr, "ABORT: l_shipdate[0]=%d <= 3000\n", v); exit(1); }
        printf("Verify: l_shipdate[0] = %d (>3000 OK)\n", v);
    }
    // Check l_extendedprice non-zero
    {
        FILE* f = fopen((out_dir+"/lineitem/l_extendedprice.bin").c_str(), "rb");
        if (!f) { fprintf(stderr, "ABORT: cannot open l_extendedprice.bin\n"); exit(1); }
        double v; fread(&v, 8, 1, f); fclose(f);
        if (v == 0.0) { fprintf(stderr, "ABORT: l_extendedprice[0]=0\n"); exit(1); }
        printf("Verify: l_extendedprice[0] = %.2f (non-zero OK)\n", v);
    }
    // Check o_orderdate > 3000
    {
        FILE* f = fopen((out_dir+"/orders/o_orderdate.bin").c_str(), "rb");
        if (!f) { fprintf(stderr, "ABORT: cannot open o_orderdate.bin\n"); exit(1); }
        int32_t v; fread(&v, 4, 1, f); fclose(f);
        if (v <= 3000) { fprintf(stderr, "ABORT: o_orderdate[0]=%d <= 3000\n", v); exit(1); }
        printf("Verify: o_orderdate[0] = %d (>3000 OK)\n", v);
    }
    // Check ps_supplycost non-zero
    {
        FILE* f = fopen((out_dir+"/partsupp/ps_supplycost.bin").c_str(), "rb");
        if (!f) { fprintf(stderr, "ABORT: cannot open ps_supplycost.bin\n"); exit(1); }
        double v; fread(&v, 8, 1, f); fclose(f);
        if (v == 0.0) { fprintf(stderr, "ABORT: ps_supplycost[0]=0\n"); exit(1); }
        printf("Verify: ps_supplycost[0] = %.2f (non-zero OK)\n", v);
    }
    printf("All verification checks passed.\n");
}

// ============ Main ============
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <data_dir> <out_dir>\n", argv[0]);
        return 1;
    }
    string data_dir = argv[1];
    string out_dir  = argv[2];
    fs::create_directories(out_dir + "/indexes");

    auto t0 = chrono::steady_clock::now();

    // Launch all table ingestions in parallel
    vector<thread> tasks;
    tasks.emplace_back([&](){ ingest_lineitem(data_dir, out_dir); });
    tasks.emplace_back([&](){ ingest_orders(data_dir, out_dir); });
    tasks.emplace_back([&](){ ingest_customer(data_dir, out_dir); });
    tasks.emplace_back([&](){ ingest_part(data_dir, out_dir); });
    tasks.emplace_back([&](){ ingest_partsupp(data_dir, out_dir); });
    tasks.emplace_back([&](){ ingest_supplier(data_dir, out_dir); });
    tasks.emplace_back([&](){ ingest_nation(data_dir, out_dir); });
    for (auto& t : tasks) t.join();

    auto t1 = chrono::steady_clock::now();
    cout << "All tables ingested in "
         << chrono::duration_cast<chrono::seconds>(t1-t0).count() << "s\n";

    verify(out_dir);
    return 0;
}
