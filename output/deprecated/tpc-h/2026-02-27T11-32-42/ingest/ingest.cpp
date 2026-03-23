// ingest.cpp — Converts TPC-H .tbl pipe-delimited files to binary columnar format.
// Lineitem is sorted by l_shipdate to accelerate range-scan queries (Q1, Q6).
// Usage: ingest <data_dir> <gendb_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <algorithm>
#include <thread>
#include <atomic>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <chrono>
#include "date_utils.h"

using namespace std;

// ── helpers ──────────────────────────────────────────────────────────────────

inline int32_t parse_i32(const char*& p) {
    int32_t v = 0;
    while ((unsigned)(*p - '0') < 10u) v = v*10 + (*p++ - '0');
    ++p; // skip delimiter
    return v;
}

// Parse "N.NN" → int8 = N*10+NN (discount/tax stored as 0–10 representing 0.00–0.10)
inline int8_t parse_percent(const char*& p) {
    while (*p != '.') ++p;
    ++p; // skip '.'
    int8_t v = 0;
    if ((unsigned)(*p-'0') < 10u) v  = (*p++ - '0') * 10;
    if ((unsigned)(*p-'0') < 10u) v += (*p++ - '0');
    ++p; // skip delimiter
    return v;
}

// Fast price parser: builds integer from all decimal digits, then scales
inline double parse_price(const char*& p) {
    int64_t v = 0;
    while ((unsigned)(*p-'0') < 10u) v = v*10 + (*p++ - '0');
    int frac = 0;
    if (*p == '.') {
        ++p;
        while ((unsigned)(*p-'0') < 10u) { v = v*10 + (*p++ - '0'); ++frac; }
    }
    ++p; // skip delimiter
    static const double S[] = {1.0, 0.1, 0.01, 0.001, 0.0001};
    return (double)v * S[frac];
}

inline int32_t parse_date_pipe(const char*& p) {
    int32_t d = gendb::date_str_to_epoch_days(p);
    p += 10; // YYYY-MM-DD
    ++p;     // skip delimiter
    return d;
}

inline void skip_field(const char*& p) {
    while (*p != '|' && *p != '\n' && *p != '\r') ++p;
    ++p;
}

template<typename T>
static void write_col(const char* dir, const char* name, const vector<T>& v) {
    char path[512];
    snprintf(path, sizeof(path), "%s/%s", dir, name);
    FILE* f = fopen(path, "wb");
    if (!f) { perror(path); exit(1); }
    fwrite(v.data(), sizeof(T), v.size(), f);
    fclose(f);
}

// ── Lineitem ─────────────────────────────────────────────────────────────────

struct LIRaw {
    int32_t orderkey, partkey, suppkey;
    double  extprice;
    int8_t  qty, discount, tax;
    uint8_t rflag, lstatus;
    int32_t shipdate;
};

static void parse_lineitem_chunk(const char* beg, const char* end,
                                  vector<LIRaw>& out) {
    const char* p = beg;
    while (p < end) {
        if (*p == '\n' || *p == '\r') { ++p; continue; }
        LIRaw r;
        r.orderkey  = parse_i32(p);
        r.partkey   = parse_i32(p);
        r.suppkey   = parse_i32(p);
        skip_field(p);                    // linenumber
        { int32_t q = parse_i32(p); r.qty = (int8_t)q; } // quantity (integer 1-50)
        r.extprice  = parse_price(p);
        r.discount  = parse_percent(p);
        r.tax       = parse_percent(p);
        r.rflag     = (uint8_t)*p; skip_field(p);
        r.lstatus   = (uint8_t)*p; skip_field(p);
        r.shipdate  = parse_date_pipe(p);
        // skip commitdate, receiptdate, shipinstruct, shipmode, comment
        skip_field(p); skip_field(p); skip_field(p); skip_field(p); skip_field(p);
        out.push_back(r);
    }
}

static void ingest_lineitem(const char* data_dir, const char* gendb_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/lineitem.tbl", data_dir);
    int fd = open(path, O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t fsz = st.st_size;
    char* mapped = (char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise(mapped, fsz, MADV_SEQUENTIAL);

    // Parallel parse: 32 threads
    int NT = 32;
    vector<vector<LIRaw>> parts(NT);
    vector<thread> thr;
    size_t chunk = fsz / NT;
    for (int t = 0; t < NT; ++t) {
        const char* beg = mapped + t * chunk;
        const char* end = (t+1 < NT) ? mapped + (t+1)*chunk : mapped + fsz;
        // align to next newline start
        if (t > 0) { while (beg < end && *(beg-1) != '\n') ++beg; }
        if (t+1 < NT) { while (end < mapped+fsz && *(end-1) != '\n') ++end; }
        parts[t].reserve(chunk / 100);
        thr.emplace_back(parse_lineitem_chunk, beg, end, ref(parts[t]));
    }
    for (auto& th : thr) th.join();
    munmap(mapped, fsz); close(fd);

    // Merge
    size_t N = 0;
    for (auto& v : parts) N += v.size();
    fprintf(stderr, "  lineitem rows: %zu\n", N);

    // Counting sort by shipdate
    // Find min/max shipdate
    int32_t mn = INT32_MAX, mx = INT32_MIN;
    for (auto& v : parts)
        for (auto& r : v) {
            mn = min(mn, r.shipdate);
            mx = max(mx, r.shipdate);
        }
    int range = mx - mn + 1;
    vector<int32_t> cnt(range+1, 0);
    for (auto& v : parts)
        for (auto& r : v) cnt[r.shipdate - mn]++;
    // prefix sum
    vector<int32_t> off(range+1, 0);
    for (int i = 1; i <= range; ++i) off[i] = off[i-1] + cnt[i-1];
    // fill sorted order
    vector<int32_t> idx(N);
    {
        vector<int32_t> pos = off; // working copy
        size_t base = 0;
        for (auto& v : parts) {
            for (size_t i = 0; i < v.size(); ++i)
                idx[pos[v[i].shipdate - mn]++] = (int32_t)(base + i);
            base += v.size();
        }
    }
    // Flatten
    vector<LIRaw*> flat; flat.reserve(N);
    for (auto& v : parts) for (auto& r : v) flat.push_back(&r);

    // Write sorted columns
    vector<int32_t> li_orderkey(N), li_partkey(N), li_suppkey(N), li_shipdate(N);
    vector<double>  li_extprice(N);
    vector<int8_t>  li_qty(N), li_discount(N), li_tax(N);
    vector<uint8_t> li_rflag(N), li_lstatus(N);

    for (size_t i = 0; i < N; ++i) {
        const LIRaw& r = *flat[idx[i]];
        li_orderkey[i]  = r.orderkey;
        li_partkey[i]   = r.partkey;
        li_suppkey[i]   = r.suppkey;
        li_extprice[i]  = r.extprice;
        li_qty[i]       = r.qty;
        li_discount[i]  = r.discount;
        li_tax[i]       = r.tax;
        li_rflag[i]     = r.rflag;
        li_lstatus[i]   = r.lstatus;
        li_shipdate[i]  = r.shipdate;
    }

    write_col(gendb_dir, "li_orderkey.bin",  li_orderkey);
    write_col(gendb_dir, "li_partkey.bin",   li_partkey);
    write_col(gendb_dir, "li_suppkey.bin",   li_suppkey);
    write_col(gendb_dir, "li_extprice.bin",  li_extprice);
    write_col(gendb_dir, "li_qty.bin",       li_qty);
    write_col(gendb_dir, "li_discount.bin",  li_discount);
    write_col(gendb_dir, "li_tax.bin",       li_tax);
    write_col(gendb_dir, "li_rflag.bin",     li_rflag);
    write_col(gendb_dir, "li_lstatus.bin",   li_lstatus);
    write_col(gendb_dir, "li_shipdate.bin",  li_shipdate);

    // Write row count and shipdate range meta
    {
        char mp[512]; snprintf(mp, sizeof(mp), "%s/li_meta.bin", gendb_dir);
        FILE* f = fopen(mp, "wb");
        int64_t nrows = (int64_t)N;
        int32_t meta[3] = {(int32_t)N, mn, mx};  // count (truncated), min_date, max_date
        fwrite(&nrows, sizeof(nrows), 1, f);
        fwrite(&mn, sizeof(mn), 1, f);
        fwrite(&mx, sizeof(mx), 1, f);
        fclose(f);
    }
    fprintf(stderr, "  lineitem written, shipdate range [%d, %d]\n", mn, mx);
}

// ── Orders ────────────────────────────────────────────────────────────────────

static void ingest_orders(const char* data_dir, const char* gendb_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/orders.tbl", data_dir);
    int fd = open(path, O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t fsz = st.st_size;
    char* mapped = (char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise(mapped, fsz, MADV_SEQUENTIAL);

    // Estimate ~15M rows
    vector<int32_t> orderkey, custkey, orderdate, shippriority;
    vector<double>  totalprice;
    orderkey.reserve(15000000); custkey.reserve(15000000);
    orderdate.reserve(15000000); shippriority.reserve(15000000);
    totalprice.reserve(15000000);

    const char* p = mapped;
    const char* end = mapped + fsz;
    while (p < end) {
        if (*p == '\n' || *p == '\r') { ++p; continue; }
        int32_t ok = parse_i32(p);
        int32_t ck = parse_i32(p);
        skip_field(p);                     // orderstatus
        double   tp = parse_price(p);
        int32_t  od = parse_date_pipe(p);
        skip_field(p);                     // orderpriority
        skip_field(p);                     // clerk
        int32_t  sp = parse_i32(p);
        skip_field(p);                     // comment
        orderkey.push_back(ok);
        custkey.push_back(ck);
        totalprice.push_back(tp);
        orderdate.push_back(od);
        shippriority.push_back(sp);
    }
    munmap(mapped, fsz); close(fd);
    fprintf(stderr, "  orders rows: %zu\n", orderkey.size());

    write_col(gendb_dir, "ord_orderkey.bin",    orderkey);
    write_col(gendb_dir, "ord_custkey.bin",     custkey);
    write_col(gendb_dir, "ord_orderdate.bin",   orderdate);
    write_col(gendb_dir, "ord_totalprice.bin",  totalprice);
    write_col(gendb_dir, "ord_shippriority.bin",shippriority);

    {
        char mp[512]; snprintf(mp, sizeof(mp), "%s/ord_meta.bin", gendb_dir);
        FILE* f = fopen(mp, "wb");
        int64_t n = (int64_t)orderkey.size();
        fwrite(&n, sizeof(n), 1, f);
        fclose(f);
    }
}

// ── Customer ──────────────────────────────────────────────────────────────────

static void ingest_customer(const char* data_dir, const char* gendb_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/customer.tbl", data_dir);
    int fd = open(path, O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t fsz = st.st_size;
    char* mapped = (char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise(mapped, fsz, MADV_SEQUENTIAL);

    vector<int32_t> custkey;
    vector<uint8_t> is_building;
    custkey.reserve(1500000); is_building.reserve(1500000);

    const char* p = mapped;
    const char* end = mapped + fsz;
    while (p < end) {
        if (*p == '\n' || *p == '\r') { ++p; continue; }
        int32_t ck = parse_i32(p);
        skip_field(p); // name
        skip_field(p); // address
        skip_field(p); // nationkey
        skip_field(p); // phone
        skip_field(p); // acctbal
        // mktsegment: read 8 chars
        uint8_t ib = (p[0]=='B' && p[1]=='U' && p[2]=='I' && p[3]=='L' && p[4]=='D') ? 1 : 0;
        skip_field(p); // mktsegment
        skip_field(p); // comment
        custkey.push_back(ck);
        is_building.push_back(ib);
    }
    munmap(mapped, fsz); close(fd);
    fprintf(stderr, "  customer rows: %zu\n", custkey.size());

    write_col(gendb_dir, "cust_custkey.bin",    custkey);
    write_col(gendb_dir, "cust_isbuilding.bin", is_building);

    {
        char mp[512]; snprintf(mp, sizeof(mp), "%s/cust_meta.bin", gendb_dir);
        FILE* f = fopen(mp, "wb");
        int64_t n = (int64_t)custkey.size();
        fwrite(&n, sizeof(n), 1, f);
        fclose(f);
    }
}

// ── Part ─────────────────────────────────────────────────────────────────────

static void ingest_part(const char* data_dir, const char* gendb_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/part.tbl", data_dir);
    int fd = open(path, O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t fsz = st.st_size;
    char* mapped = (char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise(mapped, fsz, MADV_SEQUENTIAL);

    vector<int32_t> partkey;
    vector<uint8_t> has_green;
    partkey.reserve(2000000); has_green.reserve(2000000);

    const char* p = mapped;
    const char* end = mapped + fsz;
    while (p < end) {
        if (*p == '\n' || *p == '\r') { ++p; continue; }
        int32_t pk = parse_i32(p);
        // name: check for "green"
        const char* name_start = p;
        while (*p != '|') ++p;
        // search for "green" in [name_start, p)
        uint8_t hg = 0;
        for (const char* s = name_start; s + 5 <= p; ++s) {
            if (s[0]=='g'&&s[1]=='r'&&s[2]=='e'&&s[3]=='e'&&s[4]=='n') { hg=1; break; }
        }
        ++p; // skip '|'
        skip_field(p); // mfgr
        skip_field(p); // brand
        skip_field(p); // type
        skip_field(p); // size
        skip_field(p); // container
        skip_field(p); // retailprice
        skip_field(p); // comment
        partkey.push_back(pk);
        has_green.push_back(hg);
    }
    munmap(mapped, fsz); close(fd);
    fprintf(stderr, "  part rows: %zu\n", partkey.size());

    write_col(gendb_dir, "part_partkey.bin",  partkey);
    write_col(gendb_dir, "part_hasgreen.bin", has_green);

    // Also build dense array: partkey -> has_green (max partkey ~2M)
    {
        int32_t maxpk = *max_element(partkey.begin(), partkey.end());
        vector<uint8_t> arr(maxpk+1, 0);
        for (size_t i = 0; i < partkey.size(); ++i)
            arr[partkey[i]] = has_green[i];
        char ap[512]; snprintf(ap, sizeof(ap), "%s/part_green_arr.bin", gendb_dir);
        FILE* f = fopen(ap, "wb");
        fwrite(arr.data(), 1, arr.size(), f);
        fclose(f);
        // write size
        char sp[512]; snprintf(sp, sizeof(sp), "%s/part_green_arr_sz.bin", gendb_dir);
        f = fopen(sp, "wb");
        int32_t sz = maxpk+1;
        fwrite(&sz, sizeof(sz), 1, f);
        fclose(f);
    }

    {
        char mp[512]; snprintf(mp, sizeof(mp), "%s/part_meta.bin", gendb_dir);
        FILE* f = fopen(mp, "wb");
        int64_t n = (int64_t)partkey.size();
        fwrite(&n, sizeof(n), 1, f);
        fclose(f);
    }
}

// ── PartSupp ─────────────────────────────────────────────────────────────────

static void ingest_partsupp(const char* data_dir, const char* gendb_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/partsupp.tbl", data_dir);
    int fd = open(path, O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t fsz = st.st_size;
    char* mapped = (char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise(mapped, fsz, MADV_SEQUENTIAL);

    vector<int32_t> ps_partkey, ps_suppkey;
    vector<double>  ps_supplycost;
    ps_partkey.reserve(8000000); ps_suppkey.reserve(8000000);
    ps_supplycost.reserve(8000000);

    const char* p = mapped;
    const char* end = mapped + fsz;
    while (p < end) {
        if (*p == '\n' || *p == '\r') { ++p; continue; }
        int32_t pk = parse_i32(p);
        int32_t sk = parse_i32(p);
        skip_field(p); // availqty
        double  sc = parse_price(p);
        skip_field(p); // comment
        ps_partkey.push_back(pk);
        ps_suppkey.push_back(sk);
        ps_supplycost.push_back(sc);
    }
    munmap(mapped, fsz); close(fd);
    fprintf(stderr, "  partsupp rows: %zu\n", ps_partkey.size());

    write_col(gendb_dir, "ps_partkey.bin",    ps_partkey);
    write_col(gendb_dir, "ps_suppkey.bin",    ps_suppkey);
    write_col(gendb_dir, "ps_supplycost.bin", ps_supplycost);

    {
        char mp[512]; snprintf(mp, sizeof(mp), "%s/ps_meta.bin", gendb_dir);
        FILE* f = fopen(mp, "wb");
        int64_t n = (int64_t)ps_partkey.size();
        fwrite(&n, sizeof(n), 1, f);
        fclose(f);
    }
}

// ── Supplier ─────────────────────────────────────────────────────────────────

static void ingest_supplier(const char* data_dir, const char* gendb_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/supplier.tbl", data_dir);
    int fd = open(path, O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t fsz = st.st_size;
    char* mapped = (char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise(mapped, fsz, MADV_SEQUENTIAL);

    vector<int32_t> suppkey;
    vector<int8_t>  nationkey;
    suppkey.reserve(100000); nationkey.reserve(100000);
    int32_t maxsk = 0;

    const char* p = mapped;
    const char* end = mapped + fsz;
    while (p < end) {
        if (*p == '\n' || *p == '\r') { ++p; continue; }
        int32_t sk = parse_i32(p);
        skip_field(p); // name
        skip_field(p); // address
        int32_t nk = parse_i32(p);
        skip_field(p); // phone
        skip_field(p); // acctbal
        skip_field(p); // comment
        suppkey.push_back(sk);
        nationkey.push_back((int8_t)nk);
        if (sk > maxsk) maxsk = sk;
    }
    munmap(mapped, fsz); close(fd);
    fprintf(stderr, "  supplier rows: %zu\n", suppkey.size());

    write_col(gendb_dir, "sup_suppkey.bin",   suppkey);
    write_col(gendb_dir, "sup_nationkey.bin", nationkey);

    // Dense array: suppkey -> nationkey
    {
        vector<int8_t> arr(maxsk+1, -1);
        for (size_t i = 0; i < suppkey.size(); ++i)
            arr[suppkey[i]] = nationkey[i];
        char ap[512]; snprintf(ap, sizeof(ap), "%s/sup_nat_arr.bin", gendb_dir);
        FILE* f = fopen(ap, "wb");
        fwrite(arr.data(), 1, arr.size(), f);
        fclose(f);
        char sp[512]; snprintf(sp, sizeof(sp), "%s/sup_nat_arr_sz.bin", gendb_dir);
        f = fopen(sp, "wb");
        int32_t sz = maxsk+1;
        fwrite(&sz, sizeof(sz), 1, f);
        fclose(f);
    }
    {
        char mp[512]; snprintf(mp, sizeof(mp), "%s/sup_meta.bin", gendb_dir);
        FILE* f = fopen(mp, "wb");
        int64_t n = (int64_t)suppkey.size();
        fwrite(&n, sizeof(n), 1, f);
        fclose(f);
    }
}

// ── Nation ────────────────────────────────────────────────────────────────────

static void ingest_nation(const char* data_dir, const char* gendb_dir) {
    char path[512];
    snprintf(path, sizeof(path), "%s/nation.tbl", data_dir);
    int fd = open(path, O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t fsz = st.st_size;
    char* mapped = (char*)mmap(nullptr, fsz, PROT_READ, MAP_PRIVATE, fd, 0);

    // Store 25 nation names as fixed char[26] indexed by nationkey
    char names[25][26];
    memset(names, 0, sizeof(names));

    const char* p = mapped;
    const char* end = mapped + fsz;
    while (p < end) {
        if (*p == '\n' || *p == '\r') { ++p; continue; }
        int32_t nk = parse_i32(p);
        const char* ns = p;
        while (*p != '|') ++p;
        int len = (int)(p - ns);
        if (len > 25) len = 25;
        memcpy(names[nk], ns, len);
        names[nk][len] = '\0';
        ++p; // skip '|'
        skip_field(p); // regionkey
        skip_field(p); // comment
    }
    munmap(mapped, fsz); close(fd);

    char np[512]; snprintf(np, sizeof(np), "%s/nat_names.bin", gendb_dir);
    FILE* f = fopen(np, "wb");
    fwrite(names, 26, 25, f);
    fclose(f);
    fprintf(stderr, "  nation written\n");
}

// ── Main ─────────────────────────────────────────────────────────────────────

int main(int argc, char** argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: ingest <data_dir> <gendb_dir>\n");
        return 1;
    }
    const char* data_dir  = argv[1];
    const char* gendb_dir = argv[2];

    gendb::init_date_tables();

    auto t0 = chrono::steady_clock::now();

    // Run smaller tables first (nation, supplier, part, customer, partsupp),
    // then orders and lineitem
    fprintf(stderr, "Ingesting nation...\n");
    ingest_nation(data_dir, gendb_dir);

    fprintf(stderr, "Ingesting supplier...\n");
    ingest_supplier(data_dir, gendb_dir);

    fprintf(stderr, "Ingesting part...\n");
    ingest_part(data_dir, gendb_dir);

    fprintf(stderr, "Ingesting customer...\n");
    ingest_customer(data_dir, gendb_dir);

    fprintf(stderr, "Ingesting partsupp...\n");
    ingest_partsupp(data_dir, gendb_dir);

    fprintf(stderr, "Ingesting orders...\n");
    ingest_orders(data_dir, gendb_dir);

    fprintf(stderr, "Ingesting lineitem (parallel parse + sort)...\n");
    ingest_lineitem(data_dir, gendb_dir);

    double ms = chrono::duration<double, milli>(
        chrono::steady_clock::now() - t0).count();
    printf("Ingestion complete in %.2f ms\n", ms);

    return 0;
}
