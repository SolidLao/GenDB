// ingest.cpp — TPC-H binary columnar ingestion
// Converts pipe-delimited .tbl files to compact binary columns.
// Lineitem is sorted by l_shipdate for efficient range queries.
// Usage: ./ingest <src_tbl_dir> <gendb_dir>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cstdint>
#include <vector>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "timing_utils.h"

static const int32_t BASE_DATE = 8035; // epoch days for 1992-01-01

// Howard Hinnant's algorithm: YMD -> epoch days (days since 1970-01-01)
static inline int32_t ymd_to_epoch(int y, int m, int d) {
    y -= (m <= 2);
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = (unsigned)(y - era * 400);
    unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int)doe - 719468;
}

static inline uint16_t parse_date_u16(const char*& p) {
    int y = (p[0]-'0')*1000 + (p[1]-'0')*100 + (p[2]-'0')*10 + (p[3]-'0');
    int m = (p[5]-'0')*10 + (p[6]-'0');
    int d = (p[8]-'0')*10 + (p[9]-'0');
    p += 11;
    return (uint16_t)(ymd_to_epoch(y, m, d) - BASE_DATE);
}

static inline int32_t parse_int(const char*& p) {
    int32_t v = 0;
    while (*p >= '0' && *p <= '9') v = v * 10 + (*p++ - '0');
    ++p; // skip '|' or newline
    return v;
}

static inline int32_t parse_money(const char*& p) {
    int64_t v = 0;
    while (*p != '.' && *p != '|') v = v * 10 + (*p++ - '0');
    if (*p == '.') {
        ++p;
        v = v * 100 + (*p++ - '0') * 10 + (*p++ - '0');
    } else {
        v = v * 100;
    }
    ++p; // skip '|'
    return (int32_t)v;
}

static inline uint8_t parse_pct(const char*& p) {
    p += 2; // skip "0."
    uint8_t v = (*p++ - '0') * 10 + (*p++ - '0');
    ++p;
    return v;
}

static inline uint8_t parse_qty(const char*& p) {
    uint8_t v = 0;
    while (*p >= '0' && *p <= '9') v = v * 10 + (*p++ - '0');
    if (*p == '.') { ++p; while (*p != '|') ++p; } // skip decimal digits
    ++p; // skip '|'
    return v;
}

static inline void skip_field(const char*& p) {
    while (*p++ != '|') {}
}

static inline int read_str_field(const char*& p, char* buf, int max_len) {
    int n = 0;
    while (*p != '|' && *p != '\n' && *p != '\r' && n < max_len)
        buf[n++] = *p++;
    while (*p != '|' && *p != '\n' && *p != '\r') ++p;
    if (*p == '|') ++p;
    buf[n] = '\0';
    return n;
}

static const char* mmap_tbl(const char* path, size_t* sz) {
    int fd = open(path, O_RDONLY);
    if (fd < 0) { perror(path); return nullptr; }
    struct stat st; fstat(fd, &st);
    *sz = st.st_size;
    void* ptr = mmap(nullptr, st.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) { perror("mmap"); return nullptr; }
    madvise(ptr, st.st_size, MADV_SEQUENTIAL);
    return (const char*)ptr;
}

template<typename T>
static void write_col(const char* path, const T* data, size_t n) {
    FILE* f = fopen(path, "wb");
    if (!f) { perror(path); return; }
    fwrite(data, sizeof(T), n, f);
    fclose(f);
}

struct Name26 { char s[26]; };

// ============================================================
// LINEITEM — sorted by l_shipdate
// Fields: orderkey|partkey|suppkey|linenumber|quantity|extendedprice|
//         discount|tax|returnflag|linestatus|shipdate|...
// ============================================================
static void ingest_lineitem(const char* src, const char* dst) {
    char path[512]; snprintf(path, sizeof(path), "%s/lineitem.tbl", src);
    size_t sz; const char* data = mmap_tbl(path, &sz);
    if (!data) return;

    const size_t MAXROWS = 62000000;
    int32_t*  orderkey   = new int32_t[MAXROWS];
    int32_t*  partkey    = new int32_t[MAXROWS];
    int32_t*  suppkey    = new int32_t[MAXROWS];
    uint8_t*  quantity   = new uint8_t[MAXROWS];
    int32_t*  extprice   = new int32_t[MAXROWS];
    uint8_t*  discount   = new uint8_t[MAXROWS];
    uint8_t*  tax        = new uint8_t[MAXROWS];
    uint8_t*  returnflag = new uint8_t[MAXROWS];
    uint8_t*  linestatus = new uint8_t[MAXROWS];
    uint16_t* shipdate   = new uint16_t[MAXROWS];

    size_t n = 0;
    const char* p = data, *end = data + sz;
    while (p < end && n < MAXROWS) {
        orderkey[n]   = parse_int(p);
        partkey[n]    = parse_int(p);
        suppkey[n]    = parse_int(p);
        skip_field(p);                      // linenumber
        quantity[n]   = parse_qty(p);
        extprice[n]   = parse_money(p);
        discount[n]   = parse_pct(p);
        tax[n]        = parse_pct(p);
        returnflag[n] = (uint8_t)*p; p += 2;
        linestatus[n] = (uint8_t)*p; p += 2;
        shipdate[n]   = parse_date_u16(p);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
        n++;
    }
    munmap((void*)data, sz);
    printf("  lineitem: %zu rows\n", n);

    // Counting sort by shipdate (uint16, max ~3000 for TPC-H)
    const int MAX_SD = 3000;
    uint32_t cnt[MAX_SD] = {};
    for (size_t i = 0; i < n; i++) cnt[shipdate[i]]++;
    uint32_t off[MAX_SD]; off[0] = 0;
    for (int i = 1; i < MAX_SD; i++) off[i] = off[i-1] + cnt[i-1];

    uint32_t* perm = new uint32_t[n];
    { uint32_t cur[MAX_SD]; memcpy(cur, off, sizeof(off));
      for (size_t i = 0; i < n; i++) perm[cur[shipdate[i]]++] = (uint32_t)i; }

    uint16_t* s_sd  = new uint16_t[n];
    int32_t*  s_ok  = new int32_t[n];
    int32_t*  s_pk  = new int32_t[n];
    int32_t*  s_sk  = new int32_t[n];
    uint8_t*  s_qty = new uint8_t[n];
    int32_t*  s_ep  = new int32_t[n];
    uint8_t*  s_dc  = new uint8_t[n];
    uint8_t*  s_tx  = new uint8_t[n];
    uint8_t*  s_rf  = new uint8_t[n];
    uint8_t*  s_ls  = new uint8_t[n];

    for (size_t i = 0; i < n; i++) {
        uint32_t j = perm[i];
        s_sd[i]  = shipdate[j];   s_ok[i]  = orderkey[j];
        s_pk[i]  = partkey[j];    s_sk[i]  = suppkey[j];
        s_qty[i] = quantity[j];   s_ep[i]  = extprice[j];
        s_dc[i]  = discount[j];   s_tx[i]  = tax[j];
        s_rf[i]  = returnflag[j]; s_ls[i]  = linestatus[j];
    }
    delete[] perm;
    delete[] shipdate; delete[] orderkey; delete[] partkey; delete[] suppkey;
    delete[] quantity; delete[] extprice; delete[] discount; delete[] tax;
    delete[] returnflag; delete[] linestatus;

    char d[512]; snprintf(d, sizeof(d), "%s/lineitem", dst);
    char tmp[600];
#define WC(name, arr) snprintf(tmp,sizeof(tmp),"%s/%s",d,name); write_col(tmp,arr,n)
    WC("l_shipdate.bin",   s_sd);  WC("l_orderkey.bin",   s_ok);
    WC("l_partkey.bin",    s_pk);  WC("l_suppkey.bin",    s_sk);
    WC("l_quantity.bin",   s_qty); WC("l_extprice.bin",   s_ep);
    WC("l_discount.bin",   s_dc);  WC("l_tax.bin",        s_tx);
    WC("l_returnflag.bin", s_rf);  WC("l_linestatus.bin", s_ls);
#undef WC

    delete[] s_sd; delete[] s_ok; delete[] s_pk; delete[] s_sk;
    delete[] s_qty; delete[] s_ep; delete[] s_dc; delete[] s_tx;
    delete[] s_rf; delete[] s_ls;
    printf("  lineitem: sorted and written\n");
}

// ============================================================
// ORDERS — orderkey|custkey|orderstatus|totalprice|orderdate|
//           orderpriority|clerk|shippriority|comment|
// ============================================================
static void ingest_orders(const char* src, const char* dst) {
    char path[512]; snprintf(path, sizeof(path), "%s/orders.tbl", src);
    size_t sz; const char* data = mmap_tbl(path, &sz);
    if (!data) return;

    const size_t MAXROWS = 16000000;
    int32_t*  orderkey    = new int32_t[MAXROWS];
    int32_t*  custkey     = new int32_t[MAXROWS];
    int32_t*  totalprice  = new int32_t[MAXROWS];
    uint16_t* orderdate   = new uint16_t[MAXROWS];
    uint8_t*  shippriority= new uint8_t[MAXROWS];

    size_t n = 0;
    const char* p = data, *end = data + sz;
    while (p < end && n < MAXROWS) {
        orderkey[n]     = parse_int(p);
        custkey[n]      = parse_int(p);
        skip_field(p);                    // orderstatus
        totalprice[n]   = parse_money(p);
        orderdate[n]    = parse_date_u16(p);
        skip_field(p);                    // orderpriority
        skip_field(p);                    // clerk
        shippriority[n] = (uint8_t)parse_int(p);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
        n++;
    }
    munmap((void*)data, sz);

    char d[512]; snprintf(d, sizeof(d), "%s/orders", dst);
    char tmp[600];
#define WC(name, arr) snprintf(tmp,sizeof(tmp),"%s/%s",d,name); write_col(tmp,arr,n)
    WC("o_orderkey.bin",    orderkey);    WC("o_custkey.bin",     custkey);
    WC("o_totalprice.bin",  totalprice);  WC("o_orderdate.bin",   orderdate);
    WC("o_shippriority.bin",shippriority);
#undef WC
    delete[] orderkey; delete[] custkey; delete[] totalprice;
    delete[] orderdate; delete[] shippriority;
    printf("  orders: %zu rows\n", n);
}

// ============================================================
// CUSTOMER — custkey|name|address|nationkey|phone|acctbal|mktsegment|comment|
// ============================================================
static void ingest_customer(const char* src, const char* dst) {
    char path[512]; snprintf(path, sizeof(path), "%s/customer.tbl", src);
    size_t sz; const char* data = mmap_tbl(path, &sz);
    if (!data) return;

    const size_t MAXROWS = 1600000;
    int32_t* custkey = new int32_t[MAXROWS];
    uint8_t* mktseg  = new uint8_t[MAXROWS];
    Name26*  name    = new Name26[MAXROWS];

    size_t n = 0;
    const char* p = data, *end = data + sz;
    while (p < end && n < MAXROWS) {
        custkey[n] = parse_int(p);
        read_str_field(p, name[n].s, 25);
        skip_field(p); skip_field(p); skip_field(p); skip_field(p); // addr,natkey,phone,acctbal
        mktseg[n] = (uint8_t)*p;  // first char of mktsegment
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
        n++;
    }
    munmap((void*)data, sz);

    char d[512]; snprintf(d, sizeof(d), "%s/customer", dst);
    char tmp[600];
    snprintf(tmp,sizeof(tmp),"%s/c_custkey.bin",d); write_col(tmp, custkey, n);
    snprintf(tmp,sizeof(tmp),"%s/c_mktseg.bin",d);  write_col(tmp, mktseg, n);
    snprintf(tmp,sizeof(tmp),"%s/c_name.bin",d);    write_col(tmp, name, n);
    delete[] custkey; delete[] mktseg; delete[] name;
    printf("  customer: %zu rows\n", n);
}

// ============================================================
// PARTSUPP — partkey|suppkey|availqty|supplycost|comment|
// ============================================================
static void ingest_partsupp(const char* src, const char* dst) {
    char path[512]; snprintf(path, sizeof(path), "%s/partsupp.tbl", src);
    size_t sz; const char* data = mmap_tbl(path, &sz);
    if (!data) return;

    const size_t MAXROWS = 9000000;
    int32_t* partkey    = new int32_t[MAXROWS];
    int32_t* suppkey    = new int32_t[MAXROWS];
    int32_t* supplycost = new int32_t[MAXROWS];

    size_t n = 0;
    const char* p = data, *end = data + sz;
    while (p < end && n < MAXROWS) {
        partkey[n]    = parse_int(p);
        suppkey[n]    = parse_int(p);
        skip_field(p);                  // availqty
        supplycost[n] = parse_money(p);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
        n++;
    }
    munmap((void*)data, sz);

    char d[512]; snprintf(d, sizeof(d), "%s/partsupp", dst);
    char tmp[600];
    snprintf(tmp,sizeof(tmp),"%s/ps_partkey.bin",d);    write_col(tmp, partkey, n);
    snprintf(tmp,sizeof(tmp),"%s/ps_suppkey.bin",d);    write_col(tmp, suppkey, n);
    snprintf(tmp,sizeof(tmp),"%s/ps_supplycost.bin",d); write_col(tmp, supplycost, n);
    delete[] partkey; delete[] suppkey; delete[] supplycost;
    printf("  partsupp: %zu rows\n", n);
}

// ============================================================
// PART — partkey|name|mfgr|brand|type|size|container|retailprice|comment|
// ============================================================
static void ingest_part(const char* src, const char* dst) {
    char path[512]; snprintf(path, sizeof(path), "%s/part.tbl", src);
    size_t sz; const char* data = mmap_tbl(path, &sz);
    if (!data) return;

    const size_t MAXROWS = 2200000;
    int32_t* partkey   = new int32_t[MAXROWS];
    uint8_t* has_green = new uint8_t[MAXROWS];

    size_t n = 0;
    const char* p = data, *end = data + sz;
    char namebuf[64];
    while (p < end && n < MAXROWS) {
        partkey[n] = parse_int(p);
        int nlen = read_str_field(p, namebuf, 63);
        has_green[n] = 0;
        for (int i = 0; i <= nlen - 5; i++) {
            if (namebuf[i]=='g' && namebuf[i+1]=='r' && namebuf[i+2]=='e' &&
                namebuf[i+3]=='e' && namebuf[i+4]=='n') { has_green[n]=1; break; }
        }
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
        n++;
    }
    munmap((void*)data, sz);

    char d[512]; snprintf(d, sizeof(d), "%s/part", dst);
    char tmp[600];
    snprintf(tmp,sizeof(tmp),"%s/p_partkey.bin",d);   write_col(tmp, partkey, n);
    snprintf(tmp,sizeof(tmp),"%s/p_has_green.bin",d); write_col(tmp, has_green, n);
    delete[] partkey; delete[] has_green;
    printf("  part: %zu rows\n", n);
}

// ============================================================
// SUPPLIER — suppkey|name|address|nationkey|phone|acctbal|comment|
// ============================================================
static void ingest_supplier(const char* src, const char* dst) {
    char path[512]; snprintf(path, sizeof(path), "%s/supplier.tbl", src);
    size_t sz; const char* data = mmap_tbl(path, &sz);
    if (!data) return;

    const size_t MAXROWS = 110000;
    int32_t* suppkey   = new int32_t[MAXROWS];
    uint8_t* nationkey = new uint8_t[MAXROWS];

    size_t n = 0;
    const char* p = data, *end = data + sz;
    while (p < end && n < MAXROWS) {
        suppkey[n]   = parse_int(p);
        skip_field(p); skip_field(p);    // name, address
        nationkey[n] = (uint8_t)parse_int(p);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
        n++;
    }
    munmap((void*)data, sz);

    char d[512]; snprintf(d, sizeof(d), "%s/supplier", dst);
    char tmp[600];
    snprintf(tmp,sizeof(tmp),"%s/s_suppkey.bin",d);   write_col(tmp, suppkey, n);
    snprintf(tmp,sizeof(tmp),"%s/s_nationkey.bin",d); write_col(tmp, nationkey, n);
    delete[] suppkey; delete[] nationkey;
    printf("  supplier: %zu rows\n", n);
}

// ============================================================
// NATION — nationkey|name|regionkey|comment|
// ============================================================
static void ingest_nation(const char* src, const char* dst) {
    char path[512]; snprintf(path, sizeof(path), "%s/nation.tbl", src);
    size_t sz; const char* data = mmap_tbl(path, &sz);
    if (!data) return;

    const size_t MAXROWS = 30;
    uint8_t* nationkey = new uint8_t[MAXROWS];
    Name26*  name      = new Name26[MAXROWS];

    size_t n = 0;
    const char* p = data, *end = data + sz;
    while (p < end && n < MAXROWS) {
        nationkey[n] = (uint8_t)parse_int(p);
        read_str_field(p, name[n].s, 25);
        while (p < end && *p != '\n') ++p;
        if (p < end) ++p;
        n++;
    }
    munmap((void*)data, sz);

    char d[512]; snprintf(d, sizeof(d), "%s/nation", dst);
    char tmp[600];
    snprintf(tmp,sizeof(tmp),"%s/n_nationkey.bin",d); write_col(tmp, nationkey, n);
    snprintf(tmp,sizeof(tmp),"%s/n_name.bin",d);      write_col(tmp, name, n);
    delete[] nationkey; delete[] name;
    printf("  nation: %zu rows\n", n);
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <src_tbl_dir> <gendb_dir>\n", argv[0]);
        return 1;
    }
    GENDB_PHASE("total");

    const char* src = argv[1];
    const char* dst = argv[2];

    // Run in parallel
    std::thread t1(ingest_lineitem, src, dst);
    std::thread t2(ingest_orders,   src, dst);
    std::thread t3(ingest_customer, src, dst);
    std::thread t4(ingest_partsupp, src, dst);
    std::thread t5([&]{
        ingest_part(src, dst);
        ingest_supplier(src, dst);
        ingest_nation(src, dst);
    });

    t1.join(); t2.join(); t3.join(); t4.join(); t5.join();
    printf("Ingestion complete.\n");
    return 0;
}
