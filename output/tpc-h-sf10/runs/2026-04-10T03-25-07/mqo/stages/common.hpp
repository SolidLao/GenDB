#pragma once
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cassert>
#include <cmath>
#include <string>
#include <vector>
#include <array>
#include <algorithm>
#include <map>
#include <set>
#include <unordered_map>
#include <fstream>
#include <sstream>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <omp.h>
#include "mqo_profile.hpp"

// ================================================================
// Date utilities (epoch days since 1970-01-01)
// ================================================================
static inline constexpr int32_t ymd(int y, int m, int d) {
    y -= (m <= 2);
    int era = (y >= 0 ? y : y - 399) / 400;
    unsigned yoe = static_cast<unsigned>(y - era * 400);
    unsigned doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
    unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + static_cast<int>(doe) - 719468;
}
static inline int epoch_year(int32_t e) {
    int z = e + 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = static_cast<unsigned>(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int y = static_cast<int>(yoe) + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    unsigned m = mp + (mp < 10 ? 3 : -9);
    return y + (m <= 2);
}
static inline std::string epoch_str(int32_t e) {
    int z = e + 719468;
    int era = (z >= 0 ? z : z - 146096) / 146097;
    unsigned doe = static_cast<unsigned>(z - era * 146097);
    unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    int y = static_cast<int>(yoe) + era * 400;
    unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    unsigned mp = (5 * doy + 2) / 153;
    unsigned dd = doy - (153 * mp + 2) / 5 + 1;
    unsigned m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2);
    char buf[16]; snprintf(buf, sizeof(buf), "%04d-%02d-%02d", y, m, dd); return buf;
}

// Pre-computed date constants
static constexpr int32_t D_930701 = ymd(1993,7,1),  D_931001 = ymd(1993,10,1);
static constexpr int32_t D_940101 = ymd(1994,1,1),  D_950101 = ymd(1995,1,1);
static constexpr int32_t D_950315 = ymd(1995,3,15), D_950901 = ymd(1995,9,1);
static constexpr int32_t D_951001 = ymd(1995,10,1), D_960101 = ymd(1996,1,1);
static constexpr int32_t D_960401 = ymd(1996,4,1),  D_961231 = ymd(1996,12,31);
static constexpr int32_t D_980902 = ymd(1998,9,2);

// ================================================================
// I/O utilities
// ================================================================
static inline size_t read_rowcount(const std::string& dir) {
    std::string path = dir + "/meta.txt";
    FILE* f = fopen(path.c_str(), "r");
    if (!f) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
    size_t n = 0; if (fscanf(f, "row_count=%zu", &n) < 1) n = 0; fclose(f); return n;
}

struct MMap {
    void* ptr = nullptr; size_t sz = 0;
    void open(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "open fail: %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st); sz = st.st_size;
        if (sz > 0) {
            ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
            if (ptr == MAP_FAILED) { fprintf(stderr, "mmap fail: %s\n", path.c_str()); exit(1); }
        }
        ::close(fd);
    }
    template <typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
};

static inline std::vector<std::string> read_dict(const std::string& path) {
    MMap mf; mf.open(path);
    const uint8_t* p = reinterpret_cast<const uint8_t*>(mf.ptr);
    uint32_t cnt; memcpy(&cnt, p, 4); p += 4;
    std::vector<std::string> d(cnt);
    for (uint32_t i = 0; i < cnt; i++) {
        uint16_t len; memcpy(&len, p, 2); p += 2;
        d[i].assign(reinterpret_cast<const char*>(p), len); p += len;
    }
    return d;
}
static inline int dfind(const std::vector<std::string>& d, const std::string& v) {
    for (size_t i = 0; i < d.size(); i++) if (d[i] == v) return (int)i;
    return -1;
}
static inline std::string vl_get(const uint32_t* off, const char* data, size_t row) {
    return std::string(data + off[row], off[row + 1] - off[row]);
}

// ================================================================
// String pattern matching
// ================================================================
static inline bool str_contains(const char* s, size_t sl, const char* n, size_t nl) {
    return nl <= sl && memmem(s, sl, n, nl) != nullptr;
}
// Pattern %A%B%: s contains A then later B
static inline bool str_like_ab(const char* s, size_t sl,
                               const char* a, size_t al, const char* b, size_t bl) {
    const char* p = (const char*)memmem(s, sl, a, al);
    while (p) {
        p += al; size_t rem = sl - (size_t)(p - s);
        if (memmem(p, rem, b, bl)) return true;
        p = (const char*)memmem(p, rem, a, al);
    }
    return false;
}
static inline bool ends_with(const std::string& s, const std::string& sfx) {
    return s.size() >= sfx.size() && s.compare(s.size() - sfx.size(), sfx.size(), sfx) == 0;
}

// ================================================================
// Index structures
// ================================================================
struct DRE { uint32_t start, count; };  // dense_range entry
struct PHE { int32_t pk, sk, rid, pad; }; // partsupp hash entry

static inline int32_t ps_hash_lookup(const PHE* t, uint32_t mask, int32_t pk, int32_t sk) {
    uint32_t h = (static_cast<uint32_t>(pk) * 2654435761u) ^ (static_cast<uint32_t>(sk) * 40503u);
    h &= mask;
    for (;;) {
        if (t[h].pk == pk && t[h].sk == sk) return t[h].rid;
        if (t[h].pk == 0 && t[h].sk == 0) return -1;
        h = (h + 1) & mask;
    }
}

// ================================================================
// Query bit constants
// ================================================================
static constexpr uint32_t Q1B  = 1u<<0,  Q2B  = 1u<<1,  Q3B  = 1u<<2,  Q4B  = 1u<<3;
static constexpr uint32_t Q5B  = 1u<<4,  Q6B  = 1u<<5,  Q7B  = 1u<<6,  Q8B  = 1u<<7;
static constexpr uint32_t Q9B  = 1u<<8,  Q10B = 1u<<9,  Q11B = 1u<<10, Q12B = 1u<<11;
static constexpr uint32_t Q13B = 1u<<12, Q14B = 1u<<13, Q15B = 1u<<14, Q16B = 1u<<15;
static constexpr uint32_t Q17B = 1u<<16, Q18B = 1u<<17, Q19B = 1u<<18, Q20B = 1u<<19;
static constexpr uint32_t Q21B = 1u<<20, Q22B = 1u<<21;
static constexpr uint32_t ALL_Q = (1u << 22) - 1;

static constexpr uint32_t LI_MASK  = Q1B|Q3B|Q5B|Q6B|Q7B|Q10B|Q12B|Q14B|Q15B|Q18B;
static constexpr uint32_t ORD_MASK = Q4B|Q13B;
static constexpr uint32_t IDX_MASK = Q2B|Q8B|Q9B|Q11B|Q16B|Q17B|Q19B|Q20B|Q21B|Q22B;
static constexpr uint32_t PST_MASK = Q15B|Q18B;

// ================================================================
// Helper: find nationkey / regionkey by name
// ================================================================
struct Ctx; // forward decl
static inline int find_nationkey(const struct Ctx& c, const std::string& name);
static inline int find_regionkey(const struct Ctx& c, const std::string& name);

// ================================================================
// Context: read-only data (mmap'd columns + indexes + dicts)
// ================================================================
struct Ctx {
    std::string sd, id, od; // storage_dir, index_dir, output_dir
    size_t nli, nord, ncust, npart, nps, nsupp;

    // Dictionaries
    std::vector<std::string> d_ptype, d_pbrand, d_pcont, d_pmfgr;
    std::vector<std::string> d_shipmode, d_shipinst;
    std::vector<std::string> d_ordpri, d_nname, d_rname, d_mkseg;

    // Lineitem columns
    const int32_t *l_orderkey=0, *l_partkey=0, *l_suppkey=0;
    const int32_t *l_shipdate=0, *l_commitdate=0, *l_receiptdate=0;
    const double *l_quantity=0, *l_extendedprice=0, *l_discount=0, *l_tax=0;
    const int8_t *l_returnflag=0, *l_linestatus=0;
    const uint8_t *l_shipmode_col=0, *l_shipinst_col=0;
    // Orders columns
    const int32_t *o_orderkey_col=0, *o_custkey=0, *o_orderdate=0, *o_shippriority=0;
    const int8_t *o_orderstatus=0; const uint8_t *o_orderpri=0;
    const double *o_totalprice=0;
    const uint32_t *o_cmt_off=0; const char *o_cmt_dat=0;
    // Customer columns
    const int32_t *c_nationkey=0; const uint8_t *c_mktseg=0; const double *c_acctbal=0;
    const uint32_t *c_nm_off=0, *c_ph_off=0, *c_ad_off=0, *c_cm_off=0;
    const char *c_nm_dat=0, *c_ph_dat=0, *c_ad_dat=0, *c_cm_dat=0;
    // Supplier columns
    const int32_t *s_nationkey=0; const double *s_acctbal=0;
    const uint32_t *s_nm_off=0, *s_ad_off=0, *s_ph_off=0, *s_cm_off=0;
    const char *s_nm_dat=0, *s_ad_dat=0, *s_ph_dat=0, *s_cm_dat=0;
    // Part columns
    const int32_t *p_size=0;
    const uint8_t *p_type_col=0, *p_brand_col=0, *p_cont_col=0, *p_mfgr_col=0;
    const uint32_t *p_nm_off=0; const char *p_nm_dat=0;
    // Partsupp columns
    const int32_t *ps_partkey=0, *ps_suppkey=0, *ps_availqty=0;
    const double *ps_supplycost=0;
    // Nation / Region columns
    const int32_t *n_regionkey=0; const uint8_t *n_name_col=0;
    const uint8_t *r_name_col=0;

    // Indexes
    const int32_t* opk_idx=0;      // orders_pk [60000001]
    const DRE* li_ok_idx=0;        // lineitem_orderkey_idx [60000001]
    const uint32_t *li_pk_off=0, *li_pk_rows=0; int32_t li_pk_max=2000000;
    const uint32_t *li_sk_off=0, *li_sk_rows=0; int32_t li_sk_max=100000;
    const DRE* ps_pk_idx=0;        // partsupp_partkey_idx [2000001]
    const PHE* ps_hash=0; uint32_t ps_hmask=0;
    const uint32_t *ps_sk_off=0, *ps_sk_rows=0; int32_t ps_sk_max=100000;
    const uint32_t *sup_nk_off=0, *sup_nk_rows=0; int32_t sup_nk_max=24;
    const uint32_t *cust_ok_off=0, *cust_ok_rows=0; int32_t cust_ok_max=1499999;

    MMap mfs[200]; int nmf = 0;
    MMap& mm(const std::string& p) { mfs[nmf].open(p); return mfs[nmf++]; }
    template <typename T> const T* mc(const std::string& d, const std::string& c) {
        return mm(d + "/" + c + ".bin").as<T>(); }
    const uint32_t* mco(const std::string& d, const std::string& c) {
        return mm(d + "/" + c + ".bin").as<uint32_t>(); }
    const char* mcd(const std::string& d, const std::string& c) {
        return mm(d + "/" + c + "_data.bin").as<char>(); }
};

static inline int find_nationkey(const Ctx& c, const std::string& name) {
    int dc = dfind(c.d_nname, name); if (dc < 0) return -1;
    for (int nk = 0; nk < 25; nk++) if (c.n_name_col[nk] == (uint8_t)dc) return nk;
    return -1;
}
static inline int find_regionkey(const Ctx& c, const std::string& name) {
    int dc = dfind(c.d_rname, name); if (dc < 0) return -1;
    for (int rk = 0; rk < 5; rk++) if (c.r_name_col[rk] == (uint8_t)dc) return rk;
    return -1;
}

// ================================================================
// Results: per-query intermediate and final state
// ================================================================
struct Res {
    // Q1
    struct Q1G { double sq=0,sp=0,sdp=0,sch=0,sd=0; int64_t cnt=0; } q1[6];
    // Q3
    struct Q3G { double rev=0; int32_t odate=0,osp=0; };
    std::unordered_map<int32_t, Q3G> q3;
    // Q4
    int64_t q4[5] = {};
    // Q5
    double q5[25] = {};
    // Q6
    double q6 = 0;
    // Q7 [pair 0=FR->DE, 1=DE->FR][year 0=1995, 1=1996]
    double q7[2][2] = {};
    // Q8 [year 0=1995, 1=1996] brazil/total volumes
    double q8b[2]={}, q8t[2]={};
    // Q9 (nationkey, year) -> profit
    std::map<std::pair<int,int>, double> q9;
    // Q10 custkey -> revenue
    std::unordered_map<int32_t, double> q10;
    // Q11 partkey -> value
    std::unordered_map<int32_t, double> q11; double q11_total=0;
    // Q12 [mode 0=MAIL, 1=SHIP]
    int64_t q12h[2]={}, q12l[2]={};
    // Q13 per-custkey order count
    std::vector<int32_t> q13_cnt;
    // Q14
    double q14p=0, q14t=0;
    // Q15
    std::vector<double> q15_rev;
    // Q16
    struct Q16R { std::string brand, type; int32_t size; int cnt; };
    std::vector<Q16R> q16;
    // Q17
    double q17 = 0;
    // Q18
    std::vector<double> q18_qty; // per-orderkey
    struct Q18R { std::string cname; int32_t ck,ok,odate; double otp,sq; };
    std::vector<Q18R> q18;
    // Q19
    double q19 = 0;
    // Q20
    std::vector<std::pair<std::string,std::string>> q20;
    // Q21
    std::unordered_map<std::string, int64_t> q21;
    // Q22
    std::map<std::string, std::pair<int64_t,double>> q22;
    // Q2
    struct Q2R { double s_acctbal; std::string s_name,n_name; int32_t p_partkey;
                 std::string p_mfgr,s_addr,s_phone,s_comment; };
    std::vector<Q2R> q2;
    // Q15 final
    struct Q15R { int32_t sk; std::string sname,saddr,sphone; double rev; };
    std::vector<Q15R> q15f;
};
