// MQO Fused Batch Executor — All 22 TPC-H queries in coordinated passes
#include "mqo_profile.hpp"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <numeric>
#include <tuple>
#include <set>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

// ============ Query Bit Masks ============
static constexpr uint32_t Q1B=1u<<0, Q2B=1u<<1, Q3B=1u<<2, Q4B=1u<<3;
static constexpr uint32_t Q5B=1u<<4, Q6B=1u<<5, Q7B=1u<<6, Q8B=1u<<7;
static constexpr uint32_t Q9B=1u<<8, Q10B=1u<<9, Q11B=1u<<10, Q12B=1u<<11;
static constexpr uint32_t Q13B=1u<<12, Q14B=1u<<13, Q15B=1u<<14, Q16B=1u<<15;
static constexpr uint32_t Q17B=1u<<16, Q18B=1u<<17, Q19B=1u<<18, Q20B=1u<<19;
static constexpr uint32_t Q21B=1u<<20, Q22B=1u<<21;
static constexpr uint32_t ALL_Q = (1u<<22)-1;
static const char* QN[] = {"Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10",
    "Q11","Q12","Q13","Q14","Q15","Q16","Q17","Q18","Q19","Q20","Q21","Q22"};

// ============ Date Helpers ============
static constexpr int32_t dfc(int y, int m, int d) {
    int y2 = y - (m <= 2 ? 1 : 0);
    int era = (y2 >= 0 ? y2 : y2-399) / 400;
    unsigned yoe = (unsigned)(y2 - era*400);
    unsigned doy = (153*(m > 2 ? m-3 : m+9)+2)/5 + d - 1;
    unsigned doe = yoe*365 + yoe/4 - yoe/100 + doy;
    return era*146097 + (int)doe - 719468;
}
static inline int yfd(int32_t z) { // year from epoch days
    z += 719468;
    int era = (z >= 0 ? z : z-146096) / 146097;
    unsigned doe = (unsigned)(z - era*146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp = (5*doy + 2) / 153;
    unsigned mo = mp < 10 ? mp + 3 : mp - 9;
    return (int)yoe + era*400 + (mo <= 2 ? 1 : 0);
}
static inline void cfd(int32_t z, int &y, int &m, int &d) {
    z += 719468;
    int era = (z >= 0 ? z : z-146096) / 146097;
    unsigned doe = (unsigned)(z - era*146097);
    unsigned yoe = (doe - doe/1460 + doe/36524 - doe/146096) / 365;
    y = (int)yoe + era*400;
    unsigned doy = doe - (365*yoe + yoe/4 - yoe/100);
    unsigned mp = (5*doy + 2) / 153;
    d = doy - (153*mp + 2)/5 + 1;
    m = mp < 10 ? mp + 3 : mp - 9;
    y += (m <= 2 ? 1 : 0);
}
static inline std::string dstr(int32_t z) {
    int y,m,d; cfd(z,y,m,d);
    char b[12]; snprintf(b,sizeof(b),"%04d-%02d-%02d",y,m,d); return b;
}
// Date constants
static constexpr int32_t D_19930701=dfc(1993,7,1), D_19931001=dfc(1993,10,1);
static constexpr int32_t D_19940101=dfc(1994,1,1), D_19950101=dfc(1995,1,1);
static constexpr int32_t D_19950315=dfc(1995,3,15);
static constexpr int32_t D_19950901=dfc(1995,9,1), D_19951001=dfc(1995,10,1);
static constexpr int32_t D_19960101=dfc(1996,1,1), D_19960401=dfc(1996,4,1);
static constexpr int32_t D_19961231=dfc(1996,12,31);
static constexpr int32_t D_19980902=dfc(1998,9,2);

// ============ Mmap / File Helpers ============
static size_t fsize(const std::string& p) {
    struct stat st; return (stat(p.c_str(),&st)==0) ? (size_t)st.st_size : 0;
}
template<typename T>
static const T* mmf(const std::string& p, size_t& cnt) {
    size_t sz = fsize(p); cnt = sz/sizeof(T);
    if(!sz) return nullptr;
    int fd = open(p.c_str(), O_RDONLY);
    if(fd<0){cnt=0;return nullptr;}
    void* ptr = mmap(nullptr, sz, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);
    return (ptr==MAP_FAILED) ? (cnt=0, (const T*)nullptr) : (const T*)ptr;
}
static size_t read_rc(const std::string& p) {
    FILE* f=fopen(p.c_str(),"r"); if(!f)return 0;
    size_t n=0;
    char line[256];
    while(fgets(line, sizeof(line), f)){
        if(sscanf(line, "row_count=%zu", &n)==1) break;
        if(sscanf(line, "%zu", &n)==1) break; // fallback for plain number
    }
    fclose(f); return n;
}

// ============ Dictionary ============
struct Dict {
    std::vector<std::string> v;
    void load(const std::string& path) {
        FILE* f=fopen(path.c_str(),"rb"); if(!f)return;
        uint32_t c; if(fread(&c,4,1,f)!=1){fclose(f);return;}
        v.resize(c);
        for(uint32_t i=0;i<c;++i){
            uint16_t len; if(fread(&len,2,1,f)!=1)break;
            v[i].resize(len); if(len) fread(&v[i][0],1,len,f);
        }
        fclose(f);
    }
    int find(const std::string& s) const {
        for(int i=0;i<(int)v.size();++i) if(v[i]==s) return i;
        return -1;
    }
};

// ============ Varlen Column ============
struct VL {
    const uint32_t* off=nullptr; const char* dat=nullptr;
    size_t noff=0, ndat=0;
    void load(const std::string& opath, const std::string& dpath) {
        off = mmf<uint32_t>(opath, noff);
        dat = mmf<char>(dpath, ndat);
    }
    std::string_view get(size_t i) const {
        return {dat+off[i], off[i+1]-off[i]};
    }
};

// ============ Bitset ============
struct Bits {
    std::vector<uint64_t> b;
    Bits(){}
    explicit Bits(size_t n): b((n+63)/64, 0){}
    void set(size_t i){b[i>>6]|=(1ULL<<(i&63));}
    bool test(size_t i) const {return (b[i>>6]>>(i&63))&1;}
    void or_with(const Bits& o){
        for(size_t j=0;j<b.size()&&j<o.b.size();++j) b[j]|=o.b[j];
    }
};

// ============ Pattern Matching ============
static bool has2(const char* s, size_t len, const char* p1, size_t n1, const char* p2, size_t n2){
    for(size_t i=0;i+n1<=len;++i){
        if(memcmp(s+i,p1,n1)==0){
            for(size_t j=i+n1;j+n2<=len;++j)
                if(memcmp(s+j,p2,n2)==0) return true;
            return false;
        }
    }
    return false;
}
static bool contains(const char* s, size_t len, const char* sub, size_t slen){
    if(slen>len)return false;
    for(size_t i=0;i+slen<=len;++i) if(memcmp(s+i,sub,slen)==0) return true;
    return false;
}

// ============ Hash Index ============
struct PSH { int32_t pk,sk,rid,pad; };
static inline int32_t ps_lookup(const PSH* tbl, uint64_t mask, int32_t pk, int32_t sk){
    uint64_t h = (uint64_t)(uint32_t)pk * 2654435761ULL ^ (uint64_t)(uint32_t)sk * 40503ULL;
    uint64_t slot = h & mask;
    while(tbl[slot].rid != -1){
        if(tbl[slot].pk==pk && tbl[slot].sk==sk) return tbl[slot].rid;
        slot = (slot+1) & mask;
    }
    return -1;
}

// ============ Range Index Entry ============
struct RIdx { uint32_t start, count; };

// ============ Pack/Unpack ============
static inline uint64_t pk64(int32_t a, int32_t b){return ((uint64_t)(uint32_t)a<<32)|(uint32_t)b;}

// ============ CSV string writer ============
static void csvs(FILE* f, std::string_view s){
    bool q=false;
    for(char c:s) if(c==','||c=='"'||c=='\n'){q=true;break;}
    if(q){fputc('"',f);for(char c:s){if(c=='"')fputc('"',f);fputc(c,f);}fputc('"',f);}
    else fwrite(s.data(),1,s.size(),f);
}

// ============ CONTEXT ============
struct Ctx {
    std::string gd, od; // gendb_dir, output_dir
    uint32_t active = 0;
    // Row counts
    size_t NL,NO,NC,NS,NP,NPS,NN,NR;
    // Lineitem columns
    const int32_t *l_ok,*l_pk,*l_sk;
    const double *l_qty,*l_ep,*l_disc,*l_tax;
    const int8_t *l_rf,*l_ls;
    const int32_t *l_sd,*l_cd,*l_rd;
    const uint8_t *l_si,*l_sm;
    // Orders columns
    const int32_t *o_ok,*o_ck,*o_od,*o_sp;
    const double *o_tp;
    const int8_t *o_os;
    const uint8_t *o_op;
    VL o_cmt;
    // Customer columns
    const int32_t *c_nk;
    const double *c_ab;
    const uint8_t *c_ms;
    VL c_nm, c_ad, c_ph, c_cmt;
    // Supplier columns
    const int32_t *s_nk;
    const double *s_ab;
    VL s_nm, s_ad, s_ph, s_cmt;
    // Part columns
    const int32_t *p_sz;
    const uint8_t *p_mf,*p_br,*p_ty,*p_co;
    VL p_nm;
    // Partsupp columns
    const int32_t *ps_pk,*ps_sk,*ps_aq;
    const double *ps_sc;
    // Nation/Region
    const uint8_t *n_nm_col, *r_nm_col;
    const int32_t *n_rk;
    // Dicts
    Dict dn_nm, dr_nm, dp_ty, dp_br, dp_mf, dp_co;
    Dict dc_ms, do_op, dl_sm, dl_si;
    // Indexes
    const int32_t* orders_pk; size_t orders_pk_n;
    const RIdx* li_ok_idx; size_t li_ok_idx_n;
    const RIdx* ps_pk_idx; size_t ps_pk_idx_n;
    const PSH* ps_hash; uint64_t ps_hash_mask;
    const uint32_t *cto_off, *cto_rows; size_t cto_off_n;
    // Dimension lookups
    std::string nation_names[25], region_names[5];
    bool is_asia[25]={}, is_europe[25]={}, is_america[25]={};
    int32_t france_nk=-1, germany_nk=-1, brazil_nk=-1, saudi_nk=-1, canada_nk=-1;
    // Q3
    uint8_t building_code=255;
    Bits q3_bs;
    // Q4/Q12
    uint8_t urgent_code=255, high_code=255;
    // Q8
    uint8_t eas_code=255; // economy anodized steel
    // Q12
    uint8_t mail_code=255, ship_code=255;
    // Q14
    std::vector<bool> is_promo;
    // Q17
    Bits q17_bs;
    // Q19
    std::vector<uint8_t> q19_cls;
    uint8_t air_code=255, airreg_code=255, deliver_code=255;
    // Q16 excluded suppliers
    Bits q16_excl_supp;
    // Part bitsets
    Bits bs_q2, bs_q9, bs_q16, bs_q20;
    // Q2 part mfgr
    std::unordered_map<int32_t,uint8_t> q2_pmf;
    // Q16 part attrs
    struct PA16{uint8_t br,ty;int32_t sz;};
    std::unordered_map<int32_t,PA16> q16_pa;
    // Supplier bitsets
    Bits saudi_bs, germany_bs, canada_bs;
    // ---- Accumulators ----
    // Q1
    struct Q1G{double sq,sp,sdp,sc,sd;int64_t cnt;};
    Q1G q1[6]; // key: rf_idx*2+ls_idx
    // Q3
    struct Q3E{double rev;int32_t od,sp;};
    std::unordered_map<int32_t,Q3E> q3m;
    // Q5
    double q5r[25]={};
    // Q6
    double q6r=0;
    // Q7
    double q7r[2][2]={}; // [pair][yr-1995]
    // Q8
    double q8t[2]={}, q8b[2]={}; // total, brazil per year
    // Q9
    double q9p[25][15]={}; // [nk][yr-1990]
    // Q10
    std::unordered_map<int32_t,double> q10m;
    // Q11
    std::unordered_map<int32_t,double> q11v;
    double q11tot=0;
    // Q12
    int64_t q12h[256]={}, q12l[256]={}; // by shipmode code
    // Q13
    uint32_t* q13cc=nullptr; // count per custkey
    // Q14
    double q14p=0, q14t=0;
    // Q15
    double* q15r=nullptr; // revenue per suppkey
    // Q16
    std::unordered_map<uint64_t, std::set<int32_t>> q16g; // pack(br<<48|ty<<32|sz) -> set of suppkeys
    // Q17
    struct Q17A{double sq;int64_t cnt;};
    std::unordered_map<int32_t,Q17A> q17a;
    struct Q17R{int32_t pk;double qty,ep;};
    std::vector<Q17R> q17rows;
    // Q18
    double* q18qs=nullptr; size_t q18mx=0;
    // Q19
    double q19r=0;
    // Q20
    std::unordered_map<uint64_t,double> q20qty;
    // Q21
    std::unordered_map<uint64_t,int32_t> q21c; // pack(ok,sk)->count
    // Q22
    struct Q22G{int64_t cnt;double sum;};
    Q22G q22g[100]={};
    // Q2 candidates
    struct Q2C{int32_t pk,sk;double sc;};
    std::vector<Q2C> q2c;
    // Q20 result
    std::set<int32_t> q20res;
};

// ============ LOAD DIMENSIONS ============
static void load_dims(Ctx& C) {
    MQO_TIME_PHASE("load_dimensions");
    auto S = [&](const std::string& t, const std::string& c){return C.gd+"/storage/"+t+"/"+c;};
    auto I = [&](const std::string& c){return C.gd+"/storage/indexes/"+c;};
    // Row counts
    C.NL = read_rc(S("lineitem","meta.txt"));
    C.NO = read_rc(S("orders","meta.txt"));
    C.NC = read_rc(S("customer","meta.txt"));
    C.NS = read_rc(S("supplier","meta.txt"));
    C.NP = read_rc(S("part","meta.txt"));
    C.NPS= read_rc(S("partsupp","meta.txt"));
    C.NN = read_rc(S("nation","meta.txt"));
    C.NR = read_rc(S("region","meta.txt"));
    // Lineitem
    size_t dummy;
    C.l_ok  = mmf<int32_t>(S("lineitem","l_orderkey.bin"), dummy);
    C.l_pk  = mmf<int32_t>(S("lineitem","l_partkey.bin"), dummy);
    C.l_sk  = mmf<int32_t>(S("lineitem","l_suppkey.bin"), dummy);
    C.l_qty = mmf<double>(S("lineitem","l_quantity.bin"), dummy);
    C.l_ep  = mmf<double>(S("lineitem","l_extendedprice.bin"), dummy);
    C.l_disc= mmf<double>(S("lineitem","l_discount.bin"), dummy);
    C.l_tax = mmf<double>(S("lineitem","l_tax.bin"), dummy);
    C.l_rf  = mmf<int8_t>(S("lineitem","l_returnflag.bin"), dummy);
    C.l_ls  = mmf<int8_t>(S("lineitem","l_linestatus.bin"), dummy);
    C.l_sd  = mmf<int32_t>(S("lineitem","l_shipdate.bin"), dummy);
    C.l_cd  = mmf<int32_t>(S("lineitem","l_commitdate.bin"), dummy);
    C.l_rd  = mmf<int32_t>(S("lineitem","l_receiptdate.bin"), dummy);
    C.l_si  = mmf<uint8_t>(S("lineitem","l_shipinstruct.bin"), dummy);
    C.l_sm  = mmf<uint8_t>(S("lineitem","l_shipmode.bin"), dummy);
    // Orders
    C.o_ok  = mmf<int32_t>(S("orders","o_orderkey.bin"), dummy);
    C.o_ck  = mmf<int32_t>(S("orders","o_custkey.bin"), dummy);
    C.o_od  = mmf<int32_t>(S("orders","o_orderdate.bin"), dummy);
    C.o_sp  = mmf<int32_t>(S("orders","o_shippriority.bin"), dummy);
    C.o_tp  = mmf<double>(S("orders","o_totalprice.bin"), dummy);
    C.o_os  = mmf<int8_t>(S("orders","o_orderstatus.bin"), dummy);
    C.o_op  = mmf<uint8_t>(S("orders","o_orderpriority.bin"), dummy);
    C.o_cmt.load(S("orders","o_comment.bin"), S("orders","o_comment_data.bin"));
    // Customer
    C.c_nk  = mmf<int32_t>(S("customer","c_nationkey.bin"), dummy);
    C.c_ab  = mmf<double>(S("customer","c_acctbal.bin"), dummy);
    C.c_ms  = mmf<uint8_t>(S("customer","c_mktsegment.bin"), dummy);
    C.c_nm.load(S("customer","c_name.bin"), S("customer","c_name_data.bin"));
    C.c_ad.load(S("customer","c_address.bin"), S("customer","c_address_data.bin"));
    C.c_ph.load(S("customer","c_phone.bin"), S("customer","c_phone_data.bin"));
    C.c_cmt.load(S("customer","c_comment.bin"), S("customer","c_comment_data.bin"));
    // Supplier
    C.s_nk  = mmf<int32_t>(S("supplier","s_nationkey.bin"), dummy);
    C.s_ab  = mmf<double>(S("supplier","s_acctbal.bin"), dummy);
    C.s_nm.load(S("supplier","s_name.bin"), S("supplier","s_name_data.bin"));
    C.s_ad.load(S("supplier","s_address.bin"), S("supplier","s_address_data.bin"));
    C.s_ph.load(S("supplier","s_phone.bin"), S("supplier","s_phone_data.bin"));
    C.s_cmt.load(S("supplier","s_comment.bin"), S("supplier","s_comment_data.bin"));
    // Part
    C.p_sz  = mmf<int32_t>(S("part","p_size.bin"), dummy);
    C.p_mf  = mmf<uint8_t>(S("part","p_mfgr.bin"), dummy);
    C.p_br  = mmf<uint8_t>(S("part","p_brand.bin"), dummy);
    C.p_ty  = mmf<uint8_t>(S("part","p_type.bin"), dummy);
    C.p_co  = mmf<uint8_t>(S("part","p_container.bin"), dummy);
    C.p_nm.load(S("part","p_name.bin"), S("part","p_name_data.bin"));
    // Partsupp
    C.ps_pk = mmf<int32_t>(S("partsupp","ps_partkey.bin"), dummy);
    C.ps_sk = mmf<int32_t>(S("partsupp","ps_suppkey.bin"), dummy);
    C.ps_aq = mmf<int32_t>(S("partsupp","ps_availqty.bin"), dummy);
    C.ps_sc = mmf<double>(S("partsupp","ps_supplycost.bin"), dummy);
    // Nation/Region
    C.n_nm_col = mmf<uint8_t>(S("nation","n_name.bin"), dummy);
    C.n_rk     = mmf<int32_t>(S("nation","n_regionkey.bin"), dummy);
    C.r_nm_col = mmf<uint8_t>(S("region","r_name.bin"), dummy);
    // Dicts
    C.dn_nm.load(S("nation","n_name_dict.bin"));
    C.dr_nm.load(S("region","r_name_dict.bin"));
    C.dp_ty.load(S("part","p_type_dict.bin"));
    C.dp_br.load(S("part","p_brand_dict.bin"));
    C.dp_mf.load(S("part","p_mfgr_dict.bin"));
    C.dp_co.load(S("part","p_container_dict.bin"));
    C.dc_ms.load(S("customer","c_mktsegment_dict.bin"));
    C.do_op.load(S("orders","o_orderpriority_dict.bin"));
    C.dl_sm.load(S("lineitem","l_shipmode_dict.bin"));
    C.dl_si.load(S("lineitem","l_shipinstruct_dict.bin"));
    // Indexes
    C.orders_pk = mmf<int32_t>(I("orders_pk_index.bin"), C.orders_pk_n);
    C.li_ok_idx = mmf<RIdx>(I("lineitem_orderkey_idx.bin"), C.li_ok_idx_n);
    C.ps_pk_idx = mmf<RIdx>(I("partsupp_partkey_idx.bin"), C.ps_pk_idx_n);
    size_t psh_n;
    C.ps_hash   = mmf<PSH>(I("partsupp_composite_hash.bin"), psh_n);
    C.ps_hash_mask = psh_n - 1;
    C.cto_off   = mmf<uint32_t>(I("custkey_to_orders_offsets.bin"), C.cto_off_n);
    C.cto_rows  = mmf<uint32_t>(I("custkey_to_orders_rows.bin"), dummy);

    // ---- Build dimension lookups ----
    for(int i=0;i<(int)C.NN;++i) C.nation_names[i] = C.dn_nm.v[C.n_nm_col[i]];
    for(int i=0;i<(int)C.NR;++i) C.region_names[i] = C.dr_nm.v[C.r_nm_col[i]];
    int32_t asia_rk=-1, europe_rk=-1, america_rk=-1;
    for(int i=0;i<(int)C.NR;++i){
        if(C.region_names[i]=="ASIA") asia_rk=i;
        if(C.region_names[i]=="EUROPE") europe_rk=i;
        if(C.region_names[i]=="AMERICA") america_rk=i;
    }
    for(int i=0;i<(int)C.NN;++i){
        int32_t rk = C.n_rk[i];
        if(rk==asia_rk) C.is_asia[i]=true;
        if(rk==europe_rk) C.is_europe[i]=true;
        if(rk==america_rk) C.is_america[i]=true;
        if(C.nation_names[i]=="FRANCE") C.france_nk=i;
        if(C.nation_names[i]=="GERMANY") C.germany_nk=i;
        if(C.nation_names[i]=="BRAZIL") C.brazil_nk=i;
        if(C.nation_names[i]=="SAUDI ARABIA") C.saudi_nk=i;
        if(C.nation_names[i]=="CANADA") C.canada_nk=i;
    }
    // Q3: BUILDING customer bitset
    C.building_code = (uint8_t)C.dc_ms.find("BUILDING");
    C.q3_bs = Bits(C.NC+1);
    for(size_t i=0;i<C.NC;++i)
        if(C.c_ms[i]==C.building_code) C.q3_bs.set(i+1); // custkey = i+1
    // Q4/Q12: orderpriority codes
    C.urgent_code = (uint8_t)C.do_op.find("1-URGENT");
    C.high_code   = (uint8_t)C.do_op.find("2-HIGH");
    // Q8: ECONOMY ANODIZED STEEL
    C.eas_code = (uint8_t)C.dp_ty.find("ECONOMY ANODIZED STEEL");
    // Q12: MAIL/SHIP
    C.mail_code = (uint8_t)C.dl_sm.find("MAIL");
    C.ship_code = (uint8_t)C.dl_sm.find("SHIP");
    // Q14: is_promo
    C.is_promo.resize(C.dp_ty.v.size(), false);
    for(size_t i=0;i<C.dp_ty.v.size();++i)
        if(C.dp_ty.v[i].compare(0,5,"PROMO")==0) C.is_promo[i]=true;
    // Q17: Brand#23 + MED BOX qualifying partkeys
    uint8_t br23 = (uint8_t)C.dp_br.find("Brand#23");
    uint8_t mb   = (uint8_t)C.dp_co.find("MED BOX");
    C.q17_bs = Bits(C.NP+1);
    for(size_t i=0;i<C.NP;++i)
        if(C.p_br[i]==br23 && C.p_co[i]==mb) C.q17_bs.set(i+1);
    // Q19: part classification
    uint8_t br12=(uint8_t)C.dp_br.find("Brand#12");
    uint8_t br34=(uint8_t)C.dp_br.find("Brand#34");
    C.air_code   =(uint8_t)C.dl_sm.find("AIR");
    C.airreg_code=(uint8_t)C.dl_sm.find("AIR REG");
    C.deliver_code=(uint8_t)C.dl_si.find("DELIVER IN PERSON");
    // Build container sets
    std::vector<bool> sm_co(C.dp_co.v.size(),false), med_co(C.dp_co.v.size(),false), lg_co(C.dp_co.v.size(),false);
    for(size_t i=0;i<C.dp_co.v.size();++i){
        auto& s=C.dp_co.v[i];
        if(s=="SM CASE"||s=="SM BOX"||s=="SM PACK"||s=="SM PKG") sm_co[i]=true;
        if(s=="MED BAG"||s=="MED BOX"||s=="MED PKG"||s=="MED PACK") med_co[i]=true;
        if(s=="LG CASE"||s=="LG BOX"||s=="LG PACK"||s=="LG PKG") lg_co[i]=true;
    }
    C.q19_cls.assign(C.NP, 0);
    for(size_t i=0;i<C.NP;++i){
        uint8_t br=C.p_br[i], co=C.p_co[i]; int32_t sz=C.p_sz[i];
        if(br==br12 && sm_co[co]  && sz>=1 && sz<=5)  C.q19_cls[i]|=1;
        if(br==br23 && med_co[co] && sz>=1 && sz<=10) C.q19_cls[i]|=2;
        if(br==br34 && lg_co[co]  && sz>=1 && sz<=15) C.q19_cls[i]|=4;
    }
    // Q16: excluded suppliers (s_comment LIKE '%Customer%Complaints%')
    C.q16_excl_supp = Bits(C.NS+1);
    for(size_t i=0;i<C.NS;++i){
        auto sv = C.s_cmt.get(i);
        if(has2(sv.data(),sv.size(),"Customer",8,"Complaints",10))
            C.q16_excl_supp.set(i+1); // suppkey=i+1
    }
    // Supplier nation bitsets
    C.saudi_bs = Bits(C.NS+1);
    C.germany_bs = Bits(C.NS+1);
    C.canada_bs = Bits(C.NS+1);
    for(size_t i=0;i<C.NS;++i){
        int32_t nk = C.s_nk[i];
        if(nk==C.saudi_nk) C.saudi_bs.set(i+1);
        if(nk==C.germany_nk) C.germany_bs.set(i+1);
        if(nk==C.canada_nk) C.canada_bs.set(i+1);
    }
    // Q13 alloc
    C.q13cc = (uint32_t*)calloc(C.NC+2, sizeof(uint32_t));
    // Q15 alloc
    C.q15r = (double*)calloc(C.NS+1, sizeof(double));
    // Q18 alloc
    C.q18mx = C.orders_pk_n; // max orderkey + 1
    C.q18qs = (double*)calloc(C.q18mx, sizeof(double));
    // Init Q1
    memset(C.q1, 0, sizeof(C.q1));
}

// ============ FUSED SCAN PART ============
static void fused_scan_part(Ctx& C) {
    uint32_t a = C.active;
    uint32_t pa = a & (Q2B|Q9B|Q16B|Q20B);
    if(!pa) return;
    MQO_TIME_PHASE("fused_scan_part");
    size_t N = C.NP;
    // Q2 filters
    uint8_t brand45 = (uint8_t)C.dp_br.find("Brand#45");
    std::vector<bool> brass_ty(C.dp_ty.v.size(), false);
    for(size_t i=0;i<C.dp_ty.v.size();++i){
        auto& s=C.dp_ty.v[i];
        if(s.size()>=5 && s.compare(s.size()-5,5,"BRASS")==0) brass_ty[i]=true;
    }
    std::vector<bool> medpol_ty(C.dp_ty.v.size(), false);
    for(size_t i=0;i<C.dp_ty.v.size();++i){
        auto& s=C.dp_ty.v[i];
        if(s.size()>=15 && s.compare(0,15,"MEDIUM POLISHED")==0) medpol_ty[i]=true;
    }
    static const int q16_sizes[]={49,14,23,45,19,3,36,9};
    std::vector<bool> q16sz(51,false);
    for(int s:q16_sizes) q16sz[s]=true;

    C.bs_q2  = Bits(N+1);
    C.bs_q9  = Bits(N+1);
    C.bs_q16 = Bits(N+1);
    C.bs_q20 = Bits(N+1);

    for(size_t i=0;i<N;++i){
        int32_t pk = (int32_t)(i+1); // partkey = row+1
        // Q2: p_size=15 AND p_type LIKE '%BRASS'
        if((pa&Q2B) && C.p_sz[i]==15 && brass_ty[C.p_ty[i]]){
            C.bs_q2.set(pk);
            C.q2_pmf[pk] = C.p_mf[i];
        }
        // Q9: p_name LIKE '%green%'
        if(pa&Q9B){
            auto nm = C.p_nm.get(i);
            if(contains(nm.data(),nm.size(),"green",5))
                C.bs_q9.set(pk);
        }
        // Q16: brand != Brand#45 AND type NOT LIKE 'MEDIUM POLISHED%' AND size IN set
        if((pa&Q16B) && C.p_br[i]!=brand45 && !medpol_ty[C.p_ty[i]]
            && C.p_sz[i]>=1 && C.p_sz[i]<=50 && q16sz[C.p_sz[i]]){
            C.bs_q16.set(pk);
            C.q16_pa[pk] = {C.p_br[i], C.p_ty[i], C.p_sz[i]};
        }
        // Q20: p_name LIKE 'forest%'
        if(pa&Q20B){
            auto nm = C.p_nm.get(i);
            if(nm.size()>=6 && memcmp(nm.data(),"forest",6)==0)
                C.bs_q20.set(pk);
        }
    }
}

// ============ FUSED SCAN LINEITEM ============
static void fused_scan_lineitem(Ctx& C) {
    uint32_t a = C.active;
    uint32_t la = a & (Q1B|Q3B|Q5B|Q6B|Q7B|Q8B|Q9B|Q10B|Q12B|Q14B|Q15B|Q17B|Q18B|Q19B|Q20B|Q21B);
    if(!la) return;
    MQO_TIME_PHASE("fused_scan_lineitem");
    const size_t N = C.NL;
    const int nt = omp_get_max_threads();
    // Need orders probe?
    uint32_t need_o = la & (Q3B|Q5B|Q7B|Q8B|Q9B|Q10B|Q12B|Q21B);

    // Thread-local accumulators
    struct TL {
        // Q1
        Ctx::Q1G q1[6];
        // Q3
        std::unordered_map<int32_t, Ctx::Q3E> q3;
        // Q5
        double q5[25];
        // Q6
        double q6;
        // Q7
        double q7[2][2];
        // Q8
        double q8t[2], q8b[2];
        // Q9
        double q9[25][15];
        // Q10
        std::unordered_map<int32_t, double> q10;
        // Q12
        int64_t q12h[256], q12l[256];
        // Q14
        double q14p, q14t;
        // Q15
        std::vector<double> q15;
        // Q17
        std::unordered_map<int32_t, Ctx::Q17A> q17a;
        std::vector<Ctx::Q17R> q17r;
        // Q19
        double q19;
        // Q20
        std::unordered_map<uint64_t, double> q20;
        // Q21
        std::unordered_map<uint64_t, int32_t> q21;
        TL(): q6(0), q14p(0), q14t(0), q19(0) {
            memset(q1,0,sizeof(q1));
            memset(q5,0,sizeof(q5));
            memset(q7,0,sizeof(q7));
            memset(q8t,0,sizeof(q8t));
            memset(q8b,0,sizeof(q8b));
            memset(q9,0,sizeof(q9));
            memset(q12h,0,sizeof(q12h));
            memset(q12l,0,sizeof(q12l));
            q15.assign(100001, 0.0);
        }
    };
    std::vector<TL> tl(nt);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        TL& T = tl[tid];
        int32_t last_ok = -1, last_orow = -1;

        #pragma omp for schedule(static)
        for(size_t i = 0; i < N; ++i){
            int32_t ok = C.l_ok[i];
            int32_t sd = C.l_sd[i];
            // Shared orders probe (cached)
            int32_t orow = -1;
            if(need_o){
                if(ok != last_ok){
                    last_ok = ok;
                    last_orow = (ok < (int32_t)C.orders_pk_n) ? C.orders_pk[ok] : -1;
                }
                orow = last_orow;
            }
            // ---- Q1 ----
            if((la&Q1B) && sd <= D_19980902){
                int8_t rf=C.l_rf[i], ls=C.l_ls[i];
                double qty=C.l_qty[i], ep=C.l_ep[i], d=C.l_disc[i], tx=C.l_tax[i];
                int k = (rf=='A'?0:rf=='N'?1:2)*2 + (ls=='F'?0:1);
                auto& g = T.q1[k];
                g.sq+=qty; g.sp+=ep; g.sdp+=ep*(1-d); g.sc+=ep*(1-d)*(1+tx);
                g.sd+=d; g.cnt++;
            }
            // ---- Q6 ----
            if((la&Q6B) && sd>=D_19940101 && sd<D_19950101){
                double d=C.l_disc[i];
                if(d>=0.05 && d<=0.07 && C.l_qty[i]<24.0)
                    T.q6 += C.l_ep[i]*d;
            }
            // ---- Q18 ----
            if(la&Q18B){
                #pragma omp atomic
                C.q18qs[ok] += C.l_qty[i];
            }
            // ---- Q3 ----
            if((la&Q3B) && sd > D_19950315 && orow>=0){
                int32_t od = C.o_od[orow];
                if(od < D_19950315){
                    int32_t ck = C.o_ck[orow];
                    if(ck>0 && ck<=(int32_t)C.NC && C.q3_bs.test(ck)){
                        double rev = C.l_ep[i]*(1.0-C.l_disc[i]);
                        auto& e = T.q3[ok];
                        e.rev += rev; e.od = od; e.sp = C.o_sp[orow];
                    }
                }
            }
            // ---- Q5 ----
            if((la&Q5B) && orow>=0){
                int32_t od = C.o_od[orow];
                if(od>=D_19940101 && od<D_19950101){
                    int32_t ck = C.o_ck[orow];
                    if(ck>0 && ck<=(int32_t)C.NC){
                        int32_t cn = C.c_nk[ck-1];
                        if(C.is_asia[cn]){
                            int32_t sk = C.l_sk[i];
                            if(sk>0 && sk<=(int32_t)C.NS){
                                int32_t sn = C.s_nk[sk-1];
                                if(sn==cn)
                                    T.q5[cn] += C.l_ep[i]*(1.0-C.l_disc[i]);
                            }
                        }
                    }
                }
            }
            // ---- Q7 ----
            if((la&Q7B) && sd>=D_19950101 && sd<=D_19961231 && orow>=0){
                int32_t sk = C.l_sk[i];
                if(sk>0 && sk<=(int32_t)C.NS){
                    int32_t sn = C.s_nk[sk-1];
                    if(sn==C.france_nk || sn==C.germany_nk){
                        int32_t ck = C.o_ck[orow];
                        if(ck>0 && ck<=(int32_t)C.NC){
                            int32_t cn = C.c_nk[ck-1];
                            int pair=-1;
                            if(sn==C.france_nk && cn==C.germany_nk) pair=0;
                            else if(sn==C.germany_nk && cn==C.france_nk) pair=1;
                            if(pair>=0){
                                int yr = (sd<D_19960101) ? 0 : 1;
                                T.q7[pair][yr] += C.l_ep[i]*(1.0-C.l_disc[i]);
                            }
                        }
                    }
                }
            }
            // ---- Q8 ----
            if((la&Q8B) && orow>=0){
                int32_t pk = C.l_pk[i];
                if(pk>0 && pk<=(int32_t)C.NP && C.p_ty[pk-1]==C.eas_code){
                    int32_t od = C.o_od[orow];
                    if(od>=D_19950101 && od<=D_19961231){
                        int32_t ck = C.o_ck[orow];
                        if(ck>0 && ck<=(int32_t)C.NC){
                            int32_t cn = C.c_nk[ck-1];
                            if(C.is_america[cn]){
                                double vol = C.l_ep[i]*(1.0-C.l_disc[i]);
                                int yr = (od<D_19960101)?0:1;
                                T.q8t[yr] += vol;
                                int32_t sk = C.l_sk[i];
                                if(sk>0 && sk<=(int32_t)C.NS && C.s_nk[sk-1]==C.brazil_nk)
                                    T.q8b[yr] += vol;
                            }
                        }
                    }
                }
            }
            // ---- Q9 ----
            if((la&Q9B)){
                int32_t pk = C.l_pk[i];
                if(pk>0 && pk<=(int32_t)C.NP && C.bs_q9.test(pk)){
                    int32_t sk = C.l_sk[i];
                    int32_t ps_row = ps_lookup(C.ps_hash, C.ps_hash_mask, pk, sk);
                    if(ps_row>=0 && orow>=0){
                        double psc = C.ps_sc[ps_row];
                        int32_t yr = yfd(C.o_od[orow]);
                        int32_t sn = (sk>0&&sk<=(int32_t)C.NS) ? C.s_nk[sk-1] : -1;
                        if(sn>=0 && yr>=1990 && yr<2005){
                            double amt = C.l_ep[i]*(1.0-C.l_disc[i]) - psc*C.l_qty[i];
                            T.q9[sn][yr-1990] += amt;
                        }
                    }
                }
            }
            // ---- Q10 ----
            if((la&Q10B) && C.l_rf[i]=='R' && orow>=0){
                int32_t od = C.o_od[orow];
                if(od>=D_19931001 && od<D_19940101){
                    int32_t ck = C.o_ck[orow];
                    if(ck>0) T.q10[ck] += C.l_ep[i]*(1.0-C.l_disc[i]);
                }
            }
            // ---- Q12 ----
            if((la&Q12B)){
                uint8_t sm = C.l_sm[i];
                if(sm==C.mail_code || sm==C.ship_code){
                    int32_t rd = C.l_rd[i];
                    if(rd>=D_19940101 && rd<D_19950101){
                        int32_t cd = C.l_cd[i];
                        if(cd<rd && sd<cd && orow>=0){
                            uint8_t op = C.o_op[orow];
                            if(op==C.urgent_code || op==C.high_code)
                                T.q12h[sm]++;
                            else
                                T.q12l[sm]++;
                        }
                    }
                }
            }
            // ---- Q14 ----
            if((la&Q14B) && sd>=D_19950901 && sd<D_19951001){
                double rev = C.l_ep[i]*(1.0-C.l_disc[i]);
                T.q14t += rev;
                int32_t pk = C.l_pk[i];
                if(pk>0 && pk<=(int32_t)C.NP && C.is_promo[C.p_ty[pk-1]])
                    T.q14p += rev;
            }
            // ---- Q15 ----
            if((la&Q15B) && sd>=D_19960101 && sd<D_19960401){
                int32_t sk = C.l_sk[i];
                if(sk>0 && sk<=(int32_t)C.NS)
                    T.q15[sk] += C.l_ep[i]*(1.0-C.l_disc[i]);
            }
            // ---- Q17 ----
            if((la&Q17B)){
                int32_t pk = C.l_pk[i];
                if(pk>0 && pk<=(int32_t)C.NP && C.q17_bs.test(pk)){
                    double q=C.l_qty[i], ep=C.l_ep[i];
                    auto& ac = T.q17a[pk];
                    ac.sq += q; ac.cnt++;
                    T.q17r.push_back({pk, q, ep});
                }
            }
            // ---- Q19 ----
            if((la&Q19B)){
                uint8_t sm = C.l_sm[i];
                if((sm==C.air_code||sm==C.airreg_code) && C.l_si[i]==C.deliver_code){
                    int32_t pk = C.l_pk[i];
                    if(pk>0 && pk<=(int32_t)C.NP){
                        uint8_t cls = C.q19_cls[pk-1];
                        if(cls){
                            double q = C.l_qty[i];
                            bool m = false;
                            if((cls&1) && q>=1 && q<=11) m=true;
                            if((cls&2) && q>=10 && q<=20) m=true;
                            if((cls&4) && q>=20 && q<=30) m=true;
                            if(m) T.q19 += C.l_ep[i]*(1.0-C.l_disc[i]);
                        }
                    }
                }
            }
            // ---- Q20 ----
            if((la&Q20B) && sd>=D_19940101 && sd<D_19950101){
                int32_t pk = C.l_pk[i];
                if(pk>0 && pk<=(int32_t)C.NP && C.bs_q20.test(pk)){
                    int32_t sk = C.l_sk[i];
                    T.q20[pk64(pk,sk)] += C.l_qty[i];
                }
            }
            // ---- Q21 ----
            if((la&Q21B) && orow>=0){
                int32_t sk = C.l_sk[i];
                if(sk>0 && sk<=(int32_t)C.NS && C.saudi_bs.test(sk)){
                    if(C.o_os[orow]=='F' && C.l_rd[i]>C.l_cd[i]){
                        T.q21[pk64(ok,sk)]++;
                    }
                }
            }
        } // end parallel for
    } // end parallel

    // ---- Merge thread-local results ----
    for(int t=0;t<nt;++t){
        TL& T = tl[t];
        for(int k=0;k<6;++k){
            C.q1[k].sq+=T.q1[k].sq; C.q1[k].sp+=T.q1[k].sp;
            C.q1[k].sdp+=T.q1[k].sdp; C.q1[k].sc+=T.q1[k].sc;
            C.q1[k].sd+=T.q1[k].sd; C.q1[k].cnt+=T.q1[k].cnt;
        }
        for(auto&[ok,e]:T.q3){auto&d=C.q3m[ok];d.rev+=e.rev;d.od=e.od;d.sp=e.sp;}
        for(int j=0;j<25;++j) C.q5r[j]+=T.q5[j];
        C.q6r += T.q6;
        for(int p=0;p<2;++p)for(int y=0;y<2;++y) C.q7r[p][y]+=T.q7[p][y];
        for(int y=0;y<2;++y){C.q8t[y]+=T.q8t[y];C.q8b[y]+=T.q8b[y];}
        for(int n=0;n<25;++n)for(int y=0;y<15;++y) C.q9p[n][y]+=T.q9[n][y];
        for(auto&[ck,r]:T.q10) C.q10m[ck]+=r;
        for(int j=0;j<256;++j){C.q12h[j]+=T.q12h[j];C.q12l[j]+=T.q12l[j];}
        C.q14p+=T.q14p; C.q14t+=T.q14t;
        for(size_t j=0;j<=C.NS;++j) C.q15r[j]+=T.q15[j];
        for(auto&[pk,ac]:T.q17a){auto&d=C.q17a[pk];d.sq+=ac.sq;d.cnt+=ac.cnt;}
        C.q17rows.insert(C.q17rows.end(), T.q17r.begin(), T.q17r.end());
        C.q19r += T.q19;
        for(auto&[k,v]:T.q20) C.q20qty[k]+=v;
        for(auto&[k,v]:T.q21) C.q21c[k]+=v;
    }
}

// ============ FUSED SCAN ORDERS ============
static void fused_scan_orders(Ctx& C) {
    uint32_t a = C.active;
    uint32_t oa = a & (Q4B|Q13B);
    if(!oa) return;
    MQO_TIME_PHASE("fused_scan_orders");
    const size_t N = C.NO;
    const int nt = omp_get_max_threads();

    // Q4: thread-local counts per orderpriority (max 256 codes)
    std::vector<std::vector<int64_t>> q4_tl(nt, std::vector<int64_t>(256, 0));

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        #pragma omp for schedule(static)
        for(size_t i = 0; i < N; ++i){
            // ---- Q4 ----
            if(oa & Q4B){
                int32_t od = C.o_od[i];
                if(od>=D_19930701 && od<D_19931001){
                    int32_t ok = C.o_ok[i];
                    if(ok>=0 && ok<(int32_t)C.li_ok_idx_n){
                        auto& idx = C.li_ok_idx[ok];
                        bool exists = false;
                        for(uint32_t j=idx.start; j<idx.start+idx.count && !exists; ++j){
                            if(C.l_cd[j] < C.l_rd[j]) exists = true;
                        }
                        if(exists) q4_tl[tid][C.o_op[i]]++;
                    }
                }
            }
            // ---- Q13 ----
            if(oa & Q13B){
                auto sv = C.o_cmt.get(i);
                if(!has2(sv.data(), sv.size(), "special", 7, "requests", 8)){
                    int32_t ck = C.o_ck[i];
                    if(ck>0 && ck<=(int32_t)C.NC){
                        #pragma omp atomic
                        C.q13cc[ck]++;
                    }
                }
            }
        }
    }
    // Merge Q4
    std::vector<int64_t> q4_cnt(256, 0);
    for(int t=0;t<nt;++t) for(int j=0;j<256;++j) q4_cnt[j]+=q4_tl[t][j];
    // Store in context for finalize
    // We'll use q12h/q12l arrays are for Q12. Let's store Q4 data differently.
    // Store Q4 data in a temp structure
    // Actually let's just output Q4 inline here or store separately
    // We'll handle Q4 finalize using q4_cnt
    // Store it as a static since context doesn't have Q4 storage
    // Let me just do Q4 finalize here
    if(a & Q4B){
        MQO_TIME_PHASE("Q4_finalize");
        std::string path = C.od + "/Q4.csv";
        FILE* f = fopen(path.c_str(), "w");
        fprintf(f, "o_orderpriority,order_count\n");
        // Collect and sort by orderpriority string
        std::vector<std::pair<std::string,int64_t>> rows;
        for(int j=0;j<(int)C.do_op.v.size();++j)
            if(q4_cnt[j]>0) rows.push_back({C.do_op.v[j], q4_cnt[j]});
        std::sort(rows.begin(),rows.end());
        for(auto&[s,c]:rows) fprintf(f,"%s,%ld\n",s.c_str(),c);
        fclose(f);
    }
}

// ============ FUSED SCAN PARTSUPP ============
static void fused_scan_partsupp(Ctx& C) {
    uint32_t a = C.active;
    uint32_t pa = a & (Q2B|Q11B|Q16B|Q20B);
    if(!pa) return;
    MQO_TIME_PHASE("fused_scan_partsupp");
    const size_t N = C.NPS;
    const int nt = omp_get_max_threads();

    // Thread-local for Q11
    struct TL11 {
        std::unordered_map<int32_t,double> val;
        double total = 0;
    };
    std::vector<TL11> q11_tl(nt);
    // Thread-local for Q2
    struct TL2 { std::vector<Ctx::Q2C> cands; };
    std::vector<TL2> q2_tl(nt);
    // Thread-local for Q16
    struct TL16 { std::unordered_map<uint64_t, std::vector<int32_t>> groups; }; // pack(br,ty,sz) -> suppkeys
    std::vector<TL16> q16_tl(nt);
    // Thread-local for Q20
    struct TL20 { std::set<int32_t> result; };
    std::vector<TL20> q20_tl(nt);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        #pragma omp for schedule(static)
        for(size_t i = 0; i < N; ++i){
            int32_t pk = C.ps_pk[i], sk = C.ps_sk[i];
            // ---- Q2 ----
            if((pa&Q2B) && pk>0 && pk<=(int32_t)C.NP && C.bs_q2.test(pk)){
                if(sk>0 && sk<=(int32_t)C.NS){
                    int32_t sn = C.s_nk[sk-1];
                    if(sn>=0 && sn<25 && C.is_europe[sn])
                        q2_tl[tid].cands.push_back({pk, sk, C.ps_sc[i]});
                }
            }
            // ---- Q11 ----
            if(pa&Q11B){
                if(sk>0 && sk<=(int32_t)C.NS && C.germany_bs.test(sk)){
                    double val = C.ps_sc[i] * (double)C.ps_aq[i];
                    q11_tl[tid].val[pk] += val;
                    q11_tl[tid].total += val;
                }
            }
            // ---- Q16 ----
            if((pa&Q16B) && pk>0 && pk<=(int32_t)C.NP && C.bs_q16.test(pk)){
                if(sk>0 && sk<=(int32_t)C.NS && !C.q16_excl_supp.test(sk)){
                    auto it = C.q16_pa.find(pk);
                    if(it!=C.q16_pa.end()){
                        auto& at = it->second;
                        uint64_t key = ((uint64_t)at.br<<40)|((uint64_t)at.ty<<32)|(uint32_t)at.sz;
                        q16_tl[tid].groups[key].push_back(sk);
                    }
                }
            }
            // ---- Q20 ----
            if((pa&Q20B) && pk>0 && pk<=(int32_t)C.NP && C.bs_q20.test(pk)){
                if(sk>0 && sk<=(int32_t)C.NS && C.canada_bs.test(sk)){
                    uint64_t key = pk64(pk, sk);
                    auto it = C.q20qty.find(key);
                    if(it!=C.q20qty.end()){
                        if((double)C.ps_aq[i] > 0.5 * it->second)
                            q20_tl[tid].result.insert(sk);
                    }
                }
            }
        }
    }
    // Merge
    for(int t=0;t<nt;++t){
        for(auto& c:q2_tl[t].cands) C.q2c.push_back(c);
        for(auto&[pk,v]:q11_tl[t].val) C.q11v[pk]+=v;
        C.q11tot += q11_tl[t].total;
        for(auto&[key,sks]:q16_tl[t].groups)
            for(int32_t sk:sks) C.q16g[key].insert(sk);
        for(int32_t sk:q20_tl[t].result) C.q20res.insert(sk);
    }
}

// ============ Q22 (Independent) ============
static void run_q22(Ctx& C) {
    if(!(C.active & Q22B)) return;
    MQO_TIME_PHASE("Q22_execute");
    // Target country codes
    static const int tgt[] = {13,31,23,29,30,18,17};
    bool is_tgt[100] = {};
    for(int c : tgt) is_tgt[c] = true;
    // Pass 1: compute avg(c_acctbal) for matching customers with acctbal > 0
    double sum_ab = 0; int64_t cnt_ab = 0;
    for(size_t i = 0; i < C.NC; ++i){
        auto ph = C.c_ph.get(i);
        if(ph.size()>=2){
            int cc = (ph[0]-'0')*10 + (ph[1]-'0');
            if(cc>=0 && cc<100 && is_tgt[cc] && C.c_ab[i] > 0.0){
                sum_ab += C.c_ab[i]; cnt_ab++;
            }
        }
    }
    double avg_ab = (cnt_ab>0) ? sum_ab/cnt_ab : 0.0;
    // Pass 2: filter and aggregate
    memset(C.q22g, 0, sizeof(C.q22g));
    for(size_t i = 0; i < C.NC; ++i){
        auto ph = C.c_ph.get(i);
        if(ph.size()<2) continue;
        int cc = (ph[0]-'0')*10 + (ph[1]-'0');
        if(cc<0 || cc>=100 || !is_tgt[cc]) continue;
        if(C.c_ab[i] <= avg_ab) continue;
        // NOT EXISTS: check if customer has orders
        int32_t ck = (int32_t)(i+1);
        if(ck < (int32_t)C.cto_off_n - 1){
            if(C.cto_off[ck+1] > C.cto_off[ck]) continue; // has orders
        }
        C.q22g[cc].cnt++;
        C.q22g[cc].sum += C.c_ab[i];
    }
}

// ============ FINALIZE ============
static void finalize_q1(Ctx& C) {
    if(!(C.active & Q1B)) return;
    MQO_TIME_PHASE("Q1_finalize");
    struct R{char rf,ls;double sq,sp,sdp,sc,aq,ap,ad;int64_t cnt;};
    std::vector<R> rows;
    static const char rfs[]={'A','N','R'}; static const char lss[]={'F','O'};
    for(int ri=0;ri<3;++ri) for(int li=0;li<2;++li){
        int k=ri*2+li;
        if(C.q1[k].cnt>0){
            auto& g=C.q1[k]; double n=(double)g.cnt;
            rows.push_back({rfs[ri],lss[li],g.sq,g.sp,g.sdp,g.sc,
                g.sq/n,g.sp/n,g.sd/n,g.cnt});
        }
    }
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){
        return a.rf!=b.rf ? a.rf<b.rf : a.ls<b.ls;});
    FILE* f=fopen((C.od+"/Q1.csv").c_str(),"w");
    fprintf(f,"l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
    for(auto&r:rows)
        fprintf(f,"%c,%c,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
            r.rf,r.ls,r.sq,r.sp,r.sdp,r.sc,r.aq,r.ap,r.ad,r.cnt);
    fclose(f);
}

static void finalize_q2(Ctx& C) {
    if(!(C.active & Q2B)) return;
    MQO_TIME_PHASE("Q2_finalize");
    // Find min supplycost per partkey
    std::unordered_map<int32_t,double> min_sc;
    for(auto& c:C.q2c){
        auto it=min_sc.find(c.pk);
        if(it==min_sc.end() || c.sc<it->second) min_sc[c.pk]=c.sc;
    }
    struct R{double ab;std::string sn,nn;int32_t pk;std::string mf,sa,sp,sc;};
    std::vector<R> rows;
    for(auto& c:C.q2c){
        if(std::abs(c.sc - min_sc[c.pk]) > 1e-9) continue;
        int32_t sk=c.sk;
        int32_t sn_nk = C.s_nk[sk-1];
        R r;
        r.ab = C.s_ab[sk-1];
        r.sn = std::string(C.s_nm.get(sk-1));
        r.nn = C.nation_names[sn_nk];
        r.pk = c.pk;
        r.mf = C.dp_mf.v[C.q2_pmf[c.pk]];
        r.sa = std::string(C.s_ad.get(sk-1));
        r.sp = std::string(C.s_ph.get(sk-1));
        r.sc = std::string(C.s_cmt.get(sk-1));
        rows.push_back(std::move(r));
    }
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){
        if(a.ab!=b.ab) return a.ab>b.ab;
        if(a.nn!=b.nn) return a.nn<b.nn;
        if(a.sn!=b.sn) return a.sn<b.sn;
        return a.pk<b.pk;
    });
    FILE* f=fopen((C.od+"/Q2.csv").c_str(),"w");
    fprintf(f,"s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment\n");
    int lim=std::min((int)rows.size(),100);
    for(int i=0;i<lim;++i){
        auto&r=rows[i];
        fprintf(f,"%.2f,",r.ab);
        csvs(f,r.sn); fputc(',',f);
        csvs(f,r.nn); fputc(',',f);
        fprintf(f,"%d,",r.pk);
        csvs(f,r.mf); fputc(',',f);
        csvs(f,r.sa); fputc(',',f);
        csvs(f,r.sp); fputc(',',f);
        csvs(f,r.sc); fputc('\n',f);
    }
    fclose(f);
}

static void finalize_q3(Ctx& C) {
    if(!(C.active & Q3B)) return;
    MQO_TIME_PHASE("Q3_finalize");
    struct R{int32_t ok;double rev;int32_t od,sp;};
    std::vector<R> rows;
    for(auto&[ok,e]:C.q3m) rows.push_back({ok,e.rev,e.od,e.sp});
    std::partial_sort(rows.begin(), rows.begin()+std::min((size_t)10,rows.size()),
        rows.end(), [](auto&a,auto&b){return a.rev!=b.rev?a.rev>b.rev:a.od<b.od;});
    FILE* f=fopen((C.od+"/Q3.csv").c_str(),"w");
    fprintf(f,"l_orderkey,revenue,o_orderdate,o_shippriority\n");
    int lim=std::min((int)rows.size(),10);
    for(int i=0;i<lim;++i){
        auto&r=rows[i];
        fprintf(f,"%d,%.2f,%s,%d\n",r.ok,r.rev,dstr(r.od).c_str(),r.sp);
    }
    fclose(f);
}

// Q4 is finalized inline in fused_scan_orders

static void finalize_q5(Ctx& C) {
    if(!(C.active & Q5B)) return;
    MQO_TIME_PHASE("Q5_finalize");
    struct R{std::string nn;double rev;};
    std::vector<R> rows;
    for(int i=0;i<25;++i)
        if(C.is_asia[i] && C.q5r[i]!=0.0) rows.push_back({C.nation_names[i],C.q5r[i]});
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){return a.rev>b.rev;});
    FILE* f=fopen((C.od+"/Q5.csv").c_str(),"w");
    fprintf(f,"n_name,revenue\n");
    for(auto&r:rows) fprintf(f,"%s,%.2f\n",r.nn.c_str(),r.rev);
    fclose(f);
}

static void finalize_q6(Ctx& C) {
    if(!(C.active & Q6B)) return;
    MQO_TIME_PHASE("Q6_finalize");
    FILE* f=fopen((C.od+"/Q6.csv").c_str(),"w");
    fprintf(f,"revenue\n%.2f\n",C.q6r);
    fclose(f);
}

static void finalize_q7(Ctx& C) {
    if(!(C.active & Q7B)) return;
    MQO_TIME_PHASE("Q7_finalize");
    FILE* f=fopen((C.od+"/Q7.csv").c_str(),"w");
    fprintf(f,"supp_nation,cust_nation,l_year,revenue\n");
    // pair 0: FRANCE->GERMANY, pair 1: GERMANY->FRANCE
    const char* sn[2]={nullptr,nullptr};
    const char* cn[2]={nullptr,nullptr};
    sn[0]=C.nation_names[C.france_nk].c_str(); cn[0]=C.nation_names[C.germany_nk].c_str();
    sn[1]=C.nation_names[C.germany_nk].c_str(); cn[1]=C.nation_names[C.france_nk].c_str();
    // Sort by supp_nation ASC, cust_nation ASC, l_year ASC
    struct R{const char*s,*c;int y;double r;};
    std::vector<R> rows;
    for(int p=0;p<2;++p)for(int y=0;y<2;++y)
        if(C.q7r[p][y]!=0.0) rows.push_back({sn[p],cn[p],1995+y,C.q7r[p][y]});
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){
        int cs=strcmp(a.s,b.s); if(cs)return cs<0;
        int cc=strcmp(a.c,b.c); if(cc)return cc<0;
        return a.y<b.y;
    });
    for(auto&r:rows) fprintf(f,"%s,%s,%d,%.2f\n",r.s,r.c,r.y,r.r);
    fclose(f);
}

static void finalize_q8(Ctx& C) {
    if(!(C.active & Q8B)) return;
    MQO_TIME_PHASE("Q8_finalize");
    FILE* f=fopen((C.od+"/Q8.csv").c_str(),"w");
    fprintf(f,"o_year,mkt_share\n");
    for(int y=0;y<2;++y){
        double ms = (C.q8t[y]!=0.0) ? C.q8b[y]/C.q8t[y] : 0.0;
        fprintf(f,"%d,%.2f\n",1995+y,ms);
    }
    fclose(f);
}

static void finalize_q9(Ctx& C) {
    if(!(C.active & Q9B)) return;
    MQO_TIME_PHASE("Q9_finalize");
    struct R{std::string nn;int yr;double p;};
    std::vector<R> rows;
    for(int n=0;n<25;++n) for(int y=0;y<15;++y)
        if(C.q9p[n][y]!=0.0) rows.push_back({C.nation_names[n],1990+y,C.q9p[n][y]});
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){
        return a.nn!=b.nn ? a.nn<b.nn : a.yr>b.yr;
    });
    FILE* f=fopen((C.od+"/Q9.csv").c_str(),"w");
    fprintf(f,"nation,o_year,sum_profit\n");
    for(auto&r:rows) fprintf(f,"%s,%d,%.2f\n",r.nn.c_str(),r.yr,r.p);
    fclose(f);
}

static void finalize_q10(Ctx& C) {
    if(!(C.active & Q10B)) return;
    MQO_TIME_PHASE("Q10_finalize");
    struct R{int32_t ck;double rev;};
    std::vector<R> rows;
    for(auto&[ck,r]:C.q10m) rows.push_back({ck,r});
    std::partial_sort(rows.begin(), rows.begin()+std::min((size_t)20,rows.size()),
        rows.end(), [](auto&a,auto&b){return a.rev>b.rev;});
    FILE* f=fopen((C.od+"/Q10.csv").c_str(),"w");
    fprintf(f,"c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n");
    int lim=std::min((int)rows.size(),20);
    for(int i=0;i<lim;++i){
        auto&r=rows[i];
        int32_t ck=r.ck;
        fprintf(f,"%d,",ck);
        csvs(f,C.c_nm.get(ck-1)); fputc(',',f);
        fprintf(f,"%.2f,%.2f,",r.rev,C.c_ab[ck-1]);
        csvs(f,C.nation_names[C.c_nk[ck-1]]); fputc(',',f);
        csvs(f,C.c_ad.get(ck-1)); fputc(',',f);
        csvs(f,C.c_ph.get(ck-1)); fputc(',',f);
        csvs(f,C.c_cmt.get(ck-1)); fputc('\n',f);
    }
    fclose(f);
}

static void finalize_q11(Ctx& C) {
    if(!(C.active & Q11B)) return;
    MQO_TIME_PHASE("Q11_finalize");
    double sf = (double)C.NO / 1500000.0;
    if(sf < 1.0) sf = 1.0;
    double threshold = C.q11tot * 0.0001 / sf;
    struct R{int32_t pk;double val;};
    std::vector<R> rows;
    for(auto&[pk,v]:C.q11v)
        if(v > threshold) rows.push_back({pk,v});
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){return a.val>b.val;});
    FILE* f=fopen((C.od+"/Q11.csv").c_str(),"w");
    fprintf(f,"ps_partkey,value\n");
    for(auto&r:rows) fprintf(f,"%d,%.2f\n",r.pk,r.val);
    fclose(f);
}

static void finalize_q12(Ctx& C) {
    if(!(C.active & Q12B)) return;
    MQO_TIME_PHASE("Q12_finalize");
    struct R{std::string sm;int64_t h,l;};
    std::vector<R> rows;
    for(size_t i=0;i<C.dl_sm.v.size();++i)
        if(C.q12h[i]||C.q12l[i]) rows.push_back({C.dl_sm.v[i],C.q12h[i],C.q12l[i]});
    // Only MAIL and SHIP should have values, but sort anyway
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){return a.sm<b.sm;});
    FILE* f=fopen((C.od+"/Q12.csv").c_str(),"w");
    fprintf(f,"l_shipmode,high_line_count,low_line_count\n");
    for(auto&r:rows) fprintf(f,"%s,%ld,%ld\n",r.sm.c_str(),r.h,r.l);
    fclose(f);
}

static void finalize_q13(Ctx& C) {
    if(!(C.active & Q13B)) return;
    MQO_TIME_PHASE("Q13_finalize");
    // Build histogram: c_count -> custdist
    std::unordered_map<int32_t,int64_t> hist;
    for(size_t ck=1; ck<=C.NC; ++ck){
        int32_t cc = (int32_t)C.q13cc[ck];
        hist[cc]++;
    }
    struct R{int32_t cc;int64_t cd;};
    std::vector<R> rows;
    for(auto&[cc,cd]:hist) rows.push_back({cc,cd});
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){
        return a.cd!=b.cd ? a.cd>b.cd : a.cc>b.cc;});
    FILE* f=fopen((C.od+"/Q13.csv").c_str(),"w");
    fprintf(f,"c_count,custdist\n");
    for(auto&r:rows) fprintf(f,"%d,%ld\n",r.cc,r.cd);
    fclose(f);
}

static void finalize_q14(Ctx& C) {
    if(!(C.active & Q14B)) return;
    MQO_TIME_PHASE("Q14_finalize");
    double promo = (C.q14t!=0.0) ? 100.0*C.q14p/C.q14t : 0.0;
    FILE* f=fopen((C.od+"/Q14.csv").c_str(),"w");
    fprintf(f,"promo_revenue\n%.2f\n",promo);
    fclose(f);
}

static void finalize_q15(Ctx& C) {
    if(!(C.active & Q15B)) return;
    MQO_TIME_PHASE("Q15_finalize");
    double mx = 0;
    for(size_t i=1;i<=C.NS;++i) if(C.q15r[i]>mx) mx=C.q15r[i];
    struct R{int32_t sk;std::string sn,sa,sp;double tr;};
    std::vector<R> rows;
    for(size_t i=1;i<=C.NS;++i){
        if(std::abs(C.q15r[i]-mx)<0.005 && mx>0){
            R r;
            r.sk=(int32_t)i;
            r.sn=std::string(C.s_nm.get(i-1));
            r.sa=std::string(C.s_ad.get(i-1));
            r.sp=std::string(C.s_ph.get(i-1));
            r.tr=C.q15r[i];
            rows.push_back(std::move(r));
        }
    }
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){return a.sk<b.sk;});
    FILE* f=fopen((C.od+"/Q15.csv").c_str(),"w");
    fprintf(f,"s_suppkey,s_name,s_address,s_phone,total_revenue\n");
    for(auto&r:rows){
        fprintf(f,"%d,",r.sk);
        csvs(f,r.sn); fputc(',',f);
        csvs(f,r.sa); fputc(',',f);
        csvs(f,r.sp); fputc(',',f);
        fprintf(f,"%.2f\n",r.tr);
    }
    fclose(f);
}

static void finalize_q16(Ctx& C) {
    if(!(C.active & Q16B)) return;
    MQO_TIME_PHASE("Q16_finalize");
    struct R{std::string br,ty;int32_t sz;int64_t cnt;};
    std::vector<R> rows;
    for(auto&[key,sset]:C.q16g){
        uint8_t br=(uint8_t)(key>>40);
        uint8_t ty=(uint8_t)((key>>32)&0xFF);
        int32_t sz=(int32_t)(key&0xFFFFFFFF);
        rows.push_back({C.dp_br.v[br], C.dp_ty.v[ty], sz, (int64_t)sset.size()});
    }
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){
        if(a.cnt!=b.cnt) return a.cnt>b.cnt;
        if(a.br!=b.br) return a.br<b.br;
        if(a.ty!=b.ty) return a.ty<b.ty;
        return a.sz<b.sz;
    });
    FILE* f=fopen((C.od+"/Q16.csv").c_str(),"w");
    fprintf(f,"p_brand,p_type,p_size,supplier_cnt\n");
    for(auto&r:rows) fprintf(f,"%s,%s,%d,%ld\n",r.br.c_str(),r.ty.c_str(),r.sz,r.cnt);
    fclose(f);
}

static void finalize_q17(Ctx& C) {
    if(!(C.active & Q17B)) return;
    MQO_TIME_PHASE("Q17_finalize");
    // Compute threshold per partkey
    std::unordered_map<int32_t,double> thresh;
    for(auto&[pk,ac]:C.q17a){
        if(ac.cnt>0) thresh[pk] = 0.2 * ac.sq / ac.cnt;
    }
    double total = 0;
    for(auto& r:C.q17rows){
        auto it = thresh.find(r.pk);
        if(it!=thresh.end() && r.qty < it->second)
            total += r.ep;
    }
    FILE* f=fopen((C.od+"/Q17.csv").c_str(),"w");
    fprintf(f,"avg_yearly\n%.2f\n",total/7.0);
    fclose(f);
}

static void finalize_q18(Ctx& C) {
    if(!(C.active & Q18B)) return;
    MQO_TIME_PHASE("Q18_finalize");
    // Find orderkeys where sum > 300
    struct R{std::string cn;int32_t ck,ok;int32_t od;double tp,sq;};
    std::vector<R> rows;
    for(size_t ok=1; ok<C.q18mx; ++ok){
        if(C.q18qs[ok] > 300.0){
            int32_t orow = ((int32_t)ok < (int32_t)C.orders_pk_n) ? C.orders_pk[ok] : -1;
            if(orow<0) continue;
            int32_t ck = C.o_ck[orow];
            R r;
            r.cn = std::string(C.c_nm.get(ck-1));
            r.ck = ck;
            r.ok = (int32_t)ok;
            r.od = C.o_od[orow];
            r.tp = C.o_tp[orow];
            r.sq = C.q18qs[ok];
            rows.push_back(std::move(r));
        }
    }
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){
        return a.tp!=b.tp ? a.tp>b.tp : a.od<b.od;});
    FILE* f=fopen((C.od+"/Q18.csv").c_str(),"w");
    fprintf(f,"c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
    int lim=std::min((int)rows.size(),100);
    for(int i=0;i<lim;++i){
        auto&r=rows[i];
        csvs(f,r.cn); fputc(',',f);
        fprintf(f,"%d,%d,%s,%.2f,%.2f\n",r.ck,r.ok,dstr(r.od).c_str(),r.tp,r.sq);
    }
    fclose(f);
}

static void finalize_q19(Ctx& C) {
    if(!(C.active & Q19B)) return;
    MQO_TIME_PHASE("Q19_finalize");
    FILE* f=fopen((C.od+"/Q19.csv").c_str(),"w");
    fprintf(f,"revenue\n%.2f\n",C.q19r);
    fclose(f);
}

static void finalize_q20(Ctx& C) {
    if(!(C.active & Q20B)) return;
    MQO_TIME_PHASE("Q20_finalize");
    struct R{std::string sn,sa;};
    std::vector<R> rows;
    for(int32_t sk : C.q20res){
        R r;
        r.sn = std::string(C.s_nm.get(sk-1));
        r.sa = std::string(C.s_ad.get(sk-1));
        rows.push_back(std::move(r));
    }
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){return a.sn<b.sn;});
    FILE* f=fopen((C.od+"/Q20.csv").c_str(),"w");
    fprintf(f,"s_name,s_address\n");
    for(auto&r:rows){
        csvs(f,r.sn); fputc(',',f);
        csvs(f,r.sa); fputc('\n',f);
    }
    fclose(f);
}

static void finalize_q21(Ctx& C) {
    if(!(C.active & Q21B)) return;
    MQO_TIME_PHASE("Q21_finalize");
    // Group candidates by orderkey
    std::unordered_map<int32_t, std::vector<std::pair<int32_t,int32_t>>> by_ok;
    for(auto&[key,cnt]:C.q21c){
        int32_t ok = (int32_t)(key>>32);
        int32_t sk = (int32_t)(key&0xFFFFFFFF);
        by_ok[ok].push_back({sk,cnt});
    }
    // For each order, check EXISTS and NOT EXISTS
    std::unordered_map<int32_t,int64_t> numwait; // suppkey -> count
    for(auto&[ok,supps]:by_ok){
        if(ok<0 || ok>=(int32_t)C.li_ok_idx_n) continue;
        auto& idx = C.li_ok_idx[ok];
        if(idx.count==0) continue;
        for(auto&[saudi_sk,cnt]:supps){
            bool exists_other = false;
            bool other_late = false;
            for(uint32_t j=idx.start; j<idx.start+idx.count; ++j){
                int32_t sk = C.l_sk[j];
                if(sk != saudi_sk){
                    exists_other = true;
                    if(C.l_rd[j] > C.l_cd[j]){
                        other_late = true;
                        break;
                    }
                }
            }
            if(exists_other && !other_late)
                numwait[saudi_sk] += cnt;
        }
    }
    struct R{std::string sn;int64_t nw;};
    std::vector<R> rows;
    for(auto&[sk,nw]:numwait){
        R r; r.sn=std::string(C.s_nm.get(sk-1)); r.nw=nw;
        rows.push_back(std::move(r));
    }
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){
        return a.nw!=b.nw ? a.nw>b.nw : a.sn<b.sn;});
    FILE* f=fopen((C.od+"/Q21.csv").c_str(),"w");
    fprintf(f,"s_name,numwait\n");
    int lim=std::min((int)rows.size(),100);
    for(int i=0;i<lim;++i)
        fprintf(f,"%s,%ld\n",rows[i].sn.c_str(),rows[i].nw);
    fclose(f);
}

static void finalize_q22(Ctx& C) {
    if(!(C.active & Q22B)) return;
    MQO_TIME_PHASE("Q22_finalize");
    struct R{int cc;int64_t cnt;double sum;};
    std::vector<R> rows;
    for(int cc=0;cc<100;++cc)
        if(C.q22g[cc].cnt>0) rows.push_back({cc,C.q22g[cc].cnt,C.q22g[cc].sum});
    std::sort(rows.begin(),rows.end(),[](auto&a,auto&b){return a.cc<b.cc;});
    FILE* f=fopen((C.od+"/Q22.csv").c_str(),"w");
    fprintf(f,"cntrycode,numcust,totacctbal\n");
    for(auto&r:rows) fprintf(f,"%d,%ld,%.2f\n",r.cc,r.cnt,r.sum);
    fclose(f);
}

// ============ MAIN ============
int main(int argc, char* argv[]) {
    Ctx C;
    bool list_mode = false;
    std::vector<std::string> sel_queries;

    for(int i=1; i<argc; ++i){
        std::string arg = argv[i];
        if(arg=="--gendb-dir" && i+1<argc) C.gd = argv[++i];
        else if(arg=="--output-dir" && i+1<argc) C.od = argv[++i];
        else if(arg=="--all") C.active = ALL_Q;
        else if(arg=="--query" && i+1<argc) sel_queries.push_back(argv[++i]);
        else if(arg=="--list") list_mode = true;
    }
    if(list_mode){
        printf("Supported queries:");
        for(int i=0;i<22;++i) printf(" %s",QN[i]);
        printf("\n");
        return 0;
    }
    // Set active bits from --query args
    if(!sel_queries.empty()){
        C.active = 0;
        for(auto& q:sel_queries){
            for(int i=0;i<22;++i)
                if(q==QN[i]){C.active|=(1u<<i);break;}
        }
    }
    if(C.active==0){
        fprintf(stderr,"No queries selected. Use --all or --query Q<N>\n");
        return 1;
    }
    // Create output dir
    {
        std::string cmd = "mkdir -p " + C.od;
        system(cmd.c_str());
    }
    printf("[MQO] gendb_dir=%s output_dir=%s active=0x%08x\n",C.gd.c_str(),C.od.c_str(),C.active);

    // Execute stages
    load_dims(C);
    fused_scan_part(C);
    fused_scan_lineitem(C);
    fused_scan_orders(C);
    fused_scan_partsupp(C);
    run_q22(C);

    // Finalize all queries
    {MQO_TIME_PHASE("finalize_all");
        finalize_q1(C);
        finalize_q2(C);
        finalize_q3(C);
        // Q4 already finalized in fused_scan_orders
        finalize_q5(C);
        finalize_q6(C);
        finalize_q7(C);
        finalize_q8(C);
        finalize_q9(C);
        finalize_q10(C);
        finalize_q11(C);
        finalize_q12(C);
        finalize_q13(C);
        finalize_q14(C);
        finalize_q15(C);
        finalize_q16(C);
        finalize_q17(C);
        finalize_q18(C);
        finalize_q19(C);
        finalize_q20(C);
        finalize_q21(C);
        finalize_q22(C);
    }

    // Cleanup
    free(C.q13cc);
    free(C.q15r);
    free(C.q18qs);

    MQO_PROFILE_FLUSH("profile.json");
    printf("[MQO] Done.\n");
    return 0;
}
