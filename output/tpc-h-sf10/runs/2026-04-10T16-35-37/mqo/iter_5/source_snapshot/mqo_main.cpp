#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
// ===========================================================================
// MQO Fused Batch Engine — All 22 TPC-H queries in a single fused executable
// ===========================================================================

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <cassert>
#include <string>
#include <vector>
#include <array>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <algorithm>
#include <numeric>
#include <functional>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "mqo_profile.hpp"

// ===========================================================================
// Utility: date conversion
// ===========================================================================
constexpr int32_t days_from_civil(int y, int m, int d) noexcept {
    y -= m <= 2;
    const int era = (y >= 0 ? y : y - 399) / 400;
    const unsigned yoe = (unsigned)(y - era * 400);
    const unsigned doy = (153 * (m > 2 ? m - 3 : m + 9) + 2) / 5 + d - 1;
    const unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    return era * 146097 + (int)doe - 719468;
}

inline void civil_from_days(int32_t z, int &y, unsigned &m, unsigned &d) noexcept {
    z += 719468;
    const int era = (z >= 0 ? z : z - 146096) / 146097;
    const unsigned doe = (unsigned)(z - era * 146097);
    const unsigned yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    y = (int)yoe + era * 400;
    const unsigned doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    const unsigned mp = (5 * doy + 2) / 153;
    d = doy - (153 * mp + 2) / 5 + 1;
    m = mp + (mp < 10 ? 3 : -9);
    y += (m <= 2);
}

inline int extract_year(int32_t epoch_days) {
    int y; unsigned m, d;
    civil_from_days(epoch_days, y, m, d);
    return y;
}

inline void format_date(int32_t epoch_days, char* buf) {
    int y; unsigned m, d;
    civil_from_days(epoch_days, y, m, d);
    sprintf(buf, "%04d-%02u-%02u", y, m, d);
}

// ===========================================================================
// Utility: mmap-based file loading
// ===========================================================================
struct MmapFile {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    bool open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path.c_str()); return false; }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        if (size == 0) { ::close(fd); fd = -1; return true; }
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
        if (data == MAP_FAILED) { data = nullptr; ::close(fd); fd = -1; return false; }
        madvise(data, size, MADV_SEQUENTIAL);
        return true;
    }
    void close() {
        if (data && size) munmap(data, size);
        if (fd >= 0) ::close(fd);
        data = nullptr; size = 0; fd = -1;
    }
    ~MmapFile() { close(); }
    MmapFile() = default;
    MmapFile(MmapFile&& o) noexcept : data(o.data), size(o.size), fd(o.fd) { o.data=nullptr; o.size=0; o.fd=-1; }
    MmapFile& operator=(MmapFile&& o) noexcept { close(); data=o.data; size=o.size; fd=o.fd; o.data=nullptr; o.size=0; o.fd=-1; return *this; }
    MmapFile(const MmapFile&) = delete;
    MmapFile& operator=(const MmapFile&) = delete;

    template<typename T> const T* as() const { return reinterpret_cast<const T*>(data); }
    template<typename T> size_t count() const { return size / sizeof(T); }
};

static MmapFile mmap_open(const std::string& path) {
    MmapFile f;
    if (!f.open(path)) {
        fprintf(stderr, "FATAL: cannot mmap %s\n", path.c_str());
        exit(1);
    }
    return f;
}

// ===========================================================================
// Utility: dictionary loader
// ===========================================================================
struct Dictionary {
    std::vector<std::string> entries;
    void load(const std::string& path) {
        MmapFile f = mmap_open(path);
        if (f.size == 0) return;
        const uint8_t* p = (const uint8_t*)f.data;
        const uint8_t* end = p + f.size;
        uint32_t count;
        memcpy(&count, p, 4); p += 4;
        entries.resize(count);
        for (uint32_t i = 0; i < count && p + 2 <= end; i++) {
            uint16_t len;
            memcpy(&len, p, 2); p += 2;
            if (p + len > end) break;
            entries[i].assign((const char*)p, len);
            p += len;
        }
    }
    int find(const std::string& s) const {
        for (size_t i = 0; i < entries.size(); i++)
            if (entries[i] == s) return (int)i;
        return -1;
    }
    bool ends_with(int code, const std::string& suffix) const {
        if (code < 0 || code >= (int)entries.size()) return false;
        auto& e = entries[code];
        if (e.size() < suffix.size()) return false;
        return e.compare(e.size() - suffix.size(), suffix.size(), suffix) == 0;
    }
    bool starts_with(int code, const std::string& prefix) const {
        if (code < 0 || code >= (int)entries.size()) return false;
        return entries[code].compare(0, prefix.size(), prefix) == 0;
    }
    bool contains(int code, const std::string& sub) const {
        if (code < 0 || code >= (int)entries.size()) return false;
        return entries[code].find(sub) != std::string::npos;
    }
};

// ===========================================================================
// Utility: varlen string accessor
// ===========================================================================
struct VarlenCol {
    const uint32_t* offsets = nullptr;  // nrows+1 offsets
    const char* data = nullptr;
    size_t data_size = 0;
    MmapFile foff, fdata;

    void load(const std::string& base_path) {
        foff = mmap_open(base_path + ".bin");
        fdata = mmap_open(base_path + "_data.bin");
        offsets = foff.as<uint32_t>();
        data = fdata.as<char>();
        data_size = fdata.size;
    }

    std::string get(size_t row) const {
        uint32_t start = offsets[row];
        uint32_t end = offsets[row + 1];
        if (end <= start) return "";
        return std::string(data + start, end - start);
    }

    const char* ptr(size_t row) const { return data + offsets[row]; }
    size_t len(size_t row) const { return offsets[row + 1] - offsets[row]; }
};

// ===========================================================================
// CSV quoting helper
// ===========================================================================
[[maybe_unused]] static std::string csv_quote(const std::string& s) {
    bool needs_quote = false;
    for (char c : s) {
        if (c == ',' || c == '"' || c == '\n') { needs_quote = true; break; }
    }
    if (!needs_quote) return s;
    std::string out = "\"";
    for (char c : s) {
        if (c == '"') out += "\"\"";
        else out += c;
    }
    out += '"';
    return out;
}

// ===========================================================================
// Partsupp composite hash
// ===========================================================================
struct HashEntry {
    int32_t partkey;
    int32_t suppkey;
    int32_t row_id;
    int32_t pad;
};

inline int32_t ps_hash_lookup(const HashEntry* tbl, uint64_t mask, int32_t pk, int32_t sk) {
    uint64_t h = (uint64_t)(uint32_t)pk * 2654435761ULL ^ (uint64_t)(uint32_t)sk * 40503ULL;
    uint64_t slot = h & mask;
    while (true) {
        if (tbl[slot].row_id == -1) return -1;
        if (tbl[slot].partkey == pk && tbl[slot].suppkey == sk) return tbl[slot].row_id;
        slot = (slot + 1) & mask;
    }
}

// ===========================================================================
// Grouped index
// ===========================================================================
struct GroupedIdx {
    const uint32_t* offsets = nullptr;  // max_key+2 entries
    const uint32_t* rows = nullptr;
    MmapFile foff, frows;
    uint32_t max_key = 0;

    void load(const std::string& offsets_path, const std::string& rows_path, uint32_t mk) {
        foff = mmap_open(offsets_path);
        frows = mmap_open(rows_path);
        offsets = foff.as<uint32_t>();
        rows = frows.as<uint32_t>();
        max_key = mk;
    }
    uint32_t start(uint32_t key) const { return offsets[key]; }
    uint32_t end(uint32_t key) const { return offsets[key + 1]; }
    uint32_t count(uint32_t key) const { return offsets[key + 1] - offsets[key]; }
};

// ===========================================================================
// Orderkey index entry
// ===========================================================================
struct OrdIdxEntry {
    uint32_t start;
    uint32_t count;
};

// ===========================================================================
// Query bitmask
// ===========================================================================
enum : uint32_t {
    Q1B  = 1u << 0,   Q2B  = 1u << 1,   Q3B  = 1u << 2,   Q4B  = 1u << 3,
    Q5B  = 1u << 4,   Q6B  = 1u << 5,   Q7B  = 1u << 6,   Q8B  = 1u << 7,
    Q9B  = 1u << 8,   Q10B = 1u << 9,   Q11B = 1u << 10,  Q12B = 1u << 11,
    Q13B = 1u << 12,  Q14B = 1u << 13,  Q15B = 1u << 14,  Q16B = 1u << 15,
    Q17B = 1u << 16,  Q18B = 1u << 17,  Q19B = 1u << 18,  Q20B = 1u << 19,
    Q21B = 1u << 20,  Q22B = 1u << 21,  ALL_Q = (1u << 22) - 1
};

static const char* QUERY_NAMES[] = {
    "Q1","Q2","Q3","Q4","Q5","Q6","Q7","Q8","Q9","Q10",
    "Q11","Q12","Q13","Q14","Q15","Q16","Q17","Q18","Q19","Q20","Q21","Q22"
};

// ===========================================================================
// Global data pointers
// ===========================================================================
static std::string g_gendb_dir;
static std::string g_storage_dir;
static std::string g_index_dir;
static std::string g_output_dir;
static uint32_t g_active_queries = 0;

// Table sizes
static const int64_t LINEITEM_ROWS  = 59986052;
static const int64_t ORDERS_ROWS    = 15000000;
static const int64_t CUSTOMER_ROWS  = 1500000;
static const int64_t PART_ROWS      = 2000000;
static const int64_t PARTSUPP_ROWS  = 8000000;
static const int64_t SUPPLIER_ROWS  = 100000;
static const int64_t NATION_ROWS    = 25;
static const int64_t REGION_ROWS    = 5;

// ---- Lineitem columns (mmap'd) ----
static MmapFile f_l_orderkey, f_l_partkey, f_l_suppkey;
static MmapFile f_l_quantity, f_l_extendedprice, f_l_discount, f_l_tax;
static MmapFile f_l_returnflag, f_l_linestatus;
static MmapFile f_l_shipdate, f_l_commitdate, f_l_receiptdate;
static MmapFile f_l_shipinstruct, f_l_shipmode;
static Dictionary dict_l_shipinstruct, dict_l_shipmode;

static const int32_t* l_orderkey;
static const int32_t* l_partkey;
static const int32_t* l_suppkey;
static const double* l_quantity;
static const double* l_extendedprice;
static const double* l_discount;
static const double* l_tax;
static const int8_t* l_returnflag;
static const int8_t* l_linestatus;
static const int32_t* l_shipdate;
static const int32_t* l_commitdate;
static const int32_t* l_receiptdate;
static const uint8_t* l_shipinstruct;
static const uint8_t* l_shipmode;

// ---- Orders columns ----
static MmapFile f_o_orderkey, f_o_custkey, f_o_orderstatus, f_o_totalprice;
static MmapFile f_o_orderdate, f_o_orderpriority, f_o_shippriority;
static Dictionary dict_o_orderpriority;
static VarlenCol v_o_comment;

static const int32_t* o_orderkey;
static const int32_t* o_custkey;
static const int8_t* o_orderstatus;
static const double* o_totalprice;
static const int32_t* o_orderdate;
static const uint8_t* o_orderpriority;
static const int32_t* o_shippriority;

// ---- Customer columns ----
static MmapFile f_c_custkey, f_c_nationkey, f_c_acctbal, f_c_mktsegment;
static Dictionary dict_c_mktsegment;
static VarlenCol v_c_name, v_c_address, v_c_phone, v_c_comment;

static const int32_t* c_custkey;
static const int32_t* c_nationkey;
static const double* c_acctbal;
static const uint8_t* c_mktsegment;

// ---- Part columns ----
static MmapFile f_p_partkey, f_p_size, f_p_mfgr, f_p_brand, f_p_type, f_p_container;
static Dictionary dict_p_mfgr, dict_p_brand, dict_p_type, dict_p_container;
static VarlenCol v_p_name;

static const int32_t* p_partkey;
static const int32_t* p_size;
static const uint8_t* p_mfgr;
static const uint8_t* p_brand;
static const uint8_t* p_type;
static const uint8_t* p_container;

// ---- Partsupp columns ----
static MmapFile f_ps_partkey, f_ps_suppkey, f_ps_availqty, f_ps_supplycost;

static const int32_t* ps_partkey;
static const int32_t* ps_suppkey;
static const int32_t* ps_availqty;
static const double* ps_supplycost;

// ---- Supplier columns ----
static MmapFile f_s_suppkey, f_s_nationkey, f_s_acctbal;
static VarlenCol v_s_name, v_s_address, v_s_phone, v_s_comment;

static const int32_t* s_suppkey;
static const int32_t* s_nationkey;
static const double* s_acctbal;

// ---- Nation columns ----
static MmapFile f_n_nationkey, f_n_name, f_n_regionkey;
static Dictionary dict_n_name;

static const int32_t* n_nationkey;
static const uint8_t* n_name;
static const int32_t* n_regionkey;

// ---- Region columns ----
static MmapFile f_r_regionkey, f_r_name;
static Dictionary dict_r_name;

static const int32_t* r_regionkey;
static const uint8_t* r_name;

// ---- Indexes ----
static MmapFile f_orders_pk_index;
static const int32_t* orders_pk_index;  // orderkey -> row_id, max 60000001

static MmapFile f_lineitem_orderkey_idx;
static const OrdIdxEntry* lineitem_orderkey_idx; // max 60000001

static MmapFile f_partsupp_composite_hash;
static const HashEntry* partsupp_hash_table;
static const uint64_t PS_HASH_MASK = 16777215ULL;

static GroupedIdx idx_lineitem_partkey_grouped;
// partsupp_partkey_idx is OrdIdxEntry format, loaded separately
static GroupedIdx idx_custkey_to_orders;

// ===========================================================================
// Precomputed lookups
// ===========================================================================

// Nation/region lookups
static std::string nation_names[25];
static int32_t nation_regionkey[25];

// Q5: Asian nations (ASIA region)
static int32_t asia_regionkey = -1;
static bool is_asian_nation[25] = {};

// Q7: FRANCE/GERMANY nation keys
static int32_t france_nk = -1, germany_nk = -1;

// Q8: AMERICA region -> nation set, Brazil nationkey
static int32_t america_regionkey = -1;
static bool is_america_nation[25] = {};
static int32_t brazil_nk = -1;

// Q8: ECONOMY ANODIZED STEEL type code
static int q8_type_code = -1;

// Q9: green parts bitset
static std::vector<uint8_t> green_parts_bitset;  // indexed by partkey, 1 if name contains "green"

// Q19: part classification for Q19 — 0=no match, 1=Brand#12 SM, 2=Brand#23 MED, 3=Brand#34 LG
static std::vector<uint8_t> q19_classify;

// Q21: SAUDI ARABIA nation key
static int32_t saudi_nk = -1;

// Q3: BUILDING segment code
static int q3_building_code = -1;

// Q12: MAIL and SHIP mode codes
static int q12_mail_code = -1, q12_ship_code = -1;

// Q14: promo type prefix
// Types whose name starts with "PROMO"
static std::vector<bool> is_promo_type;

// Q15: supplier revenue array (thread-local, merged after scan)
static std::vector<double> q15_revenue;  // indexed by suppkey (1-based)

// Q19: AIR and AIR REG shipmode codes; DELIVER IN PERSON shipinstruct code
static int q19_air_code = -1, q19_airreg_code = -1;
static int q19_deliver_code = -1;

// European supplier set (for Q2, Q11)
static int32_t europe_regionkey = -1;
static bool is_european_nation[25] = {};

// Q11: GERMANY nationkey
// reuse germany_nk

// Q16: complaint supplier set
static std::vector<bool> q16_complaint_suppliers;

// Q20: forest parts
static std::vector<int32_t> q20_forest_partkeys;
// Q20: CANADA nationkey
static int32_t canada_nk = -1;

// Q22: target country codes (phone prefix)
static const char* q22_codes[] = {"13","31","23","29","30","18","17"};
static bool q22_target_prefix[100] = {};  // phone_prefix -> true
static int q22_prefix_to_idx(const char* phone) {
    return (phone[0]-'0')*10 + (phone[1]-'0');
}

// ===========================================================================
// Q1 accumulator
// ===========================================================================
struct Q1Group {
    double sum_qty = 0, sum_base_price = 0, sum_disc_price = 0, sum_charge = 0;
    double sum_disc = 0;
    int64_t count = 0;
};

// Q1: key = returnflag * 4 + linestatus mapping
// returnflag: A=0, N=1, R=2; linestatus: F=0, O=1
static inline int q1_key(int8_t rf, int8_t ls) {
    int r = (rf == 'A') ? 0 : (rf == 'N' ? 1 : 2);
    int s = (ls == 'F') ? 0 : 1;
    return r * 2 + s;
}
static const int Q1_GROUPS = 6;

// ===========================================================================
// European partsupp join result (for Q2, Q11)
// ===========================================================================
struct EuroPS {
    int32_t partkey;
    int32_t suppkey;
    int32_t availqty;
    double supplycost;
    int32_t supp_nationkey;
};
static std::vector<EuroPS> euro_partsupp;

// ===========================================================================
// Q21 candidate
// ===========================================================================
struct Q21Cand {
    int32_t orderkey;
    int32_t suppkey;
};

// ===========================================================================
// STAGE: Load all data
// ===========================================================================
static void load_all_data() {
    MQO_TIME_SHARED("load_data");

    auto S = [](const std::string& tbl, const std::string& col) {
        return g_storage_dir + "/" + tbl + "/" + col + ".bin";
    };
    auto I = [](const std::string& name) {
        return g_index_dir + "/" + name;
    };

    // Lineitem
    f_l_orderkey = mmap_open(S("lineitem","l_orderkey"));       l_orderkey = f_l_orderkey.as<int32_t>();
    f_l_partkey = mmap_open(S("lineitem","l_partkey"));         l_partkey = f_l_partkey.as<int32_t>();
    f_l_suppkey = mmap_open(S("lineitem","l_suppkey"));         l_suppkey = f_l_suppkey.as<int32_t>();
    f_l_quantity = mmap_open(S("lineitem","l_quantity"));       l_quantity = f_l_quantity.as<double>();
    f_l_extendedprice = mmap_open(S("lineitem","l_extendedprice")); l_extendedprice = f_l_extendedprice.as<double>();
    f_l_discount = mmap_open(S("lineitem","l_discount"));       l_discount = f_l_discount.as<double>();
    f_l_tax = mmap_open(S("lineitem","l_tax"));                 l_tax = f_l_tax.as<double>();
    f_l_returnflag = mmap_open(S("lineitem","l_returnflag"));   l_returnflag = f_l_returnflag.as<int8_t>();
    f_l_linestatus = mmap_open(S("lineitem","l_linestatus"));   l_linestatus = f_l_linestatus.as<int8_t>();
    f_l_shipdate = mmap_open(S("lineitem","l_shipdate"));       l_shipdate = f_l_shipdate.as<int32_t>();
    f_l_commitdate = mmap_open(S("lineitem","l_commitdate"));   l_commitdate = f_l_commitdate.as<int32_t>();
    f_l_receiptdate = mmap_open(S("lineitem","l_receiptdate")); l_receiptdate = f_l_receiptdate.as<int32_t>();
    f_l_shipinstruct = mmap_open(S("lineitem","l_shipinstruct")); l_shipinstruct = f_l_shipinstruct.as<uint8_t>();
    f_l_shipmode = mmap_open(S("lineitem","l_shipmode"));       l_shipmode = f_l_shipmode.as<uint8_t>();
    dict_l_shipinstruct.load(g_storage_dir + "/lineitem/l_shipinstruct_dict.bin");
    dict_l_shipmode.load(g_storage_dir + "/lineitem/l_shipmode_dict.bin");

    // Orders
    f_o_orderkey = mmap_open(S("orders","o_orderkey"));         o_orderkey = f_o_orderkey.as<int32_t>();
    f_o_custkey = mmap_open(S("orders","o_custkey"));           o_custkey = f_o_custkey.as<int32_t>();
    f_o_orderstatus = mmap_open(S("orders","o_orderstatus"));   o_orderstatus = f_o_orderstatus.as<int8_t>();
    f_o_totalprice = mmap_open(S("orders","o_totalprice"));     o_totalprice = f_o_totalprice.as<double>();
    f_o_orderdate = mmap_open(S("orders","o_orderdate"));       o_orderdate = f_o_orderdate.as<int32_t>();
    f_o_orderpriority = mmap_open(S("orders","o_orderpriority")); o_orderpriority = f_o_orderpriority.as<uint8_t>();
    f_o_shippriority = mmap_open(S("orders","o_shippriority")); o_shippriority = f_o_shippriority.as<int32_t>();
    dict_o_orderpriority.load(g_storage_dir + "/orders/o_orderpriority_dict.bin");
    v_o_comment.load(g_storage_dir + "/orders/o_comment");

    // Customer
    f_c_custkey = mmap_open(S("customer","c_custkey"));         c_custkey = f_c_custkey.as<int32_t>();
    f_c_nationkey = mmap_open(S("customer","c_nationkey"));     c_nationkey = f_c_nationkey.as<int32_t>();
    f_c_acctbal = mmap_open(S("customer","c_acctbal"));         c_acctbal = f_c_acctbal.as<double>();
    f_c_mktsegment = mmap_open(S("customer","c_mktsegment"));   c_mktsegment = f_c_mktsegment.as<uint8_t>();
    dict_c_mktsegment.load(g_storage_dir + "/customer/c_mktsegment_dict.bin");
    v_c_name.load(g_storage_dir + "/customer/c_name");
    v_c_address.load(g_storage_dir + "/customer/c_address");
    v_c_phone.load(g_storage_dir + "/customer/c_phone");
    v_c_comment.load(g_storage_dir + "/customer/c_comment");

    // Part
    f_p_partkey = mmap_open(S("part","p_partkey"));             p_partkey = f_p_partkey.as<int32_t>();
    f_p_size = mmap_open(S("part","p_size"));                   p_size = f_p_size.as<int32_t>();
    f_p_mfgr = mmap_open(S("part","p_mfgr"));                  p_mfgr = f_p_mfgr.as<uint8_t>();
    f_p_brand = mmap_open(S("part","p_brand"));                 p_brand = f_p_brand.as<uint8_t>();
    f_p_type = mmap_open(S("part","p_type"));                   p_type = f_p_type.as<uint8_t>();
    f_p_container = mmap_open(S("part","p_container"));         p_container = f_p_container.as<uint8_t>();
    dict_p_mfgr.load(g_storage_dir + "/part/p_mfgr_dict.bin");
    dict_p_brand.load(g_storage_dir + "/part/p_brand_dict.bin");
    dict_p_type.load(g_storage_dir + "/part/p_type_dict.bin");
    dict_p_container.load(g_storage_dir + "/part/p_container_dict.bin");
    v_p_name.load(g_storage_dir + "/part/p_name");

    // Partsupp
    f_ps_partkey = mmap_open(S("partsupp","ps_partkey"));       ps_partkey = f_ps_partkey.as<int32_t>();
    f_ps_suppkey = mmap_open(S("partsupp","ps_suppkey"));       ps_suppkey = f_ps_suppkey.as<int32_t>();
    f_ps_availqty = mmap_open(S("partsupp","ps_availqty"));     ps_availqty = f_ps_availqty.as<int32_t>();
    f_ps_supplycost = mmap_open(S("partsupp","ps_supplycost")); ps_supplycost = f_ps_supplycost.as<double>();

    // Supplier
    f_s_suppkey = mmap_open(S("supplier","s_suppkey"));         s_suppkey = f_s_suppkey.as<int32_t>();
    f_s_nationkey = mmap_open(S("supplier","s_nationkey"));     s_nationkey = f_s_nationkey.as<int32_t>();
    f_s_acctbal = mmap_open(S("supplier","s_acctbal"));         s_acctbal = f_s_acctbal.as<double>();
    v_s_name.load(g_storage_dir + "/supplier/s_name");
    v_s_address.load(g_storage_dir + "/supplier/s_address");
    v_s_phone.load(g_storage_dir + "/supplier/s_phone");
    v_s_comment.load(g_storage_dir + "/supplier/s_comment");

    // Nation
    f_n_nationkey = mmap_open(S("nation","n_nationkey"));       n_nationkey = f_n_nationkey.as<int32_t>();
    f_n_name = mmap_open(S("nation","n_name"));                 n_name = f_n_name.as<uint8_t>();
    f_n_regionkey = mmap_open(S("nation","n_regionkey"));       n_regionkey = f_n_regionkey.as<int32_t>();
    dict_n_name.load(g_storage_dir + "/nation/n_name_dict.bin");

    // Region
    f_r_regionkey = mmap_open(S("region","r_regionkey"));       r_regionkey = f_r_regionkey.as<int32_t>();
    f_r_name = mmap_open(S("region","r_name"));                 r_name = f_r_name.as<uint8_t>();
    dict_r_name.load(g_storage_dir + "/region/r_name_dict.bin");

    // Indexes
    f_orders_pk_index = mmap_open(I("orders_pk_index.bin"));
    orders_pk_index = f_orders_pk_index.as<int32_t>();

    f_lineitem_orderkey_idx = mmap_open(I("lineitem_orderkey_idx.bin"));
    lineitem_orderkey_idx = (const OrdIdxEntry*)f_lineitem_orderkey_idx.data;

    f_partsupp_composite_hash = mmap_open(I("partsupp_composite_hash.bin"));
    partsupp_hash_table = (const HashEntry*)f_partsupp_composite_hash.data;

    idx_lineitem_partkey_grouped.load(
        I("lineitem_partkey_grouped_offsets.bin"),
        I("lineitem_partkey_grouped_rows.bin"),
        2000000);

    // partsupp_partkey_idx is {start,count} format — loaded separately in load_extra_indexes()

    idx_custkey_to_orders.load(
        I("custkey_to_orders_offsets.bin"),
        I("custkey_to_orders_rows.bin"),
        1499999);
}

// Partsupp partkey index: {start, count}[2000001], entries 0..2000000
static MmapFile f_partsupp_partkey_idx;
static const OrdIdxEntry* partsupp_partkey_idx;

static void load_extra_indexes() {
    f_partsupp_partkey_idx = mmap_open(g_index_dir + "/partsupp_partkey_idx.bin");
    partsupp_partkey_idx = (const OrdIdxEntry*)f_partsupp_partkey_idx.data;
}

// ===========================================================================
// STAGE: Precompute
// ===========================================================================
static void precompute() {
    MQO_TIME_SHARED("precompute");

    // Load nation/region data
    for (int i = 0; i < 25; i++) {
        nation_names[i] = dict_n_name.entries[n_name[i]];
        nation_regionkey[i] = n_regionkey[i];
    }

    // Find region keys
    for (int i = 0; i < (int)dict_r_name.entries.size(); i++) {
        if (dict_r_name.entries[i] == "ASIA") asia_regionkey = i;
        if (dict_r_name.entries[i] == "EUROPE") europe_regionkey = i;
        if (dict_r_name.entries[i] == "AMERICA") america_regionkey = i;
    }
    // Map to actual regionkey (region: row = regionkey)
    int asia_rk = -1, europe_rk = -1, america_rk = -1;
    for (int i = 0; i < REGION_ROWS; i++) {
        if (dict_r_name.entries[r_name[i]] == "ASIA") asia_rk = r_regionkey[i];
        if (dict_r_name.entries[r_name[i]] == "EUROPE") europe_rk = r_regionkey[i];
        if (dict_r_name.entries[r_name[i]] == "AMERICA") america_rk = r_regionkey[i];
    }

    // Build nation lookups
    for (int i = 0; i < 25; i++) {
        if (nation_regionkey[i] == asia_rk) is_asian_nation[i] = true;
        if (nation_regionkey[i] == europe_rk) is_european_nation[i] = true;
        if (nation_regionkey[i] == america_rk) is_america_nation[i] = true;
        if (nation_names[i] == "FRANCE") france_nk = i;
        if (nation_names[i] == "GERMANY") germany_nk = i;
        if (nation_names[i] == "BRAZIL") brazil_nk = i;
        if (nation_names[i] == "SAUDI ARABIA") saudi_nk = i;
        if (nation_names[i] == "CANADA") canada_nk = i;
    }

    // Q3: BUILDING code
    q3_building_code = dict_c_mktsegment.find("BUILDING");

    // Q8: ECONOMY ANODIZED STEEL type code
    q8_type_code = dict_p_type.find("ECONOMY ANODIZED STEEL");

    // Q12: MAIL, SHIP codes
    q12_mail_code = dict_l_shipmode.find("MAIL");
    q12_ship_code = dict_l_shipmode.find("SHIP");

    // Q19: AIR, AIR REG, DELIVER IN PERSON
    q19_air_code = dict_l_shipmode.find("AIR");
    q19_airreg_code = dict_l_shipmode.find("AIR REG");
    q19_deliver_code = dict_l_shipinstruct.find("DELIVER IN PERSON");

    // Q14: promo types
    is_promo_type.resize(dict_p_type.entries.size(), false);
    for (size_t i = 0; i < dict_p_type.entries.size(); i++) {
        if (dict_p_type.entries[i].compare(0, 5, "PROMO") == 0)
            is_promo_type[i] = true;
    }

    // Q9: green parts bitset
    if (g_active_queries & Q9B) {
        green_parts_bitset.resize(PART_ROWS + 1, 0);
        #pragma omp parallel for schedule(static)
        for (int64_t i = 0; i < PART_ROWS; i++) {
            int32_t pk = i + 1;  // partkey = row + 1
            size_t nlen = v_p_name.len(i);
            const char* nm = v_p_name.ptr(i);
            if (memmem(nm, nlen, "green", 5) != nullptr) {
                green_parts_bitset[pk] = 1;
            }
        }
    }

    // Q19: classify parts
    if (g_active_queries & Q19B) {
        // Brand#12 + SM BOX/SM CASE/SM PACK/SM PKG -> class 1
        // Brand#23 + MED BAG/MED BOX/MED PKG/MED PACK -> class 2
        // Brand#34 + LG CASE/LG BOX/LG PACK/LG PKG -> class 3
        int brand12 = dict_p_brand.find("Brand#12");
        int brand23 = dict_p_brand.find("Brand#23");
        int brand34 = dict_p_brand.find("Brand#34");

        std::unordered_set<int> sm_containers, med_containers, lg_containers;
        for (const char* s : {"SM CASE", "SM BOX", "SM PACK", "SM PKG"}) {
            int c = dict_p_container.find(s); if (c >= 0) sm_containers.insert(c);
        }
        for (const char* s : {"MED BAG", "MED BOX", "MED PKG", "MED PACK"}) {
            int c = dict_p_container.find(s); if (c >= 0) med_containers.insert(c);
        }
        for (const char* s : {"LG CASE", "LG BOX", "LG PACK", "LG PKG"}) {
            int c = dict_p_container.find(s); if (c >= 0) lg_containers.insert(c);
        }

        q19_classify.resize(PART_ROWS + 1, 0);
        #pragma omp parallel for schedule(static)
        for (int64_t i = 0; i < PART_ROWS; i++) {
            int32_t pk = (int32_t)(i + 1);
            int b = p_brand[i];
            int c = p_container[i];
            int sz = p_size[i];
            if (sz >= 1) {
                if (b == brand12 && sz <= 5 && sm_containers.count(c))
                    q19_classify[pk] = 1;
                else if (b == brand23 && sz <= 10 && med_containers.count(c))
                    q19_classify[pk] = 2;
                else if (b == brand34 && sz <= 15 && lg_containers.count(c))
                    q19_classify[pk] = 3;
            }
        }
    }

    // Q16: complaint suppliers
    if (g_active_queries & Q16B) {
        q16_complaint_suppliers.resize(SUPPLIER_ROWS + 1, false);
        for (int64_t i = 0; i < SUPPLIER_ROWS; i++) {
            size_t clen = v_s_comment.len(i);
            const char* cstr = v_s_comment.ptr(i);
            // LIKE '%Customer%Complaints%'
            const void* p1 = memmem(cstr, clen, "Customer", 8);
            if (p1) {
                size_t off = (const char*)p1 - cstr + 8;
                if (memmem(cstr + off, clen - off, "Complaints", 10) != nullptr)
                    q16_complaint_suppliers[i + 1] = true;  // suppkey = i+1
            }
        }
    }

    // Q20: forest partkeys
    if (g_active_queries & Q20B) {
        for (int64_t i = 0; i < PART_ROWS; i++) {
            size_t nlen = v_p_name.len(i);
            const char* nm = v_p_name.ptr(i);
            if (nlen >= 6 && memcmp(nm, "forest", 6) == 0) {
                q20_forest_partkeys.push_back((int32_t)(i + 1));
            }
        }
    }

    // Q22: target prefixes
    for (const char* code : q22_codes) {
        int idx = (code[0]-'0')*10 + (code[1]-'0');
        q22_target_prefix[idx] = true;
    }

    // European partsupp join (for Q2, Q11)
    if (g_active_queries & (Q2B | Q11B)) {
        // Build supplier-is-european lookup
        std::vector<EuroPS> local_results;
        local_results.reserve(PARTSUPP_ROWS / 5);  // ~20% are European

        #pragma omp parallel
        {
            std::vector<EuroPS> thread_local_results;
            thread_local_results.reserve(PARTSUPP_ROWS / 20);
            #pragma omp for schedule(static)
            for (int64_t i = 0; i < PARTSUPP_ROWS; i++) {
                int32_t sk = ps_suppkey[i];
                int32_t snk = s_nationkey[sk - 1];
                if (is_european_nation[snk]) {
                    thread_local_results.push_back({
                        ps_partkey[i], ps_suppkey[i], ps_availqty[i],
                        ps_supplycost[i], snk
                    });
                }
            }
            #pragma omp critical
            {
                local_results.insert(local_results.end(),
                    thread_local_results.begin(), thread_local_results.end());
            }
        }
        euro_partsupp = std::move(local_results);
    }

    // Q15: revenue array
    if (g_active_queries & Q15B) {
        q15_revenue.resize(SUPPLIER_ROWS + 1, 0.0);
    }
}

// ===========================================================================
// STAGE: Fused lineitem scan
// ===========================================================================

// Per-thread accumulators
struct ThreadLocalAccum {
    // Q1
    Q1Group q1[Q1_GROUPS] = {};

    // Q3: orderkey -> revenue
    std::unordered_map<int32_t, double> q3_revenue;

    // Q5: revenue by nationkey (0..24)
    double q5_revenue[25] = {};

    // Q6
    double q6_revenue = 0.0;

    // Q7: (supp_nation < cust_nation ? 0 : 1) * 2 + (year - 1995), max 4 entries
    // Actually: key = pair_idx * 2 + year_idx
    // pair 0: FRANCE->GERMANY, pair 1: GERMANY->FRANCE
    // year 0: 1995, year 1: 1996
    double q7_revenue[4] = {};

    // Q8: total_vol by year (1995, 1996), brazil_vol by year
    double q8_total[2] = {};
    double q8_brazil[2] = {};

    // Q9: (nationkey, year) -> profit
    std::unordered_map<int64_t, double> q9_profit;  // key = nationkey * 10000 + year

    // Q10: custkey -> revenue
    std::unordered_map<int32_t, double> q10_revenue;

    // Q12: shipmode_idx * 2 + (high=0, low=1), max 4
    int64_t q12_counts[4] = {};

    // Q14
    double q14_promo = 0.0;
    double q14_total = 0.0;

    // Q15: supplier revenue by suppkey
    std::vector<double> q15_rev;
    void init_q15(int n) { q15_rev.assign(n, 0.0); }

    // Q19
    double q19_revenue = 0.0;

    // Q21 candidates
    std::vector<Q21Cand> q21_candidates;
};

// Q4 sub: shared atomic bitset for orderkeys with late lineitems
static std::vector<uint64_t> q4_late_bitset;  // bit per orderkey, up to 60000001
static const int64_t Q4_BITSET_SIZE = (60000001 + 63) / 64;

static inline void set_bit_atomic(std::vector<uint64_t>& bs, int32_t key) {
    uint64_t word_idx = (uint64_t)key >> 6;
    uint64_t bit = 1ULL << ((uint64_t)key & 63);
    __sync_fetch_and_or(&bs[word_idx], bit);
}

static inline bool test_bit(const std::vector<uint64_t>& bs, int32_t key) {
    uint64_t word_idx = (uint64_t)key >> 6;
    uint64_t bit = 1ULL << ((uint64_t)key & 63);
    return (bs[word_idx] & bit) != 0;
}

static void fused_scan_lineitem() {
    MQO_TIME_SHARED("fused_scan_lineitem");

    const uint32_t Q = g_active_queries;

    // Date constants
    constexpr int32_t D_19980902 = days_from_civil(1998, 9, 2);
    constexpr int32_t D_19950315 = days_from_civil(1995, 3, 15);
    constexpr int32_t D_19940101 = days_from_civil(1994, 1, 1);
    constexpr int32_t D_19950101 = days_from_civil(1995, 1, 1);
    constexpr int32_t D_19950901 = days_from_civil(1995, 9, 1);
    constexpr int32_t D_19951001 = days_from_civil(1995, 10, 1);
    constexpr int32_t D_19960101 = days_from_civil(1996, 1, 1);
    constexpr int32_t D_19960401 = days_from_civil(1996, 4, 1);
    constexpr int32_t D_19961231 = days_from_civil(1996, 12, 31);
    constexpr int32_t D_19931001 = days_from_civil(1993, 10, 1);

    // Initialize Q4 bitset
    if (Q & Q4B) {
        q4_late_bitset.assign(Q4_BITSET_SIZE, 0);
    }

    int nthreads = omp_get_max_threads();
    std::vector<ThreadLocalAccum> tl(nthreads);
    if (Q & Q15B) {
        for (int t = 0; t < nthreads; t++)
            tl[t].init_q15(SUPPLIER_ROWS + 1);
    }

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        ThreadLocalAccum& acc = tl[tid];

        #pragma omp for schedule(static)
        for (int64_t i = 0; i < LINEITEM_ROWS; i++) {
            int32_t sd = l_shipdate[i];
            int32_t ok = l_orderkey[i];
            int32_t pk = l_partkey[i];
            int32_t sk = l_suppkey[i];
            double qty = l_quantity[i];
            double ep = l_extendedprice[i];
            double disc = l_discount[i];
            double tax = l_tax[i];
            int8_t rf = l_returnflag[i];
            int8_t ls = l_linestatus[i];
            int32_t cd = l_commitdate[i];
            int32_t rd = l_receiptdate[i];

            // Q1: shipdate <= 1998-09-02
            if ((Q & Q1B) && sd <= D_19980902) {
                int k = q1_key(rf, ls);
                auto& g = acc.q1[k];
                g.sum_qty += qty;
                g.sum_base_price += ep;
                double dp = ep * (1.0 - disc);
                g.sum_disc_price += dp;
                g.sum_charge += dp * (1.0 + tax);
                g.sum_disc += disc;
                g.count++;
            }

            // Q3: shipdate > 1995-03-15, orders.orderdate < 1995-03-15, customer.mktsegment = BUILDING
            if ((Q & Q3B) && sd > D_19950315) {
                int32_t orow = orders_pk_index[ok];
                if (orow >= 0 && o_orderdate[orow] < D_19950315) {
                    int32_t ck = o_custkey[orow];
                    if (c_mktsegment[ck - 1] == (uint8_t)q3_building_code) {
                        acc.q3_revenue[ok] += ep * (1.0 - disc);
                    }
                }
            }

            // Q4_sub: commitdate < receiptdate -> set bit on orderkey
            if ((Q & Q4B) && cd < rd) {
                set_bit_atomic(q4_late_bitset, ok);
            }

            // Q5: orders.orderdate in [1994-01-01, 1995-01-01), customer in ASIA, supplier same nation
            if (Q & Q5B) {
                int32_t orow = orders_pk_index[ok];
                if (orow >= 0) {
                    int32_t od = o_orderdate[orow];
                    if (od >= D_19940101 && od < D_19950101) {
                        int32_t ck = o_custkey[orow];
                        int32_t cnk = c_nationkey[ck - 1];
                        if (is_asian_nation[cnk]) {
                            int32_t snk = s_nationkey[sk - 1];
                            if (snk == cnk) {
                                acc.q5_revenue[cnk] += ep * (1.0 - disc);
                            }
                        }
                    }
                }
            }

            // Q6: shipdate in [1994-01-01, 1995-01-01), discount in [0.05,0.07], quantity < 24
            if ((Q & Q6B) && sd >= D_19940101 && sd < D_19950101 &&
                disc >= 0.05 && disc <= 0.07 && qty < 24.0) {
                acc.q6_revenue += ep * disc;
            }

            // Q7: shipdate in [1995-01-01, 1996-12-31], FRANCE<->GERMANY
            if ((Q & Q7B) && sd >= D_19950101 && sd <= D_19961231) {
                int32_t snk = s_nationkey[sk - 1];
                if (snk == france_nk || snk == germany_nk) {
                    int32_t orow = orders_pk_index[ok];
                    if (orow >= 0) {
                        int32_t ck = o_custkey[orow];
                        int32_t cnk = c_nationkey[ck - 1];
                        int pair_idx = -1;
                        if (snk == france_nk && cnk == germany_nk) pair_idx = 0;
                        else if (snk == germany_nk && cnk == france_nk) pair_idx = 1;
                        if (pair_idx >= 0) {
                            int yr = extract_year(sd);
                            int year_idx = yr - 1995;
                            if (year_idx >= 0 && year_idx <= 1) {
                                acc.q7_revenue[pair_idx * 2 + year_idx] += ep * (1.0 - disc);
                            }
                        }
                    }
                }
            }

            // Q8: part type = ECONOMY ANODIZED STEEL, orderdate in [1995,1996], customer in AMERICA
            // total = sum for all rows where customer region=AMERICA
            // brazil = sum where additionally supplier nation=BRAZIL
            if ((Q & Q8B) && q8_type_code >= 0 && p_type[pk - 1] == (uint8_t)q8_type_code) {
                int32_t orow = orders_pk_index[ok];
                if (orow >= 0) {
                    int32_t od = o_orderdate[orow];
                    if (od >= D_19950101 && od < days_from_civil(1997, 1, 1)) {
                        int32_t ck = o_custkey[orow];
                        int32_t cnk = c_nationkey[ck - 1];
                        if (is_america_nation[cnk]) {
                            int yr = extract_year(od);
                            int yi = yr - 1995;
                            if (yi >= 0 && yi <= 1) {
                                double vol = ep * (1.0 - disc);
                                acc.q8_total[yi] += vol;
                                int32_t snk = s_nationkey[sk - 1];
                                if (snk == brazil_nk) {
                                    acc.q8_brazil[yi] += vol;
                                }
                            }
                        }
                    }
                }
            }

            // Q9: green part -> get partsupp cost -> compute profit
            if ((Q & Q9B) && pk >= 1 && pk <= PART_ROWS && green_parts_bitset[pk]) {
                int32_t psrow = ps_hash_lookup(partsupp_hash_table, PS_HASH_MASK, pk, sk);
                if (psrow >= 0) {
                    double psc = ps_supplycost[psrow];
                    int32_t orow = orders_pk_index[ok];
                    if (orow >= 0) {
                        int yr = extract_year(o_orderdate[orow]);
                        int32_t snk = s_nationkey[sk - 1];
                        double profit = ep * (1.0 - disc) - psc * qty;
                        int64_t key = (int64_t)snk * 10000 + yr;
                        acc.q9_profit[key] += profit;
                    }
                }
            }

            // Q10: returnflag == 'R', orderdate in [1993-10-01, 1994-01-01)
            if ((Q & Q10B) && rf == 'R') {
                int32_t orow = orders_pk_index[ok];
                if (orow >= 0) {
                    int32_t od = o_orderdate[orow];
                    if (od >= D_19931001 && od < D_19940101) {
                        int32_t ck = o_custkey[orow];
                        acc.q10_revenue[ck] += ep * (1.0 - disc);
                    }
                }
            }

            // Q12: shipmode IN (MAIL, SHIP), receiptdate in [1994-01-01, 1995-01-01),
            //       commitdate < receiptdate, shipdate < commitdate
            if ((Q & Q12B)) {
                uint8_t sm = l_shipmode[i];
                if ((sm == (uint8_t)q12_mail_code || sm == (uint8_t)q12_ship_code) &&
                    rd >= D_19940101 && rd < D_19950101 &&
                    cd < rd && sd < cd) {
                    int32_t orow = orders_pk_index[ok];
                    if (orow >= 0) {
                        uint8_t op = o_orderpriority[orow];
                        const std::string& opstr = dict_o_orderpriority.entries[op];
                        bool is_high = (opstr == "1-URGENT" || opstr == "2-HIGH");
                        int mode_idx = (sm == (uint8_t)q12_mail_code) ? 0 : 1;
                        if (is_high)
                            acc.q12_counts[mode_idx * 2 + 0]++;
                        else
                            acc.q12_counts[mode_idx * 2 + 1]++;
                    }
                }
            }

            // Q14: shipdate in [1995-09-01, 1995-10-01)
            if ((Q & Q14B) && sd >= D_19950901 && sd < D_19951001) {
                double rev = ep * (1.0 - disc);
                acc.q14_total += rev;
                if (is_promo_type[p_type[pk - 1]]) {
                    acc.q14_promo += rev;
                }
            }

            // Q15: shipdate in [1996-01-01, 1996-04-01)
            if ((Q & Q15B) && sd >= D_19960101 && sd < D_19960401) {
                acc.q15_rev[sk] += ep * (1.0 - disc);
            }

            // Q19: shipmode IN (AIR, AIR REG), shipinstruct = DELIVER IN PERSON
            if ((Q & Q19B) && (l_shipmode[i] == (uint8_t)q19_air_code || l_shipmode[i] == (uint8_t)q19_airreg_code)
                && l_shipinstruct[i] == (uint8_t)q19_deliver_code) {
                uint8_t cls = q19_classify[pk];
                if (cls > 0) {
                    bool match = false;
                    if (cls == 1 && qty >= 1 && qty <= 11) match = true;
                    else if (cls == 2 && qty >= 10 && qty <= 20) match = true;
                    else if (cls == 3 && qty >= 20 && qty <= 30) match = true;
                    if (match) {
                        acc.q19_revenue += ep * (1.0 - disc);
                    }
                }
            }

            // Q21_sub: supplier in SAUDI ARABIA, receiptdate > commitdate, order status F
            if ((Q & Q21B) && s_nationkey[sk - 1] == saudi_nk && rd > cd) {
                int32_t orow = orders_pk_index[ok];
                if (orow >= 0 && o_orderstatus[orow] == 'F') {
                    acc.q21_candidates.push_back({ok, sk});
                }
            }
        }
    } // end parallel

    // Merge thread-local results
    // We'll store merged results in tl[0]
    for (int t = 1; t < nthreads; t++) {
        // Q1
        if (Q & Q1B) {
            for (int k = 0; k < Q1_GROUPS; k++) {
                tl[0].q1[k].sum_qty += tl[t].q1[k].sum_qty;
                tl[0].q1[k].sum_base_price += tl[t].q1[k].sum_base_price;
                tl[0].q1[k].sum_disc_price += tl[t].q1[k].sum_disc_price;
                tl[0].q1[k].sum_charge += tl[t].q1[k].sum_charge;
                tl[0].q1[k].sum_disc += tl[t].q1[k].sum_disc;
                tl[0].q1[k].count += tl[t].q1[k].count;
            }
        }
        // Q3
        if (Q & Q3B) {
            for (auto& [k, v] : tl[t].q3_revenue) tl[0].q3_revenue[k] += v;
        }
        // Q5
        if (Q & Q5B) {
            for (int n = 0; n < 25; n++) tl[0].q5_revenue[n] += tl[t].q5_revenue[n];
        }
        // Q6
        if (Q & Q6B) tl[0].q6_revenue += tl[t].q6_revenue;
        // Q7
        if (Q & Q7B) {
            for (int k = 0; k < 4; k++) tl[0].q7_revenue[k] += tl[t].q7_revenue[k];
        }
        // Q8
        if (Q & Q8B) {
            for (int k = 0; k < 2; k++) {
                tl[0].q8_total[k] += tl[t].q8_total[k];
                tl[0].q8_brazil[k] += tl[t].q8_brazil[k];
            }
        }
        // Q9
        if (Q & Q9B) {
            for (auto& [k, v] : tl[t].q9_profit) tl[0].q9_profit[k] += v;
        }
        // Q10
        if (Q & Q10B) {
            for (auto& [k, v] : tl[t].q10_revenue) tl[0].q10_revenue[k] += v;
        }
        // Q12
        if (Q & Q12B) {
            for (int k = 0; k < 4; k++) tl[0].q12_counts[k] += tl[t].q12_counts[k];
        }
        // Q14
        if (Q & Q14B) {
            tl[0].q14_promo += tl[t].q14_promo;
            tl[0].q14_total += tl[t].q14_total;
        }
        // Q15
        if (Q & Q15B) {
            for (int64_t s = 1; s <= SUPPLIER_ROWS; s++)
                tl[0].q15_rev[s] += tl[t].q15_rev[s];
        }
        // Q19
        if (Q & Q19B) tl[0].q19_revenue += tl[t].q19_revenue;
        // Q21
        if (Q & Q21B) {
            tl[0].q21_candidates.insert(tl[0].q21_candidates.end(),
                tl[t].q21_candidates.begin(), tl[t].q21_candidates.end());
        }
    }

    // Store merged results globally
    // We'll use global variables for each query's intermediate
    // (defined below in finalize sections)

    // Store in a place accessible to finalize
    // Use static thread_local... no, just use statics with swap
    static ThreadLocalAccum merged;
    merged = std::move(tl[0]);

    // Make accessible via a global pointer
    extern ThreadLocalAccum* g_lineitem_merged;
    g_lineitem_merged = &merged;
}

// Global pointer to merged lineitem results
ThreadLocalAccum* g_lineitem_merged = nullptr;

// ===========================================================================
// STAGE: Fused orders scan (Q4, Q13)
// ===========================================================================

// Q4 results: orderpriority -> count
static std::unordered_map<std::string, int64_t> q4_results;

// Q13 results: order count per customer (dense array)
static std::vector<int32_t> q13_order_counts;

static void fused_scan_orders() {
    MQO_TIME_SHARED("fused_scan_orders");

    const uint32_t Q = g_active_queries;

    // Q4: for each order with orderdate in [1993-07-01, 1993-10-01),
    //     if exists late lineitem (q4_late_bitset[orderkey] set) -> count by priority
    constexpr int32_t D_19930701 = days_from_civil(1993, 7, 1);
    constexpr int32_t D_19931001 = days_from_civil(1993, 10, 1);

    // Q13: count qualifying orders per customer
    if (Q & Q13B) {
        q13_order_counts.assign(CUSTOMER_ROWS + 1, 0);  // custkey 1..1500000
    }

    int nthreads = omp_get_max_threads();

    // Q4: thread-local maps
    struct Q4Local {
        std::unordered_map<std::string, int64_t> counts;
    };
    std::vector<Q4Local> q4_local(nthreads);

    // Q13: thread-local arrays
    std::vector<std::vector<int32_t>> q13_local;
    if (Q & Q13B) {
        q13_local.resize(nthreads);
        for (auto& v : q13_local) v.assign(CUSTOMER_ROWS + 1, 0);
    }

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();

        #pragma omp for schedule(static)
        for (int64_t i = 0; i < ORDERS_ROWS; i++) {
            int32_t ok = o_orderkey[i];

            // Q4
            if ((Q & Q4B)) {
                int32_t od = o_orderdate[i];
                if (od >= D_19930701 && od < D_19931001) {
                    if (test_bit(q4_late_bitset, ok)) {
                        const std::string& prio = dict_o_orderpriority.entries[o_orderpriority[i]];
                        q4_local[tid].counts[prio]++;
                    }
                }
            }

            // Q13: count orders NOT LIKE '%special%requests%'
            if ((Q & Q13B)) {
                int32_t ck = o_custkey[i];
                size_t clen = v_o_comment.len(i);
                const char* cstr = v_o_comment.ptr(i);
                // Check NOT LIKE '%special%requests%'
                bool matches_pattern = false;
                const void* p1 = memmem(cstr, clen, "special", 7);
                if (p1) {
                    size_t off = (const char*)p1 - cstr + 7;
                    if (memmem(cstr + off, clen - off, "requests", 8) != nullptr)
                        matches_pattern = true;
                }
                if (!matches_pattern) {
                    q13_local[tid][ck]++;
                }
            }
        }
    } // end parallel

    // Merge Q4
    if (Q & Q4B) {
        for (int t = 0; t < nthreads; t++) {
            for (auto& [k, v] : q4_local[t].counts)
                q4_results[k] += v;
        }
    }

    // Merge Q13
    if (Q & Q13B) {
        // Sum all thread-local arrays
        for (int t = 0; t < nthreads; t++) {
            #pragma omp parallel for schedule(static)
            for (int64_t ck = 1; ck <= CUSTOMER_ROWS; ck++) {
                q13_order_counts[ck] += q13_local[t][ck];
            }
        }
    }
}

// ===========================================================================
// STAGE: Q2, Q11 — scan European partsupp
// ===========================================================================

// Q2 results
struct Q2Row {
    double s_acctbal;
    std::string s_name;
    std::string n_name;
    int32_t p_partkey;
    std::string p_mfgr;
    std::string s_address;
    std::string s_phone;
    std::string s_comment;
};
static std::vector<Q2Row> q2_results;

// Q11 results
struct Q11Row {
    int32_t ps_partkey;
    double value;
};
static std::vector<Q11Row> q11_results;

static void scan_european_partsupp() {
    MQO_TIME_SHARED("scan_european_partsupp");

    const uint32_t Q = g_active_queries;

    // Q2: find parts with p_size=15, p_type ends with BRASS
    // For those parts, find min ps_supplycost from European suppliers
    // Then collect rows matching the min
    if (Q & Q2B) {
        // Find qualifying type codes (ends with "BRASS")
        std::unordered_set<int> brass_types;
        for (size_t i = 0; i < dict_p_type.entries.size(); i++) {
            auto& e = dict_p_type.entries[i];
            if (e.size() >= 5 && e.substr(e.size() - 5) == "BRASS")
                brass_types.insert((int)i);
        }

        // Phase 1: find min supplycost per qualifying partkey from European partsupp
        std::unordered_map<int32_t, double> min_cost;
        for (auto& eps : euro_partsupp) {
            int32_t pk = eps.partkey;
            int32_t prow = pk - 1;
            if (p_size[prow] == 15 && brass_types.count(p_type[prow])) {
                auto it = min_cost.find(pk);
                if (it == min_cost.end() || eps.supplycost < it->second) {
                    min_cost[pk] = eps.supplycost;
                }
            }
        }

        // Phase 2: collect matching rows
        for (auto& eps : euro_partsupp) {
            int32_t pk = eps.partkey;
            auto it = min_cost.find(pk);
            if (it != min_cost.end() && eps.supplycost == it->second) {
                int32_t prow = pk - 1;
                int32_t srow = eps.suppkey - 1;
                Q2Row row;
                row.s_acctbal = s_acctbal[srow];
                row.s_name = v_s_name.get(srow);
                row.n_name = nation_names[eps.supp_nationkey];
                row.p_partkey = pk;
                row.p_mfgr = dict_p_mfgr.entries[p_mfgr[prow]];
                row.s_address = v_s_address.get(srow);
                row.s_phone = v_s_phone.get(srow);
                row.s_comment = v_s_comment.get(srow);
                q2_results.push_back(std::move(row));
            }
        }

        // Sort: ORDER BY s_acctbal DESC, n_name, s_name, p_partkey
        std::sort(q2_results.begin(), q2_results.end(), [](const Q2Row& a, const Q2Row& b) {
            if (a.s_acctbal != b.s_acctbal) return a.s_acctbal > b.s_acctbal;
            if (a.n_name != b.n_name) return a.n_name < b.n_name;
            if (a.s_name != b.s_name) return a.s_name < b.s_name;
            return a.p_partkey < b.p_partkey;
        });
        if (q2_results.size() > 100) q2_results.resize(100);
    }

    // Q11: Germany nation, accumulate ps_supplycost * ps_availqty per partkey
    if (Q & Q11B) {
        std::unordered_map<int32_t, double> partkey_value;
        double global_total = 0.0;

        for (auto& eps : euro_partsupp) {
            if (eps.supp_nationkey == germany_nk) {
                double val = eps.supplycost * eps.availqty;
                partkey_value[eps.partkey] += val;
                global_total += val;
            }
        }

        double threshold = global_total * 0.0001 / 10.0;  // TPC-H: 0.0001/SF, SF=10
        for (auto& [pk, val] : partkey_value) {
            if (val > threshold) {
                q11_results.push_back({pk, val});
            }
        }

        // ORDER BY value DESC
        std::sort(q11_results.begin(), q11_results.end(), [](const Q11Row& a, const Q11Row& b) {
            return a.value > b.value;
        });
    }
}

// ===========================================================================
// STAGE: Q16, Q20 — fused partsupp scan
// ===========================================================================

// Q16 results
struct Q16Row {
    std::string p_brand;
    std::string p_type;
    int32_t p_size;
    int32_t supplier_cnt;
};
static std::vector<Q16Row> q16_results;

// Q20 results
struct Q20Row {
    std::string s_name;
    std::string s_address;
};
static std::vector<Q20Row> q20_results;

static void fused_scan_partsupp() {
    MQO_TIME_SHARED("fused_scan_partsupp");

    const uint32_t Q = g_active_queries;

    // Q16: Find parts where brand != Brand#45, type not like 'MEDIUM POLISHED%',
    //       size in {49,14,23,45,19,3,36,9}
    // For those parts, count distinct suppliers (excluding complaint suppliers)
    if (Q & Q16B) {
        int brand45_code = dict_p_brand.find("Brand#45");

        // O(1) lookup arrays instead of hash sets
        std::vector<bool> excluded_type_arr(dict_p_type.entries.size(), false);
        for (size_t i = 0; i < dict_p_type.entries.size(); i++) {
            if (dict_p_type.entries[i].compare(0, 15, "MEDIUM POLISHED") == 0)
                excluded_type_arr[i] = true;
        }

        bool target_size_arr[51] = {};
        for (int s : {49, 14, 23, 45, 19, 3, 36, 9}) target_size_arr[s] = true;

        // Pre-classify parts: assign group_id per qualifying partkey
        struct Q16GKey { uint8_t brand; uint8_t type; int32_t size; };
        std::unordered_map<uint32_t, int32_t> packed_to_gid;
        std::vector<Q16GKey> group_keys;
        // part_group[pk] = group_id (>=0) or -1 if not qualifying
        std::vector<int32_t> part_group(PART_ROWS + 1, -1);

        for (int64_t i = 0; i < PART_ROWS; i++) {
            uint8_t b = p_brand[i];
            if (b == (uint8_t)brand45_code) continue;
            uint8_t t = p_type[i];
            if (excluded_type_arr[t]) continue;
            int32_t sz = p_size[i];
            if (sz < 1 || sz > 50 || !target_size_arr[sz]) continue;

            uint32_t packed = ((uint32_t)b << 16) | ((uint32_t)t << 8) | (uint32_t)sz;
            auto it = packed_to_gid.find(packed);
            int32_t gid;
            if (it == packed_to_gid.end()) {
                gid = (int32_t)group_keys.size();
                packed_to_gid[packed] = gid;
                group_keys.push_back({b, t, sz});
            } else {
                gid = it->second;
            }
            part_group[i + 1] = gid;  // partkey = i+1
        }

        int32_t num_groups = (int32_t)group_keys.size();

        // Scan partsupp: collect (group_id, suppkey) pairs — no hash ops in hot loop
        std::vector<std::pair<int32_t, int32_t>> pairs;
        pairs.reserve(PARTSUPP_ROWS / 6);

        for (int64_t i = 0; i < PARTSUPP_ROWS; i++) {
            int32_t pk = ps_partkey[i];
            int32_t gid = part_group[pk];
            if (gid < 0) continue;
            int32_t sk = ps_suppkey[i];
            if (q16_complaint_suppliers[sk]) continue;
            pairs.push_back({gid, sk});
        }

        // Sort by (group_id, suppkey) then count distinct suppkeys per group
        std::sort(pairs.begin(), pairs.end());

        std::vector<int32_t> group_counts(num_groups, 0);
        for (size_t i = 0; i < pairs.size(); ) {
            int32_t gid = pairs[i].first;
            int32_t prev_sk = -1;
            int32_t count = 0;
            while (i < pairs.size() && pairs[i].first == gid) {
                if (pairs[i].second != prev_sk) {
                    count++;
                    prev_sk = pairs[i].second;
                }
                i++;
            }
            group_counts[gid] = count;
        }

        // Build results
        for (int32_t gid = 0; gid < num_groups; gid++) {
            if (group_counts[gid] == 0) continue;
            Q16Row row;
            row.p_brand = dict_p_brand.entries[group_keys[gid].brand];
            row.p_type = dict_p_type.entries[group_keys[gid].type];
            row.p_size = group_keys[gid].size;
            row.supplier_cnt = group_counts[gid];
            q16_results.push_back(std::move(row));
        }

        // ORDER BY supplier_cnt DESC, p_brand, p_type, p_size
        std::sort(q16_results.begin(), q16_results.end(), [](const Q16Row& a, const Q16Row& b) {
            if (a.supplier_cnt != b.supplier_cnt) return a.supplier_cnt > b.supplier_cnt;
            if (a.p_brand != b.p_brand) return a.p_brand < b.p_brand;
            if (a.p_type != b.p_type) return a.p_type < b.p_type;
            return a.p_size < b.p_size;
        });
    }

    // Q20 precompute is done in index_computations
}

// ===========================================================================
// STAGE: Q22 — two-phase customer scan
// ===========================================================================

struct Q22Group {
    int64_t numcust = 0;
    double totacctbal = 0.0;
};
static std::map<std::string, Q22Group> q22_results;

static void scan_customer_q22() {
    MQO_TIME_SHARED("scan_customer_q22");

    if (!(g_active_queries & Q22B)) return;

    // Build has_orders bitset from orders
    std::vector<uint64_t> has_orders_bitset((CUSTOMER_ROWS + 64) / 64, 0);
    for (int64_t i = 0; i < ORDERS_ROWS; i++) {
        int32_t ck = o_custkey[i];
        uint64_t word = (uint64_t)(ck) >> 6;
        uint64_t bit = 1ULL << ((uint64_t)ck & 63);
        has_orders_bitset[word] |= bit;
    }

    // Phase 1: compute average c_acctbal for customers with acctbal > 0 and matching prefix
    double sum_acctbal = 0.0;
    int64_t cnt_acctbal = 0;

    for (int64_t i = 0; i < CUSTOMER_ROWS; i++) {
        if (c_acctbal[i] > 0.0) {
            // Check phone prefix
            const char* phone = v_c_phone.ptr(i);
            int prefix = q22_prefix_to_idx(phone);
            if (prefix >= 0 && prefix < 100 && q22_target_prefix[prefix]) {
                sum_acctbal += c_acctbal[i];
                cnt_acctbal++;
            }
        }
    }
    double avg_acctbal = (cnt_acctbal > 0) ? sum_acctbal / cnt_acctbal : 0.0;

    // Phase 2: find customers with matching prefix, acctbal > avg, no orders
    for (int64_t i = 0; i < CUSTOMER_ROWS; i++) {
        int32_t ck = (int32_t)(i + 1);
        if (c_acctbal[i] > avg_acctbal) {
            const char* phone = v_c_phone.ptr(i);
            int prefix = q22_prefix_to_idx(phone);
            if (prefix >= 0 && prefix < 100 && q22_target_prefix[prefix]) {
                // Check no orders
                uint64_t word = (uint64_t)ck >> 6;
                uint64_t bit = 1ULL << ((uint64_t)ck & 63);
                if (!(has_orders_bitset[word] & bit)) {
                    char code[3] = {phone[0], phone[1], 0};
                    q22_results[code].numcust++;
                    q22_results[code].totacctbal += c_acctbal[i];
                }
            }
        }
    }
}

// ===========================================================================
// STAGE: Index computations (Q17, Q18, Q20, Q21)
// ===========================================================================

// Q17 result
static double q17_avg_yearly = 0.0;

// Q18 results
struct Q18Row {
    std::string c_name;
    int32_t c_custkey;
    int32_t o_orderkey;
    int32_t o_orderdate;
    double o_totalprice;
    double sum_qty;
};
static std::vector<Q18Row> q18_results;

// Q20: supplier qualifies set
static std::unordered_set<int32_t> q20_qualifying_suppliers;

static void index_computations() {
    MQO_TIME_SHARED("index_computations");

    const uint32_t Q = g_active_queries;

    // Q17: Brand#23, MED BOX parts. For each qualifying partkey, use lineitem_partkey_grouped
    // Pass 1: compute avg(qty). Pass 2: sum extendedprice where qty < 0.2*avg.
    if (Q & Q17B) {
        int brand23_code = dict_p_brand.find("Brand#23");
        int medbox_code = dict_p_container.find("MED BOX");

        double total_sum = 0.0;

        // Find qualifying partkeys
        std::vector<int32_t> q17_parts;
        for (int64_t i = 0; i < PART_ROWS; i++) {
            if (p_brand[i] == (uint8_t)brand23_code && p_container[i] == (uint8_t)medbox_code) {
                q17_parts.push_back((int32_t)(i + 1));
            }
        }

        #pragma omp parallel for schedule(dynamic, 64) reduction(+:total_sum)
        for (size_t pi = 0; pi < q17_parts.size(); pi++) {
            int32_t pk = q17_parts[pi];
            uint32_t start = idx_lineitem_partkey_grouped.start(pk);
            uint32_t end = idx_lineitem_partkey_grouped.end(pk);
            uint32_t cnt = end - start;
            if (cnt == 0) continue;

            // Pass 1: avg qty
            double sum_qty = 0.0;
            for (uint32_t j = start; j < end; j++) {
                uint32_t row = idx_lineitem_partkey_grouped.rows[j];
                sum_qty += l_quantity[row];
            }
            double avg_qty = sum_qty / cnt;
            double threshold = 0.2 * avg_qty;

            // Pass 2: sum extendedprice where qty < threshold
            double local_sum = 0.0;
            for (uint32_t j = start; j < end; j++) {
                uint32_t row = idx_lineitem_partkey_grouped.rows[j];
                if (l_quantity[row] < threshold) {
                    local_sum += l_extendedprice[row];
                }
            }
            total_sum += local_sum;
        }

        q17_avg_yearly = total_sum / 7.0;
    }

    // Q18: iterate orderkeys via lineitem_orderkey_idx, find heavy orders (sum qty > 300)
    if (Q & Q18B) {
        // The index has entries for orderkeys 0..60000000
        // Each entry has {start, count}
        struct Q18Cand {
            int32_t orderkey;
            double sum_qty;
        };

        std::vector<Q18Cand> candidates;

        #pragma omp parallel
        {
            std::vector<Q18Cand> local_cands;
            #pragma omp for schedule(dynamic, 1024)
            for (int64_t ok = 1; ok <= 60000000; ok++) {
                auto& entry = lineitem_orderkey_idx[ok];
                if (entry.count == 0) continue;

                double sum_qty = 0.0;
                for (uint32_t j = 0; j < entry.count; j++) {
                    sum_qty += l_quantity[entry.start + j];
                }
                if (sum_qty > 300.0) {
                    local_cands.push_back({(int32_t)ok, sum_qty});
                }
            }
            #pragma omp critical
            {
                candidates.insert(candidates.end(), local_cands.begin(), local_cands.end());
            }
        }

        // Look up order and customer info
        for (auto& cand : candidates) {
            int32_t orow = orders_pk_index[cand.orderkey];
            if (orow < 0) continue;
            int32_t ck = o_custkey[orow];
            Q18Row row;
            row.c_name = v_c_name.get(ck - 1);
            row.c_custkey = ck;
            row.o_orderkey = cand.orderkey;
            row.o_orderdate = o_orderdate[orow];
            row.o_totalprice = o_totalprice[orow];
            row.sum_qty = cand.sum_qty;
            q18_results.push_back(std::move(row));
        }

        // ORDER BY o_totalprice DESC, o_orderdate LIMIT 100
        std::sort(q18_results.begin(), q18_results.end(), [](const Q18Row& a, const Q18Row& b) {
            if (a.o_totalprice != b.o_totalprice) return a.o_totalprice > b.o_totalprice;
            return a.o_orderdate < b.o_orderdate;
        });
        if (q18_results.size() > 100) q18_results.resize(100);
    }

    // Q20: For each forest partkey, get partsupp entries. For each (pk,sk) where supplier
    // is Canadian, sum lineitem quantity where l_shipdate in [1994-01-01, 1995-01-01) and
    // l_suppkey matches. If ps_availqty > 0.5 * sum_qty, supplier qualifies.
    if (Q & Q20B) {
        constexpr int32_t D_19940101 = days_from_civil(1994, 1, 1);
        constexpr int32_t D_19950101 = days_from_civil(1995, 1, 1);

        std::vector<int32_t> qualifying_supps;

        #pragma omp parallel
        {
            std::vector<int32_t> local_supps;

            #pragma omp for schedule(dynamic, 16)
            for (size_t fi = 0; fi < q20_forest_partkeys.size(); fi++) {
                int32_t pk = q20_forest_partkeys[fi];

                // Get partsupp entries for this partkey
                auto& ps_entry = partsupp_partkey_idx[pk];
                if (ps_entry.count == 0) continue;

                // Get lineitem rows for this partkey
                uint32_t li_start = idx_lineitem_partkey_grouped.start(pk);
                uint32_t li_end = idx_lineitem_partkey_grouped.end(pk);

                for (uint32_t p = 0; p < ps_entry.count; p++) {
                    uint32_t psrow = ps_entry.start + p;
                    int32_t sk = ps_suppkey[psrow];
                    int32_t snk = s_nationkey[sk - 1];
                    if (snk != canada_nk) continue;

                    int32_t avail = ps_availqty[psrow];

                    // Sum lineitem quantity for this (pk, sk) in date range
                    double sum_qty = 0.0;
                    bool has_match = false;
                    for (uint32_t j = li_start; j < li_end; j++) {
                        uint32_t lrow = idx_lineitem_partkey_grouped.rows[j];
                        if (l_suppkey[lrow] == sk &&
                            l_shipdate[lrow] >= D_19940101 &&
                            l_shipdate[lrow] < D_19950101) {
                            sum_qty += l_quantity[lrow];
                            has_match = true;
                        }
                    }

                    // In SQL, SUM over empty set returns NULL;
                    // ps_availqty > NULL is false, so skip if no matches
                    if (has_match && avail > 0.5 * sum_qty) {
                        local_supps.push_back(sk);
                    }
                }
            }

            #pragma omp critical
            {
                qualifying_supps.insert(qualifying_supps.end(),
                    local_supps.begin(), local_supps.end());
            }
        }

        q20_qualifying_suppliers.insert(qualifying_supps.begin(), qualifying_supps.end());

        // Build Q20 results: Canadian suppliers that qualify
        std::vector<Q20Row> rows;
        for (int32_t sk : q20_qualifying_suppliers) {
            int32_t srow = sk - 1;
            Q20Row r;
            r.s_name = v_s_name.get(srow);
            r.s_address = v_s_address.get(srow);
            rows.push_back(std::move(r));
        }
        // ORDER BY s_name
        std::sort(rows.begin(), rows.end(), [](const Q20Row& a, const Q20Row& b) {
            return a.s_name < b.s_name;
        });
        q20_results = std::move(rows);
    }
}

// ===========================================================================
// Q21 finalize (uses lineitem_orderkey_idx)
// ===========================================================================
struct Q21Result {
    std::string s_name;
    int64_t numwait;
};
static std::vector<Q21Result> q21_results;

static void finalize_q21() {
    MQO_TIME_TAIL("Q21_finalize");

    if (!(g_active_queries & Q21B)) return;

    auto& candidates = g_lineitem_merged->q21_candidates;
    if (candidates.empty()) return;

    // Sort candidates by orderkey for cache-friendly grouped processing
    std::sort(candidates.begin(), candidates.end(), [](const Q21Cand& a, const Q21Cand& b) {
        return a.orderkey < b.orderkey;
    });

    // Build group ranges (start, end) in sorted candidates array
    struct OG { int32_t orderkey; uint32_t start, end; };
    std::vector<OG> groups;
    groups.reserve(candidates.size() / 2);
    {
        uint32_t gs = 0;
        for (uint32_t i = 1; i <= (uint32_t)candidates.size(); i++) {
            if (i == (uint32_t)candidates.size() || candidates[i].orderkey != candidates[gs].orderkey) {
                groups.push_back({candidates[gs].orderkey, gs, i});
                gs = i;
            }
        }
    }

    // Parallel processing with thread-local accumulation
    int nthreads = omp_get_max_threads();
    std::vector<std::unordered_map<int32_t, int64_t>> tl_counts(nthreads);

    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        auto& my_counts = tl_counts[tid];

        // Stack buffers for per-order unique suppkey sets (TPC-H orders have 1-7 lineitems)
        int32_t all_supps[128];
        int32_t late_supps[128];
        int32_t cand_supps[128];

        #pragma omp for schedule(dynamic, 256)
        for (size_t gi = 0; gi < groups.size(); gi++) {
            const auto& g = groups[gi];
            const int32_t ok = g.orderkey;

            const auto& entry = lineitem_orderkey_idx[ok];
            if (entry.count == 0) continue;

            // Collect unique suppkeys and unique late suppkeys using stack arrays
            int n_all = 0, n_late = 0;
            const uint32_t cnt = entry.count < 128u ? entry.count : 128u;
            for (uint32_t j = 0; j < cnt; j++) {
                const uint32_t row = entry.start + j;
                const int32_t sk = l_suppkey[row];
                const bool is_late = l_receiptdate[row] > l_commitdate[row];

                // Linear unique insert — fast for n < 10
                bool found = false;
                for (int k = 0; k < n_all; k++) {
                    if (all_supps[k] == sk) { found = true; break; }
                }
                if (!found) all_supps[n_all++] = sk;

                if (is_late) {
                    found = false;
                    for (int k = 0; k < n_late; k++) {
                        if (late_supps[k] == sk) { found = true; break; }
                    }
                    if (!found) late_supps[n_late++] = sk;
                }
            }

            // Unique candidate suppkeys for this order
            int n_cand = 0;
            for (uint32_t ci = g.start; ci < g.end; ci++) {
                const int32_t sk = candidates[ci].suppkey;
                bool found = false;
                for (int k = 0; k < n_cand; k++) {
                    if (cand_supps[k] == sk) { found = true; break; }
                }
                if (!found) cand_supps[n_cand++] = sk;
            }

            for (int ci = 0; ci < n_cand; ci++) {
                const int32_t sk = cand_supps[ci];

                // EXISTS another supplier on this order
                bool exists_other = false;
                for (int k = 0; k < n_all; k++) {
                    if (all_supps[k] != sk) { exists_other = true; break; }
                }
                if (!exists_other) continue;

                // NOT EXISTS another late supplier (different suppkey that is late)
                bool exists_other_late = false;
                for (int k = 0; k < n_late; k++) {
                    if (late_supps[k] != sk) { exists_other_late = true; break; }
                }

                if (!exists_other_late) {
                    my_counts[sk]++;
                }
            }
        }
    }

    // Merge thread-local counts
    auto& merged = tl_counts[0];
    for (int t = 1; t < nthreads; t++) {
        for (auto& [sk, cnt] : tl_counts[t]) {
            merged[sk] += cnt;
        }
    }

    // Build results
    for (auto& [sk, cnt] : merged) {
        int32_t srow = sk - 1;
        q21_results.push_back({v_s_name.get(srow), cnt});
    }

    // ORDER BY numwait DESC, s_name LIMIT 100
    std::sort(q21_results.begin(), q21_results.end(), [](const Q21Result& a, const Q21Result& b) {
        if (a.numwait != b.numwait) return a.numwait > b.numwait;
        return a.s_name < b.s_name;
    });
    if (q21_results.size() > 100) q21_results.resize(100);
}

// ===========================================================================
// STAGE: Finalize & output all 22 queries
// ===========================================================================

static void write_q1() {
    MQO_TIME_TAIL("Q1_finalize");
    if (!(g_active_queries & Q1B)) return;

    auto& merged = *g_lineitem_merged;
    // Collect non-empty groups
    struct Q1Out {
        char rf, ls;
        double sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc;
        int64_t count;
    };
    std::vector<Q1Out> rows;
    const char rfs[] = {'A', 'N', 'R'};
    const char lss[] = {'F', 'O'};
    for (int ri = 0; ri < 3; ri++) {
        for (int li = 0; li < 2; li++) {
            int k = ri * 2 + li;
            auto& g = merged.q1[k];
            if (g.count > 0) {
                rows.push_back({
                    rfs[ri], lss[li],
                    g.sum_qty, g.sum_base_price, g.sum_disc_price, g.sum_charge,
                    g.sum_qty / g.count, g.sum_base_price / g.count,
                    g.sum_disc / g.count, g.count
                });
            }
        }
    }
    // Already sorted by (returnflag, linestatus) due to iteration order

    std::string path = g_output_dir + "/Q1.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n");
    for (auto& r : rows) {
        fprintf(f, "%c,%c,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%.2f,%ld\n",
            r.rf, r.ls, r.sum_qty, r.sum_base_price, r.sum_disc_price, r.sum_charge,
            r.avg_qty, r.avg_price, r.avg_disc, r.count);
    }
    fclose(f);
}

// Helper: write a CSV string field, quoting if it contains comma or double-quote
static inline void write_csv_str(FILE* f, const std::string& s) {
    if (s.find(',') != std::string::npos || s.find('"') != std::string::npos) {
        fputc('"', f);
        for (char c : s) {
            if (c == '"') fputc('"', f);
            fputc(c, f);
        }
        fputc('"', f);
    } else {
        fwrite(s.data(), 1, s.size(), f);
    }
}

static void write_q2() {
    MQO_TIME_TAIL("Q2_finalize");
    if (!(g_active_queries & Q2B)) return;

    std::string path = g_output_dir + "/Q2.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment\n");
    for (auto& r : q2_results) {
        fprintf(f, "%.2f,%s,%s,%d,%s,",
            r.s_acctbal,
            r.s_name.c_str(),
            r.n_name.c_str(),
            r.p_partkey,
            r.p_mfgr.c_str());
        write_csv_str(f, r.s_address);
        fprintf(f, ",%s,", r.s_phone.c_str());
        write_csv_str(f, r.s_comment);
        fputc('\n', f);
    }
    fclose(f);
}

static void write_q3() {
    MQO_TIME_TAIL("Q3_finalize");
    if (!(g_active_queries & Q3B)) return;

    auto& merged = *g_lineitem_merged;
    struct Q3Out {
        int32_t orderkey;
        double revenue;
        int32_t orderdate;
        int32_t shippriority;
    };
    std::vector<Q3Out> rows;
    for (auto& [ok, rev] : merged.q3_revenue) {
        int32_t orow = orders_pk_index[ok];
        if (orow >= 0) {
            rows.push_back({ok, rev, o_orderdate[orow], o_shippriority[orow]});
        }
    }
    // ORDER BY revenue DESC, o_orderdate LIMIT 10
    auto q3cmp = [](const Q3Out& a, const Q3Out& b) {
        if (a.revenue != b.revenue) return a.revenue > b.revenue;
        return a.orderdate < b.orderdate;
    };
    if (rows.size() > 10) {
        std::partial_sort(rows.begin(), rows.begin() + 10, rows.end(), q3cmp);
        rows.resize(10);
    } else {
        std::sort(rows.begin(), rows.end(), q3cmp);
    }

    std::string path = g_output_dir + "/Q3.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "l_orderkey,revenue,o_orderdate,o_shippriority\n");
    for (auto& r : rows) {
        char datebuf[16];
        format_date(r.orderdate, datebuf);
        fprintf(f, "%d,%.2f,%s,%d\n", r.orderkey, r.revenue, datebuf, r.shippriority);
    }
    fclose(f);
}

static void write_q4() {
    MQO_TIME_TAIL("Q4_finalize");
    if (!(g_active_queries & Q4B)) return;

    // ORDER BY o_orderpriority
    std::vector<std::pair<std::string, int64_t>> rows(q4_results.begin(), q4_results.end());
    std::sort(rows.begin(), rows.end());

    std::string path = g_output_dir + "/Q4.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "o_orderpriority,order_count\n");
    for (auto& [prio, cnt] : rows) {
        fprintf(f, "%s,%ld\n", prio.c_str(), cnt);
    }
    fclose(f);
}

static void write_q5() {
    MQO_TIME_TAIL("Q5_finalize");
    if (!(g_active_queries & Q5B)) return;

    auto& merged = *g_lineitem_merged;
    struct Q5Out {
        std::string n_name;
        double revenue;
    };
    std::vector<Q5Out> rows;
    for (int n = 0; n < 25; n++) {
        if (merged.q5_revenue[n] > 0.0 && is_asian_nation[n]) {
            rows.push_back({nation_names[n], merged.q5_revenue[n]});
        }
    }
    // ORDER BY revenue DESC
    std::sort(rows.begin(), rows.end(), [](const Q5Out& a, const Q5Out& b) {
        return a.revenue > b.revenue;
    });

    std::string path = g_output_dir + "/Q5.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "n_name,revenue\n");
    for (auto& r : rows) {
        fprintf(f, "%s,%.2f\n", r.n_name.c_str(), r.revenue);
    }
    fclose(f);
}

static void write_q6() {
    MQO_TIME_TAIL("Q6_finalize");
    if (!(g_active_queries & Q6B)) return;

    std::string path = g_output_dir + "/Q6.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "revenue\n");
    fprintf(f, "%.2f\n", g_lineitem_merged->q6_revenue);
    fclose(f);
}

static void write_q7() {
    MQO_TIME_TAIL("Q7_finalize");
    if (!(g_active_queries & Q7B)) return;

    auto& merged = *g_lineitem_merged;
    // pair 0: FRANCE->GERMANY (supp=FRANCE, cust=GERMANY)
    // pair 1: GERMANY->FRANCE (supp=GERMANY, cust=FRANCE)
    struct Q7Out {
        std::string supp_nation;
        std::string cust_nation;
        int l_year;
        double revenue;
    };
    std::vector<Q7Out> rows;
    const char* pairs[2][2] = {{"FRANCE","GERMANY"},{"GERMANY","FRANCE"}};
    for (int p = 0; p < 2; p++) {
        for (int y = 0; y < 2; y++) {
            double rev = merged.q7_revenue[p * 2 + y];
            if (rev != 0.0) {
                rows.push_back({pairs[p][0], pairs[p][1], 1995 + y, rev});
            }
        }
    }
    // ORDER BY supp_nation, cust_nation, l_year
    std::sort(rows.begin(), rows.end(), [](const Q7Out& a, const Q7Out& b) {
        if (a.supp_nation != b.supp_nation) return a.supp_nation < b.supp_nation;
        if (a.cust_nation != b.cust_nation) return a.cust_nation < b.cust_nation;
        return a.l_year < b.l_year;
    });

    std::string path = g_output_dir + "/Q7.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "supp_nation,cust_nation,l_year,revenue\n");
    for (auto& r : rows) {
        fprintf(f, "%s,%s,%d,%.2f\n", r.supp_nation.c_str(), r.cust_nation.c_str(), r.l_year, r.revenue);
    }
    fclose(f);
}

static void write_q8() {
    MQO_TIME_TAIL("Q8_finalize");
    if (!(g_active_queries & Q8B)) return;

    auto& merged = *g_lineitem_merged;
    std::string path = g_output_dir + "/Q8.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "o_year,mkt_share\n");
    for (int y = 0; y < 2; y++) {
        double share = (merged.q8_total[y] > 0.0) ? merged.q8_brazil[y] / merged.q8_total[y] : 0.0;
        fprintf(f, "%d,%.8f\n", 1995 + y, share);
    }
    fclose(f);
}

static void write_q9() {
    MQO_TIME_TAIL("Q9_finalize");
    if (!(g_active_queries & Q9B)) return;

    auto& merged = *g_lineitem_merged;
    struct Q9Out {
        std::string nation;
        int o_year;
        double sum_profit;
    };
    std::vector<Q9Out> rows;
    for (auto& [key, profit] : merged.q9_profit) {
        int32_t nk = (int32_t)(key / 10000);
        int yr = (int)(key % 10000);
        rows.push_back({nation_names[nk], yr, profit});
    }
    // ORDER BY nation, o_year DESC
    std::sort(rows.begin(), rows.end(), [](const Q9Out& a, const Q9Out& b) {
        if (a.nation != b.nation) return a.nation < b.nation;
        return a.o_year > b.o_year;
    });

    std::string path = g_output_dir + "/Q9.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "nation,o_year,sum_profit\n");
    for (auto& r : rows) {
        fprintf(f, "%s,%d,%.2f\n", r.nation.c_str(), r.o_year, r.sum_profit);
    }
    fclose(f);
}

static void write_q10() {
    MQO_TIME_TAIL("Q10_finalize");
    if (!(g_active_queries & Q10B)) return;

    auto& merged = *g_lineitem_merged;

    // Phase 1: partial sort to find top-20 by revenue (avoid string lookups for all entries)
    struct Q10Light {
        int32_t custkey;
        double revenue;
    };
    std::vector<Q10Light> light;
    light.reserve(merged.q10_revenue.size());
    for (auto& [ck, rev] : merged.q10_revenue) {
        light.push_back({ck, rev});
    }
    if (light.size() > 20) {
        std::partial_sort(light.begin(), light.begin() + 20, light.end(),
            [](const Q10Light& a, const Q10Light& b) { return a.revenue > b.revenue; });
        light.resize(20);
    } else {
        std::sort(light.begin(), light.end(),
            [](const Q10Light& a, const Q10Light& b) { return a.revenue > b.revenue; });
    }

    // Phase 2: look up strings only for top-20
    std::string path = g_output_dir + "/Q10.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n");
    for (auto& r : light) {
        int32_t crow = r.custkey - 1;
        fprintf(f, "%d,%s,%.2f,%.2f,%s,",
            r.custkey,
            v_c_name.get(crow).c_str(),
            r.revenue,
            c_acctbal[crow],
            nation_names[c_nationkey[crow]].c_str());
        write_csv_str(f, v_c_address.get(crow));
        fprintf(f, ",%s,", v_c_phone.get(crow).c_str());
        write_csv_str(f, v_c_comment.get(crow));
        fputc('\n', f);
    }
    fclose(f);
}

static void write_q11() {
    MQO_TIME_TAIL("Q11_finalize");
    if (!(g_active_queries & Q11B)) return;

    std::string path = g_output_dir + "/Q11.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "ps_partkey,value\n");
    for (auto& r : q11_results) {
        fprintf(f, "%d,%.2f\n", r.ps_partkey, r.value);
    }
    fclose(f);
}

static void write_q12() {
    MQO_TIME_TAIL("Q12_finalize");
    if (!(g_active_queries & Q12B)) return;

    auto& merged = *g_lineitem_merged;
    // shipmode order: MAIL < SHIP alphabetically
    struct Q12Out {
        std::string shipmode;
        int64_t high_count;
        int64_t low_count;
    };

    // mode_idx 0 = MAIL, mode_idx 1 = SHIP
    // For each: high = counts[idx*2+0], low = counts[idx*2+1]
    std::vector<Q12Out> rows;

    std::string mail_name = dict_l_shipmode.entries[q12_mail_code];
    std::string ship_name = dict_l_shipmode.entries[q12_ship_code];

    // Build rows in sorted order
    std::vector<std::pair<std::string, int>> modes = {{mail_name, 0}, {ship_name, 1}};
    std::sort(modes.begin(), modes.end());

    for (auto& [name, idx] : modes) {
        rows.push_back({name, merged.q12_counts[idx * 2 + 0], merged.q12_counts[idx * 2 + 1]});
    }

    std::string path = g_output_dir + "/Q12.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "l_shipmode,high_line_count,low_line_count\n");
    for (auto& r : rows) {
        fprintf(f, "%s,%ld,%ld\n", r.shipmode.c_str(), r.high_count, r.low_count);
    }
    fclose(f);
}

static void write_q13() {
    MQO_TIME_TAIL("Q13_finalize");
    if (!(g_active_queries & Q13B)) return;

    // Build histogram: c_count -> custdist
    std::unordered_map<int32_t, int64_t> histogram;
    for (int64_t ck = 1; ck <= CUSTOMER_ROWS; ck++) {
        histogram[q13_order_counts[ck]]++;
    }

    struct Q13Out {
        int32_t c_count;
        int64_t custdist;
    };
    std::vector<Q13Out> rows;
    for (auto& [cnt, dist] : histogram) {
        rows.push_back({cnt, dist});
    }

    // ORDER BY custdist DESC, c_count DESC
    std::sort(rows.begin(), rows.end(), [](const Q13Out& a, const Q13Out& b) {
        if (a.custdist != b.custdist) return a.custdist > b.custdist;
        return a.c_count > b.c_count;
    });

    std::string path = g_output_dir + "/Q13.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "c_count,custdist\n");
    for (auto& r : rows) {
        fprintf(f, "%d,%ld\n", r.c_count, r.custdist);
    }
    fclose(f);
}

static void write_q14() {
    MQO_TIME_TAIL("Q14_finalize");
    if (!(g_active_queries & Q14B)) return;

    auto& merged = *g_lineitem_merged;
    double promo_rev = (merged.q14_total > 0.0) ? 100.0 * merged.q14_promo / merged.q14_total : 0.0;

    std::string path = g_output_dir + "/Q14.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "promo_revenue\n");
    fprintf(f, "%.2f\n", promo_rev);
    fclose(f);
}

static void write_q15() {
    MQO_TIME_TAIL("Q15_finalize");
    if (!(g_active_queries & Q15B)) return;

    // Use merged thread-local Q15 revenue
    auto& merged = *g_lineitem_merged;

    // Find max revenue among all suppliers
    double max_rev = 0.0;
    for (int64_t sk = 1; sk <= SUPPLIER_ROWS; sk++) {
        double rev = merged.q15_rev[sk];
        if (rev > max_rev) max_rev = rev;
    }

    // Collect all suppliers with max revenue
    struct Q15Out {
        int32_t suppkey;
        std::string s_name;
        std::string s_address;
        std::string s_phone;
        double total_revenue;
    };
    std::vector<Q15Out> rows;
    for (int64_t sk = 1; sk <= SUPPLIER_ROWS; sk++) {
        double rev = merged.q15_rev[sk];
        if (rev == max_rev && rev > 0.0) {
            int32_t srow = (int32_t)(sk - 1);
            rows.push_back({(int32_t)sk, v_s_name.get(srow), v_s_address.get(srow),
                            v_s_phone.get(srow), rev});
        }
    }

    // ORDER BY s_suppkey
    std::sort(rows.begin(), rows.end(), [](const Q15Out& a, const Q15Out& b) {
        return a.suppkey < b.suppkey;
    });

    std::string path = g_output_dir + "/Q15.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "s_suppkey,s_name,s_address,s_phone,total_revenue\n");
    for (auto& r : rows) {
        fprintf(f, "%d,%s,", r.suppkey, r.s_name.c_str());
        write_csv_str(f, r.s_address);
        fprintf(f, ",%s,%.2f\n", r.s_phone.c_str(), r.total_revenue);
    }
    fclose(f);
}

static void write_q16() {
    MQO_TIME_TAIL("Q16_finalize");
    if (!(g_active_queries & Q16B)) return;

    std::string path = g_output_dir + "/Q16.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "p_brand,p_type,p_size,supplier_cnt\n");
    for (auto& r : q16_results) {
        fprintf(f, "%s,%s,%d,%d\n",
            r.p_brand.c_str(),
            r.p_type.c_str(),
            r.p_size, r.supplier_cnt);
    }
    fclose(f);
}

static void write_q17() {
    MQO_TIME_TAIL("Q17_finalize");
    if (!(g_active_queries & Q17B)) return;

    std::string path = g_output_dir + "/Q17.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "avg_yearly\n");
    fprintf(f, "%.2f\n", q17_avg_yearly);
    fclose(f);
}

static void write_q18() {
    MQO_TIME_TAIL("Q18_finalize");
    if (!(g_active_queries & Q18B)) return;

    std::string path = g_output_dir + "/Q18.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "c_name,c_custkey,o_orderkey,o_orderdate,o_totalprice,sum_qty\n");
    for (auto& r : q18_results) {
        char datebuf[16];
        format_date(r.o_orderdate, datebuf);
        fprintf(f, "%s,%d,%d,%s,%.2f,%.2f\n",
            r.c_name.c_str(),
            r.c_custkey, r.o_orderkey, datebuf,
            r.o_totalprice, r.sum_qty);
    }
    fclose(f);
}

static void write_q19() {
    MQO_TIME_TAIL("Q19_finalize");
    if (!(g_active_queries & Q19B)) return;

    std::string path = g_output_dir + "/Q19.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "revenue\n");
    fprintf(f, "%.2f\n", g_lineitem_merged->q19_revenue);
    fclose(f);
}

static void write_q20() {
    MQO_TIME_TAIL("Q20_finalize");
    if (!(g_active_queries & Q20B)) return;

    std::string path = g_output_dir + "/Q20.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "s_name,s_address\n");
    for (auto& r : q20_results) {
        fprintf(f, "%s,", r.s_name.c_str());
        write_csv_str(f, r.s_address);
        fputc('\n', f);
    }
    fclose(f);
}

static void write_q21() {
    MQO_TIME_TAIL("Q21_finalize_output");
    if (!(g_active_queries & Q21B)) return;

    std::string path = g_output_dir + "/Q21.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "s_name,numwait\n");
    for (auto& r : q21_results) {
        fprintf(f, "%s,%ld\n", r.s_name.c_str(), r.numwait);
    }
    fclose(f);
}

static void write_q22() {
    MQO_TIME_TAIL("Q22_finalize");
    if (!(g_active_queries & Q22B)) return;

    std::string path = g_output_dir + "/Q22.csv";
    FILE* f = fopen(path.c_str(), "w");
    fprintf(f, "cntrycode,numcust,totacctbal\n");
    // q22_results is already std::map<string,Q22Group>, sorted by key
    for (auto& [code, grp] : q22_results) {
        fprintf(f, "%s,%ld,%.2f\n", code.c_str(), grp.numcust, grp.totacctbal);
    }
    fclose(f);
}

static void finalize_output() {
    MQO_TIME_SHARED("finalize_output");

    write_q1();
    write_q2();
    write_q3();
    write_q4();
    write_q5();
    write_q6();
    write_q7();
    write_q8();
    write_q9();
    write_q10();
    write_q11();
    write_q12();
    write_q13();
    write_q14();
    write_q15();
    write_q16();
    write_q17();
    write_q18();
    write_q19();
    write_q20();
    write_q21();
    write_q22();
}

// ===========================================================================
// CLI parsing
// ===========================================================================
static void print_usage() {
    printf("Usage: mqo --gendb-dir <path> --output-dir <dir> [--all | --query Q1 ...]\n");
    printf("       mqo --list\n");
}

static uint32_t parse_query_name(const char* name) {
    for (int i = 0; i < 22; i++) {
        if (strcasecmp(name, QUERY_NAMES[i]) == 0) return 1u << i;
    }
    // Try just number
    int n = atoi(name);
    if (n >= 1 && n <= 22) return 1u << (n - 1);
    fprintf(stderr, "Unknown query: %s\n", name);
    return 0;
}

int main(int argc, char** argv) {
    std::string gendb_dir;
    std::string output_dir;
    uint32_t queries = 0;
    bool list_mode = false;

    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--gendb-dir") == 0 && i + 1 < argc) {
            gendb_dir = argv[++i];
        } else if (strcmp(argv[i], "--output-dir") == 0 && i + 1 < argc) {
            output_dir = argv[++i];
        } else if (strcmp(argv[i], "--all") == 0) {
            queries = ALL_Q;
        } else if (strcmp(argv[i], "--query") == 0 && i + 1 < argc) {
            queries |= parse_query_name(argv[++i]);
        } else if (strcmp(argv[i], "--list") == 0) {
            list_mode = true;
        } else {
            fprintf(stderr, "Unknown argument: %s\n", argv[i]);
            print_usage();
            return 1;
        }
    }

    if (list_mode) {
        printf("Available queries:\n");
        for (int i = 0; i < 22; i++) printf("  %s\n", QUERY_NAMES[i]);
        return 0;
    }

    if (gendb_dir.empty() || output_dir.empty() || queries == 0) {
        print_usage();
        return 1;
    }

    g_gendb_dir = gendb_dir;
    g_storage_dir = gendb_dir + "/storage";
    g_index_dir = g_storage_dir + "/indexes";
    g_output_dir = output_dir;
    g_active_queries = queries;

    // Create output directory
    {
        // Create output directory via POSIX mkdir -p
        auto mkdirp = [](const std::string& path) {
            std::string cur;
            for (size_t i = 0; i < path.size(); i++) {
                cur += path[i];
                if (path[i] == '/' || i == path.size() - 1) {
                    mkdir(cur.c_str(), 0755);
                }
            }
        };
        mkdirp(output_dir);
    }

    printf("[MQO] Active queries: 0x%08x (%d queries)\n", queries, __builtin_popcount(queries));
    printf("[MQO] gendb-dir: %s\n", gendb_dir.c_str());
    printf("[MQO] output-dir: %s\n", output_dir.c_str());
    printf("[MQO] threads: %d\n", omp_get_max_threads());

    {
        MQO_TIME_DISPATCHER("dispatcher_total");

        // Stage 1: Load data
        load_all_data();
        load_extra_indexes();

        // Stage 2: Precompute
        precompute();

        // Stage 3: Fused lineitem scan
        if (queries & (Q1B|Q3B|Q4B|Q5B|Q6B|Q7B|Q8B|Q9B|Q10B|Q12B|Q14B|Q15B|Q19B|Q21B)) {
            fused_scan_lineitem();
        }

        // Stage 4: Fused orders scan (Q4, Q13)
        if (queries & (Q4B|Q13B)) {
            fused_scan_orders();
        }

        // Stage 5: European partsupp (Q2, Q11)
        if (queries & (Q2B|Q11B)) {
            scan_european_partsupp();
        }

        // Stage 6: Fused partsupp scan (Q16, Q20 prep)
        if (queries & (Q16B|Q20B)) {
            fused_scan_partsupp();
        }

        // Stage 7: Q22 customer scan
        if (queries & Q22B) {
            scan_customer_q22();
        }

        // Stage 8: Index computations (Q17, Q18, Q20, Q21)
        if (queries & (Q17B|Q18B|Q20B)) {
            index_computations();
        }

        // Q21 finalize (needs lineitem_orderkey_idx)
        if (queries & Q21B) {
            finalize_q21();
        }

        // Stage 9: Write outputs
        finalize_output();
    }

    // Flush profile
    std::string profile_path = g_output_dir + "/profile.json";
    MQO_PROFILE_FLUSH(profile_path);

    printf("[MQO] Done.\n");
    return 0;
}
