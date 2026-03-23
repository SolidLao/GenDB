// Q10: Returned Item Reporting — TPC-H
// No hash maps: per-thread (custkey, revenue) vectors → flat array merge → top-K
#include <cstdio>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

struct MMapFile {
    void* ptr = nullptr;
    size_t len = 0;
    int fd = -1;
    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "cannot open %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st); len = st.st_size;
        if (len == 0) { ptr = nullptr; return; }
        ptr = mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path.c_str()); exit(1); }
    }
    void advise(int adv) { if (ptr && len) madvise(ptr, len, adv); }
    ~MMapFile() { if (ptr && len) munmap(ptr, len); if (fd >= 0) ::close(fd); }
    template<typename T> const T* as() const { return static_cast<const T*>(ptr); }
};

static std::string read_varlen(const uint32_t* offsets, const char* data, uint32_t row) {
    uint32_t s = offsets[row], e = offsets[row + 1];
    return std::string(data + s, e - s);
}

int main(int argc, char* argv[]) {
    gendb::init_date_tables();
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    int32_t date_lo = gendb::parse_date_arg(argc, argv, "--o_orderdate_lower", gendb::date_str_to_epoch_days("1993-10-01"));
    int32_t date_hi_base = gendb::parse_date_arg(argc, argv, "--o_orderdate_upper", gendb::date_str_to_epoch_days("1993-10-01"));
    int32_t date_hi = gendb::add_months(date_hi_base, 3);
    int64_t limit_val = gendb::parse_int_arg(argc, argv, "--limit", 20);
    std::string rflag_str = gendb::parse_string_arg(argc, argv, "--l_returnflag_eq", "R");
    int8_t returnflag_val = (int8_t)rflag_str[0];

    MMapFile nation_nkey_f, nation_name_off_f, nation_name_data_f;
    MMapFile orders_orderkey_f, orders_custkey_f, orders_orderdate_f;
    MMapFile li_returnflag_f, li_extprice_f, li_discount_f;
    MMapFile zonemap_f, li_grouped_f, cust_lookup_f;
    MMapFile cust_nationkey_f, cust_name_off_f, cust_name_data_f;
    MMapFile cust_acctbal_f, cust_addr_off_f, cust_addr_data_f;
    MMapFile cust_phone_off_f, cust_phone_data_f, cust_comment_off_f, cust_comment_data_f;

    {
    GENDB_PHASE("total");

    {
    GENDB_PHASE("data_loading");
    nation_nkey_f.open(gendb_dir + "/nation/n_nationkey.bin");
    nation_name_off_f.open(gendb_dir + "/nation/n_name_offsets.bin");
    nation_name_data_f.open(gendb_dir + "/nation/n_name_data.bin");
    orders_orderkey_f.open(gendb_dir + "/orders/o_orderkey.bin");
    orders_custkey_f.open(gendb_dir + "/orders/o_custkey.bin");
    orders_orderdate_f.open(gendb_dir + "/orders/o_orderdate.bin");
    orders_orderdate_f.advise(MADV_SEQUENTIAL);
    orders_orderkey_f.advise(MADV_SEQUENTIAL);
    orders_custkey_f.advise(MADV_SEQUENTIAL);
    li_returnflag_f.open(gendb_dir + "/lineitem/l_returnflag.bin");
    li_extprice_f.open(gendb_dir + "/lineitem/l_extendedprice.bin");
    li_discount_f.open(gendb_dir + "/lineitem/l_discount.bin");
    li_returnflag_f.advise(MADV_SEQUENTIAL);
    li_extprice_f.advise(MADV_SEQUENTIAL);
    li_discount_f.advise(MADV_SEQUENTIAL);
    zonemap_f.open(gendb_dir + "/indexes/orders_o_orderdate_zonemap.bin");
    li_grouped_f.open(gendb_dir + "/indexes/lineitem_l_orderkey_grouped.bin");
    li_grouped_f.advise(MADV_SEQUENTIAL);
    cust_lookup_f.open(gendb_dir + "/indexes/customer_c_custkey_lookup.bin");
    cust_nationkey_f.open(gendb_dir + "/customer/c_nationkey.bin");
    cust_name_off_f.open(gendb_dir + "/customer/c_name_offsets.bin");
    cust_name_data_f.open(gendb_dir + "/customer/c_name_data.bin");
    cust_acctbal_f.open(gendb_dir + "/customer/c_acctbal.bin");
    cust_addr_off_f.open(gendb_dir + "/customer/c_address_offsets.bin");
    cust_addr_data_f.open(gendb_dir + "/customer/c_address_data.bin");
    cust_phone_off_f.open(gendb_dir + "/customer/c_phone_offsets.bin");
    cust_phone_data_f.open(gendb_dir + "/customer/c_phone_data.bin");
    cust_comment_off_f.open(gendb_dir + "/customer/c_comment_offsets.bin");
    cust_comment_data_f.open(gendb_dir + "/customer/c_comment_data.bin");
    }

    // Nation lookup
    const int32_t* n_nk = nation_nkey_f.as<int32_t>();
    const uint32_t* n_no = nation_name_off_f.as<uint32_t>();
    const char* n_nd = nation_name_data_f.as<char>();
    size_t nn = nation_nkey_f.len / sizeof(int32_t);
    std::string nation_names[25];
    for (size_t i = 0; i < nn && i < 25; i++)
        nation_names[n_nk[i]] = read_varlen(n_no, n_nd, (uint32_t)i);

    const int32_t* o_ok = orders_orderkey_f.as<int32_t>();
    const int32_t* o_ck = orders_custkey_f.as<int32_t>();
    const int32_t* o_od = orders_orderdate_f.as<int32_t>();
    size_t n_orders = orders_orderkey_f.len / sizeof(int32_t);

    const uint8_t* zm_raw = zonemap_f.as<uint8_t>();
    uint64_t zm_nb = *reinterpret_cast<const uint64_t*>(zm_raw);
    uint32_t zm_bs = *reinterpret_cast<const uint32_t*>(zm_raw + 8);
    const int32_t* zm_mm = reinterpret_cast<const int32_t*>(zm_raw + 12);

    const uint8_t* li_raw = li_grouped_f.as<uint8_t>();
    uint64_t li_n = *reinterpret_cast<const uint64_t*>(li_raw);
    const uint32_t* li_idx = reinterpret_cast<const uint32_t*>(li_raw + 8);

    const int8_t* l_rf = li_returnflag_f.as<int8_t>();
    const double* l_ep = li_extprice_f.as<double>();
    const double* l_dc = li_discount_f.as<double>();

    const uint8_t* cl_raw = cust_lookup_f.as<uint8_t>();
    uint64_t cl_n = *reinterpret_cast<const uint64_t*>(cl_raw);
    const int32_t* cl_d = reinterpret_cast<const int32_t*>(cl_raw + 8);

    // Qualifying blocks
    std::vector<uint32_t> qb;
    qb.reserve(zm_nb);
    for (uint64_t b = 0; b < zm_nb; b++) {
        if (zm_mm[b*2+1] < date_lo || zm_mm[b*2] >= date_hi) continue;
        qb.push_back((uint32_t)b);
    }

    // Two-pass scatter for qualifying orders
    struct OP { int32_t ok, ck; };
    OP* qual = nullptr;
    size_t nqual = 0;

    {
    GENDB_PHASE("orders_scan");
    std::vector<size_t> bc(qb.size(), 0);
    #pragma omp parallel for schedule(static)
    for (size_t qi = 0; qi < qb.size(); qi++) {
        size_t s = (size_t)qb[qi] * zm_bs, e = std::min(s + (size_t)zm_bs, n_orders);
        size_t c = 0;
        for (size_t i = s; i < e; i++) c += (o_od[i] >= date_lo && o_od[i] < date_hi);
        bc[qi] = c;
    }
    std::vector<size_t> bo(qb.size() + 1, 0);
    for (size_t i = 0; i < qb.size(); i++) bo[i+1] = bo[i] + bc[i];
    nqual = bo.back();
    qual = (OP*)malloc(nqual * sizeof(OP));
    #pragma omp parallel for schedule(static)
    for (size_t qi = 0; qi < qb.size(); qi++) {
        size_t s = (size_t)qb[qi] * zm_bs, e = std::min(s + (size_t)zm_bs, n_orders);
        size_t off = bo[qi];
        for (size_t i = s; i < e; i++) {
            if (o_od[i] >= date_lo && o_od[i] < date_hi) {
                qual[off].ok = o_ok[i]; qual[off].ck = o_ck[i]; off++;
            }
        }
    }
    }

    // Lineitem probe + per-thread (custkey, revenue) vectors
    struct KV { int32_t ck; double rev; };
    int nthreads = std::min(omp_get_max_threads(), 32);
    std::vector<std::vector<KV>> tvecs(nthreads);

    {
    GENDB_PHASE("main_scan");
    #pragma omp parallel num_threads(nthreads)
    {
        int tid = omp_get_thread_num();
        auto& vec = tvecs[tid];
        vec.reserve(nqual / nthreads + 1000);

        #pragma omp for schedule(static)
        for (size_t qi = 0; qi < nqual; qi++) {
            int32_t okey = qual[qi].ok, ckey = qual[qi].ck;
            if (__builtin_expect((uint64_t)okey >= li_n, 0)) continue;
            uint32_t ls = li_idx[(uint64_t)okey * 2], lc = li_idx[(uint64_t)okey * 2 + 1];
            double rev = 0.0;
            for (uint32_t j = 0; j < lc; j++) {
                uint32_t p = ls + j;
                if (l_rf[p] == returnflag_val) rev += l_ep[p] * (1.0 - l_dc[p]);
            }
            if (rev != 0.0) vec.push_back({ckey, rev});
        }
    }
    }
    free(qual);

    // Merge into flat array + top-K
    struct CR { int32_t ck; double rev; };
    std::vector<CR> topk;

    {
    GENDB_PHASE("aggregation");

    size_t agg_sz = cl_n;
    double* merged = (double*)mmap(nullptr, agg_sz * sizeof(double),
                                    PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
    std::vector<int32_t> ukeys;
    ukeys.reserve(500000);

    for (int t = 0; t < nthreads; t++) {
        for (auto& kv : tvecs[t]) {
            uint32_t ck = (uint32_t)kv.ck;
            if (ck < agg_sz) {
                if (merged[ck] == 0.0) ukeys.push_back(kv.ck);
                merged[ck] += kv.rev;
            }
        }
        tvecs[t].clear();
        tvecs[t].shrink_to_fit();
    }

    size_t k = (size_t)limit_val;
    topk.reserve(k);
    for (int32_t ck : ukeys) {
        double val = merged[(uint32_t)ck];
        if (val <= 0.0) continue;
        CR cr{ck, val};
        if (topk.size() < k) {
            topk.push_back(cr);
            if (topk.size() == k)
                std::make_heap(topk.begin(), topk.end(), [](auto& a, auto& b){ return a.rev != b.rev ? a.rev > b.rev : a.ck < b.ck; });
        } else if (cr.rev > topk[0].rev || (cr.rev == topk[0].rev && cr.ck < topk[0].ck)) {
            std::pop_heap(topk.begin(), topk.end(), [](auto& a, auto& b){ return a.rev != b.rev ? a.rev > b.rev : a.ck < b.ck; });
            topk.back() = cr;
            std::push_heap(topk.begin(), topk.end(), [](auto& a, auto& b){ return a.rev != b.rev ? a.rev > b.rev : a.ck < b.ck; });
        }
    }
    munmap(merged, agg_sz * sizeof(double));
    std::sort(topk.begin(), topk.end(), [](auto& a, auto& b){ return a.rev != b.rev ? a.rev > b.rev : a.ck < b.ck; });
    }

    // Late materialize + output
    {
    GENDB_PHASE("output");
    const int32_t* c_nk = cust_nationkey_f.as<int32_t>();
    const uint32_t* cn_o = cust_name_off_f.as<uint32_t>();
    const char* cn_d = cust_name_data_f.as<char>();
    const double* c_ab = cust_acctbal_f.as<double>();
    const uint32_t* ca_o = cust_addr_off_f.as<uint32_t>();
    const char* ca_d = cust_addr_data_f.as<char>();
    const uint32_t* cp_o = cust_phone_off_f.as<uint32_t>();
    const char* cp_d = cust_phone_data_f.as<char>();
    const uint32_t* cc_o = cust_comment_off_f.as<uint32_t>();
    const char* cc_d = cust_comment_data_f.as<char>();

    FILE* fout = fopen((results_dir + "/Q10.csv").c_str(), "w");
    fprintf(fout, "c_custkey,c_name,revenue,c_acctbal,n_name,c_address,c_phone,c_comment\n");

    auto mq = [](const std::string& s) -> std::string {
        std::string q = "\"";
        for (char c : s) { if (c == '"') q += "\"\""; else q += c; }
        return q + "\"";
    };

    for (auto& cr : topk) {
        int32_t rid = ((uint64_t)cr.ck < cl_n) ? cl_d[cr.ck] : -1;
        if (rid < 0) continue;
        uint32_t r = (uint32_t)rid;
        int32_t nk = c_nk[r];
        fprintf(fout, "%d,%s,%.4f,%.2f,%s,%s,%s,%s\n",
            cr.ck, mq(read_varlen(cn_o, cn_d, r)).c_str(), cr.rev, c_ab[r],
            mq((nk>=0&&nk<25)?nation_names[nk]:"").c_str(),
            mq(read_varlen(ca_o, ca_d, r)).c_str(),
            mq(read_varlen(cp_o, cp_d, r)).c_str(),
            mq(read_varlen(cc_o, cc_d, r)).c_str());
    }
    fclose(fout);
    }

    } // total
    return 0;
}
