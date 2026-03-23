#include <cstdio>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>
#include <cmath>
#include <climits>
#include <cfloat>

#include "timing_utils.h"
#include "cli_params.h"

// ---- mmap helper ----
struct MappedFile {
    void* data = nullptr;
    size_t size = 0;
    int fd = -1;

    MappedFile() = default;
    MappedFile(const std::string& path, int advice = MADV_SEQUENTIAL) {
        fd = open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::fprintf(stderr, "Cannot open %s\n", path.c_str()); return; }
        struct stat st;
        fstat(fd, &st);
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) { data = nullptr; close(fd); fd = -1; return; }
        madvise(data, size, advice);
    }
    ~MappedFile() {
        if (data && data != MAP_FAILED) munmap(data, size);
        if (fd >= 0) close(fd);
    }
    MappedFile(const MappedFile&) = delete;
    MappedFile& operator=(const MappedFile&) = delete;
    MappedFile(MappedFile&& o) noexcept : data(o.data), size(o.size), fd(o.fd) {
        o.data = nullptr; o.size = 0; o.fd = -1;
    }
    MappedFile& operator=(MappedFile&& o) noexcept {
        if (this != &o) {
            if (data && data != MAP_FAILED) munmap(data, size);
            if (fd >= 0) close(fd);
            data = o.data; size = o.size; fd = o.fd;
            o.data = nullptr; o.size = 0; o.fd = -1;
        }
        return *this;
    }
    template<typename T> const T* as() const { return reinterpret_cast<const T*>(data); }
};

// ---- varlen string reader ----
struct VarlenColumn {
    const uint32_t* offsets;
    const char* str_data;

    std::string get(int32_t row) const {
        uint32_t start = offsets[row];
        uint32_t end = offsets[row + 1];
        return std::string(str_data + start, end - start);
    }

    const char* ptr(int32_t row) const { return str_data + offsets[row]; }
    uint32_t len(int32_t row) const { return offsets[row + 1] - offsets[row]; }
};

// ---- dict reader ----
struct DictEntry {
    std::string value;
};

std::vector<DictEntry> load_dict(const std::string& path) {
    MappedFile f(path);
    const uint8_t* p = (const uint8_t*)f.data;
    uint32_t count = *(const uint32_t*)p;
    p += 4;
    std::vector<DictEntry> entries(count);
    for (uint32_t i = 0; i < count; i++) {
        uint16_t len = *(const uint16_t*)p;
        p += 2;
        entries[i].value = std::string((const char*)p, len);
        p += len;
    }
    return entries;
}

// Result tuple for final output
struct ResultTuple {
    double s_acctbal;
    int32_t p_partkey;
    int32_t supp_rowid;
    int32_t nation_rowid; // actually nationkey
    int32_t part_rowid;
};

static void write_csv_field(FILE* fp, const char* s, size_t len) {
    // Check if quoting needed
    bool need_quote = false;
    for (size_t i = 0; i < len; i++) {
        if (s[i] == ',' || s[i] == '"' || s[i] == '\n') { need_quote = true; break; }
    }
    if (need_quote) {
        fputc('"', fp);
        for (size_t i = 0; i < len; i++) {
            if (s[i] == '"') fputc('"', fp);
            fputc(s[i], fp);
        }
        fputc('"', fp);
    } else {
        fwrite(s, 1, len, fp);
    }
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    // Parse CLI params
    int64_t LIMIT = gendb::parse_int_arg(argc, argv, "--limit", 100);
    std::string r_name_eq = gendb::parse_string_arg(argc, argv, "--r_name_eq", "EUROPE");
    std::string p_type_pattern = gendb::parse_string_arg(argc, argv, "--p_type_pattern", "%BRASS");
    int32_t p_size_eq = (int32_t)gendb::parse_int_arg(argc, argv, "--p_size_eq", 15);
    // r_name_eq_2 same as r_name_eq for this query

    // Extract suffix from pattern (strip leading %)
    std::string type_suffix = p_type_pattern;
    if (!type_suffix.empty() && type_suffix[0] == '%') type_suffix = type_suffix.substr(1);

    // Mmap all files BEFORE timing scope to exclude munmap TLB shootdown
    // Region
    MappedFile region_rkey(gendb_dir + "/region/r_regionkey.bin");
    MappedFile region_name_off(gendb_dir + "/region/r_name_offsets.bin");
    MappedFile region_name_data(gendb_dir + "/region/r_name_data.bin");
    // Nation
    MappedFile nation_nkey(gendb_dir + "/nation/n_nationkey.bin");
    MappedFile nation_rkey(gendb_dir + "/nation/n_regionkey.bin");
    MappedFile nation_name_off(gendb_dir + "/nation/n_name_offsets.bin");
    MappedFile nation_name_data(gendb_dir + "/nation/n_name_data.bin");
    // Supplier
    MappedFile supp_skey(gendb_dir + "/supplier/s_suppkey.bin", MADV_SEQUENTIAL);
    MappedFile supp_nkey(gendb_dir + "/supplier/s_nationkey.bin", MADV_SEQUENTIAL);
    MappedFile supp_acctbal(gendb_dir + "/supplier/s_acctbal.bin", MADV_RANDOM);
    MappedFile supp_name_off(gendb_dir + "/supplier/s_name_offsets.bin", MADV_RANDOM);
    MappedFile supp_name_data(gendb_dir + "/supplier/s_name_data.bin", MADV_RANDOM);
    MappedFile supp_addr_off(gendb_dir + "/supplier/s_address_offsets.bin", MADV_RANDOM);
    MappedFile supp_addr_data(gendb_dir + "/supplier/s_address_data.bin", MADV_RANDOM);
    MappedFile supp_phone_off(gendb_dir + "/supplier/s_phone_offsets.bin", MADV_RANDOM);
    MappedFile supp_phone_data(gendb_dir + "/supplier/s_phone_data.bin", MADV_RANDOM);
    MappedFile supp_comment_off(gendb_dir + "/supplier/s_comment_offsets.bin", MADV_RANDOM);
    MappedFile supp_comment_data(gendb_dir + "/supplier/s_comment_data.bin", MADV_RANDOM);
    // Part
    MappedFile part_pkey(gendb_dir + "/part/p_partkey.bin", MADV_SEQUENTIAL);
    MappedFile part_size(gendb_dir + "/part/p_size.bin", MADV_SEQUENTIAL);
    MappedFile part_type(gendb_dir + "/part/p_type.bin", MADV_SEQUENTIAL);
    MappedFile part_mfgr(gendb_dir + "/part/p_mfgr.bin", MADV_RANDOM);
    // Partsupp
    MappedFile ps_partkey(gendb_dir + "/partsupp/ps_partkey.bin", MADV_RANDOM);
    MappedFile ps_suppkey(gendb_dir + "/partsupp/ps_suppkey.bin", MADV_RANDOM);
    MappedFile ps_supplycost(gendb_dir + "/partsupp/ps_supplycost.bin", MADV_RANDOM);
    // Indexes
    MappedFile idx_ps_grouped(gendb_dir + "/indexes/partsupp_ps_partkey_grouped.bin", MADV_RANDOM);
    MappedFile idx_supp_lookup(gendb_dir + "/indexes/supplier_s_suppkey_lookup.bin", MADV_RANDOM);
    // Dicts
    auto p_type_dict = load_dict(gendb_dir + "/part/p_type_dict.bin");
    auto p_mfgr_dict = load_dict(gendb_dir + "/part/p_mfgr_dict.bin");

    {
    GENDB_PHASE("total");

    // ---- Data setup ----
    const int32_t* r_regionkey = region_rkey.as<int32_t>();
    const uint32_t* r_name_offsets = region_name_off.as<uint32_t>();
    const char* r_name_data_ptr = region_name_data.as<char>();

    const int32_t* n_nationkey = nation_nkey.as<int32_t>();
    const int32_t* n_regionkey = nation_rkey.as<int32_t>();
    const uint32_t* n_name_offsets = nation_name_off.as<uint32_t>();
    const char* n_name_data_ptr = nation_name_data.as<char>();

    const int32_t* s_suppkey = supp_skey.as<int32_t>();
    const int32_t* s_nationkey = supp_nkey.as<int32_t>();
    const double* s_acctbal = supp_acctbal.as<double>();
    VarlenColumn s_name_col{supp_name_off.as<uint32_t>(), supp_name_data.as<char>()};
    VarlenColumn s_addr_col{supp_addr_off.as<uint32_t>(), supp_addr_data.as<char>()};
    VarlenColumn s_phone_col{supp_phone_off.as<uint32_t>(), supp_phone_data.as<char>()};
    VarlenColumn s_comment_col{supp_comment_off.as<uint32_t>(), supp_comment_data.as<char>()};

    const int32_t* p_partkey = part_pkey.as<int32_t>();
    const int32_t* p_size = part_size.as<int32_t>();
    const int16_t* p_type_codes = part_type.as<int16_t>();
    const int8_t* p_mfgr_codes = part_mfgr.as<int8_t>();

    const int32_t* ps_partkey_col = ps_partkey.as<int32_t>();
    const int32_t* ps_suppkey_col = ps_suppkey.as<int32_t>();
    const double* ps_supplycost_col = ps_supplycost.as<double>();

    // Grouped index: header = uint64_t num_entries, body = uint32_t[num_entries*2]
    uint64_t idx_ps_num = *(const uint64_t*)idx_ps_grouped.data;
    const uint32_t* idx_ps_body = (const uint32_t*)((const uint8_t*)idx_ps_grouped.data + 8);

    // Supplier lookup index
    uint64_t idx_supp_num = *(const uint64_t*)idx_supp_lookup.data;
    const int32_t* supp_lookup = (const int32_t*)((const uint8_t*)idx_supp_lookup.data + 8);

    const int32_t NUM_PARTS = (int32_t)(part_pkey.size / sizeof(int32_t));
    const int32_t NUM_SUPPLIERS = (int32_t)(supp_skey.size / sizeof(int32_t));

    // ---- Step 1: Filter region/nation ----
    int32_t europe_rkey = -1;
    {
        GENDB_PHASE("filter_region_nation");
        // Find EUROPE regionkey
        for (int i = 0; i < 5; i++) {
            uint32_t start = r_name_offsets[i];
            uint32_t end = r_name_offsets[i + 1];
            if ((end - start) == r_name_eq.size() &&
                memcmp(r_name_data_ptr + start, r_name_eq.c_str(), r_name_eq.size()) == 0) {
                europe_rkey = r_regionkey[i];
                break;
            }
        }
    }

    // Collect europe nation keys (bitset of 25)
    bool europe_nation_set[25] = {};
    for (int i = 0; i < 25; i++) {
        if (n_regionkey[i] == europe_rkey) {
            europe_nation_set[n_nationkey[i]] = true;
        }
    }

    // ---- Step 2: Build europe supplier bitset ----
    // Bitset indexed by suppkey
    int32_t max_suppkey = 0;
    for (int32_t i = 0; i < NUM_SUPPLIERS; i++) {
        if (s_suppkey[i] > max_suppkey) max_suppkey = s_suppkey[i];
    }

    std::vector<bool> europe_supplier_bitset(max_suppkey + 1, false);
    {
        GENDB_PHASE("build_europe_supplier_bitset");
        for (int32_t i = 0; i < NUM_SUPPLIERS; i++) {
            int32_t nk = s_nationkey[i];
            if (nk >= 0 && nk < 25 && europe_nation_set[nk]) {
                europe_supplier_bitset[s_suppkey[i]] = true;
            }
        }
    }

    // ---- Step 3: Filter part ----
    // Build matching p_type codes (ends with suffix)
    std::vector<bool> matching_type_codes(p_type_dict.size(), false);
    for (size_t i = 0; i < p_type_dict.size(); i++) {
        const std::string& s = p_type_dict[i].value;
        if (s.size() >= type_suffix.size() &&
            s.compare(s.size() - type_suffix.size(), type_suffix.size(), type_suffix) == 0) {
            matching_type_codes[i] = true;
        }
    }

    std::vector<int32_t> qualifying_partkeys;
    std::vector<int32_t> qualifying_part_rowids;
    {
        GENDB_PHASE("filter_part");
        // Two-pass parallel scatter
        int nthreads = omp_get_max_threads();
        std::vector<int32_t> counts(nthreads, 0);

        // Pass 1: count per thread
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int32_t local_count = 0;
            #pragma omp for schedule(static) nowait
            for (int32_t i = 0; i < NUM_PARTS; i++) {
                if (p_size[i] == p_size_eq && matching_type_codes[p_type_codes[i]]) {
                    local_count++;
                }
            }
            counts[tid] = local_count;
        }

        // Prefix sum
        std::vector<int32_t> offsets(nthreads + 1, 0);
        for (int i = 0; i < nthreads; i++) offsets[i + 1] = offsets[i] + counts[i];
        int32_t total_qualifying = offsets[nthreads];

        qualifying_partkeys.resize(total_qualifying);
        qualifying_part_rowids.resize(total_qualifying);

        // Pass 2: scatter
        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            int32_t pos = offsets[tid];
            #pragma omp for schedule(static) nowait
            for (int32_t i = 0; i < NUM_PARTS; i++) {
                if (p_size[i] == p_size_eq && matching_type_codes[p_type_codes[i]]) {
                    qualifying_partkeys[pos] = p_partkey[i];
                    qualifying_part_rowids[pos] = i;
                    pos++;
                }
            }
        }
    }

    int32_t num_qualifying = (int32_t)qualifying_partkeys.size();

    // ---- Step 4: Index lookup partsupp, find min cost per qualifying part ----
    std::vector<double> per_part_min_cost(num_qualifying, DBL_MAX);
    std::vector<int32_t> per_part_best_suppkey(num_qualifying, -1);
    {
        GENDB_PHASE("index_lookup_partsupp_min_cost");
        #pragma omp parallel for schedule(dynamic, 256)
        for (int32_t qi = 0; qi < num_qualifying; qi++) {
            int32_t pk = qualifying_partkeys[qi];
            if ((uint64_t)pk >= idx_ps_num) continue;
            uint32_t start = idx_ps_body[pk * 2];
            uint32_t count = idx_ps_body[pk * 2 + 1];
            double min_cost = DBL_MAX;
            int32_t best_sk = -1;
            for (uint32_t j = start; j < start + count; j++) {
                int32_t sk = ps_suppkey_col[j];
                if (sk >= 0 && sk <= max_suppkey && europe_supplier_bitset[sk]) {
                    double cost = ps_supplycost_col[j];
                    if (cost < min_cost) {
                        min_cost = cost;
                        best_sk = sk;
                    }
                }
            }
            per_part_min_cost[qi] = min_cost;
            per_part_best_suppkey[qi] = best_sk;
        }
    }

    // ---- Step 5: Build result tuples ----
    std::vector<ResultTuple> results;
    {
        GENDB_PHASE("materialize_results");
        results.reserve(num_qualifying);
        for (int32_t qi = 0; qi < num_qualifying; qi++) {
            if (per_part_best_suppkey[qi] < 0) continue;
            int32_t sk = per_part_best_suppkey[qi];
            // Get supplier row_id from lookup index
            int32_t supp_rid = (sk < (int32_t)idx_supp_num) ? supp_lookup[sk] : -1;
            if (supp_rid < 0) continue;
            int32_t nk = s_nationkey[supp_rid];
            ResultTuple rt;
            rt.s_acctbal = s_acctbal[supp_rid];
            rt.p_partkey = qualifying_partkeys[qi];
            rt.supp_rowid = supp_rid;
            rt.nation_rowid = nk; // nationkey = row index for nation
            rt.part_rowid = qualifying_part_rowids[qi];
            results.push_back(rt);
        }
    }

    // ---- Step 6: Sort top-K ----
    {
        GENDB_PHASE("sort_topk");
        int32_t k = std::min((int32_t)LIMIT, (int32_t)results.size());

        // Pre-cache n_name strings for sorting
        // nation names are tiny (25 entries)
        std::vector<std::string> nation_names(25);
        for (int i = 0; i < 25; i++) {
            uint32_t s = n_name_offsets[i];
            uint32_t e = n_name_offsets[i + 1];
            nation_names[i] = std::string(n_name_data_ptr + s, e - s);
        }

        // Pre-cache s_name for sorting (only for result set, ~8K)
        // Actually we need s_name for ordering. Let's cache for all results.
        std::vector<std::string> result_s_names(results.size());
        std::vector<std::string> result_n_names(results.size());
        for (size_t i = 0; i < results.size(); i++) {
            result_s_names[i] = s_name_col.get(results[i].supp_rowid);
            result_n_names[i] = nation_names[results[i].nation_rowid];
        }

        // Create index array for sorting
        std::vector<int32_t> idx(results.size());
        for (size_t i = 0; i < results.size(); i++) idx[i] = (int32_t)i;

        auto cmp = [&](int32_t a, int32_t b) {
            // s_acctbal DESC
            if (results[a].s_acctbal != results[b].s_acctbal)
                return results[a].s_acctbal > results[b].s_acctbal;
            // n_name ASC
            int nc = result_n_names[a].compare(result_n_names[b]);
            if (nc != 0) return nc < 0;
            // s_name ASC
            int sc = result_s_names[a].compare(result_s_names[b]);
            if (sc != 0) return sc < 0;
            // p_partkey ASC
            return results[a].p_partkey < results[b].p_partkey;
        };

        if (k > 0 && k < (int32_t)idx.size()) {
            std::partial_sort(idx.begin(), idx.begin() + k, idx.end(), cmp);
        } else {
            std::sort(idx.begin(), idx.end(), cmp);
        }

        // ---- Step 7: Output ----
        {
            GENDB_PHASE("output");
            std::string outpath = results_dir + "/Q2.csv";
            FILE* fp = fopen(outpath.c_str(), "w");
            fprintf(fp, "s_acctbal,s_name,n_name,p_partkey,p_mfgr,s_address,s_phone,s_comment\n");

            int32_t out_count = std::min(k, (int32_t)idx.size());
            for (int32_t i = 0; i < out_count; i++) {
                int32_t ri = idx[i];
                const auto& rt = results[ri];

                // s_acctbal - format as %.2f
                fprintf(fp, "%.2f,", rt.s_acctbal);
                // s_name
                write_csv_field(fp, s_name_col.ptr(rt.supp_rowid), s_name_col.len(rt.supp_rowid));
                fputc(',', fp);
                // n_name
                const std::string& nn = result_n_names[ri];
                write_csv_field(fp, nn.c_str(), nn.size());
                fputc(',', fp);
                // p_partkey
                fprintf(fp, "%d,", rt.p_partkey);
                // p_mfgr (dict decode)
                const std::string& mfgr = p_mfgr_dict[(uint8_t)p_mfgr_codes[rt.part_rowid]].value;
                write_csv_field(fp, mfgr.c_str(), mfgr.size());
                fputc(',', fp);
                // s_address
                write_csv_field(fp, s_addr_col.ptr(rt.supp_rowid), s_addr_col.len(rt.supp_rowid));
                fputc(',', fp);
                // s_phone
                write_csv_field(fp, s_phone_col.ptr(rt.supp_rowid), s_phone_col.len(rt.supp_rowid));
                fputc(',', fp);
                // s_comment
                write_csv_field(fp, s_comment_col.ptr(rt.supp_rowid), s_comment_col.len(rt.supp_rowid));
                fputc('\n', fp);
            }
            fclose(fp);
        }
    }

    } // total timer
    return 0;
}
