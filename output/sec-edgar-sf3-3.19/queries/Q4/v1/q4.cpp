#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_map>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "cli_params.h"

// ============================================================
// Lazy dictionary: keep data mmap'd, parse on demand
// ============================================================
struct LazyDict {
    const uint8_t* mmap_data = nullptr;
    size_t mmap_size = 0;
    uint32_t count = 0;
    const uint32_t* offsets = nullptr;
    const char* str_base = nullptr;

    void load(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open dict: %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        mmap_size = st.st_size;
        mmap_data = (const uint8_t*)mmap(nullptr, mmap_size, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        memcpy(&count, mmap_data, 4);
        offsets = (const uint32_t*)(mmap_data + 4);
        str_base = (const char*)(mmap_data + 4 + (count + 1) * 4);
    }

    std::string get(uint32_t code) const {
        return std::string(str_base + offsets[code], offsets[code + 1] - offsets[code]);
    }

    uint32_t find_code(const std::string& s) const {
        for (uint32_t i = 0; i < count; i++) {
            uint32_t len = offsets[i + 1] - offsets[i];
            if (len == s.size() && memcmp(str_base + offsets[i], s.data(), len) == 0)
                return i;
        }
        return UINT32_MAX;
    }

    ~LazyDict() {
        if (mmap_data) munmap((void*)mmap_data, mmap_size);
    }
};

// ============================================================
// Mmap helper
// ============================================================
struct MmapFile {
    void* ptr = nullptr;
    size_t size = 0;
    int fd = -1;

    void open(const std::string& path, int advice = MADV_SEQUENTIAL) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open: %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        size = st.st_size;
        ptr = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { fprintf(stderr, "mmap fail: %s\n", path.c_str()); exit(1); }
        if (size > 1024*1024) madvise(ptr, size, advice);
    }

    void close() {
        if (ptr && size > 0) munmap(ptr, size);
        if (fd >= 0) ::close(fd);
        ptr = nullptr; size = 0; fd = -1;
    }

    template<typename T> const T* as() const { return (const T*)ptr; }
    template<typename T> size_t count() const { return size / sizeof(T); }
};

// ============================================================
// Hash functions (verbatim from build_indexes.cpp)
// ============================================================
inline uint64_t hash_u32(uint32_t a) {
    uint64_t x = a;
    x = (x ^ (x >> 16)) * 0x45d9f3b;
    x = (x ^ (x >> 16)) * 0x45d9f3b;
    x = x ^ (x >> 16);
    return x;
}

inline uint64_t hash_u32x2(uint32_t a, uint32_t b) {
    uint64_t x = (static_cast<uint64_t>(a) << 32) | b;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x = x ^ (x >> 31);
    return x;
}

inline uint64_t hash_u32x3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = hash_u32x2(a, b);
    h ^= hash_u32(c) + 0x9e3779b97f4a7c15ULL + (h << 12) + (h >> 4);
    return h;
}

// ============================================================
// Index structures
// ============================================================
struct __attribute__((packed)) HashEntry {
    uint64_t key_hash;
    uint32_t value;
    uint32_t extra;
    uint8_t  occupied;
    uint8_t  pad[7];
};
static_assert(sizeof(HashEntry) == 24, "HashEntry must be 24 bytes");

struct HashIndex {
    const HashEntry* entries = nullptr;
    uint32_t mask = 0;

    void init(const void* data) {
        const uint32_t* header = (const uint32_t*)data;
        uint32_t capacity = header[0];
        entries = (const HashEntry*)((const uint8_t*)data + 8);
        mask = capacity - 1;
    }

    uint32_t lookup(uint64_t h) const {
        uint32_t slot = (uint32_t)h & mask;
        for (uint32_t probe = 0; probe < mask + 1; probe++) {
            const auto& e = entries[slot];
            if (!e.occupied) return UINT32_MAX;
            if (e.key_hash == h) return e.value;
            slot = (slot + 1) & mask;
        }
        return UINT32_MAX;
    }

    std::pair<uint32_t, uint32_t> lookup_multi(uint64_t h) const {
        uint32_t slot = (uint32_t)h & mask;
        for (uint32_t probe = 0; probe < mask + 1; probe++) {
            const auto& e = entries[slot];
            if (!e.occupied) return {0, 0};
            if (e.key_hash == h) return {e.value, e.extra};
            slot = (slot + 1) & mask;
        }
        return {0, 0};
    }
};

// ============================================================
// Aggregation with compact group key (sic:16 + tlabel_code:32 = 48 bits packed into uint64_t)
// ============================================================
inline uint64_t make_group_key(int16_t sic, uint32_t tlabel_code) {
    return ((uint64_t)(uint16_t)sic << 32) | tlabel_code;
}

inline int16_t group_sic(uint64_t key) { return (int16_t)(uint16_t)(key >> 32); }
inline uint32_t group_tlabel(uint64_t key) { return (uint32_t)(key & 0xFFFFFFFF); }

struct AggValue {
    int64_t sum_value_cents = 0;
    uint64_t count_value = 0;
    std::vector<int32_t> cik_list; // sort+unique at end
};

using AggMap = std::unordered_map<uint64_t, AggValue>;

// ============================================================
// Main
// ============================================================
int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir> [params...]\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    int64_t param_limit = gendb::parse_int_arg(argc, argv, "--limit", 500);
    int64_t param_abstract_eq = gendb::parse_int_arg(argc, argv, "--abstract_eq", 0);
    int64_t param_sic_lower = gendb::parse_int_arg(argc, argv, "--sic_lower", 4000);
    int64_t param_sic_upper = gendb::parse_int_arg(argc, argv, "--sic_upper", 4999);
    std::string param_uom_eq = gendb::parse_string_arg(argc, argv, "--uom_eq", "USD");
    std::string param_stmt_eq = gendb::parse_string_arg(argc, argv, "--stmt_eq", "EQ");

    // Mmap all files BEFORE timing scope
    MmapFile mf_num_adsh, mf_num_tag, mf_num_version, mf_num_uom, mf_num_value, mf_num_value_null;
    mf_num_adsh.open(gendb_dir + "/num/adsh.bin");
    mf_num_tag.open(gendb_dir + "/num/tag.bin");
    mf_num_version.open(gendb_dir + "/num/version.bin");
    mf_num_uom.open(gendb_dir + "/num/uom.bin");
    mf_num_value.open(gendb_dir + "/num/value.bin");
    mf_num_value_null.open(gendb_dir + "/num/value_null.bin");

    MmapFile mf_sub_adsh, mf_sub_sic, mf_sub_cik;
    mf_sub_adsh.open(gendb_dir + "/sub/adsh.bin");
    mf_sub_sic.open(gendb_dir + "/sub/sic.bin");
    mf_sub_cik.open(gendb_dir + "/sub/cik.bin");

    MmapFile mf_tag_abstract, mf_tag_tlabel;
    mf_tag_abstract.open(gendb_dir + "/tag/abstract.bin", MADV_RANDOM);
    mf_tag_tlabel.open(gendb_dir + "/tag/tlabel.bin", MADV_RANDOM);

    MmapFile mf_pre_stmt;
    mf_pre_stmt.open(gendb_dir + "/pre/stmt.bin", MADV_RANDOM);

    MmapFile mf_tag_pk, mf_pre_join, mf_pre_join_rowids;
    mf_tag_pk.open(gendb_dir + "/indexes/tag_pk.idx", MADV_RANDOM);
    mf_pre_join.open(gendb_dir + "/indexes/pre_join.idx", MADV_RANDOM);
    mf_pre_join_rowids.open(gendb_dir + "/indexes/pre_join_rowids.idx", MADV_RANDOM);

    {
    GENDB_PHASE("total");

    // Load dictionaries
    LazyDict dict_uom, dict_stmt, dict_tlabel;
    uint16_t usd_code;
    uint8_t eq_code;
    {
        GENDB_PHASE("data_loading");
        dict_uom.load(gendb_dir + "/dictionaries/uom.dict");
        dict_stmt.load(gendb_dir + "/dictionaries/stmt.dict");
        dict_tlabel.load(gendb_dir + "/dictionaries/tlabel.dict");

        uint32_t uc = dict_uom.find_code(param_uom_eq);
        if (uc == UINT32_MAX) { fprintf(stderr, "UOM not found\n"); return 1; }
        usd_code = (uint16_t)uc;

        uint32_t sc = dict_stmt.find_code(param_stmt_eq);
        if (sc == UINT32_MAX) { fprintf(stderr, "STMT not found\n"); return 1; }
        eq_code = (uint8_t)sc;
    }

    // Setup indexes
    HashIndex tag_pk_idx;
    tag_pk_idx.init(mf_tag_pk.ptr);

    HashIndex pre_join_idx;
    pre_join_idx.init(mf_pre_join.ptr);

    const uint32_t* pre_rowids = mf_pre_join_rowids.as<uint32_t>() + 1;

    // Column pointers
    const size_t num_rows = mf_num_adsh.count<uint32_t>();
    const uint32_t* num_adsh = mf_num_adsh.as<uint32_t>();
    const uint32_t* num_tag = mf_num_tag.as<uint32_t>();
    const uint32_t* num_version = mf_num_version.as<uint32_t>();
    const uint16_t* num_uom = mf_num_uom.as<uint16_t>();
    const double*   num_value = mf_num_value.as<double>();
    const uint8_t*  num_value_null = mf_num_value_null.as<uint8_t>();

    const size_t sub_row_count = mf_sub_sic.count<int16_t>();
    const uint32_t* sub_adsh = mf_sub_adsh.as<uint32_t>();
    const int16_t*  sub_sic = mf_sub_sic.as<int16_t>();
    const int32_t*  sub_cik = mf_sub_cik.as<int32_t>();

    const int8_t*   tag_abstract = mf_tag_abstract.as<int8_t>();
    const uint32_t* tag_tlabel = mf_tag_tlabel.as<uint32_t>();
    const uint8_t*  pre_stmt = mf_pre_stmt.as<uint8_t>();

    // Pre-filter sub
    uint32_t max_adsh_code = 0;
    for (size_t i = 0; i < sub_row_count; i++) {
        if (sub_adsh[i] > max_adsh_code) max_adsh_code = sub_adsh[i];
    }

    size_t bitset_words = (max_adsh_code + 64) / 64;
    std::vector<uint64_t> valid_adsh_bitset(bitset_words, 0);

    struct SubInfo { int16_t sic; int32_t cik; };
    std::vector<SubInfo> sub_info(max_adsh_code + 1);

    {
        GENDB_PHASE("prefilter_sub");
        for (size_t i = 0; i < sub_row_count; i++) {
            int16_t sic = sub_sic[i];
            if (sic >= (int16_t)param_sic_lower && sic <= (int16_t)param_sic_upper) {
                uint32_t ac = sub_adsh[i];
                valid_adsh_bitset[ac >> 6] |= (1ULL << (ac & 63));
                sub_info[ac].sic = sic;
                sub_info[ac].cik = sub_cik[i];
            }
        }
    }

    // Parallel scan with per-thread agg maps using vector<int32_t> for CIK
    int n_threads = omp_get_max_threads();
    if (n_threads > 32) n_threads = 32;
    omp_set_num_threads(n_threads);

    std::vector<AggMap> thread_aggs(n_threads);

    {
        GENDB_PHASE("main_scan");

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            AggMap& local_agg = thread_aggs[tid];
            local_agg.reserve(4096);

            #pragma omp for schedule(dynamic, 100000)
            for (size_t i = 0; i < num_rows; i++) {
                uint32_t adsh_code = num_adsh[i];
                if (adsh_code > max_adsh_code) continue;
                if (!(valid_adsh_bitset[adsh_code >> 6] & (1ULL << (adsh_code & 63)))) continue;

                if (num_uom[i] != usd_code) continue;
                if (num_value_null[i]) continue;

                uint32_t tag_code = num_tag[i];
                uint32_t ver_code = num_version[i];
                uint64_t tag_hash = hash_u32x2(tag_code, ver_code);
                uint32_t tag_row = tag_pk_idx.lookup(tag_hash);
                if (tag_row == UINT32_MAX) continue;
                if (tag_abstract[tag_row] != (int8_t)param_abstract_eq) continue;

                uint32_t tlabel_code = tag_tlabel[tag_row];

                uint64_t pre_hash = hash_u32x3(adsh_code, tag_code, ver_code);
                auto [pre_offset, pre_count] = pre_join_idx.lookup_multi(pre_hash);
                if (pre_count == 0) continue;

                double val = num_value[i];
                int16_t sic = sub_info[adsh_code].sic;
                int32_t cik = sub_info[adsh_code].cik;
                uint64_t gkey = make_group_key(sic, tlabel_code);

                for (uint32_t p = 0; p < pre_count; p++) {
                    uint32_t pre_row = pre_rowids[pre_offset + p];
                    if (pre_stmt[pre_row] != eq_code) continue;

                    auto& agg = local_agg[gkey];
                    agg.sum_value_cents += llround(val * 100.0);
                    agg.count_value++;
                    agg.cik_list.push_back(cik);
                }
            }
        }
    }

    // Merge thread-local maps
    AggMap global_agg;
    {
        GENDB_PHASE("aggregation");
        global_agg.reserve(8192);
        for (auto& tagg : thread_aggs) {
            for (auto& [key, val] : tagg) {
                auto& g = global_agg[key];
                g.sum_value_cents += val.sum_value_cents;
                g.count_value += val.count_value;
                // Move or append CIK list
                if (g.cik_list.empty()) {
                    g.cik_list = std::move(val.cik_list);
                } else {
                    g.cik_list.insert(g.cik_list.end(), val.cik_list.begin(), val.cik_list.end());
                }
            }
            tagg.clear();
        }
    }

    // HAVING filter
    struct ResultRow {
        int16_t sic;
        uint32_t tlabel_code;
        int64_t num_companies;
        int64_t total_value_cents;
        int64_t sum_value_cents_for_avg;
        uint64_t count_value;
    };

    std::vector<ResultRow> results;
    {
        GENDB_PHASE("having_filter");
        results.reserve(global_agg.size());
        for (auto& [key, val] : global_agg) {
            std::sort(val.cik_list.begin(), val.cik_list.end());
            auto last = std::unique(val.cik_list.begin(), val.cik_list.end());
            int64_t distinct_cik = last - val.cik_list.begin();
            if (distinct_cik >= 2) {
                results.push_back({
                    group_sic(key), group_tlabel(key), distinct_cik,
                    val.sum_value_cents,
                    val.sum_value_cents,
                    val.count_value
                });
            }
        }
    }

    // Top-K sort
    auto cmp = [](const ResultRow& a, const ResultRow& b) {
        if (a.total_value_cents != b.total_value_cents) return a.total_value_cents > b.total_value_cents;
        if (a.sic != b.sic) return a.sic < b.sic;
        return a.tlabel_code < b.tlabel_code;
    };
    size_t limit = std::min((size_t)param_limit, results.size());
    if (limit < results.size()) {
        std::partial_sort(results.begin(), results.begin() + limit, results.end(), cmp);
        results.resize(limit);
    } else {
        std::sort(results.begin(), results.end(), cmp);
    }

    // Output
    {
        GENDB_PHASE("output");
        std::string out_path = results_dir + "/Q4.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        if (!fp) { fprintf(stderr, "Cannot open output: %s\n", out_path.c_str()); return 1; }

        fprintf(fp, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
        const std::string stmt_str = dict_stmt.get(eq_code);
        for (auto& r : results) {
            std::string tlabel = dict_tlabel.get(r.tlabel_code);
            int64_t tv = r.total_value_cents;
            double avg_val = (double)r.sum_value_cents_for_avg / (double)r.count_value / 100.0;
            char tv_buf[64];
            if (tv >= 0) {
                snprintf(tv_buf, sizeof(tv_buf), "%ld.%02ld", (long)(tv / 100), (long)(tv % 100));
            } else {
                int64_t abs_tv = -tv;
                snprintf(tv_buf, sizeof(tv_buf), "-%ld.%02ld", (long)(abs_tv / 100), (long)(abs_tv % 100));
            }
            fprintf(fp, "%d,\"%s\",\"%s\",%ld,%s,%.2f\n",
                (int)r.sic, tlabel.c_str(), stmt_str.c_str(),
                (long)r.num_companies, tv_buf, avg_val);
        }
        fclose(fp);
    }

    } // total timer

    // Cleanup
    mf_num_adsh.close(); mf_num_tag.close(); mf_num_version.close();
    mf_num_uom.close(); mf_num_value.close(); mf_num_value_null.close();
    mf_sub_adsh.close(); mf_sub_sic.close(); mf_sub_cik.close();
    mf_tag_abstract.close(); mf_tag_tlabel.close();
    mf_pre_stmt.close();
    mf_tag_pk.close(); mf_pre_join.close(); mf_pre_join_rowids.close();

    return 0;
}
