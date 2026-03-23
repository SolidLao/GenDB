// Q6: 3-table join (num→sub, num→pre), grouped aggregation, top-200
// Best: two-pass scatter collect → single-pass probe with 4-partition output → 4-thread parallel agg
#include "timing_utils.h"
#include "cli_params.h"

#include <cstdint>
#include <cstdio>
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

struct MMap {
    void* ptr = nullptr; size_t len = 0; int fd = -1;
    void open(const std::string& path, int advice = MADV_SEQUENTIAL) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { std::fprintf(stderr, "Cannot open %s\n", path.c_str()); std::exit(1); }
        struct stat st; fstat(fd, &st); len = st.st_size;
        ptr = mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { std::fprintf(stderr, "mmap failed %s\n", path.c_str()); std::exit(1); }
        madvise(ptr, len, advice); madvise(ptr, len, MADV_HUGEPAGE);
    }
    ~MMap() { if (ptr && ptr != MAP_FAILED) munmap(ptr, len); if (fd >= 0) ::close(fd); }
    template<typename T> const T* as() const { return reinterpret_cast<const T*>(ptr); }
};

struct Dict {
    uint32_t count; const uint32_t* offsets; const char* str_data; MMap mm;
    void load(const std::string& path) {
        mm.open(path);
        count = *(const uint32_t*)mm.ptr;
        offsets = (const uint32_t*)((const uint8_t*)mm.ptr + 4);
        str_data = (const char*)((const uint8_t*)mm.ptr + 4 + (count + 1) * 4);
    }
    std::string get(uint32_t code) const {
        if (code >= count) return "";
        return std::string(str_data + offsets[code], str_data + offsets[code + 1]);
    }
    uint32_t find_code(const std::string& s) const {
        for (uint32_t i = 0; i < count; i++) {
            uint32_t l = offsets[i + 1] - offsets[i];
            if (l == s.size() && memcmp(str_data + offsets[i], s.data(), l) == 0) return i;
        }
        return count;
    }
};

inline uint64_t hash_u32(uint32_t a) {
    uint64_t x = a;
    x = (x ^ (x >> 16)) * 0x45d9f3b;
    x = (x ^ (x >> 16)) * 0x45d9f3b;
    return x ^ (x >> 16);
}
inline uint64_t hash_u32x2(uint32_t a, uint32_t b) {
    uint64_t x = (static_cast<uint64_t>(a) << 32) | b;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}
inline uint64_t hash_u32x3(uint32_t a, uint32_t b, uint32_t c) {
    uint64_t h = hash_u32x2(a, b);
    h ^= hash_u32(c) + 0x9e3779b97f4a7c15ULL + (h << 12) + (h >> 4);
    return h;
}
inline uint64_t agg_hash(uint32_t n, uint32_t t, uint32_t p) {
    uint64_t h = hash_u32x2(n, t);
    h ^= hash_u32(p) + 0x9e3779b97f4a7c15ULL + (h << 12) + (h >> 4);
    return h;
}

struct HashEntry {
    uint64_t key_hash; uint32_t value; uint32_t extra;
    uint8_t occupied; uint8_t pad[7];
};

struct OutTuple {
    uint32_t name_code, tag_code, plabel_code;
    double value;
};

struct AggEntry {
    uint32_t name_code, tag_code, plabel_code;
    uint8_t occupied;
    double sum;
    uint64_t count;
};

struct AggMap {
    AggEntry* table; size_t mask, count;
    AggMap() : table(nullptr), mask(0), count(0) {}
    void init(size_t expected) {
        size_t cap = 16;
        while (cap < expected * 2) cap <<= 1;
        table = (AggEntry*)std::calloc(cap, sizeof(AggEntry));
        mask = cap - 1; count = 0;
    }
    ~AggMap() { if (table) std::free(table); }
    void add(uint32_t nc, uint32_t tc, uint32_t pc, double val) {
        size_t pos = agg_hash(nc, tc, pc) & mask;
        while (true) {
            AggEntry& e = table[pos];
            if (!e.occupied) {
                e.name_code = nc; e.tag_code = tc; e.plabel_code = pc;
                e.occupied = 1; e.sum = val; e.count = 1; count++;
                if (__builtin_expect(count * 2 > mask, 0)) rehash();
                return;
            }
            if (e.name_code == nc && e.tag_code == tc && e.plabel_code == pc) {
                e.sum += val; e.count++; return;
            }
            pos = (pos + 1) & mask;
        }
    }
    void rehash() {
        size_t old_cap = mask + 1, new_cap = old_cap * 2;
        AggEntry* old = table;
        table = (AggEntry*)std::calloc(new_cap, sizeof(AggEntry));
        mask = new_cap - 1; count = 0;
        for (size_t i = 0; i < old_cap; i++) {
            if (old[i].occupied) {
                size_t pos = agg_hash(old[i].name_code, old[i].tag_code, old[i].plabel_code) & mask;
                while (table[pos].occupied) pos = (pos + 1) & mask;
                table[pos] = old[i]; count++;
            }
        }
        std::free(old);
    }
};

struct NumTuple {
    uint32_t adsh, tag, version, name_code;
    double value;
};

int main(int argc, char* argv[]) {
    if (argc < 3) return 1;
    std::string gendb_dir = argv[1], results_dir = argv[2];
    int64_t param_limit = gendb::parse_int_arg(argc, argv, "--limit", 200);
    int64_t param_fy = gendb::parse_int_arg(argc, argv, "--fy_eq", 2023);
    std::string param_uom = gendb::parse_string_arg(argc, argv, "--uom_eq", "USD");
    std::string param_stmt = gendb::parse_string_arg(argc, argv, "--stmt_eq", "IS");

    MMap mm_sub_fy, mm_sub_name, mm_num_adsh, mm_num_tag, mm_num_ver;
    MMap mm_num_uom, mm_num_val, mm_num_vn;
    MMap mm_pre_stmt, mm_pre_plabel, mm_slu, mm_pj, mm_pjr;

    {
    GENDB_PHASE("total");

    Dict dict_uom, dict_stmt, dict_tag, dict_name, dict_plabel;
    uint16_t usd_code; uint8_t is_code;
    {
        GENDB_PHASE("data_loading");
        dict_uom.load(gendb_dir + "/dictionaries/uom.dict");
        dict_stmt.load(gendb_dir + "/dictionaries/stmt.dict");
        dict_tag.load(gendb_dir + "/dictionaries/tag.dict");
        dict_name.load(gendb_dir + "/dictionaries/name.dict");
        dict_plabel.load(gendb_dir + "/dictionaries/plabel.dict");
        uint32_t c = dict_uom.find_code(param_uom); if (c >= dict_uom.count) return 1; usd_code = (uint16_t)c;
        c = dict_stmt.find_code(param_stmt); if (c >= dict_stmt.count) return 1; is_code = (uint8_t)c;
        mm_slu.open(gendb_dir + "/indexes/sub_adsh_lookup.idx", MADV_RANDOM);
        mm_pj.open(gendb_dir + "/indexes/pre_join.idx", MADV_RANDOM);
        mm_pjr.open(gendb_dir + "/indexes/pre_join_rowids.idx", MADV_RANDOM);
        mm_sub_fy.open(gendb_dir + "/sub/fy.bin");
        mm_sub_name.open(gendb_dir + "/sub/name.bin");
        mm_num_adsh.open(gendb_dir + "/num/adsh.bin");
        mm_num_tag.open(gendb_dir + "/num/tag.bin");
        mm_num_ver.open(gendb_dir + "/num/version.bin");
        mm_num_uom.open(gendb_dir + "/num/uom.bin");
        mm_num_val.open(gendb_dir + "/num/value.bin");
        mm_num_vn.open(gendb_dir + "/num/value_null.bin");
        mm_pre_stmt.open(gendb_dir + "/pre/stmt.bin", MADV_RANDOM);
        mm_pre_plabel.open(gendb_dir + "/pre/plabel.bin", MADV_RANDOM);
    }

    const int16_t* sub_fy = mm_sub_fy.as<int16_t>();
    const uint32_t* sub_name = mm_sub_name.as<uint32_t>();
    uint32_t sub_rows = (uint32_t)(mm_sub_fy.len / sizeof(int16_t));
    const uint32_t* sld = mm_slu.as<uint32_t>();
    uint32_t sls = sld[0]; const uint32_t* sla = sld + 1;

    const uint32_t* num_adsh = mm_num_adsh.as<uint32_t>();
    const uint32_t* num_tag = mm_num_tag.as<uint32_t>();
    const uint32_t* num_ver = mm_num_ver.as<uint32_t>();
    const uint16_t* num_uom = mm_num_uom.as<uint16_t>();
    const double* num_val = mm_num_val.as<double>();
    const uint8_t* num_vn = mm_num_vn.as<uint8_t>();
    uint32_t num_rows = (uint32_t)(mm_num_uom.len / sizeof(uint16_t));

    uint32_t pjc = *(const uint32_t*)mm_pj.ptr;
    const HashEntry* pjt = (const HashEntry*)((const uint8_t*)mm_pj.ptr + 8);
    uint32_t pjm = pjc - 1;
    const uint32_t* rids = mm_pjr.as<uint32_t>() + 1;
    const uint8_t* pre_stmt = mm_pre_stmt.as<uint8_t>();
    const uint32_t* pre_plabel = mm_pre_plabel.as<uint32_t>();

    // Dim filter
    uint8_t* vbits = (uint8_t*)std::calloc((sls + 7) / 8, 1);
    uint32_t* nba = (uint32_t*)std::malloc(sls * sizeof(uint32_t));
    {
        GENDB_PHASE("dim_filter");
        int16_t fyv = (int16_t)param_fy;
        for (uint32_t ac = 0; ac < sls; ac++) {
            uint32_t r = sla[ac];
            if (r != UINT32_MAX && r < sub_rows && sub_fy[r] == fyv) {
                vbits[ac >> 3] |= (1 << (ac & 7)); nba[ac] = sub_name[r];
            }
        }
    }

    int nthreads = omp_get_num_procs();

    // Fused: scan num + probe pre_join + partition output (single pass)
    constexpr int NPARTS = 4;
    constexpr int NPARTS_MASK = NPARTS - 1;
    struct PartVecs {
        std::vector<OutTuple> parts[NPARTS];
    };
    std::vector<PartVecs> pvs(nthreads);

    {
        GENDB_PHASE("main_scan");
        #pragma omp parallel num_threads(nthreads)
        {
            int tid = omp_get_thread_num();
            for (int p = 0; p < NPARTS; p++)
                pvs[tid].parts[p].reserve(4096);

            #pragma omp for schedule(dynamic, 100000) nowait
            for (uint32_t i = 0; i < num_rows; i++) {
                if (num_uom[i] != usd_code || num_vn[i] != 0) continue;
                uint32_t ac = num_adsh[i];
                if (ac >= sls || !(vbits[ac >> 3] & (1 << (ac & 7)))) continue;

                uint32_t nc = nba[ac], tc = num_tag[i], vc = num_ver[i];
                double val = num_val[i];

                uint64_t h = hash_u32x3(ac, tc, vc);
                uint32_t slot = (uint32_t)h & pjm;
                while (true) {
                    const HashEntry& e = pjt[slot];
                    if (!e.occupied) break;
                    if (e.key_hash == h) {
                        for (uint32_t j = 0; j < e.extra; j++) {
                            uint32_t pr = rids[e.value + j];
                            if (pre_stmt[pr] == is_code) {
                                uint32_t pc = pre_plabel[pr];
                                int part = (int)((agg_hash(nc, tc, pc) >> 48) & NPARTS_MASK);
                                pvs[tid].parts[part].push_back({nc, tc, pc, val});
                            }
                        }
                        break;
                    }
                    slot = (slot + 1) & pjm;
                }
            }
        }
    }

    // Parallel aggregation: 4 independent partitions
    std::vector<AggMap> amaps(NPARTS);
    {
        GENDB_PHASE("aggregation");
        #pragma omp parallel for num_threads(NPARTS) schedule(dynamic, 1)
        for (int p = 0; p < NPARTS; p++) {
            amaps[p].init(200000);
            for (int t = 0; t < nthreads; t++) {
                for (auto& ot : pvs[t].parts[p]) {
                    amaps[p].add(ot.name_code, ot.tag_code, ot.plabel_code, ot.value);
                }
            }
        }
    }

    // Output
    {
        GENDB_PHASE("output");
        struct ResultRow { uint32_t nc, tc, pc; double tv; uint64_t cnt; };
        std::vector<ResultRow> res;
        size_t tg = 0;
        for (int p = 0; p < NPARTS; p++) tg += amaps[p].count;
        res.reserve(tg);
        for (int p = 0; p < NPARTS; p++) {
            size_t gcap = amaps[p].mask + 1;
            for (size_t i = 0; i < gcap; i++) {
                if (amaps[p].table[i].occupied) {
                    auto& e = amaps[p].table[i];
                    res.push_back({e.name_code, e.tag_code, e.plabel_code, e.sum, e.count});
                }
            }
        }
        size_t k = std::min((size_t)param_limit, res.size());
        if (k > 0) std::partial_sort(res.begin(), res.begin() + k, res.end(),
            [](const ResultRow& a, const ResultRow& b) { return a.tv > b.tv; });

        FILE* fp = fopen((results_dir + "/Q6.csv").c_str(), "w"); if (!fp) return 1;
        fprintf(fp, "name,stmt,tag,plabel,total_value,cnt\n");
        auto csv = [](const std::string& s) -> std::string {
            if (s.find(',') != std::string::npos || s.find('"') != std::string::npos) {
                std::string o = "\"";
                for (char c : s) { if (c == '"') o += "\"\""; else o += c; }
                return o + '"';
            }
            return s;
        };
        for (size_t i = 0; i < k; i++) {
            auto& r = res[i];
            fprintf(fp, "%s,%s,%s,%s,%.2f,%lu\n",
                    csv(dict_name.get(r.nc)).c_str(), param_stmt.c_str(),
                    csv(dict_tag.get(r.tc)).c_str(), csv(dict_plabel.get(r.pc)).c_str(),
                    r.tv, (unsigned long)r.cnt);
        }
        fclose(fp);
    }

    std::free(vbits); std::free(nba);
    } // total
    return 0;
}
