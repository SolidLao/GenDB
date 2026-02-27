// Q6 iter_3: TBB parallel sort + per-adsh IS index + tuple-based aggregation
// Key improvements over iter_1:
// 1. Parallel pre IS collection (64 threads)
// 2. TBB parallel sort of pre IS array
// 3. Per-adsh lo/hi offsets → small binary search per num row (L1 friendly)
// 4. Collect (FullKey, value) tuples from threads + TBB sort + linear aggregation
//    This avoids merging 64 large hash maps (O(N) hash inserts → O(N log N) sort instead)

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <parallel/algorithm>
#include <vector>
#include <unordered_map>

static const int NTHREADS = 64;

static inline uint64_t pack_key3(uint32_t a, uint32_t b, uint32_t c) {
    return ((uint64_t)(a & 0x1FFFF) << 35) | ((uint64_t)(b & 0x3FFFF) << 17) | (c & 0x1FFFF);
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<SubRec> sub;
    BinCol<NumRec> num;
    BinCol<PreRec> pre;
    if (!sub.load((gendb_dir + "/sub.bin").c_str())) return 1;
    if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    Dict name_dict, tag_dict, plabel_dict;
    name_dict.load((gendb_dir + "/dict_name.bin").c_str());
    tag_dict.load((gendb_dir + "/dict_tag.bin").c_str());
    plabel_dict.load((gendb_dir + "/dict_plabel.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_usd = metadata_get_id(meta_path, "uom_ids", "USD");
    if (uom_usd == UINT32_MAX) uom_usd = 999;
    uint32_t stmt_is = metadata_get_id(meta_path, "stmt_ids", "IS");
    if (stmt_is == UINT32_MAX) stmt_is = 2;

    // Build flat sub array: adsh_id → name_id for fy=2023
    const uint32_t MAX_ADSH = (uint32_t)sub.count + 2;
    std::vector<uint32_t> sub_name(MAX_ADSH, UINT32_MAX);
    for (uint64_t i = 0; i < sub.count; i++) {
        const SubRec& s = sub.data[i];
        if (s.fy == 2023 && s.adsh_id < MAX_ADSH) {
            sub_name[s.adsh_id] = s.name_id;
        }
    }

    // Build sorted pre IS array: struct { uint64_t key (packed adsh,tag,ver); uint32_t plabel_id }
    // Parallel collection then TBB sort
    struct PreIS { uint64_t key; uint32_t plabel_id; };
    struct ThreadPreIS {
        std::vector<PreIS> entries;
        ThreadPreIS() { entries.reserve(50000); }
    };
    std::vector<ThreadPreIS> tpre(NTHREADS);
    {
        uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, pre.count);
            if (start >= pre.count) break;
            threads.emplace_back([&, t, start, end] {
                auto& entries = tpre[t].entries;
                for (uint64_t i = start; i < end; i++) {
                    const PreRec& p = pre.data[i];
                    if (p.stmt_id == stmt_is) {
                        uint64_t k = pack_key3(p.adsh_id, p.tag_id, p.ver_id);
                        entries.push_back({k, p.plabel_id});
                    }
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Concatenate
    size_t total_pre_is = 0;
    for (auto& tp : tpre) total_pre_is += tp.entries.size();
    std::vector<PreIS> pre_is;
    pre_is.reserve(total_pre_is);
    for (auto& tp : tpre) for (auto& e : tp.entries) pre_is.push_back(e);
    tpre.clear();

    // TBB parallel sort by (key, plabel_id)
    __gnu_parallel::sort(pre_is.begin(), pre_is.end(),
        [](const PreIS& a, const PreIS& b) {
            if (a.key != b.key) return a.key < b.key;
            return a.plabel_id < b.plabel_id;
        });
    const uint32_t pre_is_size = (uint32_t)pre_is.size();

    // Build per-adsh lo/hi offsets (adsh_id in upper 17 bits of key)
    std::vector<uint32_t> adsh_lo(MAX_ADSH, UINT32_MAX);
    std::vector<uint32_t> adsh_hi(MAX_ADSH, 0);
    for (uint32_t i = 0; i < pre_is_size; i++) {
        uint32_t adsh = (uint32_t)(pre_is[i].key >> 35);
        if (adsh < MAX_ADSH) {
            if (adsh_lo[adsh] == UINT32_MAX) adsh_lo[adsh] = i;
            adsh_hi[adsh] = i + 1;
        }
    }

    // Thread-local tuple collection: (name_id, tag_id, plabel_id, value)
    // 64-bit packed key for sort: name_id(18b) | tag_id(18b) | plabel_id(18b) → 54 bits fits
    // Use struct for clarity
    struct Tuple {
        uint32_t name_id, tag_id, plabel_id;
        double value;
    };
    struct ThreadData {
        std::vector<Tuple> tuples;
        ThreadData() { tuples.reserve(80000); }
    };
    std::vector<ThreadData> tdata(NTHREADS);
    {
        uint64_t chunk = (num.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, num.count);
            if (start >= num.count) break;
            threads.emplace_back([&, t, start, end] {
                auto& tuples = tdata[t].tuples;
                for (uint64_t i = start; i < end; i++) {
                    const NumRec& r = num.data[i];
                    if (r.uom_id != uom_usd || !r.has_value) continue;
                    if (r.adsh_id >= MAX_ADSH) continue;
                    uint32_t name_id = sub_name[r.adsh_id];
                    if (name_id == UINT32_MAX) continue;

                    // Per-adsh range lookup (L1 cache friendly)
                    uint32_t lo = adsh_lo[r.adsh_id];
                    if (lo == UINT32_MAX) continue; // adsh not in pre IS

                    uint32_t hi = adsh_hi[r.adsh_id];
                    uint64_t probe = pack_key3(r.adsh_id, r.tag_id, r.ver_id);

                    // Binary search within small per-adsh range
                    const PreIS* base = pre_is.data() + lo;
                    uint32_t len = hi - lo;
                    uint32_t left = 0, right = len;
                    while (left < right) {
                        uint32_t mid = (left + right) >> 1;
                        if (base[mid].key < probe) left = mid + 1;
                        else right = mid;
                    }

                    // Iterate matching entries (same key, different plabels)
                    while (left < len && base[left].key == probe) {
                        tuples.push_back({name_id, r.tag_id, base[left].plabel_id, r.value});
                        ++left;
                    }
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Concatenate all tuples
    size_t total_tuples = 0;
    for (auto& td : tdata) total_tuples += td.tuples.size();
    std::vector<Tuple> all_tuples;
    all_tuples.reserve(total_tuples);
    for (auto& td : tdata) for (auto& t : td.tuples) all_tuples.push_back(t);
    tdata.clear();

    // TBB sort by (name_id, tag_id, plabel_id) for linear aggregation
    __gnu_parallel::sort(all_tuples.begin(), all_tuples.end(),
        [](const Tuple& a, const Tuple& b) {
            if (a.name_id != b.name_id) return a.name_id < b.name_id;
            if (a.tag_id != b.tag_id) return a.tag_id < b.tag_id;
            return a.plabel_id < b.plabel_id;
        });

    // Linear aggregation (no hash map → cache-optimal sequential scan)
    struct Row { uint32_t name_id, tag_id, plabel_id; double total; uint64_t cnt; };
    std::vector<Row> results;
    results.reserve(2000);

    uint64_t idx = 0;
    while (idx < all_tuples.size()) {
        uint32_t n = all_tuples[idx].name_id;
        uint32_t t = all_tuples[idx].tag_id;
        uint32_t p = all_tuples[idx].plabel_id;
        double sum = 0.0;
        uint64_t cnt = 0;
        while (idx < all_tuples.size() &&
               all_tuples[idx].name_id == n &&
               all_tuples[idx].tag_id == t &&
               all_tuples[idx].plabel_id == p) {
            sum += all_tuples[idx].value;
            cnt++;
            idx++;
        }
        results.push_back({n, t, p, sum, cnt});
    }

    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.total > b.total;
    });
    if (results.size() > 200) results.resize(200);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q6.csv").c_str(), "w");
        fprintf(f, "name,stmt,tag,plabel,total_value,cnt\n");
        for (const auto& r : results) {
            write_csv_str(f, name_dict, r.name_id);
            fprintf(f, ",IS,");
            write_csv_str(f, tag_dict, r.tag_id);
            fputc(',', f);
            write_csv_str(f, plabel_dict, r.plabel_id);
            fprintf(f, ",%.2f,%lu\n", r.total, r.cnt);
        }
        fclose(f);
    }

    return 0;
}
