// Q6 iter_1: Parallel execution with flat sorted pre array for binary-search joins
// JOIN num+sub(fy=2023)+pre(stmt='IS'), aggregate (name,stmt,tag,plabel)

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <vector>
#include <unordered_map>

static const int NTHREADS = 64;

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

    // Build flat pre IS array sorted by (adsh_id, tag_id, ver_id)
    struct PreIS { uint32_t adsh_id, tag_id, ver_id, plabel_id; };
    std::vector<PreIS> pre_is;
    pre_is.reserve(pre.count / 4); // estimate IS is ~1/8 of pre
    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& p = pre.data[i];
        if (p.stmt_id == stmt_is) {
            pre_is.push_back({p.adsh_id, p.tag_id, p.ver_id, p.plabel_id});
        }
    }
    // Sort by (adsh_id, tag_id, ver_id) for binary search
    std::sort(pre_is.begin(), pre_is.end(), [](const PreIS& a, const PreIS& b) {
        if (a.adsh_id != b.adsh_id) return a.adsh_id < b.adsh_id;
        if (a.tag_id != b.tag_id) return a.tag_id < b.tag_id;
        return a.ver_id < b.ver_id;
    });
    const uint64_t pre_is_size = pre_is.size();

    // Thread-local aggregation: key = (name_id, tag_id, plabel_id) → {sum, cnt}
    struct AggKey { uint32_t name_id, tag_id, plabel_id; };
    struct AggKeyHash {
        size_t operator()(uint64_t k) const { return k ^ (k >> 33); }
    };
    // Use a 2-level key: pack tag_id<<32|plabel_id as secondary, with name_id as extra
    // Actually use (name_id, tag_id<<32|plabel_id) - need 3 keys
    // Pack: name_id fits 18 bits, tag_id 18 bits, plabel_id ~20 bits → might not fit in 64
    // Use struct hash instead
    struct FullKey {
        uint32_t name_id, tag_id, plabel_id;
        bool operator==(const FullKey& o) const {
            return name_id==o.name_id && tag_id==o.tag_id && plabel_id==o.plabel_id;
        }
    };
    struct FullKeyHash {
        size_t operator()(const FullKey& k) const {
            uint64_t h = (uint64_t)k.name_id * 2654435761ULL ^ (uint64_t)k.tag_id * 805459861ULL ^ (uint64_t)k.plabel_id * 3242174819ULL;
            return (size_t)(h ^ (h >> 33));
        }
    };

    struct ThreadAgg {
        std::unordered_map<FullKey, std::pair<double,uint64_t>, FullKeyHash> map;
        ThreadAgg() { map.reserve(16384); }
    };
    std::vector<ThreadAgg> taggs(NTHREADS);

    uint64_t chunk = (num.count + NTHREADS - 1) / NTHREADS;
    std::vector<std::thread> threads;
    threads.reserve(NTHREADS);

    for (int t = 0; t < NTHREADS; t++) {
        uint64_t start = (uint64_t)t * chunk;
        uint64_t end = std::min(start + chunk, num.count);
        if (start >= num.count) break;
        threads.emplace_back([&, t, start, end] {
            auto& agg = taggs[t].map;
            PreIS probe_key{0, 0, 0, 0};
            for (uint64_t i = start; i < end; i++) {
                const NumRec& r = num.data[i];
                if (r.uom_id != uom_usd || !r.has_value) continue;
                if (r.adsh_id >= MAX_ADSH) continue;
                uint32_t name_id = sub_name[r.adsh_id];
                if (name_id == UINT32_MAX) continue;

                // Binary search in pre_is for (adsh_id, tag_id, ver_id)
                probe_key.adsh_id = r.adsh_id;
                probe_key.tag_id = r.tag_id;
                probe_key.ver_id = r.ver_id;

                // lower_bound for first matching entry
                auto it = std::lower_bound(pre_is.begin(), pre_is.end(), probe_key,
                    [](const PreIS& a, const PreIS& b) {
                        if (a.adsh_id != b.adsh_id) return a.adsh_id < b.adsh_id;
                        if (a.tag_id != b.tag_id) return a.tag_id < b.tag_id;
                        return a.ver_id < b.ver_id;
                    });

                // Iterate matching entries (same adsh,tag,ver - different plabels possible)
                while (it != pre_is.end() &&
                       it->adsh_id == r.adsh_id &&
                       it->tag_id == r.tag_id &&
                       it->ver_id == r.ver_id) {
                    FullKey fk{name_id, r.tag_id, it->plabel_id};
                    auto& sv = agg[fk];
                    sv.first += r.value;
                    sv.second++;
                    ++it;
                }
            }
        });
    }
    for (auto& t : threads) t.join();

    // Merge
    auto& base = taggs[0].map;
    for (int t = 1; t < NTHREADS; t++) {
        for (auto& [k, sv] : taggs[t].map) {
            auto& b = base[k];
            b.first += sv.first;
            b.second += sv.second;
        }
    }

    // Sort and output top 200
    struct Row {
        uint32_t name_id, tag_id, plabel_id;
        double total_value;
        uint64_t cnt;
    };
    std::vector<Row> results;
    results.reserve(base.size());
    for (auto& [k, sv] : base) {
        results.push_back({k.name_id, k.tag_id, k.plabel_id, sv.first, sv.second});
    }
    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.total_value > b.total_value;
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
            fprintf(f, ",%.2f,%lu\n", r.total_value, r.cnt);
        }
        fclose(f);
    }

    return 0;
}
