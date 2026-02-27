// Q6 iter_2: Hash map approach for pre IS lookup + parallel num scan
// Use packed uint64 keys for O(1) lookup instead of binary search

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
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

    // Build pre IS map: packed_key3 → vector<plabel_id>
    // Use flat sorted array + index for efficient multi-value lookup
    struct PreIS { uint64_t key; uint32_t plabel_id; };
    std::vector<PreIS> pre_is;
    pre_is.reserve(pre.count / 4);
    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& p = pre.data[i];
        if (p.stmt_id == stmt_is) {
            uint64_t k = pack_key3(p.adsh_id, p.tag_id, p.ver_id);
            pre_is.push_back({k, p.plabel_id});
        }
    }
    // Sort by key for binary search
    std::sort(pre_is.begin(), pre_is.end(), [](const PreIS& a, const PreIS& b) {
        return a.key < b.key;
    });

    // Build index: unique_key → (first_idx, count)
    // This allows O(1) hash lookup + direct range access
    std::unordered_map<uint64_t, std::pair<uint32_t,uint32_t>> pre_idx;
    pre_idx.reserve(pre_is.size());
    {
        uint64_t i = 0;
        while (i < pre_is.size()) {
            uint64_t key = pre_is[i].key;
            uint32_t start = (uint32_t)i;
            while (i < pre_is.size() && pre_is[i].key == key) i++;
            pre_idx[key] = {start, (uint32_t)(i - start)};
        }
    }

    // Thread-local aggregation using FullKey
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
        ThreadAgg() { map.reserve(8192); }
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
            for (uint64_t i = start; i < end; i++) {
                const NumRec& r = num.data[i];
                if (r.uom_id != uom_usd || !r.has_value) continue;
                if (r.adsh_id >= MAX_ADSH) continue;
                uint32_t name_id = sub_name[r.adsh_id];
                if (name_id == UINT32_MAX) continue;

                // Hash lookup in pre_idx
                uint64_t k = pack_key3(r.adsh_id, r.tag_id, r.ver_id);
                auto it = pre_idx.find(k);
                if (it == pre_idx.end()) continue;

                // Iterate matching plabels
                uint32_t fidx = it->second.first;
                uint32_t cnt = it->second.second;
                for (uint32_t j = fidx; j < fidx + cnt; j++) {
                    FullKey fk{name_id, r.tag_id, pre_is[j].plabel_id};
                    auto& sv = agg[fk];
                    sv.first += r.value;
                    sv.second++;
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
