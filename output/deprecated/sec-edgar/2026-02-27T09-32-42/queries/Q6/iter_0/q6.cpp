// Q6: num JOIN sub(fy=2023) JOIN pre(stmt='IS'), group by (name,stmt,tag,plabel), top 200
// SELECT s.name, p.stmt, n.tag, p.plabel, SUM(n.value), COUNT(*)
// FROM num n JOIN sub s ON n.adsh=s.adsh
// JOIN pre p ON n.adsh=p.adsh AND n.tag=p.tag AND n.version=p.version
// WHERE n.uom='USD' AND p.stmt='IS' AND s.fy=2023 AND n.value IS NOT NULL
// GROUP BY s.name, p.stmt, n.tag, p.plabel ORDER BY total_value DESC LIMIT 200

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <algorithm>
#include <vector>

struct Key3 {
    uint32_t a, b, c;
    bool operator==(const Key3& o) const { return a==o.a && b==o.b && c==o.c; }
};
struct Key3Hash {
    size_t operator()(const Key3& k) const {
        uint64_t h = (uint64_t)k.a * 2654435761ULL;
        h ^= (uint64_t)k.b * 2246822519ULL;
        h ^= (uint64_t)k.c * 3266489917ULL;
        return (size_t)h;
    }
};

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
    uint32_t stmt_is = metadata_get_id(meta_path, "stmt_ids", "IS");
    if (uom_usd == UINT32_MAX) uom_usd = 999;
    if (stmt_is == UINT32_MAX) stmt_is = 999;

    // Build sub hash map: adsh_id → name_id (fy=2023 only)
    std::unordered_map<uint32_t, uint32_t> sub_map;
    sub_map.reserve(sub.count / 4);
    for (uint64_t i = 0; i < sub.count; i++) {
        if (sub.data[i].fy == 2023) {
            sub_map[sub.data[i].adsh_id] = sub.data[i].name_id;
        }
    }

    // Build pre hash multimap: (adsh_id, tag_id, ver_id) → [plabel_id, ...]
    // Since multiple pre rows per key are possible, store as linked list using vectors
    // For simplicity: unordered_map<Key3, vector<uint32_t>, Key3Hash>
    std::unordered_map<Key3, std::vector<uint32_t>, Key3Hash> pre_map;
    pre_map.reserve(2000000);
    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& p = pre.data[i];
        if (p.stmt_id != stmt_is) continue;
        Key3 k{p.adsh_id, p.tag_id, p.ver_id};
        pre_map[k].push_back(p.plabel_id);
    }

    // Aggregation: key = (name_id, tag_id, plabel_id) → {sum, count}
    // stmt is fixed to 'IS'
    struct AggState { double sum = 0.0; uint64_t cnt = 0; };
    struct AggKey {
        uint32_t name_id, tag_id, plabel_id;
        bool operator==(const AggKey& o) const {
            return name_id==o.name_id && tag_id==o.tag_id && plabel_id==o.plabel_id;
        }
    };
    struct AggKeyHash {
        size_t operator()(const AggKey& k) const {
            uint64_t h = (uint64_t)k.name_id * 2654435761ULL;
            h ^= (uint64_t)k.tag_id * 2246822519ULL;
            h ^= (uint64_t)k.plabel_id * 3266489917ULL;
            return (size_t)h;
        }
    };
    std::unordered_map<AggKey, AggState, AggKeyHash> agg;
    agg.reserve(500000);

    // Scan num
    for (uint64_t i = 0; i < num.count; i++) {
        const NumRec& r = num.data[i];
        if (r.uom_id != uom_usd || !r.has_value) continue;

        // Probe sub
        auto sit = sub_map.find(r.adsh_id);
        if (sit == sub_map.end()) continue;
        uint32_t name_id = sit->second;

        // Probe pre
        Key3 pkey{r.adsh_id, r.tag_id, r.ver_id};
        auto pit = pre_map.find(pkey);
        if (pit == pre_map.end()) continue;

        // For each matching plabel
        for (uint32_t plabel_id : pit->second) {
            AggKey ak{name_id, r.tag_id, plabel_id};
            auto& g = agg[ak];
            g.sum += r.value;
            g.cnt++;
        }
    }

    // Collect and sort results
    struct Row {
        uint32_t name_id, tag_id, plabel_id;
        double total_value;
        uint64_t cnt;
    };
    std::vector<Row> results;
    results.reserve(agg.size());
    for (auto& [k, v] : agg) {
        results.push_back({k.name_id, k.tag_id, k.plabel_id, v.sum, v.cnt});
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
            fprintf(f, ",%.2f,%llu\n", r.total_value, (unsigned long long)r.cnt);
        }
        fclose(f);
    }

    return 0;
}
