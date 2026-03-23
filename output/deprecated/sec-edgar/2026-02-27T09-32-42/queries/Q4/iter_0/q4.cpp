// Q4: 4-way join (num, sub, tag, pre), sic 4000-4999, stmt='EQ', abstract=0
// SELECT s.sic, t.tlabel, p.stmt, COUNT(DISTINCT s.cik), SUM(n.value), AVG(n.value)
// FROM num n JOIN sub s ON n.adsh=s.adsh JOIN tag t ON n.tag=t.tag AND n.version=t.version
// JOIN pre p ON n.adsh=p.adsh AND n.tag=p.tag AND n.version=p.version
// WHERE n.uom='USD' AND p.stmt='EQ' AND s.sic BETWEEN 4000 AND 4999
//   AND n.value IS NOT NULL AND t.abstract=0
// GROUP BY s.sic, t.tlabel, p.stmt HAVING COUNT(DISTINCT s.cik)>=2
// ORDER BY total_value DESC LIMIT 500

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <algorithm>
#include <unordered_set>

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
    BinCol<TagRec> tag;
    BinCol<PreRec> pre;
    if (!sub.load((gendb_dir + "/sub.bin").c_str())) return 1;
    if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;
    if (!tag.load((gendb_dir + "/tag.bin").c_str())) return 1;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    Dict tlabel_dict;
    tlabel_dict.load((gendb_dir + "/dict_tlabel.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_usd = metadata_get_id(meta_path, "uom_ids", "USD");
    uint32_t stmt_eq = metadata_get_id(meta_path, "stmt_ids", "EQ");
    if (uom_usd == UINT32_MAX) uom_usd = 999;
    if (stmt_eq == UINT32_MAX) stmt_eq = 999;

    // Build sub hash map: adsh_id → {sic, cik} (sic BETWEEN 4000 AND 4999)
    struct SubInfo { int32_t sic; uint32_t cik; };
    std::unordered_map<uint32_t, SubInfo> sub_map;
    sub_map.reserve(sub.count / 4);
    for (uint64_t i = 0; i < sub.count; i++) {
        const SubRec& s = sub.data[i];
        if (s.sic >= 4000 && s.sic <= 4999) {
            sub_map[s.adsh_id] = {s.sic, s.cik};
        }
    }

    // Build tag hash map: (tag_id, ver_id) → tlabel_id (abstract=0 only)
    std::unordered_map<uint64_t, uint32_t> tag_map;
    tag_map.reserve(tag.count);
    for (uint64_t i = 0; i < tag.count; i++) {
        const TagRec& t = tag.data[i];
        if (t.abstract == 0) {
            uint64_t key = ((uint64_t)t.tag_id << 32) | t.ver_id;
            tag_map[key] = t.tlabel_id;
        }
    }

    // Build pre hash map: (adsh_id, tag_id, ver_id) → count (stmt='EQ' only)
    std::unordered_map<Key3, uint32_t, Key3Hash> pre_map;
    pre_map.reserve(pre.count / 8);
    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& p = pre.data[i];
        if (p.stmt_id != stmt_eq) continue;
        Key3 k{p.adsh_id, p.tag_id, p.ver_id};
        pre_map[k]++;
    }

    // Aggregation: key = (sic, tlabel_id) → {sum, count, distinct_cik_set}
    struct AggState {
        double sum = 0.0;
        uint64_t count = 0;
        std::unordered_set<uint32_t> ciks;
    };
    // Encode key as: (uint64)(sic << 32) | tlabel_id
    std::unordered_map<uint64_t, AggState> agg;
    agg.reserve(50000);

    // Scan num
    for (uint64_t i = 0; i < num.count; i++) {
        const NumRec& r = num.data[i];
        if (r.uom_id != uom_usd || !r.has_value) continue;

        // Probe sub
        auto sit = sub_map.find(r.adsh_id);
        if (sit == sub_map.end()) continue;
        int32_t sic = sit->second.sic;
        uint32_t cik = sit->second.cik;

        // Probe tag
        uint64_t tkey = ((uint64_t)r.tag_id << 32) | r.ver_id;
        auto tit = tag_map.find(tkey);
        if (tit == tag_map.end()) continue;
        uint32_t tlabel_id = tit->second;

        // Probe pre
        Key3 pkey{r.adsh_id, r.tag_id, r.ver_id};
        auto pit = pre_map.find(pkey);
        if (pit == pre_map.end()) continue;
        uint32_t pre_count = pit->second;

        // Aggregate: key = (sic, tlabel_id)
        uint64_t akey = ((uint64_t)(uint32_t)sic << 32) | tlabel_id;
        auto& g = agg[akey];
        g.sum += (double)pre_count * r.value;
        g.count += pre_count;
        g.ciks.insert(cik);
    }

    // Collect results with HAVING count(distinct cik) >= 2
    struct Row {
        int32_t sic;
        uint32_t tlabel_id;
        double total_value;
        double avg_value;
        uint32_t num_companies;
    };
    std::vector<Row> results;
    results.reserve(1000);

    for (auto& [key, g] : agg) {
        if (g.ciks.size() < 2) continue;
        int32_t sic = (int32_t)(uint32_t)(key >> 32);
        uint32_t tlabel_id = (uint32_t)(key & 0xFFFFFFFF);
        double avg_val = (g.count > 0) ? g.sum / g.count : 0.0;
        results.push_back({sic, tlabel_id, g.sum, avg_val, (uint32_t)g.ciks.size()});
    }

    // Sort by total_value DESC
    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.total_value > b.total_value;
    });
    if (results.size() > 500) results.resize(500);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q4.csv").c_str(), "w");
        fprintf(f, "sic,tlabel,stmt,num_companies,total_value,avg_value\n");
        for (const auto& r : results) {
            fprintf(f, "%d,", r.sic);
            write_csv_str(f, tlabel_dict, r.tlabel_id);
            fprintf(f, ",EQ,%u,%.2f,%.2f\n",
                r.num_companies, r.total_value, r.avg_value);
        }
        fclose(f);
    }

    return 0;
}
