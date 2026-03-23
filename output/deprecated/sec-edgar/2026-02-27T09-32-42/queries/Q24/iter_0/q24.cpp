// Q24: num anti-join with pre (LEFT JOIN + IS NULL)
// SELECT n.tag, n.version, COUNT(*) AS cnt, SUM(n.value) AS total
// FROM num n LEFT JOIN pre p ON n.tag=p.tag AND n.version=p.version AND n.adsh=p.adsh
// WHERE n.uom='USD' AND n.ddate BETWEEN 20230101 AND 20231231
//   AND n.value IS NOT NULL AND p.adsh IS NULL
// GROUP BY n.tag, n.version HAVING COUNT(*) > 10
// ORDER BY cnt DESC LIMIT 100

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

    BinCol<NumRec> num;
    BinCol<PreRec> pre;
    if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;
    if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;

    Dict tag_dict, ver_dict;
    tag_dict.load((gendb_dir + "/dict_tag.bin").c_str());
    ver_dict.load((gendb_dir + "/dict_ver.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_usd = metadata_get_id(meta_path, "uom_ids", "USD");
    if (uom_usd == UINT32_MAX) uom_usd = 999;

    // Build pre hash set: (adsh_id, tag_id, ver_id) → exists
    std::unordered_set<Key3, Key3Hash> pre_set;
    pre_set.reserve(pre.count);
    for (uint64_t i = 0; i < pre.count; i++) {
        const PreRec& p = pre.data[i];
        pre_set.insert({p.adsh_id, p.tag_id, p.ver_id});
    }

    // Aggregation: (tag_id, ver_id) → {count, sum}
    struct AggState { uint64_t cnt = 0; double sum = 0.0; };
    std::unordered_map<uint64_t, AggState> agg;
    agg.reserve(100000);

    // Scan num: uom='USD', ddate in 2023, has_value, NOT IN pre
    for (uint64_t i = 0; i < num.count; i++) {
        const NumRec& r = num.data[i];
        if (r.uom_id != uom_usd || !r.has_value) continue;
        if (r.ddate < 20230101 || r.ddate > 20231231) continue;

        Key3 k{r.adsh_id, r.tag_id, r.ver_id};
        if (pre_set.count(k)) continue; // EXISTS in pre → skip

        uint64_t akey = ((uint64_t)r.tag_id << 32) | r.ver_id;
        auto& g = agg[akey];
        g.cnt++;
        g.sum += r.value;
    }

    // Collect results with HAVING count > 10
    struct Row {
        uint32_t tag_id, ver_id;
        uint64_t cnt;
        double total;
    };
    std::vector<Row> results;
    for (auto& [key, g] : agg) {
        if (g.cnt > 10) {
            uint32_t tag_id = (uint32_t)(key >> 32);
            uint32_t ver_id = (uint32_t)(key & 0xFFFFFFFF);
            results.push_back({tag_id, ver_id, g.cnt, g.sum});
        }
    }

    // Sort by cnt DESC
    std::sort(results.begin(), results.end(), [](const Row& a, const Row& b) {
        return a.cnt > b.cnt;
    });
    if (results.size() > 100) results.resize(100);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q24.csv").c_str(), "w");
        fprintf(f, "tag,version,cnt,total\n");
        for (const auto& r : results) {
            write_csv_str(f, tag_dict, r.tag_id);
            fputc(',', f);
            write_csv_str(f, ver_dict, r.ver_id);
            fprintf(f, ",%llu,%.2f\n", (unsigned long long)r.cnt, r.total);
        }
        fclose(f);
    }

    return 0;
}
