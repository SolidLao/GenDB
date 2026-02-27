// Q2: Find max pure values per (adsh,tag), join with sub(fy=2022), top 100
// SELECT s.name, n.tag, n.value FROM num n
// JOIN sub s ON n.adsh = s.adsh
// JOIN (SELECT adsh, tag, MAX(value) FROM num WHERE uom='pure' AND value IS NOT NULL GROUP BY adsh, tag) m
//      ON n.adsh = m.adsh AND n.tag = m.tag AND n.value = m.max_value
// WHERE n.uom = 'pure' AND s.fy = 2022 AND n.value IS NOT NULL
// ORDER BY n.value DESC, s.name, n.tag LIMIT 100

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <algorithm>

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    // Load binary data
    BinCol<SubRec> sub;
    BinCol<NumRec> num;
    if (!sub.load((gendb_dir + "/sub.bin").c_str())) return 1;
    if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;

    // Load dictionaries
    Dict name_dict, tag_dict;
    name_dict.load((gendb_dir + "/dict_name.bin").c_str());
    tag_dict.load((gendb_dir + "/dict_tag.bin").c_str());

    // Get uom_id for 'pure'
    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_pure = metadata_get_id(meta_path, "uom_ids", "pure");
    if (uom_pure == UINT32_MAX) { fprintf(stderr, "No 'pure' uom\n"); uom_pure = 999; }

    // Step 1: Build sub hash map: adsh_id → name_id (only fy=2022)
    std::unordered_map<uint32_t, uint32_t> sub_map;
    sub_map.reserve(sub.count);
    for (uint64_t i = 0; i < sub.count; i++) {
        if (sub.data[i].fy == 2022) {
            sub_map[sub.data[i].adsh_id] = sub.data[i].name_id;
        }
    }

    // Step 2: Scan num (pure, has_value=1) → build max_map: (adsh_id, tag_id) → max_value
    // Use uint64 key: (adsh_id << 32) | tag_id
    std::unordered_map<uint64_t, double> max_map;
    max_map.reserve(4000000);

    for (uint64_t i = 0; i < num.count; i++) {
        const NumRec& r = num.data[i];
        if (r.uom_id != uom_pure || !r.has_value) continue;
        uint64_t key = ((uint64_t)r.adsh_id << 32) | r.tag_id;
        auto it = max_map.find(key);
        if (it == max_map.end()) max_map[key] = r.value;
        else if (r.value > it->second) it->second = r.value;
    }

    // Step 3: Scan num again, collect results for fy=2022 rows where value == max
    struct Row {
        uint32_t name_id, tag_id;
        double value;
    };
    std::vector<Row> results;
    results.reserve(1000);

    for (uint64_t i = 0; i < num.count; i++) {
        const NumRec& r = num.data[i];
        if (r.uom_id != uom_pure || !r.has_value) continue;
        auto sit = sub_map.find(r.adsh_id);
        if (sit == sub_map.end()) continue; // not fy=2022
        uint64_t key = ((uint64_t)r.adsh_id << 32) | r.tag_id;
        auto mit = max_map.find(key);
        if (mit == max_map.end() || r.value != mit->second) continue;
        results.push_back({sit->second, r.tag_id, r.value});
    }

    // Sort: value DESC, name ASC, tag ASC
    std::sort(results.begin(), results.end(), [&](const Row& a, const Row& b) {
        if (a.value != b.value) return a.value > b.value;
        std::string na = name_dict.get(a.name_id), nb = name_dict.get(b.name_id);
        if (na != nb) return na < nb;
        return tag_dict.get(a.tag_id) < tag_dict.get(b.tag_id);
    });

    if (results.size() > 100) results.resize(100);

    // Write output
    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q2.csv").c_str(), "w");
        fprintf(f, "name,tag,value\n");
        for (const auto& r : results) {
            write_csv_str(f, name_dict, r.name_id);
            fputc(',', f);
            write_csv_str(f, tag_dict, r.tag_id);
            fprintf(f, ",%.2f\n", r.value);
        }
        fclose(f);
    }

    return 0;
}
