// Q2 iter_1: Parallel two-pass max-value join
// Find max value per (adsh,tag) for uom='pure', then collect matching rows joined with sub(fy=2022)

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
    if (!sub.load((gendb_dir + "/sub.bin").c_str())) return 1;
    if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;

    Dict name_dict, tag_dict;
    name_dict.load((gendb_dir + "/dict_name.bin").c_str());
    tag_dict.load((gendb_dir + "/dict_tag.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_pure = metadata_get_id(meta_path, "uom_ids", "pure");
    if (uom_pure == UINT32_MAX) uom_pure = 999;

    // Build flat sub array: adsh_id → name_id for fy=2022
    const uint32_t MAX_ADSH = (uint32_t)sub.count + 2;
    std::vector<uint32_t> sub_name(MAX_ADSH, UINT32_MAX);
    for (uint64_t i = 0; i < sub.count; i++) {
        const SubRec& s = sub.data[i];
        if (s.fy == 2022 && s.adsh_id < MAX_ADSH) {
            sub_name[s.adsh_id] = s.name_id;
        }
    }

    // Pass 1: compute max value per (adsh_id, tag_id) in parallel
    // Key: (adsh_id << 32) | tag_id  (both fit in 32 bits)
    struct MaxVal { double val; };
    struct ThreadMax {
        std::unordered_map<uint64_t, double> max_map;
        ThreadMax() { max_map.reserve(1000000); }
    };
    std::vector<ThreadMax> tmaxes(NTHREADS);

    uint64_t chunk = (num.count + NTHREADS - 1) / NTHREADS;
    {
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, num.count);
            if (start >= num.count) break;
            threads.emplace_back([&, t, start, end] {
                auto& mm = tmaxes[t].max_map;
                for (uint64_t i = start; i < end; i++) {
                    const NumRec& r = num.data[i];
                    if (r.uom_id != uom_pure || !r.has_value) continue;
                    uint64_t k = ((uint64_t)r.adsh_id << 32) | r.tag_id;
                    auto it = mm.find(k);
                    if (it == mm.end()) mm[k] = r.value;
                    else if (r.value > it->second) it->second = r.value;
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Merge max maps (take max across threads)
    auto& max_map = tmaxes[0].max_map;
    for (int t = 1; t < NTHREADS; t++) {
        for (auto& [k, v] : tmaxes[t].max_map) {
            auto it = max_map.find(k);
            if (it == max_map.end()) max_map[k] = v;
            else if (v > it->second) it->second = v;
        }
    }

    // Pass 2: collect rows where value == max, joined with sub(fy=2022)
    struct Row {
        uint32_t name_id, tag_id;
        double value;
    };
    struct ThreadRows {
        std::vector<Row> rows;
        ThreadRows() { rows.reserve(1000); }
    };
    std::vector<ThreadRows> trows(NTHREADS);
    {
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, num.count);
            if (start >= num.count) break;
            threads.emplace_back([&, t, start, end] {
                auto& rows = trows[t].rows;
                for (uint64_t i = start; i < end; i++) {
                    const NumRec& r = num.data[i];
                    if (r.uom_id != uom_pure || !r.has_value) continue;
                    if (r.adsh_id >= MAX_ADSH) continue;
                    uint32_t name_id = sub_name[r.adsh_id];
                    if (name_id == UINT32_MAX) continue;
                    uint64_t k = ((uint64_t)r.adsh_id << 32) | r.tag_id;
                    auto it = max_map.find(k);
                    if (it == max_map.end() || r.value != it->second) continue;
                    rows.push_back({name_id, r.tag_id, r.value});
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Collect all results
    std::vector<Row> results;
    results.reserve(50000);
    for (auto& tr : trows) {
        for (auto& r : tr.rows) results.push_back(r);
    }

    // Sort: value DESC, name ASC (by name_id, approximation), tag ASC
    std::sort(results.begin(), results.end(), [&](const Row& a, const Row& b) {
        if (a.value != b.value) return a.value > b.value;
        // Compare name strings
        const char* na = name_dict.ptr(a.name_id);
        uint32_t la = name_dict.len(a.name_id);
        const char* nb = name_dict.ptr(b.name_id);
        uint32_t lb = name_dict.len(b.name_id);
        int nc = memcmp(na, nb, std::min(la, lb));
        if (nc != 0) return nc < 0;
        if (la != lb) return la < lb;
        // Compare tag strings
        const char* ta = tag_dict.ptr(a.tag_id);
        uint32_t lta = tag_dict.len(a.tag_id);
        const char* tb = tag_dict.ptr(b.tag_id);
        uint32_t ltb = tag_dict.len(b.tag_id);
        int tc = memcmp(ta, tb, std::min(lta, ltb));
        if (tc != 0) return tc < 0;
        return lta < ltb;
    });
    if (results.size() > 100) results.resize(100);

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q2.csv").c_str(), "w");
        fprintf(f, "name,tag,value\n");
        for (const auto& r : results) {
            write_csv_str(f, name_dict, r.name_id);
            fputc(',', f);
            write_csv_str(f, tag_dict, r.tag_id);
            fprintf(f, ",%.6f\n", r.value);
        }
        fclose(f);
    }

    return 0;
}
