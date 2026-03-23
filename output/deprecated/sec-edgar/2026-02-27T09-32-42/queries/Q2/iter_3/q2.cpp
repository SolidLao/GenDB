// Q2 iter_3: Single-pass tuple collection + parallel sort + dedup (eliminates 2nd pass)
// Key insight: collect (adsh_id, tag_id, value) tuples for fy=2022, uom=pure in ONE pass
// Then parallel sort by (adsh_id, tag_id, -value) → keep first entry per group (= max)
// This replaces 2-pass + merge + pass2 with 1-pass + sort + linear dedup

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <parallel/algorithm>
#include <vector>

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

    // Single pass: collect (adsh_id, tag_id, value, name_id) tuples in parallel
    struct Tuple { uint32_t adsh_id, tag_id, name_id; double value; };
    struct ThreadData {
        std::vector<Tuple> tuples;
        ThreadData() { tuples.reserve(50000); }
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
                    if (r.uom_id != uom_pure || !r.has_value) continue;
                    if (r.adsh_id >= MAX_ADSH) continue;
                    uint32_t name_id = sub_name[r.adsh_id];
                    if (name_id == UINT32_MAX) continue;
                    tuples.push_back({r.adsh_id, r.tag_id, name_id, r.value});
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Concatenate
    size_t total = 0;
    for (auto& td : tdata) total += td.tuples.size();
    std::vector<Tuple> all_tuples;
    all_tuples.reserve(total);
    for (auto& td : tdata) for (auto& t : td.tuples) all_tuples.push_back(t);
    tdata.clear();

    // Sort by (adsh_id, tag_id, value DESC) → first entry per (adsh,tag) group = max
    __gnu_parallel::sort(all_tuples.begin(), all_tuples.end(),
        [](const Tuple& a, const Tuple& b) {
            if (a.adsh_id != b.adsh_id) return a.adsh_id < b.adsh_id;
            if (a.tag_id != b.tag_id) return a.tag_id < b.tag_id;
            return a.value > b.value; // DESC → first = max
        });

    // Dedup: keep ALL rows with max value per (adsh_id, tag_id) group
    // SQL: value = MAX(value) → keeps ALL rows where value == max (not just first)
    struct Row { uint32_t name_id, tag_id; double value; };
    std::vector<Row> results;
    results.reserve(10000);

    uint64_t idx = 0;
    while (idx < all_tuples.size()) {
        uint32_t a = all_tuples[idx].adsh_id;
        uint32_t t = all_tuples[idx].tag_id;
        double max_val = all_tuples[idx].value; // first = max (sorted value DESC)

        // Keep ALL rows where value == max_val for this (adsh,tag) group
        while (idx < all_tuples.size() &&
               all_tuples[idx].adsh_id == a &&
               all_tuples[idx].tag_id == t) {
            if (all_tuples[idx].value == max_val) {
                results.push_back({all_tuples[idx].name_id, t, max_val});
            }
            idx++;
        }
    }

    // Final sort: value DESC, name ASC, tag ASC
    std::sort(results.begin(), results.end(), [&](const Row& a, const Row& b) {
        if (a.value != b.value) return a.value > b.value;
        const char* na = name_dict.ptr(a.name_id);
        uint32_t la = name_dict.len(a.name_id);
        const char* nb = name_dict.ptr(b.name_id);
        uint32_t lb = name_dict.len(b.name_id);
        int nc = memcmp(na, nb, std::min(la, lb));
        if (nc != 0) return nc < 0;
        if (la != lb) return la < lb;
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
