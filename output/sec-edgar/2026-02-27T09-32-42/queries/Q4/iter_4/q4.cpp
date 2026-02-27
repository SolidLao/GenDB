// Q4 iter_4: Parallel tag_map build + per-adsh EQ index + GNU parallel sort
// Key improvement over iter_3: build tag_map in parallel (64 threads)
// tag_map build was the main bottleneck at ~100-150ms (1.07M sequential inserts)
// With parallel build: 64 thread-local maps → merge → ~30-50ms total

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
    if (uom_usd == UINT32_MAX) uom_usd = 999;
    uint32_t stmt_eq = metadata_get_id(meta_path, "stmt_ids", "EQ");
    if (stmt_eq == UINT32_MAX) stmt_eq = 4;

    // Build flat sub array
    const uint32_t MAX_ADSH = (uint32_t)sub.count + 2;
    struct SubInfo { int32_t sic; uint32_t cik; };
    std::vector<SubInfo> sub_arr(MAX_ADSH, {-1, 0});
    std::vector<bool> sub_valid(MAX_ADSH, false);
    for (uint64_t i = 0; i < sub.count; i++) {
        const SubRec& s = sub.data[i];
        if (s.sic >= 4000 && s.sic <= 4999 && s.adsh_id < MAX_ADSH) {
            sub_arr[s.adsh_id] = {s.sic, s.cik};
            sub_valid[s.adsh_id] = true;
        }
    }

    // Build tag_map in parallel: packed(tag_id, ver_id) → tlabel_id (abstract=0)
    // 64 thread-local unordered_maps → merge
    struct TagThreadData {
        std::unordered_map<uint64_t, uint32_t> map;
        TagThreadData() { map.reserve(20000); }
    };
    std::vector<TagThreadData> ttag(NTHREADS);
    {
        uint64_t chunk = (tag.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, tag.count);
            if (start >= tag.count) break;
            threads.emplace_back([&, t, start, end] {
                auto& m = ttag[t].map;
                for (uint64_t i = start; i < end; i++) {
                    const TagRec& r = tag.data[i];
                    if (r.abstract == 0) {
                        uint64_t k = ((uint64_t)r.tag_id << 32) | r.ver_id;
                        m[k] = r.tlabel_id;
                    }
                }
            });
        }
        for (auto& t : threads) t.join();
    }
    // Merge thread-local tag maps into one flat sorted array for binary search
    // (sorted array of uint64_key → avoids large merged hash map)
    struct TagEntry { uint64_t key; uint32_t tlabel_id; };
    size_t total_tags = 0;
    for (auto& td : ttag) total_tags += td.map.size();
    std::vector<TagEntry> tag_arr;
    tag_arr.reserve(total_tags);
    for (auto& td : ttag) {
        for (auto& [k, v] : td.map) tag_arr.push_back({k, v});
    }
    ttag.clear();
    __gnu_parallel::sort(tag_arr.begin(), tag_arr.end(),
        [](const TagEntry& a, const TagEntry& b) { return a.key < b.key; });

    // Build pre_eq_raw in parallel (stmt='EQ' only, NOT deduplicated)
    struct ThreadPre {
        std::vector<uint64_t> keys;
        ThreadPre() { keys.reserve(50000); }
    };
    std::vector<ThreadPre> tpre(NTHREADS);
    {
        uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, pre.count);
            if (start >= pre.count) break;
            threads.emplace_back([&, t, start, end] {
                auto& keys = tpre[t].keys;
                for (uint64_t i = start; i < end; i++) {
                    const PreRec& p = pre.data[i];
                    if (p.stmt_id == stmt_eq) {
                        keys.push_back(pack_key3(p.adsh_id, p.tag_id, p.ver_id));
                    }
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    size_t total_eq = 0;
    for (auto& tp : tpre) total_eq += tp.keys.size();
    std::vector<uint64_t> pre_eq_raw;
    pre_eq_raw.reserve(total_eq);
    for (auto& tp : tpre) for (auto k : tp.keys) pre_eq_raw.push_back(k);
    tpre.clear();

    __gnu_parallel::sort(pre_eq_raw.begin(), pre_eq_raw.end());
    const uint32_t pre_eq_size = (uint32_t)pre_eq_raw.size();

    // Build per-adsh lo/hi offsets
    std::vector<uint32_t> adsh_lo(MAX_ADSH, UINT32_MAX);
    std::vector<uint32_t> adsh_hi(MAX_ADSH, 0);
    for (uint32_t i = 0; i < pre_eq_size; i++) {
        uint32_t adsh = (uint32_t)(pre_eq_raw[i] >> 35);
        if (adsh < MAX_ADSH) {
            if (adsh_lo[adsh] == UINT32_MAX) adsh_lo[adsh] = i;
            adsh_hi[adsh] = i + 1;
        }
    }

    // Thread-local tuple collection
    struct Tuple { uint64_t agg_key; double sum; uint64_t count; uint32_t cik; };
    struct ThreadData {
        std::vector<Tuple> tuples;
        ThreadData() { tuples.reserve(100000); }
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
                    if (r.adsh_id >= MAX_ADSH || !sub_valid[r.adsh_id]) continue;

                    // tag lookup (sorted array binary search)
                    uint64_t tk = ((uint64_t)r.tag_id << 32) | r.ver_id;
                    // Binary search in tag_arr
                    uint32_t tlo = 0, thi = (uint32_t)tag_arr.size();
                    while (tlo < thi) {
                        uint32_t tmid = (tlo + thi) >> 1;
                        if (tag_arr[tmid].key < tk) tlo = tmid + 1;
                        else thi = tmid;
                    }
                    if (tlo >= tag_arr.size() || tag_arr[tlo].key != tk) continue;
                    uint32_t tlabel_id = tag_arr[tlo].tlabel_id;

                    // pre_eq per-adsh binary search
                    uint32_t lo = adsh_lo[r.adsh_id];
                    if (lo == UINT32_MAX) continue;
                    uint32_t hi = adsh_hi[r.adsh_id];

                    uint64_t pk = pack_key3(r.adsh_id, r.tag_id, r.ver_id);
                    const uint64_t* base = pre_eq_raw.data() + lo;
                    uint32_t len = hi - lo;

                    uint32_t left = 0, right = len;
                    while (left < right) {
                        uint32_t mid = (left + right) >> 1;
                        if (base[mid] < pk) left = mid + 1;
                        else right = mid;
                    }
                    if (left >= len || base[left] != pk) continue;

                    // upper_bound for count
                    uint32_t left2 = left + 1, right2 = len;
                    while (left2 < right2) {
                        uint32_t mid = (left2 + right2) >> 1;
                        if (base[mid] <= pk) left2 = mid + 1;
                        else right2 = mid;
                    }
                    uint32_t pre_count = left2 - left;

                    int32_t sic = sub_arr[r.adsh_id].sic;
                    uint32_t cik = sub_arr[r.adsh_id].cik;
                    uint64_t agg_key = ((uint64_t)(uint32_t)sic << 32) | tlabel_id;

                    tuples.push_back({agg_key, r.value * pre_count, pre_count, cik});
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    size_t total_tuples = 0;
    for (auto& td : tdata) total_tuples += td.tuples.size();
    std::vector<Tuple> all_tuples;
    all_tuples.reserve(total_tuples);
    for (auto& td : tdata) for (auto& t : td.tuples) all_tuples.push_back(t);
    tdata.clear();

    __gnu_parallel::sort(all_tuples.begin(), all_tuples.end(),
        [](const Tuple& a, const Tuple& b) {
            if (a.agg_key != b.agg_key) return a.agg_key < b.agg_key;
            return a.cik < b.cik;
        });

    struct Row { int32_t sic; uint32_t tlabel_id, num_companies; double total_value, avg_value; };
    std::vector<Row> results;
    results.reserve(1000);

    uint64_t idx = 0;
    while (idx < all_tuples.size()) {
        uint64_t agg_key = all_tuples[idx].agg_key;
        double total_sum = 0.0;
        uint64_t total_cnt = 0;
        uint32_t distinct_ciks = 0;
        uint32_t last_cik = UINT32_MAX;

        while (idx < all_tuples.size() && all_tuples[idx].agg_key == agg_key) {
            if (all_tuples[idx].cik != last_cik) {
                distinct_ciks++;
                last_cik = all_tuples[idx].cik;
            }
            total_sum += all_tuples[idx].sum;
            total_cnt += all_tuples[idx].count;
            idx++;
        }
        if (distinct_ciks >= 2) {
            int32_t sic = (int32_t)(agg_key >> 32);
            uint32_t tlabel_id = (uint32_t)(agg_key & 0xFFFFFFFF);
            double avg_val = (total_cnt > 0) ? (total_sum / total_cnt) : 0.0;
            results.push_back({sic, tlabel_id, distinct_ciks, total_sum, avg_val});
        }
    }

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
            fprintf(f, ",EQ,%u,%.2f,%.6f\n", r.num_companies, r.total_value, r.avg_value);
        }
        fclose(f);
    }

    return 0;
}
