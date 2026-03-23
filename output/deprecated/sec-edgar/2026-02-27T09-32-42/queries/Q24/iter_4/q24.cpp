// Q24 iter_4: Add GENDB_PHASE profiling + replace hash map with tuple sort+agg
// Key improvements over iter_3:
// 1. Profiling phases to identify bottleneck
// 2. Collect (agg_key, cnt_delta, value_delta) tuples → parallel sort → linear aggregation
//    This avoids hash map merge overhead and gives better cache locality
// 3. Deduplicate pre keys during collection (not after sort) using per-thread hash set

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <parallel/algorithm>
#include <vector>
#include <cstring>
#include <unordered_set>

static const int NTHREADS = 64;
static const uint32_t MAX_ADSH = 90000;

static inline uint64_t pack_key3(uint32_t a, uint32_t b, uint32_t c) {
    return ((uint64_t)(a & 0x1FFFF) << 35) | ((uint64_t)(b & 0x3FFFF) << 17) | (c & 0x1FFFF);
}

int main(int argc, char** argv) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE("total");

    BinCol<NumRec> num;
    BinCol<PreRec> pre;
    {
        GENDB_PHASE("load_bins");
        if (!num.load((gendb_dir + "/num.bin").c_str())) return 1;
        if (!pre.load((gendb_dir + "/pre.bin").c_str())) return 1;
    }

    Dict tag_dict, ver_dict;
    tag_dict.load((gendb_dir + "/dict_tag.bin").c_str());
    ver_dict.load((gendb_dir + "/dict_ver.bin").c_str());

    std::string meta_path = gendb_dir + "/metadata.json";
    uint32_t uom_usd = metadata_get_id(meta_path, "uom_ids", "USD");
    if (uom_usd == UINT32_MAX) uom_usd = 999;

    // Build packed pre key array in parallel (all pre records, no filter)
    std::vector<uint64_t> pre_packed;
    {
        GENDB_PHASE("pre_collect");
        struct ThreadPre {
            std::vector<uint64_t> keys;
            ThreadPre() { keys.reserve(200000); }
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
                        keys.push_back(pack_key3(p.adsh_id, p.tag_id, p.ver_id));
                    }
                });
            }
            for (auto& t : threads) t.join();
        }

        // Concatenate
        size_t total_pre = 0;
        for (auto& tp : tpre) total_pre += tp.keys.size();
        pre_packed.reserve(total_pre);
        for (auto& tp : tpre) for (auto k : tp.keys) pre_packed.push_back(k);
        tpre.clear();
    }

    {
        GENDB_PHASE("pre_sort_dedup");
        __gnu_parallel::sort(pre_packed.begin(), pre_packed.end());
        pre_packed.erase(std::unique(pre_packed.begin(), pre_packed.end()), pre_packed.end());
    }
    const uint32_t pre_packed_size = (uint32_t)pre_packed.size();

    // Build per-adsh lo/hi offsets into sorted pre_packed
    std::vector<uint32_t> adsh_lo(MAX_ADSH, UINT32_MAX);
    std::vector<uint32_t> adsh_hi(MAX_ADSH, 0);
    {
        GENDB_PHASE("pre_index");
        for (uint32_t i = 0; i < pre_packed_size; i++) {
            uint32_t adsh = (uint32_t)(pre_packed[i] >> 35);
            if (adsh < MAX_ADSH) {
                if (adsh_lo[adsh] == UINT32_MAX) adsh_lo[adsh] = i;
                adsh_hi[adsh] = i + 1;
            }
        }
    }

    // Collect (agg_key, value) tuples for included num rows
    // agg_key = (tag_id << 32) | ver_id
    struct AggTuple { uint64_t key; double value; };
    std::vector<AggTuple> all_tuples;
    {
        GENDB_PHASE("num_scan");
        struct ThreadData {
            std::vector<AggTuple> tuples;
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
                        if (r.uom_id != uom_usd || !r.has_value) continue;
                        if (r.ddate < 20230101 || r.ddate > 20231231) continue;
                        if (r.adsh_id >= MAX_ADSH) continue;

                        // Per-adsh binary search (tiny range → L1 cache friendly)
                        uint32_t lo = adsh_lo[r.adsh_id];
                        if (lo != UINT32_MAX) {
                            uint32_t hi = adsh_hi[r.adsh_id];
                            uint64_t probe = pack_key3(r.adsh_id, r.tag_id, r.ver_id);
                            const uint64_t* base = pre_packed.data() + lo;
                            uint32_t len = hi - lo;
                            uint32_t left = 0, right = len;
                            while (left < right) {
                                uint32_t mid = (left + right) >> 1;
                                if (base[mid] < probe) left = mid + 1;
                                else right = mid;
                            }
                            if (left < len && base[left] == probe) continue; // anti-join skip
                        }
                        // Not in pre → include
                        uint64_t key = ((uint64_t)r.tag_id << 32) | r.ver_id;
                        tuples.push_back({key, r.value});
                    }
                });
            }
            for (auto& t : threads) t.join();
        }

        // Concatenate
        size_t total_tuples = 0;
        for (auto& td : tdata) total_tuples += td.tuples.size();
        all_tuples.reserve(total_tuples);
        for (auto& td : tdata) for (auto& t : td.tuples) all_tuples.push_back(t);
        tdata.clear();
    }

    // Parallel sort by key, then linear aggregation
    {
        GENDB_PHASE("tuple_sort");
        __gnu_parallel::sort(all_tuples.begin(), all_tuples.end(),
            [](const AggTuple& a, const AggTuple& b) { return a.key < b.key; });
    }

    // Linear aggregation
    struct Row { uint32_t tag_id, ver_id; uint64_t cnt; double total; };
    std::vector<Row> results;
    {
        GENDB_PHASE("aggregate");
        results.reserve(2000);
        uint64_t idx = 0;
        while (idx < all_tuples.size()) {
            uint64_t k = all_tuples[idx].key;
            double sum = 0.0;
            uint64_t cnt = 0;
            while (idx < all_tuples.size() && all_tuples[idx].key == k) {
                sum += all_tuples[idx].value;
                cnt++;
                idx++;
            }
            if (cnt > 10) {
                uint32_t tag_id = (uint32_t)(k >> 32);
                uint32_t ver_id = (uint32_t)(k & 0xFFFFFFFF);
                results.push_back({tag_id, ver_id, cnt, sum});
            }
        }
    }

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
            fprintf(f, ",%lu,%.2f\n", r.cnt, r.total);
        }
        fclose(f);
    }

    return 0;
}
