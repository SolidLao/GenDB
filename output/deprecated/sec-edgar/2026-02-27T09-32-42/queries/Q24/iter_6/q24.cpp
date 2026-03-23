// Q24 iter_6: Replace global parallel sort with bucket sort by adsh_id
// Key improvements over iter_5:
// 1. Two-pass bucket sort instead of GNU parallel sort of 9.6M keys:
//    Pass A: parallel count per adsh_id (64 thread-local cnt arrays, then merge)
//    Pass B: compute prefix sums → adsh_lo (start) + adsh_end (end before unique)
//    Pass C: sequential fill into per-adsh buckets (scatter write)
//    Pass D: parallel per-adsh sort+unique → adsh_hi (end after unique)
// 2. Inner key = (tag_id << 32) | ver_id (no adsh_id stored; indexed by adsh_lo/hi)
// 3. For num scan probe: same inner key for binary search within per-adsh range
// Expected: pre processing from 82ms → ~15ms, Q24 total ~200ms

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <parallel/algorithm>
#include <vector>
#include <cstring>

static const int NTHREADS = 64;
static const uint32_t MAX_ADSH = 90000;

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

    // Build per-adsh sorted inner-key structure via bucket sort
    // inner_key[i] = (tag_id << 32) | ver_id for record i in bucket for adsh_id
    std::vector<uint64_t> inner_keys(pre.count);  // pre-allocated: max entries
    std::vector<uint32_t> adsh_lo(MAX_ADSH, UINT32_MAX); // start of adsh's range
    std::vector<uint32_t> adsh_end(MAX_ADSH, 0);          // end before unique
    std::vector<uint32_t> adsh_hi(MAX_ADSH, 0);           // end after unique

    {
        GENDB_PHASE("pre_count");
        // Pass A: parallel count per adsh_id into thread-local arrays
        // tcnt[t][adsh] = count for thread t's slice
        std::vector<std::vector<uint32_t>> tcnt(NTHREADS, std::vector<uint32_t>(MAX_ADSH, 0));
        {
            uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
            std::vector<std::thread> threads;
            threads.reserve(NTHREADS);
            for (int t = 0; t < NTHREADS; t++) {
                uint64_t start = (uint64_t)t * chunk;
                uint64_t end = std::min(start + chunk, pre.count);
                if (start >= pre.count) break;
                threads.emplace_back([&, t, start, end] {
                    auto* cnt = tcnt[t].data();
                    for (uint64_t i = start; i < end; i++) {
                        uint32_t adsh = pre.data[i].adsh_id;
                        if (adsh < MAX_ADSH) cnt[adsh]++;
                    }
                });
            }
            for (auto& t : threads) t.join();
        }

        // Merge counts and compute prefix sums
        uint32_t pos = 0;
        for (uint32_t a = 0; a < MAX_ADSH; a++) {
            uint32_t total_cnt = 0;
            for (int t = 0; t < NTHREADS; t++) total_cnt += tcnt[t][a];
            if (total_cnt > 0) {
                adsh_lo[a]  = pos;
                adsh_end[a] = pos + total_cnt;
                pos += total_cnt;
            }
        }
        tcnt.clear();  // free 23MB
    }

    {
        GENDB_PHASE("pre_fill");
        // Pass C: sequential scatter fill into per-adsh buckets
        // Use pos_arr[adsh] as running write position within each adsh's bucket
        std::vector<uint32_t> pos_arr(MAX_ADSH, 0);
        for (uint64_t i = 0; i < pre.count; i++) {
            const PreRec& p = pre.data[i];
            uint32_t adsh = p.adsh_id;
            if (adsh < MAX_ADSH && adsh_lo[adsh] != UINT32_MAX) {
                uint32_t idx = adsh_lo[adsh] + pos_arr[adsh]++;
                inner_keys[idx] = ((uint64_t)p.tag_id << 32) | p.ver_id;
            }
        }
    }

    {
        GENDB_PHASE("pre_sort_unique");
        // Pass D: parallel per-adsh sort + unique
        // Each thread handles a contiguous range of adsh_ids
        uint32_t adsh_per_thread = (MAX_ADSH + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint32_t a_start = (uint32_t)t * adsh_per_thread;
            uint32_t a_end = std::min(a_start + adsh_per_thread, MAX_ADSH);
            threads.emplace_back([&, a_start, a_end] {
                for (uint32_t a = a_start; a < a_end; a++) {
                    if (adsh_lo[a] == UINT32_MAX) continue;
                    uint32_t lo = adsh_lo[a];
                    uint32_t hi = adsh_end[a];
                    if (hi <= lo) continue;
                    uint64_t* begin = inner_keys.data() + lo;
                    uint64_t* end   = inner_keys.data() + hi;
                    std::sort(begin, end);                        // sort tiny range (avg 107 entries)
                    auto new_end = std::unique(begin, end);       // dedup
                    adsh_hi[a] = (uint32_t)(new_end - inner_keys.data());
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    // Num scan: collect (agg_key, value) tuples for included rows
    // Anti-join: exclude rows whose (adsh_id, tag_id, ver_id) is in the per-adsh inner_keys
    struct AggTuple { uint64_t key; double value; };
    std::vector<AggTuple> all_tuples;
    {
        GENDB_PHASE("num_scan");
        struct ThreadData {
            std::vector<AggTuple> tuples;
            ThreadData() { tuples.reserve(30000); }
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

                        // Per-adsh binary search anti-join using inner_key
                        uint32_t lo = adsh_lo[r.adsh_id];
                        if (lo != UINT32_MAX) {
                            uint32_t hi = adsh_hi[r.adsh_id];
                            if (hi > lo) {
                                uint64_t probe = ((uint64_t)r.tag_id << 32) | r.ver_id;
                                const uint64_t* base = inner_keys.data() + lo;
                                uint32_t len = hi - lo;
                                uint32_t left = 0, right = len;
                                while (left < right) {
                                    uint32_t mid = (left + right) >> 1;
                                    if (base[mid] < probe) left = mid + 1;
                                    else right = mid;
                                }
                                if (left < len && base[left] == probe) continue;
                            }
                        }
                        // Not in pre → include
                        uint64_t key = ((uint64_t)r.tag_id << 32) | r.ver_id;
                        tuples.push_back({key, r.value});
                    }
                });
            }
            for (auto& t : threads) t.join();
        }

        // Concat using insert (memcpy internally)
        size_t total_tuples = 0;
        for (auto& td : tdata) total_tuples += td.tuples.size();
        all_tuples.reserve(total_tuples);
        for (auto& td : tdata) {
            all_tuples.insert(all_tuples.end(), td.tuples.begin(), td.tuples.end());
        }
        tdata.clear();
    }

    // Sort output tuples + linear aggregation
    {
        GENDB_PHASE("tuple_sort");
        __gnu_parallel::sort(all_tuples.begin(), all_tuples.end(),
            [](const AggTuple& a, const AggTuple& b) { return a.key < b.key; });
    }

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
