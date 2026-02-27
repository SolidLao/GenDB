// Q24 iter_5: Eliminate per-thread vector allocation overhead
// Key improvements over iter_4:
// 1. pre_collect: direct parallel fill of pre_packed[i] = pack_key3(pre[i])
//    All pre records included (no filter), so we can write directly at index i
//    Eliminates: 64×200K reserve allocations + 9.6M push_backs + concat loop
//    Expected: 77ms → ~15ms (just L3 read + write bandwidth)
// 2. num_scan tuples: use vector::insert() for concat (memcpy-based, much faster)
// 3. Keep profiling phases from iter_4

#include "../../../common/query_common.h"
#include "timing_utils.h"
#include <thread>
#include <algorithm>
#include <parallel/algorithm>
#include <vector>
#include <cstring>

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

    // Build packed pre key array via DIRECT PARALLEL FILL (no per-thread vectors!)
    // Since ALL pre records are included (no filter), thread t fills pre_packed[start..end-1]
    std::vector<uint64_t> pre_packed(pre.count);
    {
        GENDB_PHASE("pre_collect");
        uint64_t chunk = (pre.count + NTHREADS - 1) / NTHREADS;
        std::vector<std::thread> threads;
        threads.reserve(NTHREADS);
        for (int t = 0; t < NTHREADS; t++) {
            uint64_t start = (uint64_t)t * chunk;
            uint64_t end = std::min(start + chunk, pre.count);
            if (start >= pre.count) break;
            threads.emplace_back([&, start, end] {
                for (uint64_t i = start; i < end; i++) {
                    const PreRec& p = pre.data[i];
                    pre_packed[i] = pack_key3(p.adsh_id, p.tag_id, p.ver_id);
                }
            });
        }
        for (auto& t : threads) t.join();
    }

    {
        GENDB_PHASE("pre_sort_dedup");
        __gnu_parallel::sort(pre_packed.begin(), pre_packed.end());
        pre_packed.erase(std::unique(pre_packed.begin(), pre_packed.end()), pre_packed.end());
    }
    const uint32_t pre_packed_size = (uint32_t)pre_packed.size();

    // Build per-adsh lo/hi offsets — parallel version
    std::vector<uint32_t> adsh_lo(MAX_ADSH, UINT32_MAX);
    std::vector<uint32_t> adsh_hi(MAX_ADSH, 0);
    {
        GENDB_PHASE("pre_index");
        // Build in parallel: each thread processes a slice of pre_packed
        // Track adsh_id ranges — within sorted array each adsh appears contiguously
        // Use thread-local lo/hi updates, then merge
        struct ThreadIdx {
            // Track which adsh_ids this thread updates
            std::vector<std::pair<uint32_t,uint32_t>> updates; // (adsh_id, first_idx_in_range)
        };
        // For simplicity and correctness, build sequentially (14ms is small)
        for (uint32_t i = 0; i < pre_packed_size; i++) {
            uint32_t adsh = (uint32_t)(pre_packed[i] >> 35);
            if (adsh < MAX_ADSH) {
                if (adsh_lo[adsh] == UINT32_MAX) adsh_lo[adsh] = i;
                adsh_hi[adsh] = i + 1;
            }
        }
    }

    // Num scan: collect (agg_key, value) tuples for included rows
    // Use per-thread vectors but concat with insert() (memcpy) instead of push_back loop
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

                        // Per-adsh binary search anti-join
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
                            if (left < len && base[left] == probe) continue;
                        }
                        uint64_t key = ((uint64_t)r.tag_id << 32) | r.ver_id;
                        tuples.push_back({key, r.value});
                    }
                });
            }
            for (auto& t : threads) t.join();
        }

        // Concat using insert (memcpy internally, much faster than push_back loop)
        size_t total_tuples = 0;
        for (auto& td : tdata) total_tuples += td.tuples.size();
        all_tuples.reserve(total_tuples);
        for (auto& td : tdata) {
            all_tuples.insert(all_tuples.end(), td.tuples.begin(), td.tuples.end());
        }
        tdata.clear();
    }

    // Parallel sort by agg key, then linear aggregation
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
