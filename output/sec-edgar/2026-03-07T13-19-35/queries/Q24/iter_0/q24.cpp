// Q24: Anti-join + aggregate (tag, version) with HAVING cnt > 10, ORDER BY cnt DESC LIMIT 100
// Pipeline: load_anti_set → zone_map_scan_num → anti_join_probe → aggregate_tagver
//           → having_filter → topk_sort → decode_strings
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <cmath>
#include <string>
#include <vector>
#include <algorithm>
#include <unordered_set>
#include <atomic>
#include <thread>
#include <stdexcept>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "timing_utils.h"
#include "mmap_utils.h"

static const size_t TAG_COUNT = 1070663;
static const size_t BLOCK_SIZE = 100000;
static const size_t NUM_ROWS = 39401761;

// Zone map struct matching binary layout (12 bytes with 2-byte pad)
#pragma pack(push, 1)
struct ZoneMap {
    int8_t  min_uom;   // offset 0
    int8_t  max_uom;   // offset 1
    int8_t  _pad[2];   // offset 2-3 (padding to align int32_t)
    int32_t min_ddate; // offset 4
    int32_t max_ddate; // offset 8
};
#pragma pack(pop)
static_assert(sizeof(ZoneMap) == 12, "ZoneMap must be 12 bytes");

// Load usd_code from uom_codes.bin
// Layout: uint8_t N; N x { int8_t code, uint8_t slen, char[slen] }
static int8_t load_usd_code(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("Cannot open " + path);
    uint8_t N;
    fread(&N, 1, 1, f);
    for (int i = 0; i < (int)N; i++) {
        int8_t code;
        uint8_t slen;
        fread(&code, 1, 1, f);
        fread(&slen, 1, 1, f);
        char buf[256] = {};
        fread(buf, 1, slen, f);
        if (slen == 3 && memcmp(buf, "USD", 3) == 0) {
            fclose(f);
            return code;
        }
    }
    fclose(f);
    throw std::runtime_error("USD not found in uom_codes.bin");
}

int main(int argc, char* argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <gendb_dir> <results_dir>\n", argv[0]);
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    GENDB_PHASE_MS("total", t_total);

    // -------------------------------------------------------------------------
    // Phase: data_loading — load anti-set, zone maps, usd_code, mmap columns
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("data_loading");

    // Load usd_code
    int8_t usd_code = load_usd_code(gendb_dir + "/indexes/uom_codes.bin");
    printf("[INFO] usd_code = %d\n", (int)usd_code);

    // Load anti-join set from pre_adsh_tagver_set.bin
    std::unordered_set<uint64_t> anti_set;
    {
        FILE* f = fopen((gendb_dir + "/indexes/pre_adsh_tagver_set.bin").c_str(), "rb");
        if (!f) throw std::runtime_error("Cannot open pre_adsh_tagver_set.bin");
        uint64_t n_unique;
        fread(&n_unique, sizeof(uint64_t), 1, f);
        printf("[INFO] anti_set n_unique = %llu\n", (unsigned long long)n_unique);
        anti_set.reserve(n_unique * 2);
        std::vector<uint64_t> keys(n_unique);
        fread(keys.data(), sizeof(uint64_t), n_unique, f);
        fclose(f);
        for (uint64_t k : keys) anti_set.insert(k);
    }

    // Load zone maps
    std::vector<ZoneMap> zone_maps;
    uint32_t n_blocks_zm = 0;
    {
        FILE* f = fopen((gendb_dir + "/indexes/num_zone_maps.bin").c_str(), "rb");
        if (!f) throw std::runtime_error("Cannot open num_zone_maps.bin");
        fread(&n_blocks_zm, sizeof(uint32_t), 1, f);
        zone_maps.resize(n_blocks_zm);
        fread(zone_maps.data(), sizeof(ZoneMap), n_blocks_zm, f);
        fclose(f);
        printf("[INFO] zone_maps n_blocks = %u\n", n_blocks_zm);
    }

    // mmap num columns
    gendb::MmapColumn<int8_t>  col_uom(gendb_dir + "/num/uom_code.bin");
    gendb::MmapColumn<int32_t> col_ddate(gendb_dir + "/num/ddate.bin");
    gendb::MmapColumn<int32_t> col_adsh(gendb_dir + "/num/adsh_code.bin");
    gendb::MmapColumn<int32_t> col_tagver(gendb_dir + "/num/tagver_code.bin");
    gendb::MmapColumn<double>  col_value(gendb_dir + "/num/value.bin");

    // Prefetch all columns
    mmap_prefetch_all(col_uom, col_ddate, col_adsh, col_tagver, col_value);

    size_t N = NUM_ROWS;
    size_t n_blocks = (N + BLOCK_SIZE - 1) / BLOCK_SIZE;

    // -------------------------------------------------------------------------
    // Phase: main_scan — morsel-driven parallel scan with thread-local aggregation
    // -------------------------------------------------------------------------
    int n_threads = std::thread::hardware_concurrency();
    if (n_threads < 1) n_threads = 1;
    printf("[INFO] n_threads = %d\n", n_threads);

    // Thread-local aggregation arrays
    // Each thread: cnt[TAG_COUNT] + sum[TAG_COUNT]
    std::vector<std::vector<int32_t>> tl_cnt(n_threads, std::vector<int32_t>(TAG_COUNT, 0));
    std::vector<std::vector<double>>  tl_sum(n_threads, std::vector<double>(TAG_COUNT, 0.0));

    std::atomic<size_t> next_block(0);

    {
    GENDB_PHASE("main_scan");

    auto worker = [&](int tid) {
        int32_t* my_cnt = tl_cnt[tid].data();
        double*  my_sum = tl_sum[tid].data();

        while (true) {
            size_t b = next_block.fetch_add(1, std::memory_order_relaxed);
            if (b >= n_blocks) break;

            // Zone map check
            const ZoneMap& zm = zone_maps[b];
            if ((int8_t)zm.min_uom > usd_code || (int8_t)zm.max_uom < usd_code) continue;
            if (zm.max_ddate < 20230101 || zm.min_ddate > 20231231) continue;

            size_t lo = b * BLOCK_SIZE;
            size_t hi = lo + BLOCK_SIZE;
            if (hi > N) hi = N;

            const int8_t*  uom    = col_uom.data;
            const int32_t* ddate  = col_ddate.data;
            const int32_t* adsh   = col_adsh.data;
            const int32_t* tagver = col_tagver.data;
            const double*  value  = col_value.data;

            for (size_t i = lo; i < hi; i++) {
                if (uom[i] != usd_code) continue;
                if (ddate[i] < 20230101 || ddate[i] > 20231231) continue;
                // value IS NOT NULL check (NaN check)
                double v = value[i];
                if (std::isnan(v)) continue;

                // Anti-join probe
                int32_t tc = tagver[i];
                uint64_t key = ((uint64_t)(uint32_t)adsh[i] << 32) | (uint32_t)tc;
                if (anti_set.count(key)) continue; // has pre match → skip

                // Accumulate — guard invalid tagver_code
                if (tc < 0 || (uint32_t)tc >= TAG_COUNT) continue;
                my_cnt[tc]++;
                my_sum[tc] += v;
            }
        }
    };

    std::vector<std::thread> threads;
    threads.reserve(n_threads);
    for (int t = 0; t < n_threads; t++) {
        threads.emplace_back(worker, t);
    }
    for (auto& th : threads) th.join();

    } // main_scan phase

    // -------------------------------------------------------------------------
    // Phase: build_joins — merge thread-local arrays
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("build_joins");

    // Merge into thread 0's arrays
    int32_t* global_cnt = tl_cnt[0].data();
    double*  global_sum = tl_sum[0].data();
    for (int t = 1; t < n_threads; t++) {
        const int32_t* tc = tl_cnt[t].data();
        const double*  ts = tl_sum[t].data();
        for (size_t k = 0; k < TAG_COUNT; k++) {
            global_cnt[k] += tc[k];
            global_sum[k] += ts[k];
        }
    }

    // -------------------------------------------------------------------------
    // Phase: dim_filter — HAVING cnt > 10 + topk sort
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("dim_filter");

    struct Group {
        int32_t tagver_code;
        int32_t cnt;
        double  total;
    };

    std::vector<Group> groups;
    groups.reserve(4096);
    for (size_t k = 0; k < TAG_COUNT; k++) {
        if (global_cnt[k] > 10) {
            groups.push_back({(int32_t)k, global_cnt[k], global_sum[k]});
        }
    }
    printf("[INFO] groups after HAVING: %zu\n", groups.size());

    // Sort by cnt DESC
    std::sort(groups.begin(), groups.end(), [](const Group& a, const Group& b) {
        return a.cnt > b.cnt;
    });

    // Limit 100
    if (groups.size() > 100) groups.resize(100);

    // -------------------------------------------------------------------------
    // Phase: output — decode strings and write CSV
    // -------------------------------------------------------------------------
    {
    GENDB_PHASE("output");

    // Load tag string data
    gendb::MmapColumn<uint32_t> tag_offsets(gendb_dir + "/tag/tag_offsets.bin");
    gendb::MmapColumn<char>     tag_data(gendb_dir + "/tag/tag_data.bin");
    gendb::MmapColumn<uint32_t> ver_offsets(gendb_dir + "/tag/version_offsets.bin");
    gendb::MmapColumn<char>     ver_data(gendb_dir + "/tag/version_data.bin");

    // Create results directory
    std::string mkdir_cmd = "mkdir -p " + results_dir;
    system(mkdir_cmd.c_str());

    std::string out_path = results_dir + "/Q24.csv";
    FILE* out = fopen(out_path.c_str(), "w");
    if (!out) throw std::runtime_error("Cannot open output file: " + out_path);

    fprintf(out, "tag,version,cnt,total\n");
    for (const auto& g : groups) {
        uint32_t tc = (uint32_t)g.tagver_code;
        // Decode tag string
        uint32_t tag_start = tag_offsets.data[tc];
        uint32_t tag_end   = tag_offsets.data[tc + 1];
        std::string tag_str(tag_data.data + tag_start, tag_end - tag_start);
        // Decode version string
        uint32_t ver_start = ver_offsets.data[tc];
        uint32_t ver_end   = ver_offsets.data[tc + 1];
        std::string ver_str(ver_data.data + ver_start, ver_end - ver_start);

        fprintf(out, "%s,%s,%d,%.2f\n",
                tag_str.c_str(), ver_str.c_str(), g.cnt, g.total);
    }
    fclose(out);
    printf("[INFO] Written %zu rows to %s\n", groups.size(), out_path.c_str());

    } // output phase
    } // dim_filter phase
    } // build_joins phase
    } // data_loading phase

    return 0;
}
