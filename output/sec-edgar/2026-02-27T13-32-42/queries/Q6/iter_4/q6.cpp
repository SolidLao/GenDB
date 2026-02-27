// Q6 iter_4: Parallel dict loading + flat group maps + parallel merge
// Key improvements over iter_3 (which took 435-460ms):
//   1. Parallel dict loading: 3 threads for name/tag/plabel dicts → saves ~40ms
//   2. Flat open-addressing group maps per scan thread (key=uint64_t, val=sum+cnt, 24B/slot)
//   3. Maps initialized inside scan threads → parallel page faults → saves ~20ms
//   4. Parallel merge: 32 merge threads, each handles hash(key)%32 partition
//      Eliminates the 188ms single-threaded merge bottleneck → expect ~10ms
// Expected total: ~200ms vs 435ms
#include <vector>
#include <string>
#include <cstdint>
#include <algorithm>
#include <unordered_map>
#include <thread>
#include <cstdio>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <chrono>
#include "timing_utils.h"

using namespace std;
using hrc = chrono::high_resolution_clock;
static inline double ms_since(hrc::time_point t) { return chrono::duration<double,milli>(hrc::now()-t).count(); }

#pragma pack(push, 1)
struct SubRow { uint32_t adsh_id, cik, name_id; int32_t sic, fy; uint32_t period, filed; uint8_t wksi; char form[11], fp[3], afs[6], countryba[3], countryinc[4], fye[5]; };
struct NumRow { uint32_t adsh_id, tag_id, version_id, ddate; double value; };
struct PreISEntry { uint64_t tv; uint32_t plabel_id; };
#pragma pack(pop)

struct VarDict {
    uint32_t count = 0; vector<const char*> strings; vector<char> data_buf;
    void load(const string& path) {
        FILE* f = fopen(path.c_str(), "rb"); fread(&count, 4, 1, f);
        vector<uint32_t> offsets(count+1); fread(offsets.data(), 4, count+1, f);
        uint32_t ds = offsets[count]; data_buf.resize(ds); fread(data_buf.data(), 1, ds, f); fclose(f);
        strings.resize(count); for (uint32_t i = 0; i < count; i++) strings[i] = data_buf.data() + offsets[i];
    }
    const char* get(uint32_t id) const { return (id < count) ? strings[id] : ""; }
};

static const uint64_t GK_EMPTY = UINT64_MAX;
struct GSlot { uint64_t key; double sum; uint64_t cnt; };

struct FlatGroupMap {
    GSlot* slots = nullptr;
    uint64_t cap, mask;

    void init_and_alloc(uint64_t capacity) {
        cap = capacity; mask = cap - 1;
        slots = (GSlot*)mmap(nullptr, cap * sizeof(GSlot), PROT_READ|PROT_WRITE,
                              MAP_PRIVATE|MAP_ANONYMOUS, -1, 0);
        for (uint64_t i = 0; i < cap; i++) { slots[i].key = GK_EMPTY; slots[i].sum = 0.0; slots[i].cnt = 0; }
    }

    void dealloc() { if (slots) { munmap(slots, cap * sizeof(GSlot)); slots = nullptr; } }

    inline void add(uint64_t key, double v) {
        uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
        uint64_t idx = h & mask;
        while (slots[idx].key != GK_EMPTY && slots[idx].key != key) idx = (idx+1) & mask;
        slots[idx].key = key;
        slots[idx].sum += v;
        slots[idx].cnt++;
    }
};

int main(int argc, char* argv[]) {
    GENDB_PHASE("total");
    if (argc < 3) return 1;
    string gendb_dir = argv[1], results_dir = argv[2];
    auto t0 = hrc::now();

    // Load dicts in parallel (3 threads)
    VarDict name_dict, tag_dict, plabel_dict;
    {
        vector<thread> dt;
        dt.emplace_back([&]() { name_dict.load(gendb_dir + "/name.dict"); });
        dt.emplace_back([&]() { tag_dict.load(gendb_dir + "/tag.dict"); });
        dt.emplace_back([&]() { plabel_dict.load(gendb_dir + "/plabel.dict"); });
        for (auto& t : dt) t.join();
    }
    fprintf(stderr, "[phase] dicts: %.1f ms\n", ms_since(t0));

    // Load sub → adsh_fy[], adsh_name[]
    uint32_t max_adsh = 0;
    vector<SubRow> sub_rows;
    {
        FILE* f = fopen((gendb_dir + "/sub.bin").c_str(), "rb");
        uint64_t count; fread(&count, 8, 1, f);
        sub_rows.resize(count); fread(sub_rows.data(), sizeof(SubRow), count, f); fclose(f);
        for (auto& r : sub_rows) if (r.adsh_id > max_adsh) max_adsh = r.adsh_id;
    }
    max_adsh++;
    vector<int32_t> adsh_fy(max_adsh, -1);
    vector<uint32_t> adsh_name(max_adsh, 0);
    for (auto& r : sub_rows) { adsh_fy[r.adsh_id] = r.fy; adsh_name[r.adsh_id] = r.name_id; }
    sub_rows.clear();
    fprintf(stderr, "[phase] sub: %.1f ms\n", ms_since(t0));

    // Load pre_is.bin (mmap, WILLNEED for random access)
    vector<uint32_t> adsh_count_is, adsh_start_is;
    const PreISEntry* pre_is_data = nullptr;
    size_t pre_is_file_size = 0;
    void* pre_is_mmap = nullptr;
    uint64_t total_is_entries = 0;
    {
        int pfd = open((gendb_dir + "/pre_is.bin").c_str(), O_RDONLY);
        if (pfd < 0) { fprintf(stderr, "ERROR: pre_is.bin not found\n"); return 1; }
        struct stat pst; fstat(pfd, &pst);
        pre_is_file_size = pst.st_size;
        pre_is_mmap = mmap(nullptr, pre_is_file_size, PROT_READ, MAP_PRIVATE, pfd, 0);
        madvise(pre_is_mmap, pre_is_file_size, MADV_WILLNEED);
        close(pfd);

        const char* m = (const char*)pre_is_mmap;
        uint64_t stored_max_adsh; memcpy(&stored_max_adsh, m, 8); m += 8;
        uint32_t smaxA = (uint32_t)stored_max_adsh;

        adsh_count_is.resize(smaxA);
        memcpy(adsh_count_is.data(), m, 4 * smaxA); m += 4 * smaxA;

        adsh_start_is.resize(smaxA + 1);
        memcpy(adsh_start_is.data(), m, 4 * (smaxA + 1)); m += 4 * (smaxA + 1);

        total_is_entries = adsh_start_is[smaxA];
        pre_is_data = reinterpret_cast<const PreISEntry*>(m);

        if (smaxA < max_adsh) max_adsh = smaxA;
    }
    fprintf(stderr, "[phase] pre_is: %.1f ms (entries=%lu)\n", ms_since(t0), (unsigned long)total_is_entries);

    // mmap num_usd.bin
    int fd = open((gendb_dir + "/num_usd.bin").c_str(), O_RDONLY);
    struct stat st; fstat(fd, &st);
    size_t file_size = st.st_size;
    const char* mapped = (const char*)mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0);
    madvise((void*)mapped, file_size, MADV_SEQUENTIAL);
    close(fd);

    uint64_t total_rows;
    memcpy(&total_rows, mapped, 8);
    const NumRow* rows = reinterpret_cast<const NumRow*>(mapped + 8);
    fprintf(stderr, "[phase] num_usd: %.1f ms (rows=%lu)\n", ms_since(t0), (unsigned long)total_rows);

    // Parallel scan: 32 threads with flat group maps (initialized inside threads)
    int num_threads = 32;
    vector<FlatGroupMap*> scan_maps(num_threads, nullptr);

    auto scan_worker = [&](int tid) {
        // Initialize map inside thread for parallel page faults
        FlatGroupMap* gm = new FlatGroupMap();
        gm->init_and_alloc(262144);  // 256K slots × 24B = 6MB per map
        scan_maps[tid] = gm;

        uint64_t start = (uint64_t)tid * total_rows / num_threads;
        uint64_t end_idx = (uint64_t)(tid+1) * total_rows / num_threads;

        for (uint64_t i = start; i < end_idx; i++) {
            const NumRow& r = rows[i];
            if (r.adsh_id >= max_adsh) continue;
            if (adsh_fy[r.adsh_id] != 2023) continue;

            uint32_t adsh = r.adsh_id;
            uint32_t cnt = adsh_count_is[adsh];
            if (cnt == 0) continue;

            uint64_t tv = ((uint64_t)r.tag_id << 20) | r.version_id;
            uint32_t lo = adsh_start_is[adsh];
            uint32_t hi = lo + cnt;

            // Binary search for tv in pre_is
            uint32_t lo2 = lo, hi2 = hi;
            while (lo2 < hi2) {
                uint32_t mid = (lo2 + hi2) >> 1;
                if (pre_is_data[mid].tv < tv) lo2 = mid + 1;
                else hi2 = mid;
            }
            if (lo2 >= hi || pre_is_data[lo2].tv != tv) continue;

            uint32_t name_id = adsh_name[adsh];
            for (uint32_t j = lo2; j < hi && pre_is_data[j].tv == tv; j++) {
                uint32_t plabel_id = pre_is_data[j].plabel_id;
                uint64_t gk = ((uint64_t)name_id << 40) | ((uint64_t)(r.tag_id & 0xFFFFF) << 20) | (plabel_id & 0xFFFFF);
                gm->add(gk, r.value);
            }
        }
    };

    {
        vector<thread> threads;
        for (int i = 0; i < num_threads; i++) threads.emplace_back(scan_worker, i);
        for (auto& t : threads) t.join();
    }
    munmap((void*)mapped, file_size);
    fprintf(stderr, "[phase] scan: %.1f ms\n", ms_since(t0));

    // Parallel merge: 32 merge threads, each handles hash(key)%32
    const int MERGE_T = 32;
    struct ResultEntry { uint64_t gk; double sum; uint64_t cnt; };
    vector<vector<ResultEntry>> partition_results(MERGE_T);

    auto merge_worker = [&](int mt) {
        auto& out = partition_results[mt];
        // Local accumulator using flat hash map
        // ~200K total groups → ~6K per partition at 32 partitions
        const uint64_t LCAP = 16384;  // 16K slots for local accumulation
        const uint64_t LMASK = LCAP - 1;
        struct LSlot { uint64_t key; double sum; uint64_t cnt; };
        static const uint64_t LEMPTY = UINT64_MAX;
        // Use local vector-based flat map to avoid heap fragmentation
        vector<LSlot> lmap(LCAP, {LEMPTY, 0.0, 0});

        for (int tid = 0; tid < num_threads; tid++) {
            const GSlot* slots = scan_maps[tid]->slots;
            uint64_t cap = scan_maps[tid]->cap;
            for (uint64_t j = 0; j < cap; j++) {
                if (slots[j].key == GK_EMPTY) continue;
                uint64_t key = slots[j].key;
                uint64_t h = key ^ (key >> 33); h *= 0xff51afd7ed558ccdULL; h ^= h >> 33;
                if ((int)(h % MERGE_T) != mt) continue;
                // Insert into local flat map
                uint64_t idx = h & LMASK;
                while (lmap[idx].key != LEMPTY && lmap[idx].key != key) idx = (idx+1) & LMASK;
                lmap[idx].key = key;
                lmap[idx].sum += slots[j].sum;
                lmap[idx].cnt += slots[j].cnt;
            }
        }

        // Collect results
        out.reserve(8192);
        for (uint64_t j = 0; j < LCAP; j++) {
            if (lmap[j].key == LEMPTY) continue;
            out.push_back({lmap[j].key, lmap[j].sum, lmap[j].cnt});
        }
    };

    {
        vector<thread> threads;
        for (int i = 0; i < MERGE_T; i++) threads.emplace_back(merge_worker, i);
        for (auto& t : threads) t.join();
    }

    // Free scan maps
    for (int i = 0; i < num_threads; i++) {
        if (scan_maps[i]) { scan_maps[i]->dealloc(); delete scan_maps[i]; }
    }
    munmap(pre_is_mmap, pre_is_file_size);
    fprintf(stderr, "[phase] merge: %.1f ms\n", ms_since(t0));

    // Collect all results
    struct Result { uint32_t name_id, tag_id, plabel_id; double total; uint64_t cnt; };
    vector<Result> results;
    results.reserve(200000);
    for (auto& part : partition_results) {
        for (auto& e : part) {
            uint32_t name_id = (uint32_t)(e.gk >> 40);
            uint32_t tag_id = (uint32_t)((e.gk >> 20) & 0xFFFFF);
            uint32_t plabel_id = (uint32_t)(e.gk & 0xFFFFF);
            results.push_back({name_id, tag_id, plabel_id, e.sum, e.cnt});
        }
    }

    sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
        return a.total > b.total;
    });
    if (results.size() > 200) results.resize(200);
    fprintf(stderr, "[phase] sort: %.1f ms\n", ms_since(t0));

    {
        GENDB_PHASE("output");
        FILE* f = fopen((results_dir + "/Q6.csv").c_str(), "w");
        fprintf(f, "name,stmt,tag,plabel,total_value,cnt\n");
        for (auto& r : results) {
            const char* name = name_dict.get(r.name_id);
            const char* tag = tag_dict.get(r.tag_id);
            const char* plabel = plabel_dict.get(r.plabel_id);
            bool name_q = (strchr(name, ',') || strchr(name, '"'));
            bool plabel_q = (strchr(plabel, ',') || strchr(plabel, '"'));
            if (name_q || plabel_q) {
                string nm(name), pl(plabel);
                auto esc = [](string& s) {
                    if (s.find(',') != string::npos || s.find('"') != string::npos) {
                        string r2 = "\"";
                        for (char c : s) { if (c == '"') r2 += '"'; r2 += c; }
                        r2 += '"'; s = r2;
                    }
                };
                esc(nm); esc(pl);
                fprintf(f, "%s,IS,%s,%s,%.2f,%lu\n", nm.c_str(), tag, pl.c_str(), r.total, r.cnt);
            } else {
                fprintf(f, "%s,IS,%s,%s,%.2f,%lu\n", name, tag, plabel, r.total, r.cnt);
            }
        }
        fclose(f);
    }
    return 0;
}
