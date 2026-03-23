#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <atomic>
#include <bitset>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "timing_utils.h"
#include "cli_params.h"
#include "mmap_utils.h"

// ---- Compact mmap helper (no MAP_POPULATE, demand-paged) ----
struct RawMmap {
    void* ptr = nullptr;
    size_t len = 0;
    int fd = -1;

    RawMmap() = default;
    RawMmap(const std::string& path, int advice = MADV_SEQUENTIAL) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        len = st.st_size;
        if (len == 0) { ::close(fd); fd = -1; return; }
        ptr = mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path.c_str()); exit(1); }
        madvise(ptr, len, advice);
#ifdef MADV_HUGEPAGE
        madvise(ptr, len, MADV_HUGEPAGE);
#endif
    }

    template<typename T> const T* as() const { return static_cast<const T*>(ptr); }
    template<typename T> size_t count() const { return len / sizeof(T); }

    void close() {
        if (ptr && len > 0) munmap(ptr, len);
        if (fd >= 0) ::close(fd);
        ptr = nullptr; len = 0; fd = -1;
    }
    ~RawMmap() { close(); }
    RawMmap(const RawMmap&) = delete;
    RawMmap& operator=(const RawMmap&) = delete;
    RawMmap(RawMmap&& o) noexcept : ptr(o.ptr), len(o.len), fd(o.fd) { o.ptr=nullptr; o.len=0; o.fd=-1; }
    RawMmap& operator=(RawMmap&& o) noexcept { if(this!=&o){close(); ptr=o.ptr;len=o.len;fd=o.fd; o.ptr=nullptr;o.len=0;o.fd=-1;} return *this; }
};

struct CandidatePair {
    int32_t orderkey;
    int32_t suppkey;
};

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();
    int64_t limit = gendb::parse_int_arg(argc, argv, "--limit", 100);
    std::string n_name_eq = gendb::parse_string_arg(argc, argv, "--n_name_eq", "SAUDI ARABIA");
    std::string o_orderstatus_eq_str = gendb::parse_string_arg(argc, argv, "--o_orderstatus_eq", "F");
    char o_orderstatus_eq = o_orderstatus_eq_str[0];

    std::string S = gendb_dir + "/";

    // Declare all mmaps at outer scope (RAII scoping: munmap after timing)
    RawMmap mm_n_nationkey, mm_n_name_offsets, mm_n_name_data;
    RawMmap mm_s_suppkey, mm_s_nationkey;
    RawMmap mm_l_orderkey, mm_l_suppkey, mm_l_receiptdate, mm_l_commitdate;
    RawMmap mm_o_orderstatus;
    RawMmap mm_orders_idx, mm_li_grouped_idx;
    RawMmap mm_s_name_offsets, mm_s_name_data;

    {
    GENDB_PHASE("total");

    // ---- Data Loading ----
    {
        GENDB_PHASE("data_loading");
        mm_n_nationkey = RawMmap(S + "nation/n_nationkey.bin");
        mm_n_name_offsets = RawMmap(S + "nation/n_name_offsets.bin");
        mm_n_name_data = RawMmap(S + "nation/n_name_data.bin");
        mm_s_suppkey = RawMmap(S + "supplier/s_suppkey.bin");
        mm_s_nationkey = RawMmap(S + "supplier/s_nationkey.bin");
        mm_l_orderkey = RawMmap(S + "lineitem/l_orderkey.bin");
        mm_l_suppkey = RawMmap(S + "lineitem/l_suppkey.bin");
        mm_l_receiptdate = RawMmap(S + "lineitem/l_receiptdate.bin");
        mm_l_commitdate = RawMmap(S + "lineitem/l_commitdate.bin");
        mm_o_orderstatus = RawMmap(S + "orders/o_orderstatus.bin", MADV_RANDOM);
        mm_orders_idx = RawMmap(S + "indexes/orders_o_orderkey_lookup.bin", MADV_RANDOM);
        mm_li_grouped_idx = RawMmap(S + "indexes/lineitem_l_orderkey_grouped.bin", MADV_RANDOM);
        mm_s_name_offsets = RawMmap(S + "supplier/s_name_offsets.bin");
        mm_s_name_data = RawMmap(S + "supplier/s_name_data.bin");
    }

    // ---- Step 1: Filter nation + supplier → SA suppkey bitset ----
    const int32_t* n_nationkey = mm_n_nationkey.as<int32_t>();
    const uint32_t* n_name_offsets = mm_n_name_offsets.as<uint32_t>();
    const char* n_name_data = mm_n_name_data.as<char>();
    size_t n_nations = mm_n_nationkey.count<int32_t>();

    int32_t sa_nationkey = -1;
    {
        GENDB_PHASE("dim_filter");
        for (size_t i = 0; i < n_nations; i++) {
            uint32_t off = n_name_offsets[i];
            uint32_t next_off = n_name_offsets[i + 1];
            size_t slen = next_off - off;
            if (slen == n_name_eq.size() && memcmp(n_name_data + off, n_name_eq.data(), slen) == 0) {
                sa_nationkey = n_nationkey[i];
                break;
            }
        }
    }

    // Build SA suppkey bitset
    static constexpr int MAX_SUPPKEY = 100001;
    std::vector<bool> sa_bitset(MAX_SUPPKEY, false);
    const int32_t* s_suppkey = mm_s_suppkey.as<int32_t>();
    const int32_t* s_nationkey = mm_s_nationkey.as<int32_t>();
    size_t n_suppliers = mm_s_suppkey.count<int32_t>();

    for (size_t i = 0; i < n_suppliers; i++) {
        if (s_nationkey[i] == sa_nationkey) {
            int32_t sk = s_suppkey[i];
            if (sk >= 0 && sk < MAX_SUPPKEY) sa_bitset[sk] = true;
        }
    }

    // ---- Step 2: Scan lineitem candidates (two-pass parallel scatter) ----
    const int32_t* l_orderkey = mm_l_orderkey.as<int32_t>();
    const int32_t* l_suppkey = mm_l_suppkey.as<int32_t>();
    const int32_t* l_receiptdate = mm_l_receiptdate.as<int32_t>();
    const int32_t* l_commitdate = mm_l_commitdate.as<int32_t>();
    size_t n_lineitem = mm_l_orderkey.count<int32_t>();

    int nthreads = std::thread::hardware_concurrency();
    if (nthreads < 1) nthreads = 1;
    if (nthreads > 64) nthreads = 64;

    CandidatePair* candidates = nullptr;
    size_t total_candidates = 0;

    {
        GENDB_PHASE("main_scan");

        constexpr size_t MORSEL = 65536;
        size_t n_morsels = (n_lineitem + MORSEL - 1) / MORSEL;

        // Pass 1: count per morsel
        std::vector<size_t> morsel_counts(n_morsels, 0);

        #pragma omp parallel for schedule(dynamic, 4) num_threads(nthreads)
        for (size_t m = 0; m < n_morsels; m++) {
            size_t start = m * MORSEL;
            size_t end = std::min(start + MORSEL, n_lineitem);
            size_t cnt = 0;
            for (size_t i = start; i < end; i++) {
                int32_t sk = l_suppkey[i];
                if (sk >= 0 && sk < MAX_SUPPKEY && sa_bitset[sk] && l_receiptdate[i] > l_commitdate[i]) {
                    cnt++;
                }
            }
            morsel_counts[m] = cnt;
        }

        // Prefix sum
        std::vector<size_t> morsel_offsets(n_morsels + 1, 0);
        for (size_t m = 0; m < n_morsels; m++) {
            morsel_offsets[m + 1] = morsel_offsets[m] + morsel_counts[m];
        }
        total_candidates = morsel_offsets[n_morsels];

        candidates = (CandidatePair*)malloc(total_candidates * sizeof(CandidatePair));

        // Pass 2: scatter
        #pragma omp parallel for schedule(dynamic, 4) num_threads(nthreads)
        for (size_t m = 0; m < n_morsels; m++) {
            size_t start = m * MORSEL;
            size_t end = std::min(start + MORSEL, n_lineitem);
            size_t pos = morsel_offsets[m];
            for (size_t i = start; i < end; i++) {
                int32_t sk = l_suppkey[i];
                if (sk >= 0 && sk < MAX_SUPPKEY && sa_bitset[sk] && l_receiptdate[i] > l_commitdate[i]) {
                    candidates[pos].orderkey = l_orderkey[i];
                    candidates[pos].suppkey = sk;
                    pos++;
                }
            }
        }
    }

    // ---- Step 3 + 4: Check order status + EXISTS/NOT EXISTS ----
    // orders_o_orderkey_lookup: Header uint64_t num_entries, then int32_t[num_entries]
    const uint8_t* orders_idx_raw = mm_orders_idx.as<uint8_t>();
    uint64_t orders_idx_n = *reinterpret_cast<const uint64_t*>(orders_idx_raw);
    const int32_t* orders_lookup = reinterpret_cast<const int32_t*>(orders_idx_raw + 8);

    const int8_t* o_orderstatus = mm_o_orderstatus.as<int8_t>();

    // lineitem_l_orderkey_grouped: Header uint64_t num_entries, then uint32_t[num_entries*2] interleaved [start, count, ...]
    const uint8_t* li_grp_raw = mm_li_grouped_idx.as<uint8_t>();
    uint64_t li_grp_n = *reinterpret_cast<const uint64_t*>(li_grp_raw);
    const uint32_t* li_grp_data = reinterpret_cast<const uint32_t*>(li_grp_raw + 8);

    // Aggregation: count by suppkey using atomic array
    std::vector<std::atomic<int32_t>> count_by_suppkey(MAX_SUPPKEY);
    for (int i = 0; i < MAX_SUPPKEY; i++) count_by_suppkey[i].store(0, std::memory_order_relaxed);

    {
        GENDB_PHASE("check_and_aggregate");

        #pragma omp parallel for schedule(dynamic, 1024) num_threads(nthreads)
        for (size_t c = 0; c < total_candidates; c++) {
            int32_t ok = candidates[c].orderkey;
            int32_t sk = candidates[c].suppkey;

            // Check order status
            if (ok < 0 || (uint64_t)ok >= orders_idx_n) continue;
            int32_t order_rowid = orders_lookup[ok];
            if (order_rowid < 0) continue;
            if (o_orderstatus[order_rowid] != o_orderstatus_eq) continue;

            // EXISTS + NOT EXISTS check via grouped index
            if (ok < 0 || (uint64_t)ok >= li_grp_n) continue;
            uint32_t li_start = li_grp_data[ok * 2];
            uint32_t li_count = li_grp_data[ok * 2 + 1];

            bool exists_other = false;
            bool not_exists_violated = false;

            for (uint32_t j = 0; j < li_count; j++) {
                uint32_t row = li_start + j;
                int32_t other_sk = l_suppkey[row];
                if (other_sk != sk) {
                    exists_other = true;
                    // Check if this other supplier was also late
                    if (l_receiptdate[row] > l_commitdate[row]) {
                        not_exists_violated = true;
                        break;  // early termination
                    }
                }
            }

            if (exists_other && !not_exists_violated) {
                count_by_suppkey[sk].fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    free(candidates);

    // ---- Step 5 + 6: Aggregate + Top-K output ----
    {
        GENDB_PHASE("output");

        // Gather nonzero counts
        struct Result {
            int32_t suppkey;
            int32_t numwait;
        };
        std::vector<Result> results;
        results.reserve(4000);
        for (int i = 0; i < MAX_SUPPKEY; i++) {
            int32_t cnt = count_by_suppkey[i].load(std::memory_order_relaxed);
            if (cnt > 0) {
                results.push_back({(int32_t)i, cnt});
            }
        }

        // Resolve s_name for each result
        // supplier_s_suppkey_lookup not strictly needed since s_suppkey == row_id in TPC-H
        // but let's use s_name_offsets directly
        const uint32_t* s_name_offsets = mm_s_name_offsets.as<uint32_t>();
        const char* s_name_data_ptr = mm_s_name_data.as<char>();

        struct OutputRow {
            std::string s_name;
            int32_t numwait;
        };
        std::vector<OutputRow> output;
        output.reserve(results.size());

        for (auto& r : results) {
            // suppkey is 1-based in TPC-H, row_id is 0-based
            // s_suppkey[i] = i+1 typically, but let's use suppkey-1 as row index
            int32_t row = r.suppkey - 1;
            if (row < 0 || (size_t)row >= n_suppliers) continue;
            uint32_t off = s_name_offsets[row];
            uint32_t next_off = s_name_offsets[row + 1];
            std::string name(s_name_data_ptr + off, next_off - off);
            output.push_back({std::move(name), r.numwait});
        }

        // Sort: numwait DESC, s_name ASC
        size_t k = std::min((size_t)limit, output.size());
        std::partial_sort(output.begin(), output.begin() + k, output.end(),
            [](const OutputRow& a, const OutputRow& b) {
                if (a.numwait != b.numwait) return a.numwait > b.numwait;
                return a.s_name < b.s_name;
            });

        // Write CSV
        std::string out_path = results_dir + "/Q21.csv";
        FILE* fp = fopen(out_path.c_str(), "w");
        fprintf(fp, "s_name,numwait\n");
        for (size_t i = 0; i < k; i++) {
            fprintf(fp, "\"%s\",%d\n", output[i].s_name.c_str(), output[i].numwait);
        }
        fclose(fp);
    }

    } // end total timer
    return 0;
}
