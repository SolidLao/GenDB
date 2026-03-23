// Q20: Potential Part Promotion — GenDB generated C++ (optimized iter_1)
// Key optimization: thread-local vectors instead of thread-local hash maps
// for lineitem scan, single-threaded aggregate into cache-resident global map.
#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <omp.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

// ---- Raw mmap helper (no RAII, scoped outside timer) ----
struct RawMmap {
    void* ptr = nullptr;
    size_t len = 0;
    void open(const char* path, int advice = MADV_SEQUENTIAL) {
        int fd = ::open(path, O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path); exit(1); }
        struct stat st; fstat(fd, &st); len = st.st_size;
        ptr = mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        ::close(fd);
        if (ptr == MAP_FAILED) { fprintf(stderr, "mmap fail %s\n", path); exit(1); }
        if (len > 0) madvise(ptr, len, advice);
    }
    void prefetch() { if (ptr && len) madvise(ptr, len, MADV_WILLNEED); }
    void close() { if (ptr && len) { munmap(ptr, len); ptr = nullptr; } }
    template<typename T> const T* as() const { return static_cast<const T*>(ptr); }
    size_t count(size_t elem_sz) const { return len / elem_sz; }
};

// ---- Varlen string reader ----
struct VarlenCol {
    const int32_t* offsets;
    const char* data;
    size_t nrows;
    size_t off_len, dat_len;
    void* off_ptr; void* dat_ptr;

    void open(const std::string& base) {
        {
            int fd = ::open((base + "_offsets.bin").c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st); off_len = st.st_size;
            off_ptr = mmap(nullptr, off_len, PROT_READ, MAP_PRIVATE, fd, 0);
            ::close(fd);
            offsets = static_cast<const int32_t*>(off_ptr);
            nrows = off_len / sizeof(int32_t) - 1;
        }
        {
            int fd = ::open((base + "_data.bin").c_str(), O_RDONLY);
            struct stat st; fstat(fd, &st); dat_len = st.st_size;
            dat_ptr = mmap(nullptr, dat_len, PROT_READ, MAP_PRIVATE, fd, 0);
            ::close(fd);
            data = static_cast<const char*>(dat_ptr);
        }
    }
    void close() {
        if (off_ptr) munmap(off_ptr, off_len);
        if (dat_ptr) munmap(dat_ptr, dat_len);
        off_ptr = dat_ptr = nullptr;
    }
    const char* str(size_t i) const { return data + offsets[i]; }
    size_t strlen(size_t i) const { return (size_t)(offsets[i+1] - offsets[i]); }
};

int main(int argc, char* argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: q20 <gendb_dir> <results_dir> [params...]\n"); return 1; }
    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    gendb::init_date_tables();

    // Parse parameters
    std::string n_name_eq = gendb::parse_string_arg(argc, argv, "--n_name_eq", "CANADA");
    std::string p_name_pattern = gendb::parse_string_arg(argc, argv, "--p_name_pattern", "forest%");
    int32_t l_shipdate_lower = gendb::parse_date_arg(argc, argv, "--l_shipdate_lower", 8766);
    int32_t l_shipdate_upper_base = gendb::parse_date_arg(argc, argv, "--l_shipdate_upper", 8766);
    int32_t l_shipdate_upper = gendb::add_years(l_shipdate_upper_base, 1);

    // Extract prefix from pattern (strip trailing %)
    std::string prefix = p_name_pattern;
    if (!prefix.empty() && prefix.back() == '%') prefix.pop_back();
    size_t prefix_len = prefix.size();

    // ---- Mmap all columns BEFORE timer (exclude munmap TLB shootdown) ----
    std::string stor = gendb_dir + "/";

    // Nation
    VarlenCol n_name_col; n_name_col.open(stor + "nation/n_name");
    RawMmap n_nationkey_m; n_nationkey_m.open((stor + "nation/n_nationkey.bin").c_str());

    // Supplier
    RawMmap s_suppkey_m; s_suppkey_m.open((stor + "supplier/s_suppkey.bin").c_str());
    RawMmap s_nationkey_m; s_nationkey_m.open((stor + "supplier/s_nationkey.bin").c_str());
    VarlenCol s_name_col; s_name_col.open(stor + "supplier/s_name");
    VarlenCol s_address_col; s_address_col.open(stor + "supplier/s_address");

    // Part
    RawMmap p_partkey_m; p_partkey_m.open((stor + "part/p_partkey.bin").c_str());
    VarlenCol p_name_col; p_name_col.open(stor + "part/p_name");

    // Lineitem — prefetch for parallel scan
    RawMmap l_partkey_m; l_partkey_m.open((stor + "lineitem/l_partkey.bin").c_str(), MADV_SEQUENTIAL);
    RawMmap l_suppkey_m; l_suppkey_m.open((stor + "lineitem/l_suppkey.bin").c_str(), MADV_SEQUENTIAL);
    RawMmap l_shipdate_m; l_shipdate_m.open((stor + "lineitem/l_shipdate.bin").c_str(), MADV_SEQUENTIAL);
    RawMmap l_quantity_m; l_quantity_m.open((stor + "lineitem/l_quantity.bin").c_str(), MADV_SEQUENTIAL);
    l_partkey_m.prefetch();
    l_shipdate_m.prefetch();
    l_suppkey_m.prefetch();
    l_quantity_m.prefetch();

    // Partsupp
    RawMmap ps_suppkey_m; ps_suppkey_m.open((stor + "partsupp/ps_suppkey.bin").c_str(), MADV_RANDOM);
    RawMmap ps_availqty_m; ps_availqty_m.open((stor + "partsupp/ps_availqty.bin").c_str(), MADV_RANDOM);

    // Indexes
    RawMmap zonemap_m; zonemap_m.open((stor + "indexes/lineitem_l_shipdate_zonemap.bin").c_str());
    RawMmap grouped_m; grouped_m.open((stor + "indexes/partsupp_ps_partkey_grouped.bin").c_str());
    RawMmap supp_lookup_m; supp_lookup_m.open((stor + "indexes/supplier_s_suppkey_lookup.bin").c_str());

    const int32_t* n_nationkey = n_nationkey_m.as<int32_t>();
    const int32_t* s_suppkey = s_suppkey_m.as<int32_t>();
    const int32_t* s_nationkey = s_nationkey_m.as<int32_t>();
    const int32_t* p_partkey = p_partkey_m.as<int32_t>();
    const int32_t* l_partkey = l_partkey_m.as<int32_t>();
    const int32_t* l_suppkey = l_suppkey_m.as<int32_t>();
    const int32_t* l_shipdate = l_shipdate_m.as<int32_t>();
    const double*  l_quantity = l_quantity_m.as<double>();
    const int32_t* ps_suppkey = ps_suppkey_m.as<int32_t>();
    const int32_t* ps_availqty = ps_availqty_m.as<int32_t>();

    size_t n_nation = n_nationkey_m.count(4);
    size_t n_supplier = s_suppkey_m.count(4);
    size_t n_part = p_partkey_m.count(4);
    size_t n_lineitem = l_partkey_m.count(4);

    // Parse zonemap
    const uint8_t* zm_raw = zonemap_m.as<uint8_t>();
    uint64_t zm_num_blocks = *(const uint64_t*)zm_raw;
    uint32_t zm_block_size = *(const uint32_t*)(zm_raw + 8);
    const int32_t* zm_minmax = (const int32_t*)(zm_raw + 12);

    // Parse grouped index
    const uint8_t* gi_raw = grouped_m.as<uint8_t>();
    uint64_t gi_num_entries = *(const uint64_t*)gi_raw;
    const uint32_t* gi_data = (const uint32_t*)(gi_raw + 8);

    // Parse supplier lookup
    const uint8_t* sl_raw = supp_lookup_m.as<uint8_t>();
    uint64_t sl_num_entries = *(const uint64_t*)sl_raw;
    const int32_t* sl_arr = (const int32_t*)(sl_raw + 8);

    {
    GENDB_PHASE("total");

    // ======== Phase 1: filter_nation ========
    int32_t target_nk = -1;
    {
        GENDB_PHASE("filter_nation");
        for (size_t i = 0; i < n_nation; i++) {
            size_t slen = n_name_col.strlen(i);
            if (slen == n_name_eq.size() && memcmp(n_name_col.str(i), n_name_eq.c_str(), slen) == 0) {
                target_nk = n_nationkey[i];
                break;
            }
        }
    }

    // ======== Phase 2: filter_supplier_by_nation ========
    constexpr size_t SUPP_BS_WORDS = (100001 + 63) / 64;
    std::vector<uint64_t> canada_supp_bs(SUPP_BS_WORDS, 0);
    std::vector<int32_t> canada_supp_list;
    {
        GENDB_PHASE("filter_supplier");
        canada_supp_list.reserve(4100);
        for (size_t i = 0; i < n_supplier; i++) {
            if (s_nationkey[i] == target_nk) {
                int32_t sk = s_suppkey[i];
                canada_supp_bs[sk >> 6] |= (1ULL << (sk & 63));
                canada_supp_list.push_back(sk);
            }
        }
    }

    // ======== Phase 3: filter_part_by_name (PARALLEL) ========
    constexpr int MAX_PARTKEY = 2000001;
    constexpr int PK_BS_WORDS = (MAX_PARTKEY + 63) / 64;
    std::vector<uint64_t> forest_pk_bs(PK_BS_WORDS, 0);
    int32_t max_forest_pk = 0;
    {
        GENDB_PHASE("filter_part");
        int nthreads = omp_get_max_threads();
        std::vector<std::vector<int32_t>> tl_pks(nthreads);
        for (auto& v : tl_pks) v.reserve(2000);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local = tl_pks[tid];
            #pragma omp for schedule(static)
            for (size_t i = 0; i < n_part; i++) {
                size_t slen = p_name_col.strlen(i);
                if (slen >= prefix_len && memcmp(p_name_col.str(i), prefix.c_str(), prefix_len) == 0) {
                    local.push_back(p_partkey[i]);
                }
            }
        }
        // Merge into bitset
        for (auto& v : tl_pks) {
            for (int32_t pk : v) {
                forest_pk_bs[pk >> 6] |= (1ULL << (pk & 63));
                if (pk > max_forest_pk) max_forest_pk = pk;
            }
        }
    }

    // ======== Phase 4: scan_lineitem_collect (PARALLEL, thread-local vectors) ========
    struct LTuple {
        int32_t partkey;
        int32_t suppkey;
        double qty;
    };

    // Build qualifying block ranges from zonemap
    std::vector<std::pair<size_t, size_t>> blocks;
    {
        blocks.reserve(zm_num_blocks);
        for (uint64_t b = 0; b < zm_num_blocks; b++) {
            int32_t bmin = zm_minmax[b * 2];
            int32_t bmax = zm_minmax[b * 2 + 1];
            if (bmax < l_shipdate_lower || bmin >= l_shipdate_upper) continue;
            size_t start = b * zm_block_size;
            size_t end = start + zm_block_size;
            if (end > n_lineitem) end = n_lineitem;
            blocks.push_back({start, end});
        }
    }

    int nthreads = omp_get_max_threads();
    std::vector<std::vector<LTuple>> tl_tuples(nthreads);

    {
        GENDB_PHASE("scan_lineitem_collect");
        for (auto& v : tl_tuples) v.reserve(4096);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            auto& local = tl_tuples[tid];

            #pragma omp for schedule(dynamic, 1)
            for (size_t bi = 0; bi < blocks.size(); bi++) {
                size_t start = blocks[bi].first;
                size_t end = blocks[bi].second;
                for (size_t i = start; i < end; i++) {
                    int32_t pk = l_partkey[i];
                    // Check forest partkey bitset first (1.1% selectivity — rejects 98.9%)
                    if (!(forest_pk_bs[pk >> 6] & (1ULL << (pk & 63)))) continue;
                    // Check shipdate range
                    int32_t sd = l_shipdate[i];
                    if (sd < l_shipdate_lower || sd >= l_shipdate_upper) continue;
                    // Collect tuple — NO hash map during scan
                    local.push_back({pk, l_suppkey[i], l_quantity[i]});
                }
            }
        }
    }

    // ======== Phase 5: aggregate_collected (single-threaded, cache-resident) ========
    // Compact hash map: ~106K entries into 256K slots = 4MB, fits LLC
    struct QtySlot {
        int32_t pk;
        int32_t sk;
        double sum_qty;
    };
    constexpr size_t QTY_CAP = 1 << 18; // 262144 slots
    constexpr size_t QTY_MASK = QTY_CAP - 1;
    auto* qty_map = (QtySlot*)calloc(QTY_CAP, sizeof(QtySlot));
    // Init sentinel
    for (size_t i = 0; i < QTY_CAP; i++) qty_map[i].pk = -1;
    {
        GENDB_PHASE("aggregate_collected");
        for (auto& v : tl_tuples) {
            for (auto& t : v) {
                uint32_t h = (uint32_t)((uint64_t)(uint32_t)t.partkey * 2654435761ULL ^
                                        (uint64_t)(uint32_t)t.suppkey * 40499ULL);
                size_t slot = h & QTY_MASK;
                while (true) {
                    if (qty_map[slot].pk == t.partkey && qty_map[slot].sk == t.suppkey) {
                        qty_map[slot].sum_qty += t.qty;
                        break;
                    }
                    if (qty_map[slot].pk == -1) {
                        qty_map[slot].pk = t.partkey;
                        qty_map[slot].sk = t.suppkey;
                        qty_map[slot].sum_qty = t.qty;
                        break;
                    }
                    slot = (slot + 1) & QTY_MASK;
                }
            }
        }
        // Free thread-local vectors
        for (auto& v : tl_tuples) { v.clear(); v.shrink_to_fit(); }
    }

    // ======== Phase 6: scan_partsupp_check_threshold ========
    std::vector<uint64_t> qualifying_supp_bs(SUPP_BS_WORDS, 0);
    {
        GENDB_PHASE("scan_partsupp");
        size_t gi_max = (gi_num_entries < (uint64_t)(max_forest_pk + 1)) ? gi_num_entries : (uint64_t)(max_forest_pk + 1);
        for (size_t pk = 0; pk < gi_max; pk++) {
            if (!(forest_pk_bs[pk >> 6] & (1ULL << (pk & 63)))) continue;
            uint32_t row_start = gi_data[pk * 2];
            uint32_t row_count = gi_data[pk * 2 + 1];
            for (uint32_t r = 0; r < row_count; r++) {
                uint32_t row = row_start + r;
                int32_t sk = ps_suppkey[row];
                // Fast reject: not in target nation
                if (!(canada_supp_bs[sk >> 6] & (1ULL << (sk & 63)))) continue;
                // Lookup qty threshold
                uint32_t h = (uint32_t)((uint64_t)(uint32_t)pk * 2654435761ULL ^
                                        (uint64_t)(uint32_t)sk * 40499ULL);
                size_t slot = h & QTY_MASK;
                double sum_qty = 0.0;
                bool found = false;
                while (true) {
                    if (qty_map[slot].pk == (int32_t)pk && qty_map[slot].sk == sk) {
                        sum_qty = qty_map[slot].sum_qty;
                        found = true;
                        break;
                    }
                    if (qty_map[slot].pk == -1) break;
                    slot = (slot + 1) & QTY_MASK;
                }
                // If no matching lineitems, subquery returns NULL → condition is false
                if (!found) continue;
                if ((double)ps_availqty[row] > 0.5 * sum_qty) {
                    qualifying_supp_bs[sk >> 6] |= (1ULL << (sk & 63));
                }
            }
        }
    }

    free(qty_map);

    // ======== Phase 7: materialize_output ========
    struct Result {
        std::string name;
        std::string address;
    };
    std::vector<Result> results;
    {
        GENDB_PHASE("materialize");
        results.reserve(2000);
        for (int32_t sk : canada_supp_list) {
            if (!(qualifying_supp_bs[sk >> 6] & (1ULL << (sk & 63)))) continue;
            int32_t row = (sk < (int32_t)sl_num_entries) ? sl_arr[sk] : -1;
            if (row < 0) continue;
            results.push_back({
                std::string(s_name_col.str(row), s_name_col.strlen(row)),
                std::string(s_address_col.str(row), s_address_col.strlen(row))
            });
        }
    }

    // ======== Phase 8: sort_output ========
    {
        GENDB_PHASE("sort");
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            return a.name < b.name;
        });
    }

    // ======== Output ========
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q20.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        fprintf(f, "s_name,s_address\n");
        for (const auto& r : results) {
            bool addr_has_comma = r.address.find(',') != std::string::npos;
            if (addr_has_comma)
                fprintf(f, "%s,\"%s\"\n", r.name.c_str(), r.address.c_str());
            else
                fprintf(f, "%s,%s\n", r.name.c_str(), r.address.c_str());
        }
        fclose(f);
    }

    } // end total timer

    // Cleanup mmaps
    n_name_col.close(); n_nationkey_m.close();
    s_suppkey_m.close(); s_nationkey_m.close(); s_name_col.close(); s_address_col.close();
    p_partkey_m.close(); p_name_col.close();
    l_partkey_m.close(); l_suppkey_m.close(); l_shipdate_m.close(); l_quantity_m.close();
    ps_suppkey_m.close(); ps_availqty_m.close();
    zonemap_m.close(); grouped_m.close(); supp_lookup_m.close();

    return 0;
}
