// Q20: Potential Part Promotion — GenDB generated C++
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

// ---- Compact hash map for (partkey, suppkey) -> double ----
struct QtyMap {
    struct Entry {
        int32_t pk, sk;
        double qty;
    };
    Entry* entries;
    uint32_t mask;
    size_t capacity;

    void init(size_t cap) {
        capacity = 1;
        while (capacity < cap) capacity <<= 1;
        mask = (uint32_t)(capacity - 1);
        entries = (Entry*)calloc(capacity, sizeof(Entry));
        for (size_t i = 0; i < capacity; i++) entries[i].pk = -1;
    }

    void destroy() { free(entries); entries = nullptr; }

    static inline uint32_t hash(int32_t pk, int32_t sk) {
        return (uint32_t)((uint64_t)(uint32_t)pk * 2654435761ULL ^ (uint64_t)(uint32_t)sk * 40499ULL);
    }

    void add(int32_t pk, int32_t sk, double q) {
        uint32_t slot = hash(pk, sk) & mask;
        while (true) {
            if (entries[slot].pk == -1) {
                entries[slot].pk = pk;
                entries[slot].sk = sk;
                entries[slot].qty = q;
                return;
            }
            if (entries[slot].pk == pk && entries[slot].sk == sk) {
                entries[slot].qty += q;
                return;
            }
            slot = (slot + 1) & mask;
        }
    }

    // Returns sum qty, sets found=true if key exists
    double lookup(int32_t pk, int32_t sk, bool& found) const {
        uint32_t slot = hash(pk, sk) & mask;
        while (true) {
            if (entries[slot].pk == -1) { found = false; return 0.0; }
            if (entries[slot].pk == pk && entries[slot].sk == sk) { found = true; return entries[slot].qty; }
            slot = (slot + 1) & mask;
        }
    }

    void merge(const QtyMap& other) {
        for (size_t i = 0; i < other.capacity; i++) {
            if (other.entries[i].pk != -1) {
                add(other.entries[i].pk, other.entries[i].sk, other.entries[i].qty);
            }
        }
    }
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

    // Lineitem
    RawMmap l_partkey_m; l_partkey_m.open((stor + "lineitem/l_partkey.bin").c_str(), MADV_SEQUENTIAL);
    RawMmap l_suppkey_m; l_suppkey_m.open((stor + "lineitem/l_suppkey.bin").c_str(), MADV_SEQUENTIAL);
    RawMmap l_shipdate_m; l_shipdate_m.open((stor + "lineitem/l_shipdate.bin").c_str(), MADV_SEQUENTIAL);
    RawMmap l_quantity_m; l_quantity_m.open((stor + "lineitem/l_quantity.bin").c_str(), MADV_SEQUENTIAL);

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

    // Parse zonemap: Header: uint64_t num_blocks, uint32_t block_size. Body: int32_t[num_blocks*2] (min,max)
    const uint8_t* zm_raw = zonemap_m.as<uint8_t>();
    uint64_t zm_num_blocks = *(const uint64_t*)zm_raw;
    uint32_t zm_block_size = *(const uint32_t*)(zm_raw + 8);
    const int32_t* zm_minmax = (const int32_t*)(zm_raw + 12);

    // Parse grouped index: Header: uint64_t num_entries. Body: uint32_t[num_entries*2] [start, count, ...]
    const uint8_t* gi_raw = grouped_m.as<uint8_t>();
    uint64_t gi_num_entries = *(const uint64_t*)gi_raw;
    const uint32_t* gi_data = (const uint32_t*)(gi_raw + 8);

    // Parse supplier lookup: Header: uint64_t num_entries. Body: int32_t[num_entries], arr[suppkey] = row_id
    const uint8_t* sl_raw = supp_lookup_m.as<uint8_t>();
    uint64_t sl_num_entries = *(const uint64_t*)sl_raw;
    const int32_t* sl_arr = (const int32_t*)(sl_raw + 8);

    {
    GENDB_PHASE("total");

    // Step 1: Filter nation → target nationkey
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

    // Step 2: Filter supplier by nation → bitset + list
    std::vector<bool> target_suppkey_bs(100001, false);
    std::vector<int32_t> target_suppkey_list;
    {
        GENDB_PHASE("filter_supplier");
        target_suppkey_list.reserve(4100);
        for (size_t i = 0; i < n_supplier; i++) {
            if (s_nationkey[i] == target_nk) {
                int32_t sk = s_suppkey[i];
                target_suppkey_bs[sk] = true;
                target_suppkey_list.push_back(sk);
            }
        }
    }

    // Step 3: Filter part by name prefix → bitset
    constexpr int MAX_PARTKEY = 2000001;
    constexpr int BITSET_WORDS = (MAX_PARTKEY + 63) / 64;
    std::vector<uint64_t> forest_pk_bs(BITSET_WORDS, 0);
    int32_t max_forest_pk = 0;
    {
        GENDB_PHASE("filter_part");
        for (size_t i = 0; i < n_part; i++) {
            size_t slen = p_name_col.strlen(i);
            if (slen >= prefix_len && memcmp(p_name_col.str(i), prefix.c_str(), prefix_len) == 0) {
                int32_t pk = p_partkey[i];
                forest_pk_bs[pk >> 6] |= (1ULL << (pk & 63));
                if (pk > max_forest_pk) max_forest_pk = pk;
            }
        }
    }

    // Step 4: Scan lineitem with zonemap, aggregate qty by (partkey, suppkey)
    QtyMap global_qty;
    {
        GENDB_PHASE("scan_lineitem");

        // Build qualifying block ranges from zonemap
        std::vector<std::pair<size_t, size_t>> blocks;
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

        int nthreads = omp_get_max_threads();
        std::vector<QtyMap> thread_maps(nthreads);
        for (auto& m : thread_maps) m.init(256 * 1024);

        #pragma omp parallel
        {
            int tid = omp_get_thread_num();
            QtyMap& local = thread_maps[tid];

            #pragma omp for schedule(dynamic, 1)
            for (size_t bi = 0; bi < blocks.size(); bi++) {
                size_t start = blocks[bi].first;
                size_t end = blocks[bi].second;
                for (size_t i = start; i < end; i++) {
                    int32_t pk = l_partkey[i];
                    // Check forest partkey bitset first (cheaper filter)
                    if (!(forest_pk_bs[pk >> 6] & (1ULL << (pk & 63)))) continue;
                    // Check shipdate range
                    int32_t sd = l_shipdate[i];
                    if (sd < l_shipdate_lower || sd >= l_shipdate_upper) continue;
                    // Accumulate into thread-local map
                    local.add(pk, l_suppkey[i], l_quantity[i]);
                }
            }
        }

        // Merge thread-local maps
        {
            GENDB_PHASE("merge_maps");
            global_qty.init(256 * 1024);
            for (auto& m : thread_maps) {
                global_qty.merge(m);
                m.destroy();
            }
        }
    }

    // Step 5: Scan partsupp via grouped index, check threshold
    std::vector<bool> qualifying_suppkey_bs(100001, false);
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
                if (!target_suppkey_bs[sk]) continue;
                // Lookup qty threshold — if no matching lineitems, subquery is NULL → skip
                bool found = false;
                double sum_qty = global_qty.lookup((int32_t)pk, sk, found);
                if (!found) continue;
                if ((double)ps_availqty[row] > 0.5 * sum_qty) {
                    qualifying_suppkey_bs[sk] = true;
                }
            }
        }
    }

    // Step 6: Materialize output
    struct Result {
        std::string name;
        std::string address;
    };
    std::vector<Result> results;
    {
        GENDB_PHASE("materialize");
        results.reserve(2000);
        for (int32_t sk : target_suppkey_list) {
            if (!qualifying_suppkey_bs[sk]) continue;
            int32_t row = (sk < (int32_t)sl_num_entries) ? sl_arr[sk] : -1;
            if (row < 0) continue;
            results.push_back({
                std::string(s_name_col.str(row), s_name_col.strlen(row)),
                std::string(s_address_col.str(row), s_address_col.strlen(row))
            });
        }
    }

    // Step 7: Sort by s_name ASC
    {
        GENDB_PHASE("sort");
        std::sort(results.begin(), results.end(), [](const Result& a, const Result& b) {
            return a.name < b.name;
        });
    }

    // Step 8: Output
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q20.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        fprintf(f, "s_name,s_address\n");
        for (const auto& r : results) {
            // Quote fields containing commas for proper CSV
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
