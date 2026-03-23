#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <atomic>
#include <thread>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

#include "timing_utils.h"
#include "date_utils.h"
#include "cli_params.h"

// ---- Raw mmap helper (no RAII, for outer-scope resource scoping) ----
struct RawMmap {
    void* ptr = nullptr;
    size_t len = 0;
    int fd = -1;

    void open(const char* path) {
        fd = ::open(path, O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open %s\n", path); exit(1); }
        struct stat st;
        fstat(fd, &st);
        len = st.st_size;
        ptr = mmap(nullptr, len, PROT_READ, MAP_PRIVATE, fd, 0);
        if (ptr == MAP_FAILED) { fprintf(stderr, "mmap failed %s\n", path); exit(1); }
    }

    void advise_sequential() {
        if (ptr && len > 0) madvise(ptr, len, MADV_SEQUENTIAL);
    }

    void advise_hugepage() {
#ifdef MADV_HUGEPAGE
        if (ptr && len > 0) madvise(ptr, len, MADV_HUGEPAGE);
#endif
    }

    void close() {
        if (ptr && len > 0) munmap(ptr, len);
        if (fd >= 0) ::close(fd);
        ptr = nullptr; len = 0; fd = -1;
    }

    template<typename T> const T* as() const { return static_cast<const T*>(ptr); }
    template<typename T> size_t count() const { return len / sizeof(T); }
};

// ---- Dict loader ----
struct Dict {
    std::vector<std::string> entries;

    void load(const std::string& path) {
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) { fprintf(stderr, "Cannot open dict %s\n", path.c_str()); exit(1); }
        struct stat st; fstat(fd, &st);
        std::vector<char> buf(st.st_size);
        ::read(fd, buf.data(), st.st_size);
        ::close(fd);

        const char* p = buf.data();
        uint32_t count = *reinterpret_cast<const uint32_t*>(p); p += 4;
        entries.resize(count);
        for (uint32_t i = 0; i < count; i++) {
            uint16_t slen = *reinterpret_cast<const uint16_t*>(p); p += 2;
            entries[i] = std::string(p, slen);
            p += slen;
        }
    }

    int8_t find_code(const std::string& val) const {
        for (size_t i = 0; i < entries.size(); i++) {
            if (entries[i] == val) return (int8_t)i;
        }
        return -1;
    }
};

int main(int argc, char* argv[]) {
    gendb::init_date_tables();

    std::string gendb_dir = argv[1];
    std::string results_dir = argv[2];

    // Parse CLI params - both default to 1994-01-01, upper gets +1 year
    int32_t receiptdate_lower = gendb::parse_date_arg(argc, argv, "--l_receiptdate_lower",
                                    gendb::date_str_to_epoch_days("1994-01-01"));
    int32_t receiptdate_upper_base = gendb::parse_date_arg(argc, argv, "--l_receiptdate_upper",
                                    gendb::date_str_to_epoch_days("1994-01-01"));
    int32_t receiptdate_upper = gendb::add_years(receiptdate_upper_base, 1);

    // ---- Declare all mmap resources at outer scope (RAII timer scoping) ----
    RawMmap mm_shipmode, mm_receiptdate, mm_commitdate, mm_shipdate, mm_orderkey;
    RawMmap mm_zonemap, mm_ok_lookup, mm_orderpriority;

    {
    GENDB_PHASE("total");

    // ---- Load dicts ----
    Dict shipmode_dict, orderpriority_dict;
    int8_t mail_code, ship_code, urgent_code, high_code;
    {
        GENDB_PHASE("load_dicts");
        shipmode_dict.load(gendb_dir + "/lineitem/l_shipmode_dict.bin");
        orderpriority_dict.load(gendb_dir + "/orders/o_orderpriority_dict.bin");

        mail_code = shipmode_dict.find_code("MAIL");
        ship_code = shipmode_dict.find_code("SHIP");
        urgent_code = orderpriority_dict.find_code("1-URGENT");
        high_code = orderpriority_dict.find_code("2-HIGH");
    }

    // Map shipmode codes to output index (alphabetical: MAIL=0, SHIP=1)
    int8_t code_to_idx[128];
    memset(code_to_idx, -1, sizeof(code_to_idx));
    // MAIL < SHIP alphabetically
    code_to_idx[(uint8_t)mail_code] = 0;
    code_to_idx[(uint8_t)ship_code] = 1;

    // ---- Data loading (mmap) ----
    {
        GENDB_PHASE("data_loading");

        mm_shipmode.open((gendb_dir + "/lineitem/l_shipmode.bin").c_str());
        mm_receiptdate.open((gendb_dir + "/lineitem/l_receiptdate.bin").c_str());
        mm_commitdate.open((gendb_dir + "/lineitem/l_commitdate.bin").c_str());
        mm_shipdate.open((gendb_dir + "/lineitem/l_shipdate.bin").c_str());
        mm_orderkey.open((gendb_dir + "/lineitem/l_orderkey.bin").c_str());

        mm_shipmode.advise_sequential(); mm_shipmode.advise_hugepage();
        mm_receiptdate.advise_sequential(); mm_receiptdate.advise_hugepage();
        mm_commitdate.advise_sequential(); mm_commitdate.advise_hugepage();
        mm_shipdate.advise_sequential(); mm_shipdate.advise_hugepage();
        mm_orderkey.advise_sequential(); mm_orderkey.advise_hugepage();

        mm_zonemap.open((gendb_dir + "/indexes/lineitem_l_receiptdate_zonemap.bin").c_str());
        mm_ok_lookup.open((gendb_dir + "/indexes/orders_o_orderkey_lookup.bin").c_str());
        mm_orderpriority.open((gendb_dir + "/orders/o_orderpriority.bin").c_str());
        mm_ok_lookup.advise_sequential();
        mm_orderpriority.advise_sequential();
    }

    const int8_t*  l_shipmode    = mm_shipmode.as<int8_t>();
    const int32_t* l_receiptdate = mm_receiptdate.as<int32_t>();
    const int32_t* l_commitdate  = mm_commitdate.as<int32_t>();
    const int32_t* l_shipdate    = mm_shipdate.as<int32_t>();
    const int32_t* l_orderkey    = mm_orderkey.as<int32_t>();
    const size_t   nrows         = mm_receiptdate.count<int32_t>();

    // Zonemap: uint64_t num_blocks, uint32_t block_size, then int32_t[num_blocks*2] min/max pairs
    const uint8_t* zm_raw = mm_zonemap.as<uint8_t>();
    uint64_t num_blocks = *reinterpret_cast<const uint64_t*>(zm_raw);
    uint32_t block_size = *reinterpret_cast<const uint32_t*>(zm_raw + 8);
    const int32_t* zm_minmax = reinterpret_cast<const int32_t*>(zm_raw + 12);

    // Dense PK lookup: uint64_t num_entries, then int32_t[num_entries]
    const uint8_t* lookup_raw = mm_ok_lookup.as<uint8_t>();
    uint64_t lookup_num_entries = *reinterpret_cast<const uint64_t*>(lookup_raw);
    const int32_t* ok_lookup = reinterpret_cast<const int32_t*>(lookup_raw + 8);
    (void)lookup_num_entries;

    const int8_t* o_orderpriority = mm_orderpriority.as<int8_t>();

    // ---- Zonemap prefilter ----
    std::vector<uint32_t> surviving_blocks;
    {
        GENDB_PHASE("zonemap_prefilter");
        for (uint64_t b = 0; b < num_blocks; b++) {
            int32_t bmin = zm_minmax[b * 2];
            int32_t bmax = zm_minmax[b * 2 + 1];
            if (bmax < receiptdate_lower || bmin >= receiptdate_upper) continue;
            surviving_blocks.push_back((uint32_t)b);
        }
    }

    // ---- Parallel scan + filter + aggregate ----
    int num_threads = std::max(1, (int)std::thread::hardware_concurrency());
    // Thread-local counters: [thread][shipmode_idx][0=high, 1=low]
    std::vector<int64_t> all_counters(num_threads * 4, 0); // 2 shipmodes * 2 counters

    {
        GENDB_PHASE("main_scan");

        std::atomic<uint32_t> block_cursor{0};
        uint32_t n_surviving = (uint32_t)surviving_blocks.size();

        auto worker = [&](int tid) {
            int64_t local[4] = {0, 0, 0, 0}; // [mail_high, mail_low, ship_high, ship_low]

            while (true) {
                uint32_t bi = block_cursor.fetch_add(1, std::memory_order_relaxed);
                if (bi >= n_surviving) break;

                uint32_t blk = surviving_blocks[bi];
                size_t start = (size_t)blk * block_size;
                size_t end = std::min(start + block_size, nrows);

                for (size_t i = start; i < end; i++) {
                    // Filter 1: l_shipmode IN (MAIL, SHIP)
                    int8_t sm = l_shipmode[i];
                    if (sm != mail_code && sm != ship_code) continue;

                    // Filter 2: l_receiptdate range
                    int32_t rd = l_receiptdate[i];
                    if (rd < receiptdate_lower || rd >= receiptdate_upper) continue;

                    // Filter 3: l_commitdate < l_receiptdate
                    if (l_commitdate[i] >= rd) continue;

                    // Filter 4: l_shipdate < l_commitdate
                    if (l_shipdate[i] >= l_commitdate[i]) continue;

                    // Join: lookup order row_id
                    int32_t okey = l_orderkey[i];
                    int32_t row_id = ok_lookup[okey];
                    if (row_id < 0) continue;

                    // Get orderpriority and classify
                    int8_t opri = o_orderpriority[row_id];
                    int idx = code_to_idx[(uint8_t)sm]; // 0=MAIL, 1=SHIP
                    if (opri == urgent_code || opri == high_code) {
                        local[idx * 2]++;     // high_line_count
                    } else {
                        local[idx * 2 + 1]++; // low_line_count
                    }
                }
            }

            // Store to shared array
            memcpy(&all_counters[tid * 4], local, sizeof(local));
        };

        std::vector<std::thread> threads;
        for (int t = 0; t < num_threads; t++) {
            threads.emplace_back(worker, t);
        }
        for (auto& t : threads) t.join();
    }

    // ---- Reduction ----
    int64_t mail_high = 0, mail_low = 0, ship_high = 0, ship_low = 0;
    for (int t = 0; t < num_threads; t++) {
        mail_high += all_counters[t * 4];
        mail_low  += all_counters[t * 4 + 1];
        ship_high += all_counters[t * 4 + 2];
        ship_low  += all_counters[t * 4 + 3];
    }

    // ---- Output ----
    {
        GENDB_PHASE("output");
        std::string outpath = results_dir + "/Q12.csv";
        FILE* f = fopen(outpath.c_str(), "w");
        fprintf(f, "l_shipmode,high_line_count,low_line_count\n");
        // MAIL before SHIP (alphabetical)
        fprintf(f, "MAIL,%ld,%ld\n", mail_high, mail_low);
        fprintf(f, "SHIP,%ld,%ld\n", ship_high, ship_low);
        fclose(f);
    }

    } // end total timer

    // ---- Cleanup outside timer (RAII resource scoping) ----
    mm_shipmode.close();
    mm_receiptdate.close();
    mm_commitdate.close();
    mm_shipdate.close();
    mm_orderkey.close();
    mm_zonemap.close();
    mm_ok_lookup.close();
    mm_orderpriority.close();

    return 0;
}
