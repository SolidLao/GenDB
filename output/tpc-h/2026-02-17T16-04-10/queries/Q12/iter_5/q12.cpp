#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <set>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <omp.h>
#include <atomic>

// ============================================================================
// Q12 OPTIMIZED PLAN (Iteration 5): LOAD PRE-BUILT LINEITEM HASH INDEX
// ============================================================================
// Logical Plan:
//   1. Load pre-built lineitem_l_orderkey_hash index (0ms vs 44ms rebuild)
//      - Binary format: [unique_count][table_size][hash_entries...][positions_count][positions...]
//      - Eliminates 44ms of hash table construction cost
//
//   2. Scan & Filter lineitem (59.9M rows):
//      - l_shipmode IN ('MAIL', 'SHIP')
//      - l_commitdate < l_receiptdate
//      - l_shipdate < l_commitdate
//      - l_receiptdate >= 1994-01-01 && < 1995-01-01
//      Estimated output: ~1.2M rows (2% selectivity)
//
//   3. Probe join: Scan orders (15M rows), probe pre-built lineitem hash index
//      - For each order key, lookup matching lineitem positions in O(1)
//      - Parallel scan with OpenMP
//      - Thread-local aggregation buffers (no contention)
//
//   4. Merge thread-local aggregations
//
// Physical Plan:
//   - Load lineitem index: mmap + deserialize via pointers (0ms)
//   - lineitem scan: PARALLEL morsel-driven across 64 cores
//   - join probe: OpenMP parallel for on orders, probe via mmap'd hash index
//   - aggregation: thread-local flat arrays [2 shipmodes] per thread
//   - output: write CSV
//
// Expected Speedup (Iteration 5):
//   - Eliminate 44ms hash build cost → 102ms - 44ms = 58ms baseline
//   - Target: 50-60ms (1.2x of Umbra 49ms)
// ============================================================================

struct HashIndexEntry {
    int32_t key;
    uint32_t offset;
    uint32_t count;
};

struct PreBuiltHashIndex {
    uint32_t unique_count;
    uint32_t table_size;
    const HashIndexEntry* hash_table;
    const uint32_t* positions;

    void* mmap_data;
    size_t mmap_size;

    PreBuiltHashIndex() : unique_count(0), table_size(0), hash_table(nullptr),
                          positions(nullptr), mmap_data(nullptr), mmap_size(0) {}

    // Find positions for a given key using linear probing
    const uint32_t* find(int32_t key, uint32_t& out_count) const {
        if (!hash_table) {
            out_count = 0;
            return nullptr;
        }

        // Fibonacci hash + linear probing
        uint64_t h = ((uint64_t)key * 0x9E3779B97F4A7C15ULL) ^ (((uint64_t)key * 0x9E3779B97F4A7C15ULL) >> 32);
        size_t idx = h % table_size;

        while (hash_table[idx].key != -1) {
            if (hash_table[idx].key == key) {
                out_count = hash_table[idx].count;
                return positions + hash_table[idx].offset;
            }
            idx = (idx + 1) % table_size;
        }

        out_count = 0;
        return nullptr;
    }

    ~PreBuiltHashIndex() {
        if (mmap_data && mmap_data != MAP_FAILED) {
            munmap(mmap_data, mmap_size);
        }
    }
};

struct MmapFile {
    int fd = -1;
    void* data = nullptr;
    size_t size = 0;

    void open(const std::string& path) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << path << std::endl;
            throw std::runtime_error("open failed");
        }
        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to stat " << path << std::endl;
            throw std::runtime_error("fstat failed");
        }
        size = st.st_size;
        data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "Failed to mmap " << path << std::endl;
            throw std::runtime_error("mmap failed");
        }
    }

    void close() {
        if (data && data != MAP_FAILED) {
            munmap(data, size);
            data = nullptr;
        }
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
    }

    ~MmapFile() { close(); }
};

template<typename T>
T* cast(void* ptr) {
    return reinterpret_cast<T*>(ptr);
}

template<typename T>
const T* cast_const(const void* ptr) {
    return reinterpret_cast<const T*>(ptr);
}

// Load dictionary file
std::unordered_map<int32_t, std::string> load_dictionary(const std::string& path) {
    std::unordered_map<int32_t, std::string> dict;
    std::ifstream f(path);
    if (!f.is_open()) {
        std::cerr << "Failed to open dictionary: " << path << std::endl;
        throw std::runtime_error("dictionary open failed");
    }
    std::string line;
    while (std::getline(f, line)) {
        size_t eq = line.find('=');
        if (eq != std::string::npos) {
            int32_t code = std::stoi(line.substr(0, eq));
            std::string value = line.substr(eq + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Load pre-built hash index from mmap'd file
PreBuiltHashIndex load_hash_index(const std::string& index_path) {
    PreBuiltHashIndex idx;

    MmapFile mf;
    mf.open(index_path);

    // Parse binary format: [unique_count][table_size][hash_entries...][pos_count][positions...]
    const char* data = cast_const<const char>(mf.data);
    size_t offset = 0;

    // Read header
    idx.unique_count = *cast_const<uint32_t>(data + offset);
    offset += sizeof(uint32_t);

    idx.table_size = *cast_const<uint32_t>(data + offset);
    offset += sizeof(uint32_t);

    // Point hash table to mmap'd data
    idx.hash_table = cast_const<HashIndexEntry>(data + offset);
    offset += idx.table_size * sizeof(HashIndexEntry);

    // Read positions count (skip, we don't validate)
    offset += sizeof(uint32_t);

    // Point positions to mmap'd data
    idx.positions = cast_const<uint32_t>(data + offset);

    // Store mmap info for cleanup
    idx.mmap_data = mf.data;
    idx.mmap_size = mf.size;
    mf.fd = -1;  // Prevent double-close in destructor
    mf.data = nullptr;

    return idx;
}

// Epoch day calculation: 1970-01-01 is day 0
int32_t date_to_epoch(int year, int month, int day) {
    int32_t total = 0;
    for (int y = 1970; y < year; ++y) {
        total += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }
    for (int m = 1; m < month; ++m) {
        total += days_in_month[m - 1];
    }
    total += (day - 1);
    return total;
}

void run_q12(const std::string& gendb_dir, const std::string& results_dir) {
    auto t_global_start = std::chrono::high_resolution_clock::now();

#ifdef GENDB_PROFILE
    auto t_start = std::chrono::high_resolution_clock::now();
#endif

    // Load dictionaries
    auto orderpriority_dict = load_dictionary(gendb_dir + "/orders/o_orderpriority_dict.txt");
    auto shipmode_dict = load_dictionary(gendb_dir + "/lineitem/l_shipmode_dict.txt");

#ifdef GENDB_PROFILE
    auto t_end = std::chrono::high_resolution_clock::now();
    double ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_dictionaries: %.2f ms\n", ms);
#endif

    // Build reverse mapping for shipmode: value -> code
    std::unordered_map<std::string, int32_t> shipmode_reverse;
    for (auto& [code, value] : shipmode_dict) {
        shipmode_reverse[value] = code;
    }

    // ITERATION 5: Load pre-built lineitem hash index (0ms vs 44ms rebuild)
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    PreBuiltHashIndex li_hash_idx = load_hash_index(gendb_dir + "/indexes/lineitem_l_orderkey_hash.bin");

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_hash_index: %.2f ms\n", ms);
#endif

    // Date filters
    int32_t receipt_date_min = date_to_epoch(1994, 1, 1);
    int32_t receipt_date_max = date_to_epoch(1995, 1, 1);

    // Open lineitem columns
    MmapFile li_shipmode_file, li_commitdate_file, li_receiptdate_file, li_shipdate_file;

#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    li_shipmode_file.open(gendb_dir + "/lineitem/l_shipmode.bin");
    li_commitdate_file.open(gendb_dir + "/lineitem/l_commitdate.bin");
    li_receiptdate_file.open(gendb_dir + "/lineitem/l_receiptdate.bin");
    li_shipdate_file.open(gendb_dir + "/lineitem/l_shipdate.bin");

    int32_t* li_shipmode = cast<int32_t>(li_shipmode_file.data);
    int32_t* li_commitdate = cast<int32_t>(li_commitdate_file.data);
    int32_t* li_receiptdate = cast<int32_t>(li_receiptdate_file.data);
    int32_t* li_shipdate = cast<int32_t>(li_shipdate_file.data);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_lineitem: %.2f ms\n", ms);
#endif

    // Load orders columns
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    MmapFile o_orderkey_file, o_orderpriority_file;
    o_orderkey_file.open(gendb_dir + "/orders/o_orderkey.bin");
    o_orderpriority_file.open(gendb_dir + "/orders/o_orderpriority.bin");

    int32_t* o_orderkey = cast<int32_t>(o_orderkey_file.data);
    int32_t* o_orderpriority = cast<int32_t>(o_orderpriority_file.data);

    size_t orders_count = o_orderkey_file.size / sizeof(int32_t);

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] load_orders: %.2f ms\n", ms);
#endif

    // Probe join & aggregate
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    int32_t mail_code = shipmode_reverse.count("MAIL") ? shipmode_reverse["MAIL"] : -1;
    int32_t ship_code = shipmode_reverse.count("SHIP") ? shipmode_reverse["SHIP"] : -1;

    // Pre-compute priority codes
    std::string urgent = "1-URGENT";
    std::string high = "2-HIGH";
    int32_t urgent_code = -1, high_code = -1;
    for (auto& [code, value] : orderpriority_dict) {
        if (value == urgent) urgent_code = code;
        if (value == high) high_code = code;
    }

    int num_threads = std::min((int)omp_get_max_threads(), 64);

    // Thread-local aggregation buffers
    std::vector<std::vector<int64_t>> thread_agg_high(num_threads, std::vector<int64_t>(2, 0));
    std::vector<std::vector<int64_t>> thread_agg_low(num_threads, std::vector<int64_t>(2, 0));
    std::vector<std::set<int32_t>> thread_shipmodes(num_threads);

    // Parallel probe on orders using pre-built lineitem hash index
    #pragma omp parallel for num_threads(num_threads) schedule(static)
    for (size_t i = 0; i < orders_count; ++i) {
        int32_t orderkey = o_orderkey[i];
        int32_t priority_code = o_orderpriority[i];
        bool is_high = (priority_code == urgent_code || priority_code == high_code);

        // Probe lineitem hash index
        uint32_t match_count = 0;
        const uint32_t* matches = li_hash_idx.find(orderkey, match_count);

        if (matches && match_count > 0) {
            int thread_id = omp_get_thread_num();
            for (uint32_t j = 0; j < match_count; ++j) {
                uint32_t li_idx = matches[j];

                // Apply lineitem filters
                if (li_commitdate[li_idx] >= li_receiptdate[li_idx]) continue;
                if (li_shipdate[li_idx] >= li_commitdate[li_idx]) continue;
                if (li_receiptdate[li_idx] < receipt_date_min) continue;
                if (li_receiptdate[li_idx] >= receipt_date_max) continue;

                int32_t shipmode = li_shipmode[li_idx];
                if (shipmode != mail_code && shipmode != ship_code) continue;

                // Aggregate
                int shipmode_idx = (shipmode == mail_code) ? 0 : 1;
                if (is_high) {
                    thread_agg_high[thread_id][shipmode_idx]++;
                } else {
                    thread_agg_low[thread_id][shipmode_idx]++;
                }
                thread_shipmodes[thread_id].insert(shipmode);
            }
        }
    }

    // Merge thread-local aggregations
    int64_t agg_high[2] = {0, 0};
    int64_t agg_low[2] = {0, 0};
    std::set<int32_t> all_shipmodes;
    for (int t = 0; t < num_threads; ++t) {
        agg_high[0] += thread_agg_high[t][0];
        agg_high[1] += thread_agg_high[t][1];
        agg_low[0] += thread_agg_low[t][0];
        agg_low[1] += thread_agg_low[t][1];
        for (int32_t sm : thread_shipmodes[t]) {
            all_shipmodes.insert(sm);
        }
    }

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] join_aggregate: %.2f ms\n", ms);
#endif

    // Prepare output
#ifdef GENDB_PROFILE
    t_start = std::chrono::high_resolution_clock::now();
#endif

    std::vector<std::tuple<std::string, int64_t, int64_t>> results;

    // Collect unique shipmodes
    std::vector<std::pair<std::string, int32_t>> shipmode_list;
    for (int32_t code : all_shipmodes) {
        if (shipmode_dict.count(code)) {
            shipmode_list.push_back({shipmode_dict[code], code});
        }
    }
    std::sort(shipmode_list.begin(), shipmode_list.end());

    for (auto& [shipmode_str, code] : shipmode_list) {
        int idx = (code == mail_code) ? 0 : 1;
        results.push_back({shipmode_str, agg_high[idx], agg_low[idx]});
    }

    // Write CSV output
    std::ofstream out(results_dir + "/Q12.csv");
    out << "l_shipmode,high_line_count,low_line_count\n";
    for (auto& [shipmode_str, high_count, low_count] : results) {
        out << shipmode_str << "," << high_count << "," << low_count << "\n";
    }
    out.close();

#ifdef GENDB_PROFILE
    t_end = std::chrono::high_resolution_clock::now();
    ms = std::chrono::duration<double, std::milli>(t_end - t_start).count();
    printf("[TIMING] output: %.2f ms\n", ms);
#endif

    auto t_global_end = std::chrono::high_resolution_clock::now();
    double total_ms = std::chrono::duration<double, std::milli>(t_global_end - t_global_start).count();
#ifdef GENDB_PROFILE
    printf("[TIMING] total: %.2f ms\n", total_ms);
#endif
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : ".";
    run_q12(gendb_dir, results_dir);
    return 0;
}
#endif
