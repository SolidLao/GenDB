#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <string>
#include <cstring>
#include <algorithm>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <thread>
#include <mutex>
#include <chrono>
#include <iomanip>
#include <ctime>
#include <cmath>
#include <immintrin.h>
#include <cstdint>
#include <bitset>
#include <atomic>

// Zone map metadata for block-level min/max filtering (forward declaration)
struct ZoneMapMetadata {
    int32_t min_shipdate;
    int32_t max_shipdate;
    size_t row_count;
};

// Helper to load dictionary from text file
std::unordered_map<uint8_t, std::string> loadDictionary(const std::string& filepath) {
    std::unordered_map<uint8_t, std::string> dict;
    std::ifstream file(filepath);
    std::string line;
    while (std::getline(file, line)) {
        auto pos = line.find('=');
        if (pos != std::string::npos) {
            uint8_t code = std::stoi(line.substr(0, pos));
            std::string value = line.substr(pos + 1);
            dict[code] = value;
        }
    }
    return dict;
}

// Helper to mmap a binary file
void* mmapFile(const std::string& filepath, size_t& size) {
    int fd = open(filepath.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << filepath << std::endl;
        return nullptr;
    }

    struct stat sb;
    fstat(fd, &sb);
    size = sb.st_size;

    void* ptr = mmap(nullptr, size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Failed to mmap " << filepath << std::endl;
        return nullptr;
    }

    return ptr;
}

// Convert epoch days to YYYY-MM-DD string
inline std::string epochDaysToString(int32_t days) {
    std::time_t t = (std::time_t)days * 86400LL;
    struct tm* tm = std::gmtime(&t);
    char buf[11];
    std::strftime(buf, sizeof(buf), "%Y-%m-%d", tm);
    return std::string(buf);
}

// Try to load pre-computed zone maps from storage, fallback to computing if not found
std::vector<ZoneMapMetadata> loadOrComputeZoneMaps(const std::string& gendb_dir, const int32_t* l_shipdate, size_t total_rows, size_t block_size = 100000) {
    std::vector<ZoneMapMetadata> zones;
    size_t num_blocks = (total_rows + block_size - 1) / block_size;
    zones.reserve(num_blocks);

    // OPTIMIZATION 1: Try to load pre-computed zone maps from storage
    std::string zonemap_path = gendb_dir + "/lineitem/l_shipdate_zonemap.bin";
    std::ifstream zonemap_file(zonemap_path, std::ios::binary);

    if (zonemap_file.good()) {
        // Pre-computed zone maps exist; load them
        for (size_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
            ZoneMapMetadata zone;
            if (zonemap_file.read((char*)&zone.min_shipdate, sizeof(int32_t)) &&
                zonemap_file.read((char*)&zone.max_shipdate, sizeof(int32_t)) &&
                zonemap_file.read((char*)&zone.row_count, sizeof(size_t))) {
                zones.push_back(zone);
            } else {
                // Incomplete file; fallback to computing
                zones.clear();
                break;
            }
        }
        zonemap_file.close();

        if (!zones.empty() && zones.size() == num_blocks) {
            // Successfully loaded pre-computed zone maps
            return zones;
        }
    }

    // FALLBACK: Compute zone maps (original implementation)
    zones.clear();
    zones.reserve(num_blocks);

    for (size_t block_idx = 0; block_idx < num_blocks; ++block_idx) {
        size_t start = block_idx * block_size;
        size_t end = std::min(start + block_size, total_rows);

        int32_t min_val = INT32_MAX;
        int32_t max_val = INT32_MIN;

        for (size_t i = start; i < end; ++i) {
            min_val = std::min(min_val, l_shipdate[i]);
            max_val = std::max(max_val, l_shipdate[i]);
        }

        zones.push_back({min_val, max_val, end - start});
    }

    return zones;
}

// Compact Robin Hood hash table for int32_t keys (filtered orders mapping)
struct CompactHashTableInt32 {
    static constexpr size_t LOAD_FACTOR = 75;  // Resize at 75% full

    struct Entry {
        int32_t key;
        std::pair<int32_t, int32_t> value;  // (orderdate, shippriority)
        uint8_t distance;  // Distance from ideal bucket
    };

    std::vector<Entry> table;
    size_t count = 0;
    size_t capacity = 0;
    static constexpr Entry EMPTY = {-1, {0, 0}, 0};

    CompactHashTableInt32(size_t initial_capacity = 256) {
        capacity = 1;
        while (capacity < initial_capacity) capacity *= 2;
        table.resize(capacity, EMPTY);
    }

    void insert(int32_t key, const std::pair<int32_t, int32_t>& value) {
        if (count * 100 >= capacity * LOAD_FACTOR) {
            rehash();
        }

        size_t pos = hash_fn(key) & (capacity - 1);
        uint8_t dist = 0;
        Entry new_entry = {key, value, dist};

        while (true) {
            if (table[pos].key == -1) {
                table[pos] = new_entry;
                count++;
                return;
            }
            if (table[pos].distance < new_entry.distance) {
                std::swap(table[pos], new_entry);
            }
            pos = (pos + 1) & (capacity - 1);
            new_entry.distance++;
        }
    }

    bool find(int32_t key, std::pair<int32_t, int32_t>& out_value) const {
        size_t pos = hash_fn(key) & (capacity - 1);
        uint8_t dist = 0;

        while (dist <= table[pos].distance) {
            if (table[pos].key == key) {
                out_value = table[pos].value;
                return true;
            }
            pos = (pos + 1) & (capacity - 1);
            dist++;
        }
        return false;
    }

private:
    void rehash() {
        std::vector<Entry> old_table = table;
        capacity *= 2;
        table.clear();
        table.resize(capacity, EMPTY);
        count = 0;

        for (const auto& entry : old_table) {
            if (entry.key != -1) {
                insert(entry.key, entry.value);
            }
        }
    }

    static uint64_t hash_fn(int32_t k) {
        uint64_t x = k;
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        return x;
    }
};

// Compact hash set for customer keys
struct CompactHashSetInt32 {
    static constexpr size_t LOAD_FACTOR = 75;

    std::vector<int32_t> table;
    size_t count = 0;
    size_t capacity = 0;

    CompactHashSetInt32(size_t initial_capacity = 256) {
        capacity = 1;
        while (capacity < initial_capacity) capacity *= 2;
        table.resize(capacity, -1);
    }

    void insert(int32_t key) {
        if (count * 100 >= capacity * LOAD_FACTOR) {
            rehash();
        }

        size_t pos = hash_fn(key) & (capacity - 1);
        while (table[pos] != -1) {
            if (table[pos] == key) return;
            pos = (pos + 1) & (capacity - 1);
        }
        table[pos] = key;
        count++;
    }

    bool find(int32_t key) const {
        size_t pos = hash_fn(key) & (capacity - 1);
        while (table[pos] != -1) {
            if (table[pos] == key) return true;
            pos = (pos + 1) & (capacity - 1);
        }
        return false;
    }

private:
    void rehash() {
        std::vector<int32_t> old_table = table;
        capacity *= 2;
        table.clear();
        table.resize(capacity, -1);
        count = 0;

        for (int32_t key : old_table) {
            if (key != -1) {
                insert(key);
            }
        }
    }

    static uint64_t hash_fn(int32_t k) {
        uint64_t x = k;
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        return x;
    }
};

// Bloom filter for semi-join reduction on orderkeys
// Reduces expensive hash table lookups by filtering out non-matching orderkeys early
class SimpleBloomFilter {
private:
    std::vector<uint64_t> bits;
    size_t num_bits;
    static constexpr size_t NUM_HASHES = 3;

public:
    SimpleBloomFilter(size_t expected_elements) {
        // Target ~2% false positive rate: ~9.6 bits per element
        num_bits = std::max(size_t(1000), expected_elements * 10);
        bits.resize((num_bits + 63) / 64, 0);
    }

    void insert(int32_t key) {
        for (size_t i = 0; i < NUM_HASHES; ++i) {
            uint64_t hash = hash_with_seed(key, i);
            size_t bit_pos = hash % num_bits;
            size_t word_pos = bit_pos / 64;
            size_t bit_offset = bit_pos % 64;
            bits[word_pos] |= (1ULL << bit_offset);
        }
    }

    bool might_contain(int32_t key) const {
        for (size_t i = 0; i < NUM_HASHES; ++i) {
            uint64_t hash = hash_with_seed(key, i);
            size_t bit_pos = hash % num_bits;
            size_t word_pos = bit_pos / 64;
            size_t bit_offset = bit_pos % 64;
            if (!(bits[word_pos] & (1ULL << bit_offset))) {
                return false;  // Definitely not in set
            }
        }
        return true;  // Probably in set (might be false positive)
    }

private:
    static uint64_t hash_with_seed(int32_t key, size_t seed) {
        uint64_t x = key ^ seed;
        x ^= x >> 33;
        x *= 0xff51afd7ed558ccdULL;
        x ^= x >> 33;
        return x;
    }
};

// Group key struct for Q3 aggregation
struct GroupKey {
    int32_t l_orderkey;
    int32_t o_orderdate;
    int32_t o_shippriority;

    bool operator==(const GroupKey& other) const {
        return l_orderkey == other.l_orderkey &&
               o_orderdate == other.o_orderdate &&
               o_shippriority == other.o_shippriority;
    }
};

struct GroupKeyHash {
    size_t operator()(const GroupKey& k) const {
        // Better hash function for GroupKey
        uint64_t x = (uint64_t)k.l_orderkey;
        x = x * 0xff51afd7ed558ccdULL;
        x ^= (uint64_t)k.o_orderdate * 0xc4ceb9fe1a85ec53ULL;
        x ^= (uint64_t)k.o_shippriority * 0x27d4eb2d;
        x ^= x >> 33;
        return x;
    }
};

struct AggregateValue {
    double revenue;
};

// OPTIMIZATION 3: Radix partitioning data structure for cache-oblivious join
// Partition both filtered_orders and lineitem by lower bits of orderkey
template<typename KeyType, typename ValueType>
struct RadixPartitionedTable {
    static constexpr size_t NUM_PARTITIONS = 256;  // 8-bit radix partitioning
    std::vector<std::vector<std::pair<KeyType, ValueType>>> partitions;

    RadixPartitionedTable() : partitions(NUM_PARTITIONS) {}

    void insert(KeyType key, const ValueType& value) {
        size_t partition_idx = key & (NUM_PARTITIONS - 1);  // Lower 8 bits
        partitions[partition_idx].push_back({key, value});
    }

    size_t get_partition_idx(KeyType key) const {
        return key & (NUM_PARTITIONS - 1);
    }

    const auto& get_partition(size_t idx) const {
        return partitions[idx];
    }
};

// Helper: Extract partition index for lineitem rows
struct RadixPartitionedLineitem {
    static constexpr size_t NUM_PARTITIONS = 256;
    std::vector<std::vector<size_t>> partition_indices;  // Indices into original lineitem arrays

    RadixPartitionedLineitem() : partition_indices(NUM_PARTITIONS) {}

    void insert_index(int32_t orderkey, size_t row_idx) {
        size_t partition_idx = orderkey & (NUM_PARTITIONS - 1);
        partition_indices[partition_idx].push_back(row_idx);
    }

    const auto& get_partition_indices(size_t idx) const {
        return partition_indices[idx];
    }
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Constants for date filtering
    // 1995-03-15 = 9204 days since epoch 1970-01-01
    const int32_t cutoff_date = 9204;

    // Load dictionaries
    std::unordered_map<uint8_t, std::string> c_mktsegment_dict =
        loadDictionary(gendb_dir + "/customer/c_mktsegment_dict.txt");

    // Find code for 'BUILDING'
    uint8_t building_code = 255; // invalid code
    for (const auto& [code, value] : c_mktsegment_dict) {
        if (value == "BUILDING") {
            building_code = code;
            break;
        }
    }

    if (building_code == 255) {
        std::cerr << "Failed to find BUILDING in dictionary" << std::endl;
        return;
    }

    // mmap customer table
    size_t customer_size_custkey = 0, customer_size_mktsegment = 0;
    const int32_t* c_custkey = (const int32_t*)mmapFile(gendb_dir + "/customer/c_custkey.bin", customer_size_custkey);
    const uint8_t* c_mktsegment = (const uint8_t*)mmapFile(gendb_dir + "/customer/c_mktsegment.bin", customer_size_mktsegment);

    if (!c_custkey || !c_mktsegment) {
        std::cerr << "Failed to mmap customer columns" << std::endl;
        return;
    }

    size_t customer_rows = customer_size_custkey / sizeof(int32_t);

    // Apply madvise for sequential access pattern (customer scan)
    madvise((void*)c_custkey, customer_size_custkey, MADV_SEQUENTIAL);
    madvise((void*)c_mktsegment, customer_size_mktsegment, MADV_SEQUENTIAL);

    // OPTIMIZATION 1: Parallelize customer filtering across threads
    // Instead of single-threaded filtering, distribute 1.5M customer rows across threads
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t customer_morsel_size = (customer_rows + num_threads - 1) / num_threads;
    std::vector<CompactHashSetInt32> thread_filtered_customers(num_threads, CompactHashSetInt32(256000));

    std::vector<std::thread> customer_threads;
    for (size_t t = 0; t < num_threads; ++t) {
        customer_threads.emplace_back([&, t]() {
            size_t start_idx = t * customer_morsel_size;
            size_t end_idx = std::min(start_idx + customer_morsel_size, customer_rows);

            for (size_t i = start_idx; i < end_idx; ++i) {
                if (c_mktsegment[i] == building_code) {
                    thread_filtered_customers[t].insert(c_custkey[i]);
                }
            }
        });
    }

    for (auto& th : customer_threads) {
        th.join();
    }

    // Merge thread-local customer sets into single hash set
    CompactHashSetInt32 filtered_customers(512000);
    for (size_t t = 0; t < num_threads; ++t) {
        for (int32_t key : thread_filtered_customers[t].table) {
            if (key != -1) {
                filtered_customers.insert(key);
            }
        }
    }

    // mmap orders table
    size_t orders_size_orderkey = 0, orders_size_custkey = 0, orders_size_orderdate = 0, orders_size_shippriority = 0;
    const int32_t* o_orderkey = (const int32_t*)mmapFile(gendb_dir + "/orders/o_orderkey.bin", orders_size_orderkey);
    const int32_t* o_custkey = (const int32_t*)mmapFile(gendb_dir + "/orders/o_custkey.bin", orders_size_custkey);
    const int32_t* o_orderdate = (const int32_t*)mmapFile(gendb_dir + "/orders/o_orderdate.bin", orders_size_orderdate);
    const int32_t* o_shippriority = (const int32_t*)mmapFile(gendb_dir + "/orders/o_shippriority.bin", orders_size_shippriority);

    if (!o_orderkey || !o_custkey || !o_orderdate || !o_shippriority) {
        std::cerr << "Failed to mmap orders columns" << std::endl;
        return;
    }

    size_t orders_rows = orders_size_orderkey / sizeof(int32_t);

    // Apply madvise for orders scan
    madvise((void*)o_orderkey, orders_size_orderkey, MADV_SEQUENTIAL);
    madvise((void*)o_custkey, orders_size_custkey, MADV_SEQUENTIAL);
    madvise((void*)o_orderdate, orders_size_orderdate, MADV_SEQUENTIAL);
    madvise((void*)o_shippriority, orders_size_shippriority, MADV_SEQUENTIAL);

    // OPTIMIZATION 1B: Parallelize orders filtering and build bloom filter
    // Distribute 15M orders rows across threads with thread-local hash tables
    const size_t orders_morsel_size = (orders_rows + num_threads - 1) / num_threads;
    std::vector<CompactHashTableInt32> thread_filtered_orders(num_threads, CompactHashTableInt32(256000));

    std::vector<std::thread> orders_threads;
    for (size_t t = 0; t < num_threads; ++t) {
        orders_threads.emplace_back([&, t]() {
            size_t start_idx = t * orders_morsel_size;
            size_t end_idx = std::min(start_idx + orders_morsel_size, orders_rows);

            for (size_t i = start_idx; i < end_idx; ++i) {
                int32_t orderdate_stored = o_orderdate[i];
                int32_t orderdate_actual = orderdate_stored - 1;  // Correct for storage offset
                if (filtered_customers.find(o_custkey[i]) && orderdate_actual < cutoff_date) {
                    thread_filtered_orders[t].insert(o_orderkey[i], {orderdate_actual, o_shippriority[i]});
                }
            }
        });
    }

    for (auto& th : orders_threads) {
        th.join();
    }

    // Merge thread-local orders hash tables
    CompactHashTableInt32 filtered_orders(256000);
    for (size_t t = 0; t < num_threads; ++t) {
        for (const auto& entry : thread_filtered_orders[t].table) {
            if (entry.key != -1) {
                filtered_orders.insert(entry.key, entry.value);
            }
        }
    }

    // OPTIMIZATION 3: Build bloom filter on filtered_orders.orderkeys for semi-join reduction
    SimpleBloomFilter orderkey_bloom(std::max(size_t(1000), filtered_orders.count * 2));
    for (const auto& entry : filtered_orders.table) {
        if (entry.key != -1) {
            orderkey_bloom.insert(entry.key);
        }
    }

    // mmap lineitem table
    size_t lineitem_size_orderkey = 0, lineitem_size_shipdate = 0,
           lineitem_size_extprice = 0, lineitem_size_discount = 0;
    const int32_t* l_orderkey = (const int32_t*)mmapFile(gendb_dir + "/lineitem/l_orderkey.bin", lineitem_size_orderkey);
    const int32_t* l_shipdate = (const int32_t*)mmapFile(gendb_dir + "/lineitem/l_shipdate.bin", lineitem_size_shipdate);
    const double* l_extendedprice = (const double*)mmapFile(gendb_dir + "/lineitem/l_extendedprice.bin", lineitem_size_extprice);
    const double* l_discount = (const double*)mmapFile(gendb_dir + "/lineitem/l_discount.bin", lineitem_size_discount);

    if (!l_orderkey || !l_shipdate || !l_extendedprice || !l_discount) {
        std::cerr << "Failed to mmap lineitem columns" << std::endl;
        return;
    }

    size_t lineitem_rows = lineitem_size_orderkey / sizeof(int32_t);

    // Apply madvise for lineitem scan (primary bottleneck)
    madvise((void*)l_orderkey, lineitem_size_orderkey, MADV_SEQUENTIAL);
    madvise((void*)l_shipdate, lineitem_size_shipdate, MADV_SEQUENTIAL);
    madvise((void*)l_extendedprice, lineitem_size_extprice, MADV_SEQUENTIAL);
    madvise((void*)l_discount, lineitem_size_discount, MADV_SEQUENTIAL);

    // OPTIMIZATION 1 (continued): Load or compute zone maps for lineitem l_shipdate (100K rows per block)
    // This enables block-skipping for the l_shipdate > 1995-03-15 filter (67% selectivity)
    std::vector<ZoneMapMetadata> zone_maps = loadOrComputeZoneMaps(gendb_dir, l_shipdate, lineitem_rows, 100000);

    // OPTIMIZATION 3: Radix partitioning for cache-oblivious join
    // First pass: partition filtered_orders by lower 8 bits of orderkey
    RadixPartitionedTable<int32_t, std::pair<int32_t, int32_t>> partitioned_orders;
    for (const auto& entry : filtered_orders.table) {
        if (entry.key != -1) {
            partitioned_orders.insert(entry.key, entry.value);
        }
    }

    // Build per-partition hash tables for better cache locality
    std::vector<CompactHashTableInt32> partition_hash_tables(256, CompactHashTableInt32(1024));
    for (size_t p = 0; p < 256; ++p) {
        for (const auto& [key, value] : partitioned_orders.partitions[p]) {
            partition_hash_tables[p].insert(key, value);
        }
    }

    // Aggregate: GROUP BY l_orderkey, o_orderdate, o_shippriority
    // SUM(l_extendedprice * (1 - l_discount))
    std::unordered_map<GroupKey, AggregateValue, GroupKeyHash> aggregates;
    std::mutex agg_lock;

    // Parallel scan of lineitem with morsel-driven approach + radix partitioning
    const size_t morsel_size = (lineitem_rows + num_threads - 1) / num_threads;

    std::vector<std::thread> threads;
    std::vector<std::unordered_map<GroupKey, AggregateValue, GroupKeyHash>> thread_aggregates(num_threads);

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            size_t start_idx = t * morsel_size;
            size_t end_idx = std::min(start_idx + morsel_size, lineitem_rows);

            // Process bulk of data with SIMD-friendly batching and prefetching
            constexpr size_t PREFETCH_DISTANCE = 16;
            constexpr size_t BLOCK_SIZE = 100000;

            for (size_t i = start_idx; i < end_idx; ++i) {
                // Zone map block skipping: if entire block's shipdate range doesn't contain cutoff_date
                size_t block_idx = i / BLOCK_SIZE;
                if (block_idx < zone_maps.size()) {
                    const auto& zone = zone_maps[block_idx];
                    // Skip block if all values are <= cutoff_date (predicate is shipdate > cutoff_date)
                    if (zone.max_shipdate <= cutoff_date) {
                        // Skip to next block
                        i = (block_idx + 1) * BLOCK_SIZE - 1;
                        continue;
                    }
                }

                // Prefetch ahead: load future orderkeys, shipdates
                if (i + PREFETCH_DISTANCE < end_idx) {
                    _mm_prefetch((const char*)(l_orderkey + i + PREFETCH_DISTANCE), _MM_HINT_T0);
                    _mm_prefetch((const char*)(l_shipdate + i + PREFETCH_DISTANCE), _MM_HINT_T0);
                    _mm_prefetch((const char*)(l_extendedprice + i + PREFETCH_DISTANCE), _MM_HINT_T1);
                    _mm_prefetch((const char*)(l_discount + i + PREFETCH_DISTANCE), _MM_HINT_T1);
                }

                int32_t orderkey = l_orderkey[i];

                // Early predicate: check shipdate BEFORE hash lookup (branch prediction friendly)
                int32_t shipdate_stored = l_shipdate[i];
                int32_t shipdate_actual = shipdate_stored - 1;  // Correct for storage offset

                // Check shipdate > 1995-03-15 (fast path)
                if (shipdate_actual > cutoff_date) {
                    // OPTIMIZATION 3 (continued): Bloom filter check BEFORE expensive hash lookup
                    // Bloom filter has ~2% false positive rate but eliminates 98% of misses
                    if (orderkey_bloom.might_contain(orderkey)) {
                        // Use partition-specific hash table for better cache locality
                        size_t partition_idx = orderkey & 0xFF;  // Lower 8 bits
                        std::pair<int32_t, int32_t> order_info;
                        if (partition_hash_tables[partition_idx].find(orderkey, order_info)) {
                            // SIMD-friendly arithmetic: compute revenue
                            double extprice = l_extendedprice[i];
                            double discount = l_discount[i];
                            double revenue = extprice * (1.0 - discount);

                            GroupKey key{orderkey, order_info.first, order_info.second};
                            thread_aggregates[t][key].revenue += revenue;
                        }
                    }
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Merge thread aggregates
    for (size_t t = 0; t < num_threads; ++t) {
        for (const auto& [key, val] : thread_aggregates[t]) {
            aggregates[key].revenue += val.revenue;
        }
    }

    // OPTIMIZATION 2: Use partial sort for Top-K instead of full sort
    // Convert aggregates to result tuples
    std::vector<std::tuple<int32_t, double, int32_t, int32_t>> results;
    results.reserve(aggregates.size());

    for (const auto& [key, val] : aggregates) {
        results.emplace_back(key.l_orderkey, val.revenue, key.o_orderdate, key.o_shippriority);
    }

    // Partial sort for top 10: O(n log k) instead of O(n log n)
    size_t top_k = std::min(size_t(10), results.size());
    std::partial_sort(results.begin(), results.begin() + top_k, results.end(),
        [](const auto& a, const auto& b) {
            if (std::get<1>(a) != std::get<1>(b)) {
                return std::get<1>(a) > std::get<1>(b); // DESC by revenue
            }
            return std::get<2>(a) < std::get<2>(b); // ASC by orderdate
        });

    // Take top 10
    if (results.size() > 10) {
        results.resize(10);
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

    // Write results to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/q3.csv");
        outfile << "l_orderkey,revenue,o_orderdate,o_shippriority\n";

        for (const auto& [orderkey, revenue, orderdate, shippriority] : results) {
            outfile << orderkey << ",";
            outfile << std::fixed << std::setprecision(2) << revenue << ",";
            outfile << epochDaysToString(orderdate) << ",";
            outfile << shippriority << "\n";
        }
        outfile.close();
    }

    std::cout << "Query returned " << results.size() << " rows\n";
    std::cout << "Execution time: " << duration.count() << " ms\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q3(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
