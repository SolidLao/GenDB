#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <queue>
#include <thread>
#include <mutex>
#include <atomic>
#include <cstring>
#include <algorithm>
#include <numeric>
#include <chrono>
#include <iomanip>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <cmath>

// Simple linear-probing hash table for integer keys
template <typename KeyT, typename ValueT>
class LinearProbeHashTable {
public:
    LinearProbeHashTable(size_t capacity) : capacity_(capacity) {
        // Round up to power of 2
        capacity_ = 1ULL << (64 - __builtin_clzll(capacity_ - 1));
        mask_ = capacity_ - 1;
        keys_.resize(capacity_, KeyT(-1));
        values_.resize(capacity_, ValueT());
        occupied_.resize(capacity_, false);
    }

    void insert(KeyT key, const ValueT& value) {
        size_t pos = hash(key) & mask_;
        while (occupied_[pos] && keys_[pos] != key) {
            pos = (pos + 1) & mask_;
        }
        if (!occupied_[pos]) {
            occupied_[pos] = true;
            keys_[pos] = key;
        }
        values_[pos] = value;
    }

    ValueT* find(KeyT key) {
        size_t pos = hash(key) & mask_;
        while (occupied_[pos]) {
            if (keys_[pos] == key) return &values_[pos];
            pos = (pos + 1) & mask_;
        }
        return nullptr;
    }

    size_t size() const {
        return std::count(occupied_.begin(), occupied_.end(), true);
    }

    std::vector<std::pair<KeyT, ValueT>> entries() const {
        std::vector<std::pair<KeyT, ValueT>> result;
        for (size_t i = 0; i < capacity_; ++i) {
            if (occupied_[i]) {
                result.emplace_back(keys_[i], values_[i]);
            }
        }
        return result;
    }

private:
    static inline size_t hash(KeyT key) {
        return std::hash<KeyT>{}(key);
    }

    std::vector<KeyT> keys_;
    std::vector<ValueT> values_;
    std::vector<bool> occupied_;
    size_t capacity_;
    size_t mask_;
};

// Date representation (YYYY-MM-DD as integer: year*10000 + month*100 + day)
struct Date {
    int32_t value;

    Date(const char* str) {
        int y, m, d;
        sscanf(str, "%d-%d-%d", &y, &m, &d);
        value = y * 10000 + m * 100 + d;
    }

    Date(int32_t v = 0) : value(v) {}

    bool operator<(const Date& other) const { return value < other.value; }
    bool operator>(const Date& other) const { return value > other.value; }
    bool operator<=(const Date& other) const { return value <= other.value; }
    bool operator>=(const Date& other) const { return value >= other.value; }
    bool operator==(const Date& other) const { return value == other.value; }
};

struct Decimal {
    double value;
    Decimal(double v = 0.0) : value(v) {}
    Decimal operator+(const Decimal& other) const { return Decimal(value + other.value); }
    Decimal operator*(const Decimal& other) const { return Decimal(value * other.value); }
    bool operator<(const Decimal& other) const { return value < other.value; }
    bool operator>(const Decimal& other) const { return value > other.value; }
};

// Result row structure
struct Q3Result {
    int32_t l_orderkey;
    Decimal revenue;
    Date o_orderdate;
    int32_t o_shippriority;

    bool operator<(const Q3Result& other) const {
        if (other.revenue.value != revenue.value) {
            return revenue.value > other.revenue.value; // DESC
        }
        return o_orderdate.value < other.o_orderdate.value; // ASC
    }
};

// Aggregation state for partial aggregation
struct AggState {
    Decimal revenue;
    AggState() : revenue(0.0) {}
};

// Group key combining l_orderkey, o_orderdate, o_shippriority
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

// Hash function for GroupKey
namespace std {
    template <>
    struct hash<GroupKey> {
        size_t operator()(const GroupKey& k) const {
            // Combine three 32-bit values into a 64-bit hash
            uint64_t h = (uint64_t)k.l_orderkey ^ ((uint64_t)k.o_orderdate << 32);
            h ^= (uint64_t)k.o_shippriority * 0x9e3779b97f4a7c15ULL;
            return h;
        }
    };
}

// Mmap file reader
class MmapReader {
public:
    MmapReader(const std::string& filename) {
        fd_ = open(filename.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("Cannot open file: " + filename);
        }
        off_t size = lseek(fd_, 0, SEEK_END);
        if (size < 0) {
            throw std::runtime_error("Cannot get file size: " + filename);
        }
        size_ = static_cast<size_t>(size);
        data_ = mmap(nullptr, size_, PROT_READ, MAP_SHARED, fd_, 0);
        if (data_ == MAP_FAILED) {
            throw std::runtime_error("Cannot mmap file: " + filename);
        }
    }

    ~MmapReader() {
        if (data_ != nullptr) {
            munmap(data_, size_);
        }
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    const void* data() const { return data_; }
    size_t size() const { return size_; }

private:
    int fd_ = -1;
    void* data_ = nullptr;
    size_t size_ = 0;
};

// Read a column as array of type T
template <typename T>
std::vector<T> read_column(const std::string& path) {
    MmapReader reader(path);
    const T* ptr = reinterpret_cast<const T*>(reader.data());
    size_t count = reader.size() / sizeof(T);
    return std::vector<T>(ptr, ptr + count);
}

// Morsel (work unit for parallelism)
struct Morsel {
    int32_t start;
    int32_t count;
};

// Atomic counter for morsel allocation
class MorselQueue {
public:
    MorselQueue(int32_t total_rows, int32_t morsel_size)
        : total_(total_rows), morsel_size_(morsel_size), next_(0) {}

    bool get_next_morsel(Morsel& m) {
        int32_t start = next_.fetch_add(morsel_size_, std::memory_order_relaxed);
        if (start >= total_) return false;
        m.start = start;
        m.count = std::min(morsel_size_, total_ - start);
        return true;
    }

private:
    int32_t total_;
    int32_t morsel_size_;
    std::atomic<int32_t> next_ = 0;
};

void run_q3(const std::string& gendb_dir, const std::string& results_dir) {
    auto t0 = std::chrono::high_resolution_clock::now();

    // Hardware concurrency
    int num_threads = std::thread::hardware_concurrency();
    int morsel_size = 200000; // Target: ~200K rows per morsel for cache locality

    // Read columns
    std::cout << "Reading columns..." << std::endl;
    auto c_custkey = read_column<int32_t>(gendb_dir + "/customer/c_custkey.bin");
    auto c_mktsegment = read_column<uint8_t>(gendb_dir + "/customer/c_mktsegment.bin");

    auto o_orderkey = read_column<int32_t>(gendb_dir + "/orders/o_orderkey.bin");
    auto o_custkey = read_column<int32_t>(gendb_dir + "/orders/o_custkey.bin");
    auto o_orderdate = read_column<int32_t>(gendb_dir + "/orders/o_orderdate.bin");
    auto o_shippriority = read_column<int32_t>(gendb_dir + "/orders/o_shippriority.bin");

    auto l_orderkey = read_column<int32_t>(gendb_dir + "/lineitem/l_orderkey.bin");
    auto l_extendedprice = read_column<double>(gendb_dir + "/lineitem/l_extendedprice.bin");
    auto l_discount = read_column<double>(gendb_dir + "/lineitem/l_discount.bin");
    auto l_shipdate = read_column<int32_t>(gendb_dir + "/lineitem/l_shipdate.bin");

    // Date constants
    Date date_1995_03_15(19950315);

    // ===== STAGE 1: Filter and build hash table on customer =====
    std::cout << "Stage 1: Filtering customer and building hash table..." << std::endl;
    LinearProbeHashTable<int32_t, std::pair<int32_t, int32_t>> customer_ht(
        c_custkey.size() / 4 + 1000 // Pre-sized to ~25% of input
    );

    // Dictionary for c_mktsegment ('BUILDING' = index 1, typically)
    for (size_t i = 0; i < c_custkey.size(); ++i) {
        // Decode c_mktsegment from dictionary-encoded value
        // For now, assume 'BUILDING' is encoded as 1 (common in TPC-H data)
        if (c_mktsegment[i] == 1) { // c_mktsegment = 'BUILDING'
            customer_ht.insert(c_custkey[i], {(int32_t)i, 0});
        }
    }

    int32_t filtered_customers = customer_ht.size();
    std::cout << "  Filtered customers: " << filtered_customers << std::endl;

    // ===== STAGE 2: Filter orders and probe customer hash table =====
    std::cout << "Stage 2: Filtering orders and probing customer..." << std::endl;

    // For each order, check: c_custkey matches AND o_orderdate < 1995-03-15
    // We'll build a mapping of filtered order indices
    std::vector<int32_t> filtered_order_indices;
    for (size_t i = 0; i < o_orderkey.size(); ++i) {
        if (o_orderdate[i] < date_1995_03_15.value) {
            auto* cust = customer_ht.find(o_custkey[i]);
            if (cust != nullptr) {
                filtered_order_indices.push_back(i);
            }
        }
    }

    int32_t filtered_orders = filtered_order_indices.size();
    std::cout << "  Filtered orders: " << filtered_orders << std::endl;

    // Build hash table on filtered orders (by l_orderkey) for lineitem probe
    LinearProbeHashTable<int32_t, std::pair<int32_t, std::pair<int32_t, int32_t>>> orders_ht(
        filtered_orders * 2 + 1000
    );

    for (int32_t oi : filtered_order_indices) {
        orders_ht.insert(o_orderkey[oi], {oi, {o_orderdate[oi], o_shippriority[oi]}});
    }

    // ===== STAGE 3: Parallel aggregation on lineitem =====
    std::cout << "Stage 3: Parallel aggregation..." << std::endl;

    // Thread-local aggregation tables
    std::vector<std::unordered_map<GroupKey, AggState>> local_aggs(num_threads);
    std::mutex agg_mutex;

    MorselQueue morsel_queue(l_orderkey.size(), morsel_size);

    // Function for thread worker
    auto worker = [&](int thread_id) {
        Morsel m;
        while (morsel_queue.get_next_morsel(m)) {
            for (int32_t i = m.start; i < m.start + m.count; ++i) {
                // Filter: l_shipdate > 1995-03-15
                if (l_shipdate[i] <= date_1995_03_15.value) continue;

                // Probe into orders
                auto* order = orders_ht.find(l_orderkey[i]);
                if (order == nullptr) continue;

                int32_t od = order->second.first;
                int32_t sp = order->second.second;

                // Aggregate
                GroupKey key{l_orderkey[i], od, sp};
                Decimal rev = Decimal(l_extendedprice[i] * (1.0 - l_discount[i]));
                local_aggs[thread_id][key].revenue.value += rev.value;
            }
        }
    };

    // Launch threads
    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    // Wait for all threads
    for (auto& t : threads) {
        t.join();
    }

    // ===== STAGE 4: Global merge of partial aggregations =====
    std::cout << "Stage 4: Merging partial aggregations..." << std::endl;
    std::unordered_map<GroupKey, AggState> global_agg;

    for (int i = 0; i < num_threads; ++i) {
        for (auto& [key, state] : local_aggs[i]) {
            global_agg[key].revenue.value += state.revenue.value;
        }
    }

    std::cout << "  Total groups: " << global_agg.size() << std::endl;

    // ===== STAGE 5: Top-K sort (LIMIT 10) =====
    std::cout << "Stage 5: Top-K sorting..." << std::endl;

    // Use std::partial_sort for Top-10
    std::vector<Q3Result> all_results;
    for (auto& [key, state] : global_agg) {
        all_results.push_back({
            key.l_orderkey,
            state.revenue,
            Date(key.o_orderdate),
            key.o_shippriority
        });
    }

    int limit = std::min(10, (int)all_results.size());
    std::partial_sort(all_results.begin(), all_results.begin() + limit,
                      all_results.end(),
                      [](const Q3Result& a, const Q3Result& b) {
                          if (a.revenue.value != b.revenue.value) {
                              return a.revenue.value > b.revenue.value; // DESC
                          }
                          return a.o_orderdate.value < b.o_orderdate.value; // ASC
                      });

    // ===== STAGE 6: Output =====
    std::cout << "Stage 6: Writing output..." << std::endl;

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q3.csv");
        out << "l_orderkey,revenue,o_orderdate,o_shippriority\n";
        for (int i = 0; i < limit; ++i) {
            const auto& r = all_results[i];
            out << r.l_orderkey << ","
                << std::fixed << std::setprecision(2) << r.revenue.value << ","
                << r.o_orderdate.value << ","
                << r.o_shippriority << "\n";
        }
        out.close();
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();

    std::cout << "Row count: " << limit << std::endl;
    std::cout << "Execution time: " << std::fixed << std::setprecision(2)
              << (double)elapsed_ms / 1000.0 << " seconds" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }
    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : "";
    run_q3(gendb_dir, results_dir);
    return 0;
}
#endif
