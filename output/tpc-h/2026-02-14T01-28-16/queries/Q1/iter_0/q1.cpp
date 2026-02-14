// q1.cpp - TPC-H Q1: Pricing Summary Report
// Self-contained implementation with parallel execution and dictionary handling

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <immintrin.h>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>

// Aggregate state for one group
struct AggState {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_qty_for_avg = 0.0;
    double sum_price_for_avg = 0.0;
    double sum_disc_for_avg = 0.0;
    int64_t count = 0;
};

// Result row
struct ResultRow {
    uint8_t returnflag;
    uint8_t linestatus;
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double avg_qty;
    double avg_price;
    double avg_disc;
    int64_t count_order;
};

// Zone map entry
struct ZoneMapEntry {
    int32_t min_val;
    int32_t max_val;
    int64_t start_offset;
    int64_t end_offset;
};

// Memory-mapped file wrapper
class MMapFile {
public:
    MMapFile(const std::string& path) : fd_(-1), data_(nullptr), size_(0) {
        fd_ = open(path.c_str(), O_RDONLY);
        if (fd_ < 0) {
            throw std::runtime_error("Failed to open: " + path);
        }
        struct stat st;
        if (fstat(fd_, &st) != 0) {
            close(fd_);
            throw std::runtime_error("Failed to stat: " + path);
        }
        size_ = st.st_size;
        data_ = mmap(nullptr, size_, PROT_READ, MAP_PRIVATE, fd_, 0);
        if (data_ == MAP_FAILED) {
            close(fd_);
            throw std::runtime_error("Failed to mmap: " + path);
        }
        madvise(data_, size_, MADV_SEQUENTIAL | MADV_WILLNEED);
    }

    ~MMapFile() {
        if (data_ != nullptr && data_ != MAP_FAILED) {
            munmap(data_, size_);
        }
        if (fd_ >= 0) {
            close(fd_);
        }
    }

    template<typename T>
    const T* data() const { return static_cast<const T*>(data_); }

    size_t size() const { return size_; }

    template<typename T>
    size_t count() const { return size_ / sizeof(T); }

private:
    int fd_;
    void* data_;
    size_t size_;
};

// Load zone map
std::vector<ZoneMapEntry> load_zone_map(const std::string& path) {
    std::vector<ZoneMapEntry> entries;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) return entries;

    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        return entries;
    }

    size_t size = st.st_size;
    void* data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        close(fd);
        return entries;
    }

    const int32_t* ptr = static_cast<const int32_t*>(data);
    size_t num_entries = size / (4 * sizeof(int32_t));

    for (size_t i = 0; i < num_entries; ++i) {
        ZoneMapEntry entry;
        entry.min_val = ptr[i * 4];
        entry.max_val = ptr[i * 4 + 1];
        entry.start_offset = static_cast<int64_t>(ptr[i * 4 + 2]);
        entry.end_offset = static_cast<int64_t>(ptr[i * 4 + 3]);
        entries.push_back(entry);
    }

    munmap(data, size);
    close(fd);
    return entries;
}

// Load dictionary mapping from metadata.json
std::unordered_map<uint8_t, char> load_dict_from_metadata(
    const std::string& metadata_path,
    const std::string& col_name
) {
    std::unordered_map<uint8_t, char> dict;
    std::ifstream f(metadata_path);
    if (!f.is_open()) return dict;

    std::string content((std::istreambuf_iterator<char>(f)),
                        std::istreambuf_iterator<char>());

    // Parse JSON manually for the specific dictionary
    std::string key = "\"" + col_name + "\"";
    size_t pos = content.find(key);
    if (pos == std::string::npos) return dict;

    pos = content.find('[', pos);
    if (pos == std::string::npos) return dict;

    size_t end = content.find(']', pos);
    if (end == std::string::npos) return dict;

    std::string values_str = content.substr(pos + 1, end - pos - 1);

    // Parse array of quoted values
    uint8_t idx = 0;
    size_t start = 0;
    while (start < values_str.size()) {
        size_t quote1 = values_str.find('"', start);
        if (quote1 == std::string::npos) break;
        size_t quote2 = values_str.find('"', quote1 + 1);
        if (quote2 == std::string::npos) break;

        std::string val = values_str.substr(quote1 + 1, quote2 - quote1 - 1);
        if (!val.empty()) {
            dict[idx++] = val[0];  // For single-char values
        }
        start = quote2 + 1;
    }

    return dict;
}

// Parallel aggregation worker
void aggregate_worker(
    const int32_t* shipdate,
    const uint8_t* returnflag,
    const uint8_t* linestatus,
    const double* quantity,
    const double* extendedprice,
    const double* discount,
    const double* tax,
    size_t start,
    size_t end,
    int32_t date_threshold,
    std::vector<AggState>& local_aggs
) {
    // Local hash table: key = (returnflag * 10 + linestatus)
    // returnflag: 0,1,2 (N=0,R=1,A=2), linestatus: 0,1 (O=0,F=1)
    // Max 6 combinations
    std::vector<AggState> aggs(30, AggState{});  // Over-allocate for safety

    for (size_t i = start; i < end; ++i) {
        if (shipdate[i] > date_threshold) continue;

        uint8_t rf = returnflag[i];
        uint8_t ls = linestatus[i];
        uint8_t key = rf * 10 + ls;

        double qty = quantity[i];
        double price = extendedprice[i];
        double disc = discount[i];
        double t = tax[i];

        double disc_price = price * (1.0 - disc);
        double charge = disc_price * (1.0 + t);

        aggs[key].sum_qty += qty;
        aggs[key].sum_base_price += price;
        aggs[key].sum_disc_price += disc_price;
        aggs[key].sum_charge += charge;
        aggs[key].sum_qty_for_avg += qty;
        aggs[key].sum_price_for_avg += price;
        aggs[key].sum_disc_for_avg += disc;
        aggs[key].count++;
    }

    local_aggs = std::move(aggs);
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Calculate date threshold: 1998-12-01 - 90 days = 1998-09-02
    // 1998-12-01 = 10562 days since 1970-01-01
    // 10562 - 90 = 10472
    const int32_t date_threshold = 10472;

    // Load metadata for dictionaries
    std::string metadata_path = gendb_dir + "/metadata.json";
    auto returnflag_dict = load_dict_from_metadata(metadata_path, "lineitem_l_returnflag");
    auto linestatus_dict = load_dict_from_metadata(metadata_path, "lineitem_l_linestatus");

    // Load zone map
    std::string zonemap_path = gendb_dir + "/lineitem_l_shipdate_zonemap.idx";
    auto zone_map = load_zone_map(zonemap_path);

    // Load columns
    MMapFile shipdate_file(gendb_dir + "/lineitem_l_shipdate.bin");
    MMapFile returnflag_file(gendb_dir + "/lineitem_l_returnflag.bin");
    MMapFile linestatus_file(gendb_dir + "/lineitem_l_linestatus.bin");
    MMapFile quantity_file(gendb_dir + "/lineitem_l_quantity.bin");
    MMapFile extendedprice_file(gendb_dir + "/lineitem_l_extendedprice.bin");
    MMapFile discount_file(gendb_dir + "/lineitem_l_discount.bin");
    MMapFile tax_file(gendb_dir + "/lineitem_l_tax.bin");

    const int32_t* shipdate = shipdate_file.data<int32_t>();
    const uint8_t* returnflag = returnflag_file.data<uint8_t>();
    const uint8_t* linestatus = linestatus_file.data<uint8_t>();
    const double* quantity = quantity_file.data<double>();
    const double* extendedprice = extendedprice_file.data<double>();
    const double* discount = discount_file.data<double>();
    const double* tax = tax_file.data<double>();

    size_t row_count = shipdate_file.count<int32_t>();

    // Zone map pruning: identify valid row ranges
    std::vector<std::pair<size_t, size_t>> valid_ranges;
    if (!zone_map.empty()) {
        for (const auto& entry : zone_map) {
            if (entry.min_val <= date_threshold) {
                valid_ranges.emplace_back(entry.start_offset, entry.end_offset);
            }
        }
    } else {
        // No zone map, scan all
        valid_ranges.emplace_back(0, row_count);
    }

    // Parallel execution
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 64;

    std::vector<std::thread> threads;
    std::vector<std::vector<AggState>> thread_local_aggs(num_threads);

    // Distribute work across valid ranges
    size_t total_work = 0;
    for (const auto& range : valid_ranges) {
        total_work += (range.second - range.first);
    }

    const size_t morsel_size = 50000;
    std::atomic<size_t> next_morsel{0};

    // Build morsel list from valid ranges
    std::vector<std::pair<size_t, size_t>> morsels;
    for (const auto& range : valid_ranges) {
        size_t start = range.first;
        while (start < range.second) {
            size_t end = std::min(start + morsel_size, range.second);
            morsels.emplace_back(start, end);
            start = end;
        }
    }

    // Worker function
    auto worker = [&](size_t thread_id) {
        std::vector<AggState> local_aggs(30, AggState{});

        while (true) {
            size_t morsel_idx = next_morsel.fetch_add(1, std::memory_order_relaxed);
            if (morsel_idx >= morsels.size()) break;

            auto [start, end] = morsels[morsel_idx];

            // Process morsel
            for (size_t i = start; i < end; ++i) {
                if (shipdate[i] > date_threshold) continue;

                uint8_t rf = returnflag[i];
                uint8_t ls = linestatus[i];
                uint8_t key = rf * 10 + ls;

                double qty = quantity[i];
                double price = extendedprice[i];
                double disc = discount[i];
                double t = tax[i];

                double disc_price = price * (1.0 - disc);
                double charge = disc_price * (1.0 + t);

                local_aggs[key].sum_qty += qty;
                local_aggs[key].sum_base_price += price;
                local_aggs[key].sum_disc_price += disc_price;
                local_aggs[key].sum_charge += charge;
                local_aggs[key].sum_qty_for_avg += qty;
                local_aggs[key].sum_price_for_avg += price;
                local_aggs[key].sum_disc_for_avg += disc;
                local_aggs[key].count++;
            }
        }

        thread_local_aggs[thread_id] = std::move(local_aggs);
    };

    // Launch threads
    for (unsigned int i = 0; i < num_threads; ++i) {
        threads.emplace_back(worker, i);
    }

    // Wait for completion
    for (auto& t : threads) {
        t.join();
    }

    // Merge thread-local results
    std::vector<AggState> global_aggs(30, AggState{});
    for (const auto& local : thread_local_aggs) {
        for (size_t key = 0; key < 30; ++key) {
            if (local[key].count > 0) {
                global_aggs[key].sum_qty += local[key].sum_qty;
                global_aggs[key].sum_base_price += local[key].sum_base_price;
                global_aggs[key].sum_disc_price += local[key].sum_disc_price;
                global_aggs[key].sum_charge += local[key].sum_charge;
                global_aggs[key].sum_qty_for_avg += local[key].sum_qty_for_avg;
                global_aggs[key].sum_price_for_avg += local[key].sum_price_for_avg;
                global_aggs[key].sum_disc_for_avg += local[key].sum_disc_for_avg;
                global_aggs[key].count += local[key].count;
            }
        }
    }

    // Build result rows
    std::vector<ResultRow> results;
    for (size_t key = 0; key < 30; ++key) {
        if (global_aggs[key].count > 0) {
            ResultRow row;
            row.returnflag = key / 10;
            row.linestatus = key % 10;
            row.sum_qty = global_aggs[key].sum_qty;
            row.sum_base_price = global_aggs[key].sum_base_price;
            row.sum_disc_price = global_aggs[key].sum_disc_price;
            row.sum_charge = global_aggs[key].sum_charge;
            row.avg_qty = global_aggs[key].sum_qty_for_avg / global_aggs[key].count;
            row.avg_price = global_aggs[key].sum_price_for_avg / global_aggs[key].count;
            row.avg_disc = global_aggs[key].sum_disc_for_avg / global_aggs[key].count;
            row.count_order = global_aggs[key].count;
            results.push_back(row);
        }
    }

    // Sort by returnflag, linestatus
    std::sort(results.begin(), results.end(), [](const ResultRow& a, const ResultRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();

    // Output results
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";
        out << std::fixed << std::setprecision(2);

        for (const auto& row : results) {
            char rf = returnflag_dict.count(row.returnflag) ? returnflag_dict[row.returnflag] : '?';
            char ls = linestatus_dict.count(row.linestatus) ? linestatus_dict[row.linestatus] : '?';

            out << rf << "," << ls << ","
                << row.sum_qty << ","
                << row.sum_base_price << ","
                << row.sum_disc_price << ","
                << row.sum_charge << ","
                << row.avg_qty << ","
                << row.avg_price << ","
                << row.avg_disc << ","
                << row.count_order << "\n";
        }
        out.close();
    }

    // Print summary to terminal
    std::cout << "Q1 Results: " << results.size() << " rows" << std::endl;
    std::cout << "Execution time: " << std::fixed << std::setprecision(3)
              << (elapsed * 1000.0) << " ms" << std::endl;
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = argc > 2 ? argv[2] : "";

    try {
        run_q1(gendb_dir, results_dir);
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
#endif
