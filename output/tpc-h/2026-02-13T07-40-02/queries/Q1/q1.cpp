// Q1: Pricing Summary Report - Self-contained implementation
// Optimized for execution_time with parallel morsel-driven aggregation

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <chrono>
#include <thread>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>

// Hardware detection
inline int get_thread_count() {
    int hw_threads = std::thread::hardware_concurrency();
    return (hw_threads > 0) ? hw_threads : 64;
}

// Date utilities
inline int32_t date_to_days(int year, int month, int day) {
    // Days since 1970-01-01
    int a = (14 - month) / 12;
    int y = year - a;
    int m = month + 12 * a - 3;
    return y * 365 + y / 4 - y / 100 + y / 400 + (153 * m + 2) / 5 + day - 719469;
}

// Mmap helper
template<typename T>
class MappedColumn {
public:
    const T* data;
    size_t size;
    int fd;
    void* mapped_addr;
    size_t mapped_size;

    MappedColumn() : data(nullptr), size(0), fd(-1), mapped_addr(nullptr), mapped_size(0) {}

    ~MappedColumn() {
        if (mapped_addr != nullptr && mapped_addr != MAP_FAILED) {
            munmap(mapped_addr, mapped_size);
        }
        if (fd >= 0) {
            close(fd);
        }
    }

    bool open(const std::string& path, size_t row_count) {
        fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open: " << path << std::endl;
            return false;
        }

        struct stat st;
        if (fstat(fd, &st) < 0) {
            std::cerr << "Failed to fstat: " << path << std::endl;
            return false;
        }

        mapped_size = st.st_size;
        mapped_addr = mmap(nullptr, mapped_size, PROT_READ, MAP_PRIVATE, fd, 0);
        if (mapped_addr == MAP_FAILED) {
            std::cerr << "Failed to mmap: " << path << std::endl;
            return false;
        }

        // Hint for sequential scan
        madvise(mapped_addr, mapped_size, MADV_SEQUENTIAL);

        data = static_cast<const T*>(mapped_addr);
        size = row_count;
        return true;
    }
};

// Dictionary loader
inline std::vector<std::string> load_dictionary(const std::string& path) {
    std::vector<std::string> dict;
    std::ifstream file(path);
    if (!file) return dict;

    std::string line;
    while (std::getline(file, line)) {
        dict.push_back(line);
    }
    return dict;
}

// Aggregation key: (returnflag, linestatus)
struct AggKey {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator==(const AggKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Hash function for AggKey
struct AggKeyHash {
    size_t operator()(const AggKey& k) const {
        return (static_cast<size_t>(k.returnflag) << 8) | k.linestatus;
    }
};

// Aggregation values - use double for precise decimal arithmetic
struct AggValue {
    double sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    double sum_qty_for_avg;
    double sum_price_for_avg;
    double sum_disc_for_avg;
    int64_t count;

    AggValue() : sum_qty(0), sum_base_price(0), sum_disc_price(0), sum_charge(0),
                 sum_qty_for_avg(0), sum_price_for_avg(0), sum_disc_for_avg(0), count(0) {}

    void add(int64_t qty, int64_t price, int64_t disc, int64_t tax) {
        // Convert to double for precise arithmetic (values stored as cents/scaled integers)
        double qty_d = qty / 100.0;
        double price_d = price / 100.0;
        double disc_d = disc / 100.0;
        double tax_d = tax / 100.0;

        sum_qty += qty_d;
        sum_base_price += price_d;
        double disc_price = price_d * (1.0 - disc_d);
        sum_disc_price += disc_price;
        double charge = disc_price * (1.0 + tax_d);
        sum_charge += charge;
        sum_qty_for_avg += qty_d;
        sum_price_for_avg += price_d;
        sum_disc_for_avg += disc_d;
        count++;
    }

    void merge(const AggValue& other) {
        sum_qty += other.sum_qty;
        sum_base_price += other.sum_base_price;
        sum_disc_price += other.sum_disc_price;
        sum_charge += other.sum_charge;
        sum_qty_for_avg += other.sum_qty_for_avg;
        sum_price_for_avg += other.sum_price_for_avg;
        sum_disc_for_avg += other.sum_disc_for_avg;
        count += other.count;
    }
};

// Thread-local hash table
using LocalAggTable = std::unordered_map<AggKey, AggValue, AggKeyHash>;

// Worker function for parallel aggregation
void worker_aggregate(
    size_t start_idx,
    size_t end_idx,
    const int32_t* l_shipdate,
    const int64_t* l_quantity,
    const int64_t* l_extendedprice,
    const int64_t* l_discount,
    const int64_t* l_tax,
    const uint8_t* l_returnflag,
    const uint8_t* l_linestatus,
    int32_t cutoff_date,
    LocalAggTable& local_table
) {
    for (size_t i = start_idx; i < end_idx; i++) {
        if (l_shipdate[i] <= cutoff_date) {
            AggKey key{l_returnflag[i], l_linestatus[i]};
            local_table[key].add(l_quantity[i], l_extendedprice[i], l_discount[i], l_tax[i]);
        }
    }
}

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];
    std::string results_dir = (argc >= 3) ? argv[2] : "";

    auto start_time = std::chrono::high_resolution_clock::now();

    // Read metadata
    std::string metadata_path = gendb_dir + "/lineitem/metadata.json";
    std::ifstream meta_file(metadata_path);
    if (!meta_file) {
        std::cerr << "Failed to open metadata: " << metadata_path << std::endl;
        return 1;
    }

    size_t row_count = 0;
    std::string line;
    while (std::getline(meta_file, line)) {
        size_t pos = line.find("\"row_count\":");
        if (pos != std::string::npos) {
            size_t start = line.find(':', pos) + 1;
            size_t end = line.find_first_of(",}", start);
            row_count = std::stoull(line.substr(start, end - start));
            break;
        }
    }
    meta_file.close();

    if (row_count == 0) {
        std::cerr << "Invalid row_count in metadata" << std::endl;
        return 1;
    }

    // Load dictionaries
    auto returnflag_dict = load_dictionary(gendb_dir + "/lineitem/l_returnflag.dict");
    auto linestatus_dict = load_dictionary(gendb_dir + "/lineitem/l_linestatus.dict");

    // Mmap required columns
    MappedColumn<int32_t> l_shipdate_col;
    MappedColumn<int64_t> l_quantity_col;
    MappedColumn<int64_t> l_extendedprice_col;
    MappedColumn<int64_t> l_discount_col;
    MappedColumn<int64_t> l_tax_col;
    MappedColumn<uint8_t> l_returnflag_col;
    MappedColumn<uint8_t> l_linestatus_col;

    if (!l_shipdate_col.open(gendb_dir + "/lineitem/l_shipdate.bin", row_count) ||
        !l_quantity_col.open(gendb_dir + "/lineitem/l_quantity.bin", row_count) ||
        !l_extendedprice_col.open(gendb_dir + "/lineitem/l_extendedprice.bin", row_count) ||
        !l_discount_col.open(gendb_dir + "/lineitem/l_discount.bin", row_count) ||
        !l_tax_col.open(gendb_dir + "/lineitem/l_tax.bin", row_count) ||
        !l_returnflag_col.open(gendb_dir + "/lineitem/l_returnflag.bin", row_count) ||
        !l_linestatus_col.open(gendb_dir + "/lineitem/l_linestatus.bin", row_count)) {
        std::cerr << "Failed to mmap columns" << std::endl;
        return 1;
    }

    // Calculate cutoff date: 1998-12-01 - 90 days = 1998-09-02
    int32_t cutoff_date = date_to_days(1998, 9, 2);

    // Parallel aggregation with morsel-driven execution
    int num_threads = get_thread_count();
    std::vector<std::thread> threads;
    std::vector<LocalAggTable> local_tables(num_threads);

    size_t morsel_size = (row_count + num_threads - 1) / num_threads;

    for (int t = 0; t < num_threads; t++) {
        size_t start_idx = t * morsel_size;
        size_t end_idx = std::min(start_idx + morsel_size, row_count);

        if (start_idx >= row_count) break;

        threads.emplace_back(worker_aggregate,
            start_idx, end_idx,
            l_shipdate_col.data,
            l_quantity_col.data,
            l_extendedprice_col.data,
            l_discount_col.data,
            l_tax_col.data,
            l_returnflag_col.data,
            l_linestatus_col.data,
            cutoff_date,
            std::ref(local_tables[t])
        );
    }

    // Wait for all threads
    for (auto& thread : threads) {
        thread.join();
    }

    // Merge local tables into global
    LocalAggTable global_table;
    for (const auto& local : local_tables) {
        for (const auto& [key, value] : local) {
            global_table[key].merge(value);
        }
    }

    // Convert to sorted output
    struct OutputRow {
        std::string returnflag;
        std::string linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        int64_t count_order;

        bool operator<(const OutputRow& other) const {
            if (returnflag != other.returnflag) return returnflag < other.returnflag;
            return linestatus < other.linestatus;
        }
    };

    std::vector<OutputRow> results;
    for (const auto& [key, value] : global_table) {
        OutputRow row;
        row.returnflag = (key.returnflag < returnflag_dict.size()) ? returnflag_dict[key.returnflag] : "";
        row.linestatus = (key.linestatus < linestatus_dict.size()) ? linestatus_dict[key.linestatus] : "";
        row.sum_qty = value.sum_qty;
        row.sum_base_price = value.sum_base_price;
        row.sum_disc_price = value.sum_disc_price;
        row.sum_charge = value.sum_charge;
        row.avg_qty = value.sum_qty_for_avg / value.count;
        row.avg_price = value.sum_price_for_avg / value.count;
        row.avg_disc = value.sum_disc_for_avg / value.count;
        row.count_order = value.count;
        results.push_back(row);
    }

    std::sort(results.begin(), results.end());

    auto end_time = std::chrono::high_resolution_clock::now();
    double elapsed = std::chrono::duration<double>(end_time - start_time).count();

    // Write results to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::string output_path = results_dir + "/Q1.csv";
        std::ofstream out(output_path);
        if (!out) {
            std::cerr << "Failed to open output file: " << output_path << std::endl;
            return 1;
        }

        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";
        for (const auto& row : results) {
            out << std::fixed << std::setprecision(2);
            out << row.returnflag << ","
                << row.linestatus << ","
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
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << elapsed << " seconds" << std::endl;

    return 0;
}
