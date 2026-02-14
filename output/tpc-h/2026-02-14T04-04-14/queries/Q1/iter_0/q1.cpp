#include <iostream>
#include <fstream>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <cstring>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>
#include <sys/stat.h>
#include <iomanip>
#include <chrono>
#include <algorithm>
#include <cmath>

// Constants for TPC-H dictionary values (empirically determined from storage)
// l_returnflag codes: 0=N, 1=R, 2=A
// l_linestatus codes: 0=O, 1=F
const char RETURNFLAG_VALUES[] = {'N', 'R', 'A'};
const char LINESTATUS_VALUES[] = {'O', 'F'};

// Date constant: 1998-09-02 = 10471 days since epoch
constexpr int32_t DATE_THRESHOLD = 10471;

// Aggregation result per group
// Use long double for accumulation to reduce floating-point errors
struct AggregationResult {
    long double sum_qty = 0.0;
    long double sum_base_price = 0.0;
    long double sum_disc_price = 0.0;
    long double sum_charge = 0.0;
    long double sum_quantity_for_avg = 0.0;
    long double sum_price_for_avg = 0.0;
    long double sum_discount_for_avg = 0.0;
    int64_t count = 0;
};

// Pair of (returnflag, linestatus) for grouping
struct GroupKey {
    char returnflag;
    char linestatus;

    bool operator==(const GroupKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

// Hash function for GroupKey
struct GroupKeyHash {
    size_t operator()(const GroupKey& key) const {
        return (static_cast<size_t>(key.returnflag) << 8) | static_cast<size_t>(key.linestatus);
    }
};

// Helper function to mmap a file
template<typename T>
const T* mmap_file(const std::string& filename, size_t& row_count) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error: Cannot open " << filename << std::endl;
        return nullptr;
    }

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        std::cerr << "Error: Cannot stat " << filename << std::endl;
        close(fd);
        return nullptr;
    }

    size_t file_size = sb.st_size;
    row_count = file_size / sizeof(T);

    void* ptr = mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);

    if (ptr == MAP_FAILED) {
        std::cerr << "Error: Cannot mmap " << filename << std::endl;
        return nullptr;
    }

    // Hint for sequential access
    madvise(ptr, file_size, MADV_SEQUENTIAL);

    return static_cast<const T*>(ptr);
}

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    // Open column files
    std::string lineitem_dir = gendb_dir + "/lineitem.";

    size_t row_count_shipdate = 0;
    const int32_t* l_shipdate_delta = mmap_file<int32_t>(lineitem_dir + "l_shipdate.col", row_count_shipdate);
    if (!l_shipdate_delta) return;

    size_t row_count_returnflag = 0;
    const uint8_t* l_returnflag = mmap_file<uint8_t>(lineitem_dir + "l_returnflag.col", row_count_returnflag);
    if (!l_returnflag) return;

    size_t row_count_linestatus = 0;
    const uint8_t* l_linestatus = mmap_file<uint8_t>(lineitem_dir + "l_linestatus.col", row_count_linestatus);
    if (!l_linestatus) return;

    size_t row_count_quantity = 0;
    const double* l_quantity = mmap_file<double>(lineitem_dir + "l_quantity.col", row_count_quantity);
    if (!l_quantity) return;

    size_t row_count_extendedprice = 0;
    const double* l_extendedprice = mmap_file<double>(lineitem_dir + "l_extendedprice.col", row_count_extendedprice);
    if (!l_extendedprice) return;

    size_t row_count_discount = 0;
    const double* l_discount = mmap_file<double>(lineitem_dir + "l_discount.col", row_count_discount);
    if (!l_discount) return;

    size_t row_count_tax = 0;
    const double* l_tax = mmap_file<double>(lineitem_dir + "l_tax.col", row_count_tax);
    if (!l_tax) return;

    // Verify all columns have same row count
    const size_t total_rows = row_count_shipdate;
    if (row_count_returnflag != total_rows || row_count_linestatus != total_rows ||
        row_count_quantity != total_rows || row_count_extendedprice != total_rows ||
        row_count_discount != total_rows || row_count_tax != total_rows) {
        std::cerr << "Error: Row counts mismatch" << std::endl;
        return;
    }

    // Note: Despite being labeled "delta" encoding, l_shipdate values are already
    // absolute epoch days (not deltas). Use them directly.
    const int32_t* l_shipdate = l_shipdate_delta;

    // Thread-safe global aggregation
    std::mutex agg_mutex;
    std::unordered_map<GroupKey, AggregationResult, GroupKeyHash> global_aggregation;

    // Morsel-driven parallelism
    const size_t num_threads = std::thread::hardware_concurrency();
    const size_t morsel_size = 100000;
    std::vector<std::thread> threads;

    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            // Local aggregation per thread
            std::unordered_map<GroupKey, AggregationResult, GroupKeyHash> local_agg;

            // Process morsels assigned to this thread
            for (size_t start_idx = t * morsel_size; start_idx < total_rows;
                 start_idx += num_threads * morsel_size) {
                size_t end_idx = std::min(start_idx + morsel_size, total_rows);

                for (size_t i = start_idx; i < end_idx; ++i) {
                    // Filter: l_shipdate <= DATE '1998-09-02' (10471 days)
                    if (l_shipdate[i] > DATE_THRESHOLD) {
                        continue;
                    }

                    // Decode dictionary values
                    char returnflag = RETURNFLAG_VALUES[l_returnflag[i]];
                    char linestatus = LINESTATUS_VALUES[l_linestatus[i]];

                    GroupKey key = {returnflag, linestatus};

                    // Aggregation calculations
                    double qty = l_quantity[i];
                    double price = l_extendedprice[i];
                    double disc = l_discount[i];
                    double tax = l_tax[i];

                    double disc_price = price * (1.0 - disc);
                    double charge = price * (1.0 - disc) * (1.0 + tax);

                    // Update local aggregation
                    AggregationResult& result = local_agg[key];
                    result.sum_qty += qty;
                    result.sum_base_price += price;
                    result.sum_disc_price += disc_price;
                    result.sum_charge += charge;
                    result.sum_quantity_for_avg += qty;
                    result.sum_price_for_avg += price;
                    result.sum_discount_for_avg += disc;
                    result.count += 1;
                }
            }

            // Merge local aggregation into global (with locking)
            {
                std::lock_guard<std::mutex> lock(agg_mutex);
                for (auto& [key, result] : local_agg) {
                    AggregationResult& global_result = global_aggregation[key];
                    global_result.sum_qty += result.sum_qty;
                    global_result.sum_base_price += result.sum_base_price;
                    global_result.sum_disc_price += result.sum_disc_price;
                    global_result.sum_charge += result.sum_charge;
                    global_result.sum_quantity_for_avg += result.sum_quantity_for_avg;
                    global_result.sum_price_for_avg += result.sum_price_for_avg;
                    global_result.sum_discount_for_avg += result.sum_discount_for_avg;
                    global_result.count += result.count;
                }
            }
        });
    }

    // Wait for all threads to finish
    for (auto& thread : threads) {
        thread.join();
    }

    // Sort results by returnflag and linestatus
    std::vector<std::pair<GroupKey, AggregationResult>> sorted_results;
    for (auto& [key, result] : global_aggregation) {
        sorted_results.push_back({key, result});
    }
    std::sort(sorted_results.begin(), sorted_results.end(),
              [](const auto& a, const auto& b) {
                  if (a.first.returnflag != b.first.returnflag) {
                      return a.first.returnflag < b.first.returnflag;
                  }
                  return a.first.linestatus < b.first.linestatus;
              });

    // Calculate total rows
    int64_t total_result_rows = sorted_results.size();

    // Write results to CSV if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream outfile(results_dir + "/Q1.csv");
        outfile << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        for (const auto& [key, result] : sorted_results) {
            double avg_qty = result.count > 0 ? result.sum_quantity_for_avg / result.count : 0.0;
            double avg_price = result.count > 0 ? result.sum_price_for_avg / result.count : 0.0;
            double avg_disc = result.count > 0 ? result.sum_discount_for_avg / result.count : 0.0;

            outfile << key.returnflag << ","
                    << key.linestatus << ","
                    << std::fixed << std::setprecision(2) << result.sum_qty << ","
                    << std::fixed << std::setprecision(2) << result.sum_base_price << ","
                    << std::fixed << std::setprecision(4) << result.sum_disc_price << ","
                    << std::fixed << std::setprecision(6) << result.sum_charge << ","
                    << std::fixed << std::setprecision(2) << avg_qty << ","
                    << std::fixed << std::setprecision(2) << avg_price << ","
                    << std::fixed << std::setprecision(2) << avg_disc << ","
                    << std::fixed << std::setprecision(0) << result.count << "\n";
        }
        outfile.close();
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    std::cout << "Query returned " << total_result_rows << " rows\n";
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << duration_ms << " ms\n";

    // Cleanup mmapped files
    munmap((void*)l_shipdate_delta, row_count_shipdate * sizeof(int32_t));
    munmap((void*)l_returnflag, row_count_returnflag * sizeof(uint8_t));
    munmap((void*)l_linestatus, row_count_linestatus * sizeof(uint8_t));
    munmap((void*)l_quantity, row_count_quantity * sizeof(double));
    munmap((void*)l_extendedprice, row_count_extendedprice * sizeof(double));
    munmap((void*)l_discount, row_count_discount * sizeof(double));
    munmap((void*)l_tax, row_count_tax * sizeof(double));
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }
    run_q1(argv[1], argc > 2 ? argv[2] : "");
    return 0;
}
#endif
