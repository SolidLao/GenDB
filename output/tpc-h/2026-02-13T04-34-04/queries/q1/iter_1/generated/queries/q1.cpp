/**
 * TPC-H Query 1 — High-Performance Implementation
 *
 * Single-table aggregation over lineitem with date filter.
 * Optimizations:
 *   - Column projection (7 of 16 columns)
 *   - Row group pruning on l_shipdate (FIXED: correct <= filter logic)
 *   - Thread-parallel processing (64 cores)
 *   - Fused filter + compute + aggregate
 *   - Open-addressing hash table for cache locality
 *   - AVX-512 SIMD vectorization (8-wide doubles)
 *   - Integer key packing for reduced hash table overhead
 */

#include "parquet_reader.h"
#include <iostream>
#include <fstream>
#include <iomanip>
#include <chrono>
#include <thread>
#include <vector>
#include <string>
#include <algorithm>
#include <cmath>
#include <mutex>
#include <cstring>
#include <immintrin.h>

// Aggregation key: (l_returnflag, l_linestatus) packed into int32_t
// This reduces hash table overhead vs struct key
inline int32_t make_group_key(char returnflag, char linestatus) {
    return (static_cast<int32_t>(returnflag) << 8) | static_cast<int32_t>(linestatus);
}

inline void unpack_group_key(int32_t key, char& returnflag, char& linestatus) {
    returnflag = static_cast<char>((key >> 8) & 0xFF);
    linestatus = static_cast<char>(key & 0xFF);
}

// Aggregation values (simple summation, no Kahan - double precision is sufficient)
struct AggValues {
    double sum_qty = 0.0;
    double sum_base_price = 0.0;
    double sum_disc_price = 0.0;
    double sum_charge = 0.0;
    double sum_discount = 0.0;
    int64_t count = 0;

    void merge(const AggValues& other) {
        sum_qty += other.sum_qty;
        sum_base_price += other.sum_base_price;
        sum_disc_price += other.sum_disc_price;
        sum_charge += other.sum_charge;
        sum_discount += other.sum_discount;
        count += other.count;
    }
};

// Open-addressing hash table with linear probing
// Power-of-2 sizing for fast modulo via bitmasking
class OpenAddressHashTable {
private:
    static constexpr int32_t EMPTY_KEY = -1;
    static constexpr size_t INITIAL_CAPACITY = 16;
    static constexpr double MAX_LOAD_FACTOR = 0.75;

    struct Entry {
        int32_t key;
        AggValues value;
    };

    std::vector<Entry> entries_;
    size_t size_;
    size_t capacity_;
    size_t mask_;

    // MurmurHash3 finalizer for good distribution
    static inline uint32_t hash_int32(int32_t key) {
        uint32_t h = static_cast<uint32_t>(key);
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }

    void resize() {
        size_t new_capacity = capacity_ * 2;
        size_t new_mask = new_capacity - 1;
        std::vector<Entry> new_entries(new_capacity);
        for (auto& e : new_entries) e.key = EMPTY_KEY;

        for (const auto& old_entry : entries_) {
            if (old_entry.key != EMPTY_KEY) {
                size_t idx = hash_int32(old_entry.key) & new_mask;
                while (new_entries[idx].key != EMPTY_KEY) {
                    idx = (idx + 1) & new_mask;
                }
                new_entries[idx] = old_entry;
            }
        }

        entries_ = std::move(new_entries);
        capacity_ = new_capacity;
        mask_ = new_mask;
    }

public:
    OpenAddressHashTable() : size_(0), capacity_(INITIAL_CAPACITY), mask_(INITIAL_CAPACITY - 1) {
        entries_.resize(capacity_);
        for (auto& e : entries_) e.key = EMPTY_KEY;
    }

    AggValues& operator[](int32_t key) {
        if (size_ >= capacity_ * MAX_LOAD_FACTOR) {
            resize();
        }

        size_t idx = hash_int32(key) & mask_;
        while (true) {
            if (entries_[idx].key == EMPTY_KEY) {
                entries_[idx].key = key;
                size_++;
                return entries_[idx].value;
            }
            if (entries_[idx].key == key) {
                return entries_[idx].value;
            }
            idx = (idx + 1) & mask_;
        }
    }

    // Iterator support for range-based for loops
    struct Iterator {
        std::vector<Entry>::iterator it, end;
        Iterator(std::vector<Entry>::iterator start, std::vector<Entry>::iterator finish) : it(start), end(finish) {
            while (it != end && it->key == EMPTY_KEY) ++it;
        }
        Iterator& operator++() {
            do { ++it; } while (it != end && it->key == EMPTY_KEY);
            return *this;
        }
        bool operator!=(const Iterator& other) const { return it != other.it; }
        std::pair<int32_t, AggValues&> operator*() { return {it->key, it->value}; }
    };

    Iterator begin() { return Iterator(entries_.begin(), entries_.end()); }
    Iterator end() { return Iterator(entries_.end(), entries_.end()); }
};

using AggMap = OpenAddressHashTable;

// SIMD-vectorized chunk processing with AVX-512
void process_chunk(
    int64_t start_row,
    int64_t end_row,
    const std::vector<std::string>& returnflag,
    const std::vector<std::string>& linestatus,
    const double* quantity,
    const double* extendedprice,
    const double* discount,
    const double* tax,
    const int32_t* shipdate,
    int32_t date_threshold,
    AggMap& local_map)
{
    // Check for AVX-512 support at runtime
    bool has_avx512 = __builtin_cpu_supports("avx512f");

    int64_t i = start_row;

    // SIMD processing (8 doubles per iteration with AVX-512, or 4 with AVX2)
    if (has_avx512) {
        // AVX-512 path: process 8 rows at a time
        constexpr int SIMD_WIDTH = 8;
        int64_t simd_end = end_row - (end_row - start_row) % SIMD_WIDTH;

        __m512d ones = _mm512_set1_pd(1.0);

        for (; i < simd_end; i += SIMD_WIDTH) {
            // Load shipdate (8 int32s) and create mask for valid rows
            __m256i dates_256 = _mm256_loadu_si256((__m256i*)&shipdate[i]);
            __m256i thresh_256 = _mm256_set1_epi32(date_threshold);
            __mmask8 valid_mask = _mm256_cmp_epi32_mask(dates_256, thresh_256, _MM_CMPINT_LE);

            if (valid_mask == 0) continue; // All rows filtered out

            // Load data for valid rows (always load, mask will handle filtering)
            __m512d qty = _mm512_loadu_pd(&quantity[i]);
            __m512d price = _mm512_loadu_pd(&extendedprice[i]);
            __m512d disc = _mm512_loadu_pd(&discount[i]);
            __m512d tx = _mm512_loadu_pd(&tax[i]);

            // Compute derived values: disc_price = price * (1 - disc)
            __m512d one_minus_disc = _mm512_sub_pd(ones, disc);
            __m512d disc_price = _mm512_mul_pd(price, one_minus_disc);

            // Compute charge = disc_price * (1 + tax)
            __m512d one_plus_tax = _mm512_add_pd(ones, tx);
            __m512d charge = _mm512_mul_pd(disc_price, one_plus_tax);

            // Scalar fallback for group keys and accumulation (can't vectorize hash table lookup efficiently)
            alignas(64) double qty_arr[SIMD_WIDTH];
            alignas(64) double price_arr[SIMD_WIDTH];
            alignas(64) double disc_arr[SIMD_WIDTH];
            alignas(64) double disc_price_arr[SIMD_WIDTH];
            alignas(64) double charge_arr[SIMD_WIDTH];

            _mm512_store_pd(qty_arr, qty);
            _mm512_store_pd(price_arr, price);
            _mm512_store_pd(disc_arr, disc);
            _mm512_store_pd(disc_price_arr, disc_price);
            _mm512_store_pd(charge_arr, charge);

            for (int j = 0; j < SIMD_WIDTH; j++) {
                if (!(valid_mask & (1 << j))) continue;

                int64_t idx = i + j;
                char rf = returnflag[idx].empty() ? ' ' : returnflag[idx][0];
                char ls = linestatus[idx].empty() ? ' ' : linestatus[idx][0];
                int32_t key = make_group_key(rf, ls);

                // Use hash table directly (no local SIMD accumulators due to unpredictable groups)
                AggValues& agg = local_map[key];
                agg.sum_qty += qty_arr[j];
                agg.sum_base_price += price_arr[j];
                agg.sum_disc_price += disc_price_arr[j];
                agg.sum_charge += charge_arr[j];
                agg.sum_discount += disc_arr[j];
                agg.count++;
            }
        }
    }

    // Scalar fallback for remainder or if no AVX-512
    for (; i < end_row; i++) {
        if (shipdate[i] > date_threshold) continue;

        char rf = returnflag[i].empty() ? ' ' : returnflag[i][0];
        char ls = linestatus[i].empty() ? ' ' : linestatus[i][0];
        int32_t key = make_group_key(rf, ls);

        AggValues& agg = local_map[key];

        double qty = quantity[i];
        double price = extendedprice[i];
        double disc = discount[i];
        double tx = tax[i];
        double disc_price = price * (1.0 - disc);
        double charge = disc_price * (1.0 + tx);

        agg.sum_qty += qty;
        agg.sum_base_price += price;
        agg.sum_disc_price += disc_price;
        agg.sum_charge += charge;
        agg.sum_discount += disc;
        agg.count++;
    }
}

void run_q1(const std::string& parquet_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Date filter: '1998-12-01' - 90 days = '1998-09-02'
    int32_t date_threshold = date_to_days(1998, 9, 2);

    std::string lineitem_path = parquet_dir + "/lineitem.parquet";

    // Step 1: Row group pruning on l_shipdate
    auto stats = get_row_group_stats(lineitem_path, "l_shipdate");
    std::vector<int> relevant_rgs;
    for (const auto& s : stats) {
        if (s.has_min_max) {
            // Include row group if min <= threshold (any row could match filter l_shipdate <= threshold)
            if (s.min_int <= date_threshold) {
                relevant_rgs.push_back(s.row_group_index);
            }
        } else {
            // No stats, must include
            relevant_rgs.push_back(s.row_group_index);
        }
    }

    std::cout << "Row groups: " << stats.size() << " total, "
              << relevant_rgs.size() << " after pruning on l_shipdate\n";

    // Step 2: Load data (column projection + row group pruning)
    std::vector<std::string> columns = {
        "l_returnflag", "l_linestatus", "l_quantity", "l_extendedprice",
        "l_discount", "l_tax", "l_shipdate"
    };

    ParquetTable table;
    if (relevant_rgs.size() == stats.size()) {
        // All row groups needed, read full table
        table = read_parquet(lineitem_path, columns);
    } else {
        // Read only relevant row groups
        table = read_parquet_row_groups(lineitem_path, columns, relevant_rgs);
    }

    int64_t N = table.num_rows;
    std::cout << "Loaded " << N << " rows from lineitem\n";

    // Extract columns
    const auto& returnflag = table.string_column("l_returnflag");
    const auto& linestatus = table.string_column("l_linestatus");
    const double* quantity = table.column<double>("l_quantity");
    const double* extendedprice = table.column<double>("l_extendedprice");
    const double* discount = table.column<double>("l_discount");
    const double* tax = table.column<double>("l_tax");
    const int32_t* shipdate = table.column<int32_t>("l_shipdate");

    // Step 3: Thread-parallel processing
    unsigned int num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0) num_threads = 4;

    std::vector<std::thread> threads;
    std::vector<AggMap> thread_maps(num_threads);

    int64_t chunk_size = (N + num_threads - 1) / num_threads;

    for (unsigned int t = 0; t < num_threads; t++) {
        int64_t start_row = t * chunk_size;
        int64_t end_row = std::min(start_row + chunk_size, N);

        if (start_row >= N) break;

        threads.emplace_back(process_chunk,
            start_row, end_row,
            std::ref(returnflag), std::ref(linestatus),
            quantity, extendedprice, discount, tax, shipdate,
            date_threshold,
            std::ref(thread_maps[t]));
    }

    for (auto& th : threads) {
        th.join();
    }

    // Step 4: Merge thread-local results
    AggMap final_map;
    for (auto& tmap : thread_maps) {
        for (auto [key, values] : tmap) {
            final_map[key].merge(values);
        }
    }

    // Step 5: Compute averages and prepare output
    struct OutputRow {
        char returnflag;
        char linestatus;
        double sum_qty;
        double sum_base_price;
        double sum_disc_price;
        double sum_charge;
        double avg_qty;
        double avg_price;
        double avg_disc;
        int64_t count_order;
    };

    std::vector<OutputRow> results;
    for (const auto& [key, agg] : final_map) {
        OutputRow row;
        unpack_group_key(key, row.returnflag, row.linestatus);
        row.sum_qty = agg.sum_qty;
        row.sum_base_price = agg.sum_base_price;
        row.sum_disc_price = agg.sum_disc_price;
        row.sum_charge = agg.sum_charge;
        row.avg_qty = agg.count > 0 ? agg.sum_qty / agg.count : 0.0;
        row.avg_price = agg.count > 0 ? agg.sum_base_price / agg.count : 0.0;
        row.avg_disc = agg.count > 0 ? agg.sum_discount / agg.count : 0.0;
        row.count_order = agg.count;
        results.push_back(row);
    }

    // Step 6: Sort by returnflag, linestatus
    std::sort(results.begin(), results.end(), [](const OutputRow& a, const OutputRow& b) {
        if (a.returnflag != b.returnflag) return a.returnflag < b.returnflag;
        return a.linestatus < b.linestatus;
    });

    // Step 7: Write CSV output
    std::string output_path = results_dir + "/Q1.csv";
    std::ofstream out(output_path);
    if (!out.is_open()) {
        throw std::runtime_error("Failed to open output file: " + output_path);
    }

    out << std::fixed << std::setprecision(2);

    // Header
    out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,"
        << "sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

    // Data rows
    for (const auto& row : results) {
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

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Q1 completed: " << results.size() << " groups, "
              << duration.count() << " ms\n";
}
