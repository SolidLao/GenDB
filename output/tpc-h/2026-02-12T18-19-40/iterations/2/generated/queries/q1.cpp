#include "../storage/storage.h"
#include "../operators/scan.h"
#include "../operators/hash_agg.h"
#include "../utils/date_utils.h"
#include <iostream>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include <unordered_map>
#include <chrono>
#include <sys/mman.h>

// Q1: Lineitem aggregation grouped by returnflag and linestatus
// WHERE l_shipdate <= '1998-09-02' (1998-12-01 - 90 days)
// GROUP BY l_returnflag, l_linestatus

struct Q1AggState {
    int64_t sum_qty = 0;
    int64_t sum_base_price = 0;
    int64_t sum_disc_price = 0;
    int64_t sum_charge = 0;
    int64_t sum_qty_for_avg = 0;
    int64_t sum_price_for_avg = 0;
    int64_t sum_disc_for_avg = 0;
    int64_t count = 0;

    void update(int64_t qty, int64_t price, int64_t disc, int64_t tax) {
        sum_qty += qty;
        sum_base_price += price;

        // CORRECTNESS FIX: Preserve precision - don't divide during aggregation
        // disc_price = price * (1 - discount) = price * (100 - discount) / 100
        // Keep intermediate precision: store price * (100 - discount), divide by 100 at output
        int64_t disc_price_scaled = price * (100 - disc);
        sum_disc_price += disc_price_scaled;

        // charge = disc_price * (1 + tax) = disc_price_scaled * (100 + tax) / 100
        // Keep full precision: disc_price_scaled * (100 + tax), divide by 10000 at output
        int64_t charge_scaled = disc_price_scaled * (100 + tax);
        sum_charge += charge_scaled;

        sum_qty_for_avg += qty;
        sum_price_for_avg += price;
        sum_disc_for_avg += disc;
        count++;
    }

    double avg_qty() const {
        return count > 0 ? static_cast<double>(sum_qty_for_avg) / (count * 100.0) : 0.0;
    }

    double avg_price() const {
        return count > 0 ? static_cast<double>(sum_price_for_avg) / (count * 100.0) : 0.0;
    }

    double avg_disc() const {
        return count > 0 ? static_cast<double>(sum_disc_for_avg) / (count * 100.0) : 0.0;
    }
};

// Composite key: returnflag (1 byte) + linestatus (1 byte)
struct CompositeKey {
    uint8_t returnflag;
    uint8_t linestatus;

    bool operator==(const CompositeKey& other) const {
        return returnflag == other.returnflag && linestatus == other.linestatus;
    }
};

struct CompositeKeyHash {
    size_t operator()(const CompositeKey& k) const {
        return (static_cast<size_t>(k.returnflag) << 8) | k.linestatus;
    }
};

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start_time = std::chrono::high_resolution_clock::now();

    // Calculate the filter date: '1998-12-01' - 90 days = '1998-09-02'
    int32_t filter_date = date_utils::date_to_days("1998-09-02");

    // Memory-map the 7 required columns
    storage::MappedColumn col_shipdate, col_returnflag, col_linestatus;
    storage::MappedColumn col_quantity, col_extendedprice, col_discount, col_tax;

    col_shipdate.open(gendb_dir + "/lineitem.l_shipdate");
    col_returnflag.open(gendb_dir + "/lineitem.l_returnflag");
    col_linestatus.open(gendb_dir + "/lineitem.l_linestatus");
    col_quantity.open(gendb_dir + "/lineitem.l_quantity");
    col_extendedprice.open(gendb_dir + "/lineitem.l_extendedprice");
    col_discount.open(gendb_dir + "/lineitem.l_discount");
    col_tax.open(gendb_dir + "/lineitem.l_tax");

    // Load dictionaries for returnflag and linestatus
    storage::Dictionary dict_returnflag = storage::read_dictionary(gendb_dir + "/lineitem.l_returnflag.dict");
    storage::Dictionary dict_linestatus = storage::read_dictionary(gendb_dir + "/lineitem.l_linestatus.dict");

    // Get row count from first column (stored at beginning of file)
    size_t row_count = *col_shipdate.as<size_t>();

    // Get pointers to the actual data (skip size_t count header)
    const int32_t* shipdate = reinterpret_cast<const int32_t*>(
        reinterpret_cast<const char*>(col_shipdate.data) + sizeof(size_t));
    const uint8_t* returnflag = reinterpret_cast<const uint8_t*>(
        reinterpret_cast<const char*>(col_returnflag.data) + sizeof(size_t));
    const uint8_t* linestatus = reinterpret_cast<const uint8_t*>(
        reinterpret_cast<const char*>(col_linestatus.data) + sizeof(size_t));
    const int64_t* quantity = reinterpret_cast<const int64_t*>(
        reinterpret_cast<const char*>(col_quantity.data) + sizeof(size_t));
    const int64_t* extendedprice = reinterpret_cast<const int64_t*>(
        reinterpret_cast<const char*>(col_extendedprice.data) + sizeof(size_t));
    const int64_t* discount = reinterpret_cast<const int64_t*>(
        reinterpret_cast<const char*>(col_discount.data) + sizeof(size_t));
    const int64_t* tax = reinterpret_cast<const int64_t*>(
        reinterpret_cast<const char*>(col_tax.data) + sizeof(size_t));

    // Hash aggregation table
    std::unordered_map<CompositeKey, Q1AggState, CompositeKeyHash> agg_table;

    // Scan and aggregate
    // Note: Since we have 98% selectivity and shipdate is sorted, we could use scan_range
    // but for simplicity and clarity, we'll do a full scan with filter
    for (size_t i = 0; i < row_count; i++) {
        if (shipdate[i] <= filter_date) {
            CompositeKey key{returnflag[i], linestatus[i]};
            agg_table[key].update(quantity[i], extendedprice[i], discount[i], tax[i]);
        }
    }

    // Sort results by returnflag, linestatus
    std::vector<std::pair<CompositeKey, Q1AggState>> sorted_results(agg_table.begin(), agg_table.end());
    std::sort(sorted_results.begin(), sorted_results.end(),
        [&](const auto& a, const auto& b) {
            if (a.first.returnflag != b.first.returnflag) {
                return dict_returnflag.decode(a.first.returnflag) < dict_returnflag.decode(b.first.returnflag);
            }
            return dict_linestatus.decode(a.first.linestatus) < dict_linestatus.decode(b.first.linestatus);
        });

    // Write results if results_dir is provided
    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");

        // CORRECTNESS FIX: Add CSV header with comma delimiter
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        out << std::fixed << std::setprecision(2);

        // CORRECTNESS FIX: Use comma delimiter as per CSV standard
        // CORRECTNESS FIX: Account for precision scaling in disc_price and charge
        for (const auto& [key, agg] : sorted_results) {
            out << dict_returnflag.decode(key.returnflag) << ","
                << dict_linestatus.decode(key.linestatus) << ","
                << (agg.sum_qty / 100.0) << ","
                << (agg.sum_base_price / 100.0) << ",";

            // sum_disc_price is scaled by 100 (from price) * 100 (from discount calc) = 10000
            out << std::setprecision(4) << (agg.sum_disc_price / 10000.0) << ",";

            // sum_charge is scaled by 100 (price) * 100 (discount) * 100 (tax) = 1000000
            out << std::setprecision(6) << (agg.sum_charge / 1000000.0) << ",";

            out << std::setprecision(2)
                << agg.avg_qty() << ","
                << agg.avg_price() << ","
                << agg.avg_disc() << ","
                << agg.count << "\n";
        }
        out.close();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);

    std::cout << "Q1: " << sorted_results.size() << " rows, " << duration.count() << " ms" << std::endl;
}
