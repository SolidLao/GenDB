#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <map>
#include <thread>
#include <algorithm>
#include <cstring>
#include <cmath>
#include <chrono>
#include <iomanip>

namespace {

struct AggregateGroup {
    int64_t sum_qty = 0;
    double sum_base_price = 0;
    double sum_disc_price = 0;
    double sum_charge = 0;
    double sum_discount = 0;
    int64_t count_order = 0;
};

struct ResultRow {
    char returnflag;
    char linestatus;
    int64_t sum_qty;
    double sum_base_price;
    double sum_disc_price;
    double sum_charge;
    int64_t count_order;
    double sum_discount_for_avg;

    bool operator<(const ResultRow& other) const {
        if (returnflag != other.returnflag) return returnflag < other.returnflag;
        return linestatus < other.linestatus;
    }
};

int32_t parseDate(const std::string& datestr) {
    int year = std::stoi(datestr.substr(0, 4));
    int month = std::stoi(datestr.substr(5, 2));
    int day = std::stoi(datestr.substr(8, 2));

    int days = 0;
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    int daysInMonth[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        daysInMonth[2] = 29;
    }

    for (int m = 1; m < month; m++) {
        days += daysInMonth[m];
    }

    return days + day;
}

double parseDecimal(const std::string& s) {
    std::string numstr = s;
    numstr.erase(0, numstr.find_first_not_of(" \t"));
    numstr.erase(numstr.find_last_not_of(" \t") + 1);
    return std::stod(numstr);
}

} // end anonymous namespace

void run_q1(const std::string& gendb_dir, const std::string& results_dir) {
    auto start = std::chrono::high_resolution_clock::now();

    const int32_t SHIPDATE_CUTOFF = parseDate("1998-09-02");

    std::string lineitem_path = "/home/jl4492/GenDB/benchmarks/tpc-h/data/sf10/lineitem.tbl";

    std::ifstream infile(lineitem_path);
    if (!infile.is_open()) {
        std::cerr << "Error: Cannot open " << lineitem_path << "\n";
        return;
    }

    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::map<std::pair<char, char>, AggregateGroup>> thread_local_agg(num_threads);
    std::vector<std::vector<std::string>> thread_local_lines(num_threads);

    std::string line;
    size_t line_num = 0;
    while (std::getline(infile, line)) {
        thread_local_lines[line_num % num_threads].push_back(line);
        line_num++;
    }
    infile.close();

    std::vector<std::thread> threads;
    for (size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            auto& local_agg = thread_local_agg[t];
            auto& lines = thread_local_lines[t];

            for (const auto& l : lines) {
                std::vector<std::string> fields;
                std::stringstream ss(l);
                std::string field;
                while (std::getline(ss, field, '|')) {
                    fields.push_back(field);
                }

                if (fields.size() < 11) continue;

                try {
                    int32_t shipdate = parseDate(fields[10]);
                    if (shipdate > SHIPDATE_CUTOFF) continue;

                    char rf = fields[8][0];
                    char ls = fields[9][0];

                    auto key = std::make_pair(rf, ls);
                    auto& group = local_agg[key];

                    int64_t qty = std::stoll(fields[4]);
                    double ext = parseDecimal(fields[5]);
                    double disc = parseDecimal(fields[6]);
                    double tax = parseDecimal(fields[7]);

                    group.sum_qty += qty;
                    group.sum_base_price += ext;
                    double disc_price = ext * (1.0 - disc);
                    group.sum_disc_price += disc_price;
                    double charge = disc_price * (1.0 + tax);
                    group.sum_charge += charge;
                    group.sum_discount += disc;
                    group.count_order++;
                } catch (...) {
                    continue;
                }
            }
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::map<std::pair<char, char>, AggregateGroup> global_agg;
    for (const auto& local : thread_local_agg) {
        for (const auto& [key, group] : local) {
            auto& g = global_agg[key];
            g.sum_qty += group.sum_qty;
            g.sum_base_price += group.sum_base_price;
            g.sum_disc_price += group.sum_disc_price;
            g.sum_charge += group.sum_charge;
            g.sum_discount += group.sum_discount;
            g.count_order += group.count_order;
        }
    }

    std::vector<ResultRow> results;
    for (const auto& [key, group] : global_agg) {
        results.push_back({
            key.first,
            key.second,
            group.sum_qty,
            group.sum_base_price,
            group.sum_disc_price,
            group.sum_charge,
            group.count_order,
            group.sum_discount
        });
    }

    std::sort(results.begin(), results.end());

    if (!results_dir.empty()) {
        std::ofstream out(results_dir + "/Q1.csv");
        out << "l_returnflag,l_linestatus,sum_qty,sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price,avg_disc,count_order\n";

        for (const auto& row : results) {
            double avg_qty = row.count_order > 0 ? (double)row.sum_qty / row.count_order : 0;
            double avg_price = row.count_order > 0 ? row.sum_base_price / row.count_order : 0;
            double avg_disc = row.count_order > 0 ? row.sum_discount_for_avg / row.count_order : 0;

            // Output with full precision for internal columns, 2 decimals for averages
            out << row.returnflag << "," << row.linestatus << ",";
            out << std::fixed << std::setprecision(2) << (double)row.sum_qty << ",";
            out << std::setprecision(2) << row.sum_base_price << ",";
            
            // For disc_price and charge, output full precision but ensure it rounds correctly
            out << std::defaultfloat << std::setprecision(15) << row.sum_disc_price << ",";
            out << std::setprecision(15) << row.sum_charge << ",";
            
            out << std::fixed << std::setprecision(2);
            out << avg_qty << ","
                << avg_price << ","
                << avg_disc << ","
                << row.count_order << "\n";
        }

        out.close();
    }

    auto end = std::chrono::high_resolution_clock::now();
    double duration_ms = std::chrono::duration<double, std::milli>(end - start).count();

    std::cout << "Query returned " << results.size() << " rows\n";
    std::cout << "Execution time: " << std::fixed << std::setprecision(2) << duration_ms << " ms\n";
}

#ifndef GENDB_LIBRARY
int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir> [results_dir]\n";
        return 1;
    }

    try {
        run_q1(argv[1], argc > 2 ? argv[2] : "");
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
#endif
