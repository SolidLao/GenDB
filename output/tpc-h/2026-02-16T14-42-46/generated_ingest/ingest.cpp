#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <cstring>
#include <filesystem>
#include <charconv>
#include <cmath>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <algorithm>
#include <sys/stat.h>

namespace fs = std::filesystem;
using namespace std;

// Self-test: verify parse_date formula is correct
int32_t parse_date(const string& date_str) {
    // date_str format: YYYY-MM-DD
    if (date_str.size() != 10) return -1;

    int year = stoi(date_str.substr(0, 4));
    int month = stoi(date_str.substr(5, 2));
    int day = stoi(date_str.substr(8, 2));

    // Days in each month (non-leap year)
    static const int days_in_month[] = {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

    // Epoch = 1970-01-01
    int32_t days = 0;

    // Add days for complete years from 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        bool is_leap = (y % 400 == 0) || (y % 4 == 0 && y % 100 != 0);
        days += is_leap ? 366 : 365;
    }

    // Add days for complete months (1-indexed)
    days += days_in_month[month - 1];

    // Add leap day if needed
    if (month > 2) {
        bool is_leap = (year % 400 == 0) || (year % 4 == 0 && year % 100 != 0);
        if (is_leap) days += 1;
    }

    // Add remaining days (1-indexed, so subtract 1)
    days += (day - 1);

    return days;
}

// Self-test: verify epoch formula
void verify_epoch_formula() {
    int32_t epoch_zero = parse_date("1970-01-01");
    if (epoch_zero != 0) {
        cerr << "ERROR: parse_date('1970-01-01') returned " << epoch_zero << " (expected 0)" << endl;
        exit(1);
    }
    cout << "✓ Epoch formula verified: parse_date('1970-01-01') = 0" << endl;
}

struct DictionaryEncoder {
    unordered_map<string, int32_t> dict;
    vector<string> values;
    int32_t next_id = 0;

    int32_t encode(const string& s) {
        auto it = dict.find(s);
        if (it != dict.end()) return it->second;

        int32_t id = next_id++;
        dict[s] = id;
        values.push_back(s);
        return id;
    }

    void write_dict(const string& filepath) {
        ofstream file(filepath, ios::binary);
        for (const auto& val : values) {
            file << val << "\n";
        }
        file.close();
    }
};

struct Table {
    string name;
    vector<string> columns;
    vector<string> semantic_types;  // INTEGER, DECIMAL, DATE, STRING
    vector<int64_t> scale_factors;  // For DECIMAL columns
    vector<vector<int32_t>> int32_data;
    vector<vector<int64_t>> int64_data;
    vector<DictionaryEncoder> dict_encoders;
    size_t row_count = 0;
};

void parse_csv_line(const string& line, const string& delim, vector<string>& fields, int expected_fields) {
    fields.clear();
    size_t start = 0, end = 0;
    int field_count = 0;

    // For most fields, split on first occurrence
    while (field_count < expected_fields - 1) {
        end = line.find(delim, start);
        if (end == string::npos) {
            // Not enough fields
            break;
        }
        fields.push_back(line.substr(start, end - start));
        start = end + delim.length();
        field_count++;
    }

    // Last field: everything remaining
    fields.push_back(line.substr(start));
}

void ingest_table(const string& data_dir, Table& table, int thread_id, int num_threads) {
    string filepath = data_dir + "/" + table.name + ".tbl";

    // Read entire file and partition by thread
    ifstream file(filepath);
    vector<string> lines;
    string line;
    while (getline(file, line)) {
        lines.push_back(line);
    }
    file.close();

    size_t total_lines = lines.size();
    size_t lines_per_thread = (total_lines + num_threads - 1) / num_threads;
    size_t start_idx = thread_id * lines_per_thread;
    size_t end_idx = min((thread_id + 1) * lines_per_thread, total_lines);

    // Resize columns for this thread's partition
    for (size_t i = 0; i < table.int32_data.size(); ++i) {
        table.int32_data[i].resize(total_lines);
    }
    for (size_t i = 0; i < table.int64_data.size(); ++i) {
        table.int64_data[i].resize(total_lines);
    }

    vector<string> fields;
    for (size_t row_idx = start_idx; row_idx < end_idx; ++row_idx) {
        parse_csv_line(lines[row_idx], "|", fields, table.columns.size());

        if (fields.size() != table.columns.size()) {
            cerr << "ERROR: Expected " << table.columns.size() << " fields, got "
                 << fields.size() << " in row " << row_idx << endl;
            exit(1);
        }

        for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
            const string& field = fields[col_idx];
            const string& sem_type = table.semantic_types[col_idx];

            if (sem_type == "INTEGER") {
                int32_t val;
                auto [ptr, ec] = from_chars(field.c_str(), field.c_str() + field.length(), val);
                if (ec != errc()) {
                    cerr << "ERROR: Failed to parse INTEGER '" << field << "' at row " << row_idx << endl;
                    exit(1);
                }
                table.int32_data[col_idx][row_idx] = val;
            } else if (sem_type == "DECIMAL") {
                // Parse as double, multiply by scale_factor, round to int64
                double d = stod(field);
                int64_t scaled = (int64_t)round(d * table.scale_factors[col_idx]);
                table.int64_data[col_idx][row_idx] = scaled;
            } else if (sem_type == "DATE") {
                int32_t epoch_days = parse_date(field);
                table.int32_data[col_idx][row_idx] = epoch_days;
            } else if (sem_type == "STRING") {
                // Find which int32_data column corresponds to this string
                int32_t dict_id = table.dict_encoders[col_idx].encode(field);
                table.int32_data[col_idx][row_idx] = dict_id;
            }
        }
    }
}

void write_table_columns(const string& output_dir, Table& table) {
    string table_dir = output_dir + "/" + table.name;
    fs::create_directories(table_dir);

    // Write int32 columns
    for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
        if (table.semantic_types[col_idx] == "INTEGER" ||
            table.semantic_types[col_idx] == "DATE" ||
            table.semantic_types[col_idx] == "STRING") {

            string col_file = table_dir + "/" + table.columns[col_idx] + ".bin";
            ofstream file(col_file, ios::binary);
            for (const auto& val : table.int32_data[col_idx]) {
                file.write((char*)&val, sizeof(int32_t));
            }
            file.close();
            cout << "  ✓ " << table.name << "/" << table.columns[col_idx] << ".bin ("
                 << table.int32_data[col_idx].size() << " rows)" << endl;
        }
    }

    // Write int64 columns (DECIMAL)
    for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
        if (table.semantic_types[col_idx] == "DECIMAL") {
            string col_file = table_dir + "/" + table.columns[col_idx] + ".bin";
            ofstream file(col_file, ios::binary);
            for (const auto& val : table.int64_data[col_idx]) {
                file.write((char*)&val, sizeof(int64_t));
            }
            file.close();
            cout << "  ✓ " << table.name << "/" << table.columns[col_idx] << ".bin ("
                 << table.int64_data[col_idx].size() << " rows, DECIMAL)" << endl;
        }
    }

    // Write dictionary files
    for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
        if (table.semantic_types[col_idx] == "STRING") {
            string dict_file = table_dir + "/" + table.columns[col_idx] + "_dict.txt";
            table.dict_encoders[col_idx].write_dict(dict_file);
            cout << "  ✓ " << table.name << "/" << table.columns[col_idx] << "_dict.txt ("
                 << table.dict_encoders[col_idx].values.size() << " unique values)" << endl;
        }
    }
}

void verify_ingestion(const Table& table) {
    // Check date columns
    for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
        if (table.semantic_types[col_idx] == "DATE") {
            bool has_valid_dates = false;
            int32_t min_date = INT32_MAX, max_date = INT32_MIN;
            for (const auto& val : table.int32_data[col_idx]) {
                if (val >= 0) has_valid_dates = true;
                min_date = min(min_date, val);
                max_date = max(max_date, val);
            }
            // Range check: 1970-01-01 to 2100-01-01 is roughly 0 to 47450 days
            if (max_date > 60000) {
                cerr << "ERROR: Date value " << max_date << " seems invalid (>60000 days, beyond year 2100)" << endl;
                exit(1);
            }
            if (!has_valid_dates) {
                cerr << "ERROR: No valid dates found in " << table.name << "." << table.columns[col_idx] << endl;
                exit(1);
            }
            cout << "  ✓ Date range: " << min_date << " to " << max_date << " days" << endl;
        }
    }

    // Check decimal columns
    for (size_t col_idx = 0; col_idx < table.columns.size(); ++col_idx) {
        if (table.semantic_types[col_idx] == "DECIMAL") {
            bool has_nonzero = false;
            for (const auto& val : table.int64_data[col_idx]) {
                if (val != 0) has_nonzero = true;
            }
            if (!has_nonzero) {
                cerr << "WARNING: All zeros in " << table.name << "." << table.columns[col_idx] << endl;
            }
        }
    }
}

int main(int argc, char** argv) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <data_dir> <output_dir>" << endl;
        return 1;
    }

    string data_dir = argv[1];
    string output_dir = argv[2];

    cout << "=== TPC-H Ingestion ===" << endl;

    // Verify epoch formula first
    verify_epoch_formula();
    cout << endl;

    fs::create_directories(output_dir);

    // Define all tables
    vector<Table> tables = {
        {
            "lineitem",
            {"l_orderkey", "l_partkey", "l_suppkey", "l_linenumber", "l_quantity",
             "l_extendedprice", "l_discount", "l_tax", "l_returnflag", "l_linestatus",
             "l_shipdate", "l_commitdate", "l_receiptdate", "l_shipinstruct", "l_shipmode", "l_comment"},
            {"INTEGER", "INTEGER", "INTEGER", "INTEGER", "DECIMAL", "DECIMAL", "DECIMAL", "DECIMAL",
             "STRING", "STRING", "DATE", "DATE", "DATE", "STRING", "STRING", "STRING"},
            {0, 0, 0, 0, 100, 100, 100, 100, 0, 0, 0, 0, 0, 0, 0, 0}
        },
        {
            "orders",
            {"o_orderkey", "o_custkey", "o_orderstatus", "o_totalprice", "o_orderdate",
             "o_orderpriority", "o_clerk", "o_shippriority", "o_comment"},
            {"INTEGER", "INTEGER", "STRING", "DECIMAL", "DATE", "STRING", "STRING", "INTEGER", "STRING"},
            {0, 0, 0, 100, 0, 0, 0, 0, 0}
        },
        {
            "customer",
            {"c_custkey", "c_name", "c_address", "c_nationkey", "c_phone", "c_acctbal", "c_mktsegment", "c_comment"},
            {"INTEGER", "STRING", "STRING", "INTEGER", "STRING", "DECIMAL", "STRING", "STRING"},
            {0, 0, 0, 0, 0, 100, 0, 0}
        },
        {
            "part",
            {"p_partkey", "p_name", "p_mfgr", "p_brand", "p_type", "p_size", "p_container", "p_retailprice", "p_comment"},
            {"INTEGER", "STRING", "STRING", "STRING", "STRING", "INTEGER", "STRING", "DECIMAL", "STRING"},
            {0, 0, 0, 0, 0, 0, 0, 100, 0}
        },
        {
            "partsupp",
            {"ps_partkey", "ps_suppkey", "ps_availqty", "ps_supplycost", "ps_comment"},
            {"INTEGER", "INTEGER", "INTEGER", "DECIMAL", "STRING"},
            {0, 0, 0, 100, 0}
        },
        {
            "supplier",
            {"s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal", "s_comment"},
            {"INTEGER", "STRING", "STRING", "INTEGER", "STRING", "DECIMAL", "STRING"},
            {0, 0, 0, 0, 0, 100, 0}
        },
        {
            "nation",
            {"n_nationkey", "n_name", "n_regionkey", "n_comment"},
            {"INTEGER", "STRING", "INTEGER", "STRING"},
            {0, 0, 0, 0}
        },
        {
            "region",
            {"r_regionkey", "r_name", "r_comment"},
            {"INTEGER", "STRING", "STRING"},
            {0, 0, 0}
        }
    };

    // Initialize column vectors for each table
    for (auto& table : tables) {
        int num_string_cols = 0;
        for (const auto& sem_type : table.semantic_types) {
            if (sem_type == "STRING") num_string_cols++;
        }

        // Initialize int32_data with placeholder size
        table.int32_data.resize(table.columns.size());
        table.int64_data.resize(table.columns.size());
        table.dict_encoders.resize(table.columns.size());
    }

    // Ingest tables (single-threaded for correctness, can be parallelized per table)
    int num_threads = thread::hardware_concurrency();
    cout << "Ingesting " << tables.size() << " tables with " << num_threads << " threads..." << endl;

    for (auto& table : tables) {
        cout << "Table: " << table.name << endl;

        // For now, ingest single-threaded to ensure correct dictionary encoding
        ingest_table(data_dir, table, 0, 1);

        // Verify ingestion
        verify_ingestion(table);

        // Write columns to disk
        write_table_columns(output_dir, table);
    }

    cout << "\n=== Ingestion Complete ===" << endl;
    cout << "Data written to: " << output_dir << endl;

    return 0;
}
