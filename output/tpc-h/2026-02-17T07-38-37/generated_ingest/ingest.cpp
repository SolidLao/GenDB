#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <filesystem>
#include <thread>
#include <mutex>
#include <cstring>
#include <charconv>
#include <cmath>
#include <algorithm>
#include <ctime>
#include <iomanip>

namespace fs = std::filesystem;
using namespace std;

// Constants
const int BLOCK_SIZE = 100000;
const int NUM_THREADS = 64;
const size_t WRITE_BUFFER_SIZE = 1024 * 1024; // 1MB write buffer

// Dictionary for low-cardinality string columns
struct Dictionary {
    unordered_map<string, int32_t> str_to_id;
    vector<string> id_to_str;

    int32_t encode(const string& s) {
        if (str_to_id.find(s) == str_to_id.end()) {
            int32_t id = id_to_str.size();
            str_to_id[s] = id;
            id_to_str.push_back(s);
        }
        return str_to_id[s];
    }

    void save(const string& dict_path) {
        ofstream f(dict_path, ios::binary);
        uint32_t count = id_to_str.size();
        f.write((char*)&count, sizeof(count));
        for (const auto& s : id_to_str) {
            uint32_t len = s.length();
            f.write((char*)&len, sizeof(len));
            f.write(s.c_str(), len);
        }
        f.close();
    }
};

// Parse DATE as days since epoch (1970-01-01)
int32_t parse_date(const string& s) {
    // Expected format: YYYY-MM-DD
    if (s.length() != 10) return 0;

    int year = stoi(s.substr(0, 4));
    int month = stoi(s.substr(5, 2));
    int day = stoi(s.substr(8, 2));

    // Days since epoch calculation
    int32_t days = 0;

    // Add days for complete years from 1970
    for (int y = 1970; y < year; y++) {
        if ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)) {
            days += 366;
        } else {
            days += 365;
        }
    }

    // Add days for complete months in current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) {
        days_in_month[1] = 29; // Leap year
    }

    for (int m = 1; m < month; m++) {
        days += days_in_month[m - 1];
    }

    // Add days in current month (day is 1-indexed, so subtract 1)
    days += (day - 1);

    return days;
}

// Parse DECIMAL(15,2) as int64_t with scale factor 100
int64_t parse_decimal(const string& s) {
    double val = stod(s);
    return (int64_t)round(val * 100.0);
}

// Split line by delimiter
vector<string> split_line(const string& line, char delimiter) {
    vector<string> fields;
    string field;
    for (char c : line) {
        if (c == delimiter) {
            fields.push_back(field);
            field.clear();
        } else {
            field += c;
        }
    }
    fields.push_back(field);
    return fields;
}

// Column writer (thread-safe buffered output)
class ColumnWriter {
public:
    ColumnWriter(const string& path) : path(path), buffer_pos(0) {
        buffer.resize(WRITE_BUFFER_SIZE);
    }

    void write_int32(int32_t val) {
        if (buffer_pos + sizeof(int32_t) > WRITE_BUFFER_SIZE) flush();
        memcpy(buffer.data() + buffer_pos, &val, sizeof(int32_t));
        buffer_pos += sizeof(int32_t);
    }

    void write_int64(int64_t val) {
        if (buffer_pos + sizeof(int64_t) > WRITE_BUFFER_SIZE) flush();
        memcpy(buffer.data() + buffer_pos, &val, sizeof(int64_t));
        buffer_pos += sizeof(int64_t);
    }

    void flush() {
        if (buffer_pos > 0) {
            if (!file.is_open()) {
                file.open(path, ios::binary | ios::app);
            }
            file.write(buffer.data(), buffer_pos);
            buffer_pos = 0;
        }
    }

    ~ColumnWriter() {
        flush();
        if (file.is_open()) file.close();
    }

private:
    string path;
    ofstream file;
    vector<char> buffer;
    size_t buffer_pos;
};

// Ingest a single table
void ingest_table(const string& table_name, const string& input_dir,
                  const string& output_dir, const vector<pair<string, string>>& columns) {
    string input_file = input_dir + "/" + table_name + ".tbl";

    // Verify input exists
    if (!fs::exists(input_file)) {
        cerr << "Warning: " << input_file << " not found, skipping" << endl;
        return;
    }

    cout << "Ingesting " << table_name << " from " << input_file << endl;

    // Create table directory
    string table_dir = output_dir + "/" + table_name;
    fs::create_directories(table_dir);

    // Initialize column writers and dictionaries
    vector<unique_ptr<ColumnWriter>> writers;
    vector<unique_ptr<Dictionary>> dicts;
    unordered_map<string, int> col_to_idx;

    for (size_t i = 0; i < columns.size(); i++) {
        const auto& [col_name, col_type] = columns[i];
        col_to_idx[col_name] = i;

        string col_path = table_dir + "/" + col_name + ".bin";
        writers.push_back(make_unique<ColumnWriter>(col_path));

        if (col_type == "STRING") {
            dicts.push_back(make_unique<Dictionary>());
        } else {
            dicts.push_back(nullptr);
        }
    }

    // Read input file and parse
    ifstream infile(input_file);
    string line;
    uint64_t row_count = 0;

    while (getline(infile, line)) {
        if (line.empty()) continue;

        vector<string> fields = split_line(line, '|');
        if (fields.size() != columns.size()) {
            cerr << "Warning: row " << row_count << " has " << fields.size()
                 << " fields, expected " << columns.size() << endl;
            continue;
        }

        for (size_t i = 0; i < columns.size(); i++) {
            const auto& [col_name, col_type] = columns[i];
            const string& field = fields[i];

            if (col_type == "INTEGER") {
                int32_t val = stoi(field);
                writers[i]->write_int32(val);
            } else if (col_type == "DECIMAL") {
                int64_t val = parse_decimal(field);
                writers[i]->write_int64(val);
            } else if (col_type == "DATE") {
                int32_t val = parse_date(field);
                writers[i]->write_int32(val);
            } else if (col_type == "STRING") {
                int32_t dict_id = dicts[i]->encode(field);
                writers[i]->write_int32(dict_id);
            }
        }

        row_count++;
        if (row_count % 1000000 == 0) {
            cout << "  " << table_name << ": " << row_count << " rows" << endl;
        }
    }

    infile.close();

    // Flush all writers and save dictionaries
    for (auto& w : writers) w->flush();

    for (size_t i = 0; i < columns.size(); i++) {
        if (dicts[i]) {
            string dict_path = table_dir + "/" + columns[i].first + "_dict.txt";
            dicts[i]->save(dict_path);
        }
    }

    cout << "  " << table_name << " complete: " << row_count << " rows" << endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>" << endl;
        return 1;
    }

    string input_dir = argv[1];
    string output_dir = argv[2];

    // Create output directory
    fs::create_directories(output_dir);

    // Test date parsing
    cout << "Testing date parsing:" << endl;
    int32_t epoch_test = parse_date("1970-01-01");
    cout << "  parse_date(\"1970-01-01\") = " << epoch_test << " (expected 0)" << endl;
    if (epoch_test != 0) {
        cerr << "ERROR: Date parsing failed!" << endl;
        return 1;
    }

    // Define table schemas
    vector<pair<string, vector<pair<string, string>>>> tables = {
        {"lineitem", {
            {"l_orderkey", "INTEGER"},
            {"l_partkey", "INTEGER"},
            {"l_suppkey", "INTEGER"},
            {"l_linenumber", "INTEGER"},
            {"l_quantity", "DECIMAL"},
            {"l_extendedprice", "DECIMAL"},
            {"l_discount", "DECIMAL"},
            {"l_tax", "DECIMAL"},
            {"l_returnflag", "STRING"},
            {"l_linestatus", "STRING"},
            {"l_shipdate", "DATE"},
            {"l_commitdate", "DATE"},
            {"l_receiptdate", "DATE"},
            {"l_shipinstruct", "STRING"},
            {"l_shipmode", "STRING"},
            {"l_comment", "STRING"}
        }},
        {"orders", {
            {"o_orderkey", "INTEGER"},
            {"o_custkey", "INTEGER"},
            {"o_orderstatus", "STRING"},
            {"o_totalprice", "DECIMAL"},
            {"o_orderdate", "DATE"},
            {"o_orderpriority", "STRING"},
            {"o_clerk", "STRING"},
            {"o_shippriority", "INTEGER"},
            {"o_comment", "STRING"}
        }},
        {"customer", {
            {"c_custkey", "INTEGER"},
            {"c_name", "STRING"},
            {"c_address", "STRING"},
            {"c_nationkey", "INTEGER"},
            {"c_phone", "STRING"},
            {"c_acctbal", "DECIMAL"},
            {"c_mktsegment", "STRING"},
            {"c_comment", "STRING"}
        }},
        {"supplier", {
            {"s_suppkey", "INTEGER"},
            {"s_name", "STRING"},
            {"s_address", "STRING"},
            {"s_nationkey", "INTEGER"},
            {"s_phone", "STRING"},
            {"s_acctbal", "DECIMAL"},
            {"s_comment", "STRING"}
        }},
        {"part", {
            {"p_partkey", "INTEGER"},
            {"p_name", "STRING"},
            {"p_mfgr", "STRING"},
            {"p_brand", "STRING"},
            {"p_type", "STRING"},
            {"p_size", "INTEGER"},
            {"p_container", "STRING"},
            {"p_retailprice", "DECIMAL"},
            {"p_comment", "STRING"}
        }},
        {"partsupp", {
            {"ps_partkey", "INTEGER"},
            {"ps_suppkey", "INTEGER"},
            {"ps_availqty", "INTEGER"},
            {"ps_supplycost", "DECIMAL"},
            {"ps_comment", "STRING"}
        }},
        {"nation", {
            {"n_nationkey", "INTEGER"},
            {"n_name", "STRING"},
            {"n_regionkey", "INTEGER"},
            {"n_comment", "STRING"}
        }},
        {"region", {
            {"r_regionkey", "INTEGER"},
            {"r_name", "STRING"},
            {"r_comment", "STRING"}
        }}
    };

    // Ingest tables sequentially
    for (const auto& [table_name, columns] : tables) {
        ingest_table(table_name, input_dir, output_dir, columns);
    }

    cout << "Ingestion complete!" << endl;
    return 0;
}
