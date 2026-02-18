#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <cstring>
#include <filesystem>
#include <algorithm>
#include <ctime>
#include <cmath>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

namespace fs = std::filesystem;

// ============================================================================
// Date parsing: Convert YYYY-MM-DD to days since 1970-01-01
// ============================================================================
int32_t parse_date(const std::string& date_str) {
    if (date_str.length() != 10) {
        std::cerr << "Invalid date format: " << date_str << std::endl;
        return 0;
    }
    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days since epoch (1970-01-01)
    int days = 0;
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[1] = 29;
    }

    for (int m = 1; m < month; ++m) {
        days += days_in_month[m - 1];
    }

    days += (day - 1);
    return days;
}

// ============================================================================
// Decimal parsing: "123.45" → int64_t (12345 with scale_factor=2)
// ============================================================================
int64_t parse_decimal(const std::string& value, int scale_factor) {
    // Precompute powers of 10
    static const int64_t powers[] = {1, 10, 100, 1000, 10000, 100000, 1000000};
    int64_t scale = powers[scale_factor];

    size_t dot_pos = value.find('.');
    if (dot_pos == std::string::npos) {
        return std::stoll(value) * scale;
    }

    std::string integer_part = value.substr(0, dot_pos);
    std::string decimal_part = value.substr(dot_pos + 1);

    while ((int)decimal_part.length() < scale_factor) {
        decimal_part += '0';
    }
    decimal_part = decimal_part.substr(0, scale_factor);

    int64_t result = std::stoll(integer_part) * scale;
    result += std::stoll(decimal_part);
    return result;
}

// ============================================================================
// Dictionary management for string columns
// ============================================================================
class StringDictionary {
public:
    int32_t encode(const std::string& value) {
        auto it = string_to_id.find(value);
        if (it != string_to_id.end()) {
            return it->second;
        }
        int32_t id = next_id++;
        string_to_id[value] = id;
        id_to_string[id] = value;
        return id;
    }

    void save(const std::string& filename) {
        std::ofstream file(filename);
        for (const auto& [id, str] : id_to_string) {
            file << id << "|" << str << "\n";
        }
    }

private:
    std::unordered_map<std::string, int32_t> string_to_id;
    std::unordered_map<int32_t, std::string> id_to_string;
    int32_t next_id = 0;
};

// ============================================================================
// Column writer: buffered output for each column
// ============================================================================
template <typename T>
class ColumnWriter {
public:
    explicit ColumnWriter(const std::string& filename, size_t buffer_size = 1024 * 1024)
        : filename(filename), pos(0), buffer_size(buffer_size), buffer(buffer_size) {
        file.open(filename, std::ios::binary);
        if (!file) {
            throw std::runtime_error("Failed to open " + filename);
        }
    }

    void write(T value) {
        if (pos + sizeof(T) > buffer_size) {
            flush();
        }
        std::memcpy(buffer.data() + pos, &value, sizeof(T));
        pos += sizeof(T);
    }

    void flush() {
        if (pos > 0) {
            file.write(buffer.data(), pos);
            pos = 0;
        }
    }

    ~ColumnWriter() {
        flush();
        file.close();
    }

private:
    std::string filename;
    std::ofstream file;
    size_t pos;
    size_t buffer_size;
    std::vector<char> buffer;
};

// ============================================================================
// Table-specific ingestion: lineitem
// ============================================================================
void ingest_lineitem(const std::string& src_file, const std::string& output_dir) {
    std::ifstream in(src_file);
    if (!in) throw std::runtime_error("Cannot open " + src_file);

    StringDictionary dict_returnflag, dict_linestatus, dict_shipinstruct, dict_shipmode, dict_comment;

    ColumnWriter<int32_t> w_orderkey(output_dir + "/lineitem/l_orderkey.bin");
    ColumnWriter<int32_t> w_partkey(output_dir + "/lineitem/l_partkey.bin");
    ColumnWriter<int32_t> w_suppkey(output_dir + "/lineitem/l_suppkey.bin");
    ColumnWriter<int32_t> w_linenumber(output_dir + "/lineitem/l_linenumber.bin");
    ColumnWriter<int64_t> w_quantity(output_dir + "/lineitem/l_quantity.bin");
    ColumnWriter<int64_t> w_extendedprice(output_dir + "/lineitem/l_extendedprice.bin");
    ColumnWriter<int64_t> w_discount(output_dir + "/lineitem/l_discount.bin");
    ColumnWriter<int64_t> w_tax(output_dir + "/lineitem/l_tax.bin");
    ColumnWriter<int32_t> w_returnflag(output_dir + "/lineitem/l_returnflag.bin");
    ColumnWriter<int32_t> w_linestatus(output_dir + "/lineitem/l_linestatus.bin");
    ColumnWriter<int32_t> w_shipdate(output_dir + "/lineitem/l_shipdate.bin");
    ColumnWriter<int32_t> w_commitdate(output_dir + "/lineitem/l_commitdate.bin");
    ColumnWriter<int32_t> w_receiptdate(output_dir + "/lineitem/l_receiptdate.bin");
    ColumnWriter<int32_t> w_shipinstruct(output_dir + "/lineitem/l_shipinstruct.bin");
    ColumnWriter<int32_t> w_shipmode(output_dir + "/lineitem/l_shipmode.bin");
    ColumnWriter<int32_t> w_comment(output_dir + "/lineitem/l_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string token;
        std::vector<std::string> fields;

        while (std::getline(iss, token, '|')) {
            fields.push_back(token);
        }

        if (fields.size() < 16) continue;

        w_orderkey.write(std::stoi(fields[0]));
        w_partkey.write(std::stoi(fields[1]));
        w_suppkey.write(std::stoi(fields[2]));
        w_linenumber.write(std::stoi(fields[3]));
        w_quantity.write(parse_decimal(fields[4], 2));
        w_extendedprice.write(parse_decimal(fields[5], 2));
        w_discount.write(parse_decimal(fields[6], 2));
        w_tax.write(parse_decimal(fields[7], 2));
        w_returnflag.write(dict_returnflag.encode(fields[8]));
        w_linestatus.write(dict_linestatus.encode(fields[9]));
        w_shipdate.write(parse_date(fields[10]));
        w_commitdate.write(parse_date(fields[11]));
        w_receiptdate.write(parse_date(fields[12]));
        w_shipinstruct.write(dict_shipinstruct.encode(fields[13]));
        w_shipmode.write(dict_shipmode.encode(fields[14]));
        w_comment.write(dict_comment.encode(fields[15]));

        row_count++;
    }

    dict_returnflag.save(output_dir + "/lineitem/l_returnflag_dict.txt");
    dict_linestatus.save(output_dir + "/lineitem/l_linestatus_dict.txt");
    dict_shipinstruct.save(output_dir + "/lineitem/l_shipinstruct_dict.txt");
    dict_shipmode.save(output_dir + "/lineitem/l_shipmode_dict.txt");
    dict_comment.save(output_dir + "/lineitem/l_comment_dict.txt");

    std::cout << "lineitem: " << row_count << " rows ingested\n";
}

// ============================================================================
// Table-specific ingestion: orders
// ============================================================================
void ingest_orders(const std::string& src_file, const std::string& output_dir) {
    std::ifstream in(src_file);
    if (!in) throw std::runtime_error("Cannot open " + src_file);

    StringDictionary dict_status, dict_priority, dict_clerk, dict_comment;

    ColumnWriter<int32_t> w_orderkey(output_dir + "/orders/o_orderkey.bin");
    ColumnWriter<int32_t> w_custkey(output_dir + "/orders/o_custkey.bin");
    ColumnWriter<int32_t> w_status(output_dir + "/orders/o_orderstatus.bin");
    ColumnWriter<int64_t> w_totalprice(output_dir + "/orders/o_totalprice.bin");
    ColumnWriter<int32_t> w_orderdate(output_dir + "/orders/o_orderdate.bin");
    ColumnWriter<int32_t> w_priority(output_dir + "/orders/o_orderpriority.bin");
    ColumnWriter<int32_t> w_clerk(output_dir + "/orders/o_clerk.bin");
    ColumnWriter<int32_t> w_shippriority(output_dir + "/orders/o_shippriority.bin");
    ColumnWriter<int32_t> w_comment(output_dir + "/orders/o_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string token;
        std::vector<std::string> fields;

        while (std::getline(iss, token, '|')) {
            fields.push_back(token);
        }

        if (fields.size() < 9) continue;

        w_orderkey.write(std::stoi(fields[0]));
        w_custkey.write(std::stoi(fields[1]));
        w_status.write(dict_status.encode(fields[2]));
        w_totalprice.write(parse_decimal(fields[3], 2));
        w_orderdate.write(parse_date(fields[4]));
        w_priority.write(dict_priority.encode(fields[5]));
        w_clerk.write(dict_clerk.encode(fields[6]));
        w_shippriority.write(std::stoi(fields[7]));
        w_comment.write(dict_comment.encode(fields[8]));

        row_count++;
    }

    dict_status.save(output_dir + "/orders/o_orderstatus_dict.txt");
    dict_priority.save(output_dir + "/orders/o_orderpriority_dict.txt");
    dict_clerk.save(output_dir + "/orders/o_clerk_dict.txt");
    dict_comment.save(output_dir + "/orders/o_comment_dict.txt");

    std::cout << "orders: " << row_count << " rows ingested\n";
}

// ============================================================================
// Table-specific ingestion: customer
// ============================================================================
void ingest_customer(const std::string& src_file, const std::string& output_dir) {
    std::ifstream in(src_file);
    if (!in) throw std::runtime_error("Cannot open " + src_file);

    StringDictionary dict_name, dict_address, dict_phone, dict_segment, dict_comment;

    ColumnWriter<int32_t> w_custkey(output_dir + "/customer/c_custkey.bin");
    ColumnWriter<int32_t> w_name(output_dir + "/customer/c_name.bin");
    ColumnWriter<int32_t> w_address(output_dir + "/customer/c_address.bin");
    ColumnWriter<int32_t> w_nationkey(output_dir + "/customer/c_nationkey.bin");
    ColumnWriter<int32_t> w_phone(output_dir + "/customer/c_phone.bin");
    ColumnWriter<int64_t> w_acctbal(output_dir + "/customer/c_acctbal.bin");
    ColumnWriter<int32_t> w_segment(output_dir + "/customer/c_mktsegment.bin");
    ColumnWriter<int32_t> w_comment(output_dir + "/customer/c_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string token;
        std::vector<std::string> fields;

        while (std::getline(iss, token, '|')) {
            fields.push_back(token);
        }

        if (fields.size() < 8) continue;

        w_custkey.write(std::stoi(fields[0]));
        w_name.write(dict_name.encode(fields[1]));
        w_address.write(dict_address.encode(fields[2]));
        w_nationkey.write(std::stoi(fields[3]));
        w_phone.write(dict_phone.encode(fields[4]));
        w_acctbal.write(parse_decimal(fields[5], 2));
        w_segment.write(dict_segment.encode(fields[6]));
        w_comment.write(dict_comment.encode(fields[7]));

        row_count++;
    }

    dict_name.save(output_dir + "/customer/c_name_dict.txt");
    dict_address.save(output_dir + "/customer/c_address_dict.txt");
    dict_phone.save(output_dir + "/customer/c_phone_dict.txt");
    dict_segment.save(output_dir + "/customer/c_mktsegment_dict.txt");
    dict_comment.save(output_dir + "/customer/c_comment_dict.txt");

    std::cout << "customer: " << row_count << " rows ingested\n";
}

// ============================================================================
// Table-specific ingestion: partsupp
// ============================================================================
void ingest_partsupp(const std::string& src_file, const std::string& output_dir) {
    std::ifstream in(src_file);
    if (!in) throw std::runtime_error("Cannot open " + src_file);

    StringDictionary dict_comment;

    ColumnWriter<int32_t> w_partkey(output_dir + "/partsupp/ps_partkey.bin");
    ColumnWriter<int32_t> w_suppkey(output_dir + "/partsupp/ps_suppkey.bin");
    ColumnWriter<int32_t> w_availqty(output_dir + "/partsupp/ps_availqty.bin");
    ColumnWriter<int64_t> w_supplycost(output_dir + "/partsupp/ps_supplycost.bin");
    ColumnWriter<int32_t> w_comment(output_dir + "/partsupp/ps_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string token;
        std::vector<std::string> fields;

        while (std::getline(iss, token, '|')) {
            fields.push_back(token);
        }

        if (fields.size() < 5) continue;

        w_partkey.write(std::stoi(fields[0]));
        w_suppkey.write(std::stoi(fields[1]));
        w_availqty.write(std::stoi(fields[2]));
        w_supplycost.write(parse_decimal(fields[3], 2));
        w_comment.write(dict_comment.encode(fields[4]));

        row_count++;
    }

    dict_comment.save(output_dir + "/partsupp/ps_comment_dict.txt");

    std::cout << "partsupp: " << row_count << " rows ingested\n";
}

// ============================================================================
// Table-specific ingestion: part
// ============================================================================
void ingest_part(const std::string& src_file, const std::string& output_dir) {
    std::ifstream in(src_file);
    if (!in) throw std::runtime_error("Cannot open " + src_file);

    StringDictionary dict_name, dict_mfgr, dict_brand, dict_type, dict_container, dict_comment;

    ColumnWriter<int32_t> w_partkey(output_dir + "/part/p_partkey.bin");
    ColumnWriter<int32_t> w_name(output_dir + "/part/p_name.bin");
    ColumnWriter<int32_t> w_mfgr(output_dir + "/part/p_mfgr.bin");
    ColumnWriter<int32_t> w_brand(output_dir + "/part/p_brand.bin");
    ColumnWriter<int32_t> w_type(output_dir + "/part/p_type.bin");
    ColumnWriter<int32_t> w_size(output_dir + "/part/p_size.bin");
    ColumnWriter<int32_t> w_container(output_dir + "/part/p_container.bin");
    ColumnWriter<int64_t> w_retailprice(output_dir + "/part/p_retailprice.bin");
    ColumnWriter<int32_t> w_comment(output_dir + "/part/p_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string token;
        std::vector<std::string> fields;

        while (std::getline(iss, token, '|')) {
            fields.push_back(token);
        }

        if (fields.size() < 9) continue;

        w_partkey.write(std::stoi(fields[0]));
        w_name.write(dict_name.encode(fields[1]));
        w_mfgr.write(dict_mfgr.encode(fields[2]));
        w_brand.write(dict_brand.encode(fields[3]));
        w_type.write(dict_type.encode(fields[4]));
        w_size.write(std::stoi(fields[5]));
        w_container.write(dict_container.encode(fields[6]));
        w_retailprice.write(parse_decimal(fields[7], 2));
        w_comment.write(dict_comment.encode(fields[8]));

        row_count++;
    }

    dict_name.save(output_dir + "/part/p_name_dict.txt");
    dict_mfgr.save(output_dir + "/part/p_mfgr_dict.txt");
    dict_brand.save(output_dir + "/part/p_brand_dict.txt");
    dict_type.save(output_dir + "/part/p_type_dict.txt");
    dict_container.save(output_dir + "/part/p_container_dict.txt");
    dict_comment.save(output_dir + "/part/p_comment_dict.txt");

    std::cout << "part: " << row_count << " rows ingested\n";
}

// ============================================================================
// Table-specific ingestion: supplier
// ============================================================================
void ingest_supplier(const std::string& src_file, const std::string& output_dir) {
    std::ifstream in(src_file);
    if (!in) throw std::runtime_error("Cannot open " + src_file);

    StringDictionary dict_name, dict_address, dict_phone, dict_comment;

    ColumnWriter<int32_t> w_suppkey(output_dir + "/supplier/s_suppkey.bin");
    ColumnWriter<int32_t> w_name(output_dir + "/supplier/s_name.bin");
    ColumnWriter<int32_t> w_address(output_dir + "/supplier/s_address.bin");
    ColumnWriter<int32_t> w_nationkey(output_dir + "/supplier/s_nationkey.bin");
    ColumnWriter<int32_t> w_phone(output_dir + "/supplier/s_phone.bin");
    ColumnWriter<int64_t> w_acctbal(output_dir + "/supplier/s_acctbal.bin");
    ColumnWriter<int32_t> w_comment(output_dir + "/supplier/s_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string token;
        std::vector<std::string> fields;

        while (std::getline(iss, token, '|')) {
            fields.push_back(token);
        }

        if (fields.size() < 7) continue;

        w_suppkey.write(std::stoi(fields[0]));
        w_name.write(dict_name.encode(fields[1]));
        w_address.write(dict_address.encode(fields[2]));
        w_nationkey.write(std::stoi(fields[3]));
        w_phone.write(dict_phone.encode(fields[4]));
        w_acctbal.write(parse_decimal(fields[5], 2));
        w_comment.write(dict_comment.encode(fields[6]));

        row_count++;
    }

    dict_name.save(output_dir + "/supplier/s_name_dict.txt");
    dict_address.save(output_dir + "/supplier/s_address_dict.txt");
    dict_phone.save(output_dir + "/supplier/s_phone_dict.txt");
    dict_comment.save(output_dir + "/supplier/s_comment_dict.txt");

    std::cout << "supplier: " << row_count << " rows ingested\n";
}

// ============================================================================
// Table-specific ingestion: nation
// ============================================================================
void ingest_nation(const std::string& src_file, const std::string& output_dir) {
    std::ifstream in(src_file);
    if (!in) throw std::runtime_error("Cannot open " + src_file);

    StringDictionary dict_name, dict_comment;

    ColumnWriter<int32_t> w_nationkey(output_dir + "/nation/n_nationkey.bin");
    ColumnWriter<int32_t> w_name(output_dir + "/nation/n_name.bin");
    ColumnWriter<int32_t> w_regionkey(output_dir + "/nation/n_regionkey.bin");
    ColumnWriter<int32_t> w_comment(output_dir + "/nation/n_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string token;
        std::vector<std::string> fields;

        while (std::getline(iss, token, '|')) {
            fields.push_back(token);
        }

        if (fields.size() < 4) continue;

        w_nationkey.write(std::stoi(fields[0]));
        w_name.write(dict_name.encode(fields[1]));
        w_regionkey.write(std::stoi(fields[2]));
        w_comment.write(dict_comment.encode(fields[3]));

        row_count++;
    }

    dict_name.save(output_dir + "/nation/n_name_dict.txt");
    dict_comment.save(output_dir + "/nation/n_comment_dict.txt");

    std::cout << "nation: " << row_count << " rows ingested\n";
}

// ============================================================================
// Table-specific ingestion: region
// ============================================================================
void ingest_region(const std::string& src_file, const std::string& output_dir) {
    std::ifstream in(src_file);
    if (!in) throw std::runtime_error("Cannot open " + src_file);

    StringDictionary dict_name, dict_comment;

    ColumnWriter<int32_t> w_regionkey(output_dir + "/region/r_regionkey.bin");
    ColumnWriter<int32_t> w_name(output_dir + "/region/r_name.bin");
    ColumnWriter<int32_t> w_comment(output_dir + "/region/r_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        if (line.empty()) continue;

        std::istringstream iss(line);
        std::string token;
        std::vector<std::string> fields;

        while (std::getline(iss, token, '|')) {
            fields.push_back(token);
        }

        if (fields.size() < 3) continue;

        w_regionkey.write(std::stoi(fields[0]));
        w_name.write(dict_name.encode(fields[1]));
        w_comment.write(dict_comment.encode(fields[2]));

        row_count++;
    }

    dict_name.save(output_dir + "/region/r_name_dict.txt");
    dict_comment.save(output_dir + "/region/r_comment_dict.txt");

    std::cout << "region: " << row_count << " rows ingested\n";
}

// ============================================================================
// Main: Orchestrate parallel ingestion
// ============================================================================
int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>\n";
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];

    // Create output directories
    fs::create_directories(output_dir + "/lineitem");
    fs::create_directories(output_dir + "/orders");
    fs::create_directories(output_dir + "/customer");
    fs::create_directories(output_dir + "/partsupp");
    fs::create_directories(output_dir + "/part");
    fs::create_directories(output_dir + "/supplier");
    fs::create_directories(output_dir + "/nation");
    fs::create_directories(output_dir + "/region");

    // Parallel ingestion using thread pool
    std::vector<std::thread> threads;

    threads.emplace_back([&] { ingest_lineitem(input_dir + "/lineitem.tbl", output_dir); });
    threads.emplace_back([&] { ingest_orders(input_dir + "/orders.tbl", output_dir); });
    threads.emplace_back([&] { ingest_customer(input_dir + "/customer.tbl", output_dir); });
    threads.emplace_back([&] { ingest_partsupp(input_dir + "/partsupp.tbl", output_dir); });
    threads.emplace_back([&] { ingest_part(input_dir + "/part.tbl", output_dir); });
    threads.emplace_back([&] { ingest_supplier(input_dir + "/supplier.tbl", output_dir); });
    threads.emplace_back([&] { ingest_nation(input_dir + "/nation.tbl", output_dir); });
    threads.emplace_back([&] { ingest_region(input_dir + "/region.tbl", output_dir); });

    for (auto& t : threads) {
        t.join();
    }

    std::cout << "Ingestion complete!\n";
    return 0;
}
