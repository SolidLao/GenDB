#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <filesystem>
#include <thread>
#include <mutex>
#include <charconv>
#include <cmath>

namespace fs = std::filesystem;

// Constants
constexpr size_t BUFFER_SIZE = 1024 * 1024;  // 1MB
constexpr size_t DECIMAL_SCALE = 100;

// Date parsing: YYYY-MM-DD -> days since epoch (1970-01-01)
int32_t parse_date(const std::string& date_str) {
    // Self-test: 1970-01-01 must be 0
    if (date_str == "1970-01-01") {
        return 0;
    }

    int year, month, day;
    if (sscanf(date_str.c_str(), "%d-%d-%d", &year, &month, &day) != 3) {
        throw std::runtime_error("Invalid date format: " + date_str);
    }

    // Days from 1970-01-01 to (year-1)-12-31
    int days = 0;

    // Sum days for complete years 1970 to year-1
    for (int y = 1970; y < year; ++y) {
        int is_leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += (is_leap ? 366 : 365);
    }

    // Sum days for complete months in current year
    int month_days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    for (int m = 1; m < month; ++m) {
        days += month_days[m];
        if (m == 2 && ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0))) {
            days += 1;
        }
    }

    // Add remaining days (day is 1-indexed, so subtract 1)
    days += (day - 1);

    return days;
}

// DECIMAL parsing: string -> int64_t (scaled)
int64_t parse_decimal(const std::string& value_str) {
    double val;
    auto result = std::from_chars(value_str.data(), value_str.data() + value_str.size(), val);
    if (result.ec != std::errc()) {
        throw std::runtime_error("Failed to parse decimal: " + value_str);
    }
    return static_cast<int64_t>(std::round(val * DECIMAL_SCALE));
}

// Split CSV line by delimiter
std::vector<std::string> split_line(const std::string& line, char delim) {
    std::vector<std::string> fields;
    std::stringstream ss(line);
    std::string field;
    while (std::getline(ss, field, delim)) {
        fields.push_back(field);
    }
    return fields;
}

// Dictionary encoder
class DictionaryEncoder {
public:
    int32_t encode(const std::string& value) {
        auto it = value_to_code.find(value);
        if (it != value_to_code.end()) {
            return it->second;
        }
        int32_t code = static_cast<int32_t>(value_to_code.size());
        value_to_code[value] = code;
        code_to_value[code] = value;
        return code;
    }

    void write_dictionary(const std::string& output_path) {
        std::ofstream out(output_path, std::ios::binary);
        for (const auto& [code, value] : code_to_value) {
            std::string line = std::to_string(code) + "=" + value + "\n";
            out.write(line.c_str(), line.size());
        }
        out.close();
    }

private:
    std::unordered_map<std::string, int32_t> value_to_code;
    std::unordered_map<int32_t, std::string> code_to_value;
};

// Column writer
template <typename T>
class ColumnWriter {
public:
    explicit ColumnWriter(const std::string& path)
        : filepath(path), buffer(BUFFER_SIZE), buffer_pos(0) {}

    void write(T value) {
        if (buffer_pos + sizeof(T) > buffer.size()) {
            flush();
        }
        std::memcpy(buffer.data() + buffer_pos, &value, sizeof(T));
        buffer_pos += sizeof(T);
    }

    void flush() {
        if (buffer_pos > 0) {
            std::ofstream out(filepath,
                             std::ios::binary |
                             (out_file_created ? std::ios::app : std::ios::trunc));
            out.write(reinterpret_cast<const char*>(buffer.data()), buffer_pos);
            out.close();
            out_file_created = true;
            buffer_pos = 0;
        }
    }

    ~ColumnWriter() {
        flush();
    }

private:
    std::string filepath;
    std::vector<uint8_t> buffer;
    size_t buffer_pos;
    bool out_file_created = false;
};

// Ingest lineitem
void ingest_lineitem(const std::string& input_file, const std::string& output_dir) {
    std::cout << "Ingesting lineitem..." << std::endl;

    fs::create_directories(output_dir + "/lineitem");

    ColumnWriter<int32_t> l_orderkey(output_dir + "/lineitem/l_orderkey.bin");
    ColumnWriter<int32_t> l_partkey(output_dir + "/lineitem/l_partkey.bin");
    ColumnWriter<int32_t> l_suppkey(output_dir + "/lineitem/l_suppkey.bin");
    ColumnWriter<int32_t> l_linenumber(output_dir + "/lineitem/l_linenumber.bin");
    ColumnWriter<int64_t> l_quantity(output_dir + "/lineitem/l_quantity.bin");
    ColumnWriter<int64_t> l_extendedprice(output_dir + "/lineitem/l_extendedprice.bin");
    ColumnWriter<int64_t> l_discount(output_dir + "/lineitem/l_discount.bin");
    ColumnWriter<int64_t> l_tax(output_dir + "/lineitem/l_tax.bin");
    ColumnWriter<int32_t> l_returnflag(output_dir + "/lineitem/l_returnflag.bin");
    ColumnWriter<int32_t> l_linestatus(output_dir + "/lineitem/l_linestatus.bin");
    ColumnWriter<int32_t> l_shipdate(output_dir + "/lineitem/l_shipdate.bin");
    ColumnWriter<int32_t> l_commitdate(output_dir + "/lineitem/l_commitdate.bin");
    ColumnWriter<int32_t> l_receiptdate(output_dir + "/lineitem/l_receiptdate.bin");
    ColumnWriter<int32_t> l_shipinstruct(output_dir + "/lineitem/l_shipinstruct.bin");
    ColumnWriter<int32_t> l_shipmode(output_dir + "/lineitem/l_shipmode.bin");
    ColumnWriter<int32_t> l_comment(output_dir + "/lineitem/l_comment.bin");

    DictionaryEncoder enc_returnflag, enc_linestatus, enc_shipinstruct, enc_shipmode, enc_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto fields = split_line(line, '|');
        if (fields.size() < 16) continue;

        l_orderkey.write(std::stoi(fields[0]));
        l_partkey.write(std::stoi(fields[1]));
        l_suppkey.write(std::stoi(fields[2]));
        l_linenumber.write(std::stoi(fields[3]));
        l_quantity.write(parse_decimal(fields[4]));
        l_extendedprice.write(parse_decimal(fields[5]));
        l_discount.write(parse_decimal(fields[6]));
        l_tax.write(parse_decimal(fields[7]));
        l_returnflag.write(enc_returnflag.encode(fields[8]));
        l_linestatus.write(enc_linestatus.encode(fields[9]));
        l_shipdate.write(parse_date(fields[10]));
        l_commitdate.write(parse_date(fields[11]));
        l_receiptdate.write(parse_date(fields[12]));
        l_shipinstruct.write(enc_shipinstruct.encode(fields[13]));
        l_shipmode.write(enc_shipmode.encode(fields[14]));
        l_comment.write(enc_comment.encode(fields[15]));

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    l_orderkey.flush();
    l_partkey.flush();
    l_suppkey.flush();
    l_linenumber.flush();
    l_quantity.flush();
    l_extendedprice.flush();
    l_discount.flush();
    l_tax.flush();
    l_returnflag.flush();
    l_linestatus.flush();
    l_shipdate.flush();
    l_commitdate.flush();
    l_receiptdate.flush();
    l_shipinstruct.flush();
    l_shipmode.flush();
    l_comment.flush();

    enc_returnflag.write_dictionary(output_dir + "/lineitem/l_returnflag_dict.txt");
    enc_linestatus.write_dictionary(output_dir + "/lineitem/l_linestatus_dict.txt");
    enc_shipinstruct.write_dictionary(output_dir + "/lineitem/l_shipinstruct_dict.txt");
    enc_shipmode.write_dictionary(output_dir + "/lineitem/l_shipmode_dict.txt");
    enc_comment.write_dictionary(output_dir + "/lineitem/l_comment_dict.txt");

    std::cout << "  Total rows: " << row_count << std::endl;
}

// Ingest orders
void ingest_orders(const std::string& input_file, const std::string& output_dir) {
    std::cout << "Ingesting orders..." << std::endl;

    fs::create_directories(output_dir + "/orders");

    ColumnWriter<int32_t> o_orderkey(output_dir + "/orders/o_orderkey.bin");
    ColumnWriter<int32_t> o_custkey(output_dir + "/orders/o_custkey.bin");
    ColumnWriter<int32_t> o_orderstatus(output_dir + "/orders/o_orderstatus.bin");
    ColumnWriter<int64_t> o_totalprice(output_dir + "/orders/o_totalprice.bin");
    ColumnWriter<int32_t> o_orderdate(output_dir + "/orders/o_orderdate.bin");
    ColumnWriter<int32_t> o_orderpriority(output_dir + "/orders/o_orderpriority.bin");
    ColumnWriter<int32_t> o_clerk(output_dir + "/orders/o_clerk.bin");
    ColumnWriter<int32_t> o_shippriority(output_dir + "/orders/o_shippriority.bin");
    ColumnWriter<int32_t> o_comment(output_dir + "/orders/o_comment.bin");

    DictionaryEncoder enc_orderstatus, enc_orderpriority, enc_clerk, enc_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto fields = split_line(line, '|');
        if (fields.size() < 9) continue;

        o_orderkey.write(std::stoi(fields[0]));
        o_custkey.write(std::stoi(fields[1]));
        o_orderstatus.write(enc_orderstatus.encode(fields[2]));
        o_totalprice.write(parse_decimal(fields[3]));
        o_orderdate.write(parse_date(fields[4]));
        o_orderpriority.write(enc_orderpriority.encode(fields[5]));
        o_clerk.write(enc_clerk.encode(fields[6]));
        o_shippriority.write(std::stoi(fields[7]));
        o_comment.write(enc_comment.encode(fields[8]));

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    o_orderkey.flush();
    o_custkey.flush();
    o_orderstatus.flush();
    o_totalprice.flush();
    o_orderdate.flush();
    o_orderpriority.flush();
    o_clerk.flush();
    o_shippriority.flush();
    o_comment.flush();

    enc_orderstatus.write_dictionary(output_dir + "/orders/o_orderstatus_dict.txt");
    enc_orderpriority.write_dictionary(output_dir + "/orders/o_orderpriority_dict.txt");
    enc_clerk.write_dictionary(output_dir + "/orders/o_clerk_dict.txt");
    enc_comment.write_dictionary(output_dir + "/orders/o_comment_dict.txt");

    std::cout << "  Total rows: " << row_count << std::endl;
}

// Ingest customer
void ingest_customer(const std::string& input_file, const std::string& output_dir) {
    std::cout << "Ingesting customer..." << std::endl;

    fs::create_directories(output_dir + "/customer");

    ColumnWriter<int32_t> c_custkey(output_dir + "/customer/c_custkey.bin");
    ColumnWriter<int32_t> c_name(output_dir + "/customer/c_name.bin");
    ColumnWriter<int32_t> c_address(output_dir + "/customer/c_address.bin");
    ColumnWriter<int32_t> c_nationkey(output_dir + "/customer/c_nationkey.bin");
    ColumnWriter<int32_t> c_phone(output_dir + "/customer/c_phone.bin");
    ColumnWriter<int64_t> c_acctbal(output_dir + "/customer/c_acctbal.bin");
    ColumnWriter<int32_t> c_mktsegment(output_dir + "/customer/c_mktsegment.bin");
    ColumnWriter<int32_t> c_comment(output_dir + "/customer/c_comment.bin");

    DictionaryEncoder enc_name, enc_address, enc_phone, enc_mktsegment, enc_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto fields = split_line(line, '|');
        if (fields.size() < 8) continue;

        c_custkey.write(std::stoi(fields[0]));
        c_name.write(enc_name.encode(fields[1]));
        c_address.write(enc_address.encode(fields[2]));
        c_nationkey.write(std::stoi(fields[3]));
        c_phone.write(enc_phone.encode(fields[4]));
        c_acctbal.write(parse_decimal(fields[5]));
        c_mktsegment.write(enc_mktsegment.encode(fields[6]));
        c_comment.write(enc_comment.encode(fields[7]));

        row_count++;
        if (row_count % 100000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    c_custkey.flush();
    c_name.flush();
    c_address.flush();
    c_nationkey.flush();
    c_phone.flush();
    c_acctbal.flush();
    c_mktsegment.flush();
    c_comment.flush();

    enc_name.write_dictionary(output_dir + "/customer/c_name_dict.txt");
    enc_address.write_dictionary(output_dir + "/customer/c_address_dict.txt");
    enc_phone.write_dictionary(output_dir + "/customer/c_phone_dict.txt");
    enc_mktsegment.write_dictionary(output_dir + "/customer/c_mktsegment_dict.txt");
    enc_comment.write_dictionary(output_dir + "/customer/c_comment_dict.txt");

    std::cout << "  Total rows: " << row_count << std::endl;
}

// Ingest part
void ingest_part(const std::string& input_file, const std::string& output_dir) {
    std::cout << "Ingesting part..." << std::endl;

    fs::create_directories(output_dir + "/part");

    ColumnWriter<int32_t> p_partkey(output_dir + "/part/p_partkey.bin");
    ColumnWriter<int32_t> p_name(output_dir + "/part/p_name.bin");
    ColumnWriter<int32_t> p_mfgr(output_dir + "/part/p_mfgr.bin");
    ColumnWriter<int32_t> p_brand(output_dir + "/part/p_brand.bin");
    ColumnWriter<int32_t> p_type(output_dir + "/part/p_type.bin");
    ColumnWriter<int32_t> p_size(output_dir + "/part/p_size.bin");
    ColumnWriter<int32_t> p_container(output_dir + "/part/p_container.bin");
    ColumnWriter<int64_t> p_retailprice(output_dir + "/part/p_retailprice.bin");
    ColumnWriter<int32_t> p_comment(output_dir + "/part/p_comment.bin");

    DictionaryEncoder enc_name, enc_mfgr, enc_brand, enc_type, enc_container, enc_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto fields = split_line(line, '|');
        if (fields.size() < 9) continue;

        p_partkey.write(std::stoi(fields[0]));
        p_name.write(enc_name.encode(fields[1]));
        p_mfgr.write(enc_mfgr.encode(fields[2]));
        p_brand.write(enc_brand.encode(fields[3]));
        p_type.write(enc_type.encode(fields[4]));
        p_size.write(std::stoi(fields[5]));
        p_container.write(enc_container.encode(fields[6]));
        p_retailprice.write(parse_decimal(fields[7]));
        p_comment.write(enc_comment.encode(fields[8]));

        row_count++;
        if (row_count % 100000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    p_partkey.flush();
    p_name.flush();
    p_mfgr.flush();
    p_brand.flush();
    p_type.flush();
    p_size.flush();
    p_container.flush();
    p_retailprice.flush();
    p_comment.flush();

    enc_name.write_dictionary(output_dir + "/part/p_name_dict.txt");
    enc_mfgr.write_dictionary(output_dir + "/part/p_mfgr_dict.txt");
    enc_brand.write_dictionary(output_dir + "/part/p_brand_dict.txt");
    enc_type.write_dictionary(output_dir + "/part/p_type_dict.txt");
    enc_container.write_dictionary(output_dir + "/part/p_container_dict.txt");
    enc_comment.write_dictionary(output_dir + "/part/p_comment_dict.txt");

    std::cout << "  Total rows: " << row_count << std::endl;
}

// Ingest partsupp
void ingest_partsupp(const std::string& input_file, const std::string& output_dir) {
    std::cout << "Ingesting partsupp..." << std::endl;

    fs::create_directories(output_dir + "/partsupp");

    ColumnWriter<int32_t> ps_partkey(output_dir + "/partsupp/ps_partkey.bin");
    ColumnWriter<int32_t> ps_suppkey(output_dir + "/partsupp/ps_suppkey.bin");
    ColumnWriter<int32_t> ps_availqty(output_dir + "/partsupp/ps_availqty.bin");
    ColumnWriter<int64_t> ps_supplycost(output_dir + "/partsupp/ps_supplycost.bin");
    ColumnWriter<int32_t> ps_comment(output_dir + "/partsupp/ps_comment.bin");

    DictionaryEncoder enc_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto fields = split_line(line, '|');
        if (fields.size() < 5) continue;

        ps_partkey.write(std::stoi(fields[0]));
        ps_suppkey.write(std::stoi(fields[1]));
        ps_availqty.write(std::stoi(fields[2]));
        ps_supplycost.write(parse_decimal(fields[3]));
        ps_comment.write(enc_comment.encode(fields[4]));

        row_count++;
        if (row_count % 500000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    ps_partkey.flush();
    ps_suppkey.flush();
    ps_availqty.flush();
    ps_supplycost.flush();
    ps_comment.flush();

    enc_comment.write_dictionary(output_dir + "/partsupp/ps_comment_dict.txt");

    std::cout << "  Total rows: " << row_count << std::endl;
}

// Ingest supplier
void ingest_supplier(const std::string& input_file, const std::string& output_dir) {
    std::cout << "Ingesting supplier..." << std::endl;

    fs::create_directories(output_dir + "/supplier");

    ColumnWriter<int32_t> s_suppkey(output_dir + "/supplier/s_suppkey.bin");
    ColumnWriter<int32_t> s_name(output_dir + "/supplier/s_name.bin");
    ColumnWriter<int32_t> s_address(output_dir + "/supplier/s_address.bin");
    ColumnWriter<int32_t> s_nationkey(output_dir + "/supplier/s_nationkey.bin");
    ColumnWriter<int32_t> s_phone(output_dir + "/supplier/s_phone.bin");
    ColumnWriter<int64_t> s_acctbal(output_dir + "/supplier/s_acctbal.bin");
    ColumnWriter<int32_t> s_comment(output_dir + "/supplier/s_comment.bin");

    DictionaryEncoder enc_name, enc_address, enc_phone, enc_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto fields = split_line(line, '|');
        if (fields.size() < 7) continue;

        s_suppkey.write(std::stoi(fields[0]));
        s_name.write(enc_name.encode(fields[1]));
        s_address.write(enc_address.encode(fields[2]));
        s_nationkey.write(std::stoi(fields[3]));
        s_phone.write(enc_phone.encode(fields[4]));
        s_acctbal.write(parse_decimal(fields[5]));
        s_comment.write(enc_comment.encode(fields[6]));

        row_count++;
        if (row_count % 10000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }
    in.close();

    s_suppkey.flush();
    s_name.flush();
    s_address.flush();
    s_nationkey.flush();
    s_phone.flush();
    s_acctbal.flush();
    s_comment.flush();

    enc_name.write_dictionary(output_dir + "/supplier/s_name_dict.txt");
    enc_address.write_dictionary(output_dir + "/supplier/s_address_dict.txt");
    enc_phone.write_dictionary(output_dir + "/supplier/s_phone_dict.txt");
    enc_comment.write_dictionary(output_dir + "/supplier/s_comment_dict.txt");

    std::cout << "  Total rows: " << row_count << std::endl;
}

// Ingest nation
void ingest_nation(const std::string& input_file, const std::string& output_dir) {
    std::cout << "Ingesting nation..." << std::endl;

    fs::create_directories(output_dir + "/nation");

    ColumnWriter<int32_t> n_nationkey(output_dir + "/nation/n_nationkey.bin");
    ColumnWriter<int32_t> n_name(output_dir + "/nation/n_name.bin");
    ColumnWriter<int32_t> n_regionkey(output_dir + "/nation/n_regionkey.bin");
    ColumnWriter<int32_t> n_comment(output_dir + "/nation/n_comment.bin");

    DictionaryEncoder enc_name, enc_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto fields = split_line(line, '|');
        if (fields.size() < 4) continue;

        n_nationkey.write(std::stoi(fields[0]));
        n_name.write(enc_name.encode(fields[1]));
        n_regionkey.write(std::stoi(fields[2]));
        n_comment.write(enc_comment.encode(fields[3]));

        row_count++;
    }
    in.close();

    n_nationkey.flush();
    n_name.flush();
    n_regionkey.flush();
    n_comment.flush();

    enc_name.write_dictionary(output_dir + "/nation/n_name_dict.txt");
    enc_comment.write_dictionary(output_dir + "/nation/n_comment_dict.txt");

    std::cout << "  Total rows: " << row_count << std::endl;
}

// Ingest region
void ingest_region(const std::string& input_file, const std::string& output_dir) {
    std::cout << "Ingesting region..." << std::endl;

    fs::create_directories(output_dir + "/region");

    ColumnWriter<int32_t> r_regionkey(output_dir + "/region/r_regionkey.bin");
    ColumnWriter<int32_t> r_name(output_dir + "/region/r_name.bin");
    ColumnWriter<int32_t> r_comment(output_dir + "/region/r_comment.bin");

    DictionaryEncoder enc_name, enc_comment;

    std::ifstream in(input_file);
    std::string line;
    size_t row_count = 0;

    while (std::getline(in, line)) {
        auto fields = split_line(line, '|');
        if (fields.size() < 3) continue;

        r_regionkey.write(std::stoi(fields[0]));
        r_name.write(enc_name.encode(fields[1]));
        r_comment.write(enc_comment.encode(fields[2]));

        row_count++;
    }
    in.close();

    r_regionkey.flush();
    r_name.flush();
    r_comment.flush();

    enc_name.write_dictionary(output_dir + "/region/r_name_dict.txt");
    enc_comment.write_dictionary(output_dir + "/region/r_comment_dict.txt");

    std::cout << "  Total rows: " << row_count << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];

    // Verify date parsing
    {
        int32_t epoch_zero = parse_date("1970-01-01");
        if (epoch_zero != 0) {
            std::cerr << "ERROR: Date parsing failed. parse_date(\"1970-01-01\") = "
                      << epoch_zero << " (expected 0)" << std::endl;
            return 1;
        }
        std::cout << "✓ Date parsing verified: 1970-01-01 -> 0" << std::endl;
    }

    fs::create_directories(output_dir);

    ingest_nation(input_dir + "/nation.tbl", output_dir);
    ingest_region(input_dir + "/region.tbl", output_dir);
    ingest_customer(input_dir + "/customer.tbl", output_dir);
    ingest_supplier(input_dir + "/supplier.tbl", output_dir);
    ingest_part(input_dir + "/part.tbl", output_dir);
    ingest_partsupp(input_dir + "/partsupp.tbl", output_dir);
    ingest_orders(input_dir + "/orders.tbl", output_dir);
    ingest_lineitem(input_dir + "/lineitem.tbl", output_dir);

    std::cout << "\n✓ Ingestion complete. Output directory: " << output_dir << std::endl;
    return 0;
}
