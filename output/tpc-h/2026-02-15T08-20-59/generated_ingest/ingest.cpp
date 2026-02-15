#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <map>
#include <unordered_map>
#include <charconv>
#include <algorithm>
#include <cstring>
#include <filesystem>
#include <sstream>
#include <thread>
#include <mutex>
#include <ctime>
#include <cmath>

namespace fs = std::filesystem;

// Thread pool for parallel I/O
struct ThreadPool {
    std::vector<std::thread> workers;
    std::mutex queue_mutex;
    std::vector<std::function<void()>> tasks;
    bool stop = false;

    ThreadPool(int num_threads) {
        for (int i = 0; i < num_threads; ++i) {
            workers.emplace_back([this]() {
                while (true) {
                    std::function<void()> task;
                    {
                        std::unique_lock<std::mutex> lock(queue_mutex);
                        while (tasks.empty() && !stop) {
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        }
                        if (stop && tasks.empty()) break;
                        if (!tasks.empty()) {
                            task = std::move(tasks.back());
                            tasks.pop_back();
                        }
                    }
                    if (task) task();
                }
            });
        }
    }

    ~ThreadPool() {
        {
            std::unique_lock<std::mutex> lock(queue_mutex);
            stop = true;
        }
        for (auto& w : workers) w.join();
    }
};

// Date parsing: YYYY-MM-DD to days since 1970-01-01
int32_t parse_date(const std::string& date_str) {
    if (date_str.length() != 10) return 0;
    int year, month, day;
    std::from_chars(date_str.data(), date_str.data() + 4, year);
    std::from_chars(date_str.data() + 5, date_str.data() + 7, month);
    std::from_chars(date_str.data() + 8, date_str.data() + 10, day);

    // Days since 1970-01-01
    int days = 0;
    for (int y = 1970; y < year; ++y) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) days_in_month[1] = 29;

    for (int m = 1; m < month; ++m) {
        days += days_in_month[m - 1];
    }
    days += day;

    return days;
}

// Decimal parsing: string to int64_t with scale_factor
int64_t parse_decimal(const std::string& str, int64_t scale_factor) {
    double val = 0.0;
    std::from_chars(str.data(), str.data() + str.length(), val);
    return static_cast<int64_t>(std::round(val * scale_factor));
}

// Dictionary encoder for low-cardinality columns
class DictionaryEncoder {
public:
    std::unordered_map<std::string, uint8_t> value_to_code;
    std::vector<std::string> code_to_value;
    uint8_t next_code = 0;

    uint8_t encode(const std::string& value) {
        auto it = value_to_code.find(value);
        if (it != value_to_code.end()) return it->second;
        uint8_t code = next_code++;
        value_to_code[value] = code;
        code_to_value.push_back(value);
        return code;
    }

    void write_dictionary(const std::string& path) {
        std::ofstream f(path, std::ios::binary);
        for (size_t i = 0; i < code_to_value.size(); ++i) {
            f << i << "=" << code_to_value[i] << "\n";
        }
        f.close();
    }
};

// Writer for buffered I/O
class BufferedColumnWriter {
public:
    std::ofstream file;
    static const size_t BUFFER_SIZE = 1024 * 1024;
    std::vector<uint8_t> buffer;
    size_t buffer_pos = 0;

    BufferedColumnWriter(const std::string& path) : file(path, std::ios::binary) {
        buffer.resize(BUFFER_SIZE);
    }

    template<typename T>
    void write(const T& value) {
        size_t size = sizeof(T);
        if (buffer_pos + size > BUFFER_SIZE) flush();
        std::memcpy(buffer.data() + buffer_pos, &value, size);
        buffer_pos += size;
    }

    void write_string(const std::string& str) {
        uint32_t len = str.length();
        write(len);
        if (buffer_pos + len > BUFFER_SIZE) flush();
        std::memcpy(buffer.data() + buffer_pos, str.data(), len);
        buffer_pos += len;
    }

    void flush() {
        if (buffer_pos > 0) {
            file.write(reinterpret_cast<char*>(buffer.data()), buffer_pos);
            buffer_pos = 0;
        }
    }

    ~BufferedColumnWriter() {
        flush();
        file.close();
    }
};

// Lineitem ingestion
void ingest_lineitem(const std::string& file_path, const std::string& output_dir) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << file_path << std::endl;
        return;
    }

    // Output files
    BufferedColumnWriter l_orderkey(output_dir + "/lineitem/l_orderkey.bin");
    BufferedColumnWriter l_partkey(output_dir + "/lineitem/l_partkey.bin");
    BufferedColumnWriter l_suppkey(output_dir + "/lineitem/l_suppkey.bin");
    BufferedColumnWriter l_linenumber(output_dir + "/lineitem/l_linenumber.bin");
    BufferedColumnWriter l_quantity(output_dir + "/lineitem/l_quantity.bin");
    BufferedColumnWriter l_extendedprice(output_dir + "/lineitem/l_extendedprice.bin");
    BufferedColumnWriter l_discount(output_dir + "/lineitem/l_discount.bin");
    BufferedColumnWriter l_tax(output_dir + "/lineitem/l_tax.bin");
    BufferedColumnWriter l_returnflag(output_dir + "/lineitem/l_returnflag.bin");
    BufferedColumnWriter l_linestatus(output_dir + "/lineitem/l_linestatus.bin");
    BufferedColumnWriter l_shipdate(output_dir + "/lineitem/l_shipdate.bin");
    BufferedColumnWriter l_commitdate(output_dir + "/lineitem/l_commitdate.bin");
    BufferedColumnWriter l_receiptdate(output_dir + "/lineitem/l_receiptdate.bin");
    BufferedColumnWriter l_shipinstruct(output_dir + "/lineitem/l_shipinstruct.bin");
    BufferedColumnWriter l_shipmode(output_dir + "/lineitem/l_shipmode.bin");
    BufferedColumnWriter l_comment(output_dir + "/lineitem/l_comment.bin");

    DictionaryEncoder returnflag_enc, linestatus_enc, shipinstruct_enc, shipmode_enc;

    std::string line;
    size_t row_count = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(iss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 16) continue;

        int32_t l_orderkey_val;
        int32_t l_partkey_val;
        int32_t l_suppkey_val;
        int32_t l_linenumber_val;
        int64_t l_quantity_val = parse_decimal(fields[4], 100);
        int64_t l_extendedprice_val = parse_decimal(fields[5], 100);
        int64_t l_discount_val = parse_decimal(fields[6], 100);
        int64_t l_tax_val = parse_decimal(fields[7], 100);
        uint8_t l_returnflag_val = returnflag_enc.encode(fields[8]);
        uint8_t l_linestatus_val = linestatus_enc.encode(fields[9]);
        int32_t l_shipdate_val = parse_date(fields[10]);
        int32_t l_commitdate_val = parse_date(fields[11]);
        int32_t l_receiptdate_val = parse_date(fields[12]);
        uint8_t l_shipinstruct_val = shipinstruct_enc.encode(fields[13]);
        uint8_t l_shipmode_val = shipmode_enc.encode(fields[14]);

        std::from_chars(fields[0].data(), fields[0].data() + fields[0].length(), l_orderkey_val);
        std::from_chars(fields[1].data(), fields[1].data() + fields[1].length(), l_partkey_val);
        std::from_chars(fields[2].data(), fields[2].data() + fields[2].length(), l_suppkey_val);
        std::from_chars(fields[3].data(), fields[3].data() + fields[3].length(), l_linenumber_val);

        l_orderkey.write(l_orderkey_val);
        l_partkey.write(l_partkey_val);
        l_suppkey.write(l_suppkey_val);
        l_linenumber.write(l_linenumber_val);
        l_quantity.write(l_quantity_val);
        l_extendedprice.write(l_extendedprice_val);
        l_discount.write(l_discount_val);
        l_tax.write(l_tax_val);
        l_returnflag.write(l_returnflag_val);
        l_linestatus.write(l_linestatus_val);
        l_shipdate.write(l_shipdate_val);
        l_commitdate.write(l_commitdate_val);
        l_receiptdate.write(l_receiptdate_val);
        l_shipinstruct.write(l_shipinstruct_val);
        l_shipmode.write(l_shipmode_val);
        l_comment.write_string(fields[15]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "Ingested " << row_count << " lineitem rows" << std::endl;
        }
    }

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

    returnflag_enc.write_dictionary(output_dir + "/lineitem/l_returnflag_dict.txt");
    linestatus_enc.write_dictionary(output_dir + "/lineitem/l_linestatus_dict.txt");
    shipinstruct_enc.write_dictionary(output_dir + "/lineitem/l_shipinstruct_dict.txt");
    shipmode_enc.write_dictionary(output_dir + "/lineitem/l_shipmode_dict.txt");

    std::cout << "Lineitem ingestion complete: " << row_count << " rows" << std::endl;
}

// Orders ingestion
void ingest_orders(const std::string& file_path, const std::string& output_dir) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << file_path << std::endl;
        return;
    }

    BufferedColumnWriter o_orderkey(output_dir + "/orders/o_orderkey.bin");
    BufferedColumnWriter o_custkey(output_dir + "/orders/o_custkey.bin");
    BufferedColumnWriter o_orderstatus(output_dir + "/orders/o_orderstatus.bin");
    BufferedColumnWriter o_totalprice(output_dir + "/orders/o_totalprice.bin");
    BufferedColumnWriter o_orderdate(output_dir + "/orders/o_orderdate.bin");
    BufferedColumnWriter o_orderpriority(output_dir + "/orders/o_orderpriority.bin");
    BufferedColumnWriter o_clerk(output_dir + "/orders/o_clerk.bin");
    BufferedColumnWriter o_shippriority(output_dir + "/orders/o_shippriority.bin");
    BufferedColumnWriter o_comment(output_dir + "/orders/o_comment.bin");

    DictionaryEncoder orderstatus_enc, orderpriority_enc, clerk_enc;

    std::string line;
    size_t row_count = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(iss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 9) continue;

        int32_t o_orderkey_val, o_custkey_val, o_shippriority_val;
        std::from_chars(fields[0].data(), fields[0].data() + fields[0].length(), o_orderkey_val);
        std::from_chars(fields[1].data(), fields[1].data() + fields[1].length(), o_custkey_val);
        uint8_t o_orderstatus_val = orderstatus_enc.encode(fields[2]);
        int64_t o_totalprice_val = parse_decimal(fields[3], 100);
        int32_t o_orderdate_val = parse_date(fields[4]);
        uint8_t o_orderpriority_val = orderpriority_enc.encode(fields[5]);
        uint8_t o_clerk_val = clerk_enc.encode(fields[6]);
        std::from_chars(fields[7].data(), fields[7].data() + fields[7].length(), o_shippriority_val);

        o_orderkey.write(o_orderkey_val);
        o_custkey.write(o_custkey_val);
        o_orderstatus.write(o_orderstatus_val);
        o_totalprice.write(o_totalprice_val);
        o_orderdate.write(o_orderdate_val);
        o_orderpriority.write(o_orderpriority_val);
        o_clerk.write(o_clerk_val);
        o_shippriority.write(o_shippriority_val);
        o_comment.write_string(fields[8]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "Ingested " << row_count << " orders rows" << std::endl;
        }
    }

    o_orderkey.flush();
    o_custkey.flush();
    o_orderstatus.flush();
    o_totalprice.flush();
    o_orderdate.flush();
    o_orderpriority.flush();
    o_clerk.flush();
    o_shippriority.flush();
    o_comment.flush();

    orderstatus_enc.write_dictionary(output_dir + "/orders/o_orderstatus_dict.txt");
    orderpriority_enc.write_dictionary(output_dir + "/orders/o_orderpriority_dict.txt");
    clerk_enc.write_dictionary(output_dir + "/orders/o_clerk_dict.txt");

    std::cout << "Orders ingestion complete: " << row_count << " rows" << std::endl;
}

// Customer ingestion
void ingest_customer(const std::string& file_path, const std::string& output_dir) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << file_path << std::endl;
        return;
    }

    BufferedColumnWriter c_custkey(output_dir + "/customer/c_custkey.bin");
    BufferedColumnWriter c_name(output_dir + "/customer/c_name.bin");
    BufferedColumnWriter c_address(output_dir + "/customer/c_address.bin");
    BufferedColumnWriter c_nationkey(output_dir + "/customer/c_nationkey.bin");
    BufferedColumnWriter c_phone(output_dir + "/customer/c_phone.bin");
    BufferedColumnWriter c_acctbal(output_dir + "/customer/c_acctbal.bin");
    BufferedColumnWriter c_mktsegment(output_dir + "/customer/c_mktsegment.bin");
    BufferedColumnWriter c_comment(output_dir + "/customer/c_comment.bin");

    DictionaryEncoder mktsegment_enc;

    std::string line;
    size_t row_count = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(iss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 8) continue;

        int32_t c_custkey_val, c_nationkey_val;
        std::from_chars(fields[0].data(), fields[0].data() + fields[0].length(), c_custkey_val);
        std::from_chars(fields[3].data(), fields[3].data() + fields[3].length(), c_nationkey_val);
        int64_t c_acctbal_val = parse_decimal(fields[5], 100);
        uint8_t c_mktsegment_val = mktsegment_enc.encode(fields[6]);

        c_custkey.write(c_custkey_val);
        c_name.write_string(fields[1]);
        c_address.write_string(fields[2]);
        c_nationkey.write(c_nationkey_val);
        c_phone.write_string(fields[4]);
        c_acctbal.write(c_acctbal_val);
        c_mktsegment.write(c_mktsegment_val);
        c_comment.write_string(fields[7]);

        row_count++;
        if (row_count % 100000 == 0) {
            std::cout << "Ingested " << row_count << " customer rows" << std::endl;
        }
    }

    c_custkey.flush();
    c_name.flush();
    c_address.flush();
    c_nationkey.flush();
    c_phone.flush();
    c_acctbal.flush();
    c_mktsegment.flush();
    c_comment.flush();

    mktsegment_enc.write_dictionary(output_dir + "/customer/c_mktsegment_dict.txt");

    std::cout << "Customer ingestion complete: " << row_count << " rows" << std::endl;
}

// Partsupp ingestion
void ingest_partsupp(const std::string& file_path, const std::string& output_dir) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << file_path << std::endl;
        return;
    }

    BufferedColumnWriter ps_partkey(output_dir + "/partsupp/ps_partkey.bin");
    BufferedColumnWriter ps_suppkey(output_dir + "/partsupp/ps_suppkey.bin");
    BufferedColumnWriter ps_availqty(output_dir + "/partsupp/ps_availqty.bin");
    BufferedColumnWriter ps_supplycost(output_dir + "/partsupp/ps_supplycost.bin");
    BufferedColumnWriter ps_comment(output_dir + "/partsupp/ps_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(iss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 5) continue;

        int32_t ps_partkey_val, ps_suppkey_val, ps_availqty_val;
        std::from_chars(fields[0].data(), fields[0].data() + fields[0].length(), ps_partkey_val);
        std::from_chars(fields[1].data(), fields[1].data() + fields[1].length(), ps_suppkey_val);
        std::from_chars(fields[2].data(), fields[2].data() + fields[2].length(), ps_availqty_val);
        int64_t ps_supplycost_val = parse_decimal(fields[3], 100);

        ps_partkey.write(ps_partkey_val);
        ps_suppkey.write(ps_suppkey_val);
        ps_availqty.write(ps_availqty_val);
        ps_supplycost.write(ps_supplycost_val);
        ps_comment.write_string(fields[4]);

        row_count++;
        if (row_count % 1000000 == 0) {
            std::cout << "Ingested " << row_count << " partsupp rows" << std::endl;
        }
    }

    ps_partkey.flush();
    ps_suppkey.flush();
    ps_availqty.flush();
    ps_supplycost.flush();
    ps_comment.flush();

    std::cout << "Partsupp ingestion complete: " << row_count << " rows" << std::endl;
}

// Part ingestion
void ingest_part(const std::string& file_path, const std::string& output_dir) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << file_path << std::endl;
        return;
    }

    BufferedColumnWriter p_partkey(output_dir + "/part/p_partkey.bin");
    BufferedColumnWriter p_name(output_dir + "/part/p_name.bin");
    BufferedColumnWriter p_mfgr(output_dir + "/part/p_mfgr.bin");
    BufferedColumnWriter p_brand(output_dir + "/part/p_brand.bin");
    BufferedColumnWriter p_type(output_dir + "/part/p_type.bin");
    BufferedColumnWriter p_size(output_dir + "/part/p_size.bin");
    BufferedColumnWriter p_container(output_dir + "/part/p_container.bin");
    BufferedColumnWriter p_retailprice(output_dir + "/part/p_retailprice.bin");
    BufferedColumnWriter p_comment(output_dir + "/part/p_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(iss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 9) continue;

        int32_t p_partkey_val, p_size_val;
        std::from_chars(fields[0].data(), fields[0].data() + fields[0].length(), p_partkey_val);
        std::from_chars(fields[5].data(), fields[5].data() + fields[5].length(), p_size_val);
        int64_t p_retailprice_val = parse_decimal(fields[7], 100);

        p_partkey.write(p_partkey_val);
        p_name.write_string(fields[1]);
        p_mfgr.write_string(fields[2]);
        p_brand.write_string(fields[3]);
        p_type.write_string(fields[4]);
        p_size.write(p_size_val);
        p_container.write_string(fields[6]);
        p_retailprice.write(p_retailprice_val);
        p_comment.write_string(fields[8]);

        row_count++;
        if (row_count % 100000 == 0) {
            std::cout << "Ingested " << row_count << " part rows" << std::endl;
        }
    }

    p_partkey.flush();
    p_name.flush();
    p_mfgr.flush();
    p_brand.flush();
    p_type.flush();
    p_size.flush();
    p_container.flush();
    p_retailprice.flush();
    p_comment.flush();

    std::cout << "Part ingestion complete: " << row_count << " rows" << std::endl;
}

// Supplier ingestion
void ingest_supplier(const std::string& file_path, const std::string& output_dir) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << file_path << std::endl;
        return;
    }

    BufferedColumnWriter s_suppkey(output_dir + "/supplier/s_suppkey.bin");
    BufferedColumnWriter s_name(output_dir + "/supplier/s_name.bin");
    BufferedColumnWriter s_address(output_dir + "/supplier/s_address.bin");
    BufferedColumnWriter s_nationkey(output_dir + "/supplier/s_nationkey.bin");
    BufferedColumnWriter s_phone(output_dir + "/supplier/s_phone.bin");
    BufferedColumnWriter s_acctbal(output_dir + "/supplier/s_acctbal.bin");
    BufferedColumnWriter s_comment(output_dir + "/supplier/s_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(iss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 7) continue;

        int32_t s_suppkey_val, s_nationkey_val;
        std::from_chars(fields[0].data(), fields[0].data() + fields[0].length(), s_suppkey_val);
        std::from_chars(fields[3].data(), fields[3].data() + fields[3].length(), s_nationkey_val);
        int64_t s_acctbal_val = parse_decimal(fields[5], 100);

        s_suppkey.write(s_suppkey_val);
        s_name.write_string(fields[1]);
        s_address.write_string(fields[2]);
        s_nationkey.write(s_nationkey_val);
        s_phone.write_string(fields[4]);
        s_acctbal.write(s_acctbal_val);
        s_comment.write_string(fields[6]);

        row_count++;
        if (row_count % 10000 == 0) {
            std::cout << "Ingested " << row_count << " supplier rows" << std::endl;
        }
    }

    s_suppkey.flush();
    s_name.flush();
    s_address.flush();
    s_nationkey.flush();
    s_phone.flush();
    s_acctbal.flush();
    s_comment.flush();

    std::cout << "Supplier ingestion complete: " << row_count << " rows" << std::endl;
}

// Nation ingestion
void ingest_nation(const std::string& file_path, const std::string& output_dir) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << file_path << std::endl;
        return;
    }

    BufferedColumnWriter n_nationkey(output_dir + "/nation/n_nationkey.bin");
    BufferedColumnWriter n_name(output_dir + "/nation/n_name.bin");
    BufferedColumnWriter n_regionkey(output_dir + "/nation/n_regionkey.bin");
    BufferedColumnWriter n_comment(output_dir + "/nation/n_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(iss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 4) continue;

        int32_t n_nationkey_val, n_regionkey_val;
        std::from_chars(fields[0].data(), fields[0].data() + fields[0].length(), n_nationkey_val);
        std::from_chars(fields[2].data(), fields[2].data() + fields[2].length(), n_regionkey_val);

        n_nationkey.write(n_nationkey_val);
        n_name.write_string(fields[1]);
        n_regionkey.write(n_regionkey_val);
        n_comment.write_string(fields[3]);

        row_count++;
    }

    n_nationkey.flush();
    n_name.flush();
    n_regionkey.flush();
    n_comment.flush();

    std::cout << "Nation ingestion complete: " << row_count << " rows" << std::endl;
}

// Region ingestion
void ingest_region(const std::string& file_path, const std::string& output_dir) {
    std::ifstream file(file_path);
    if (!file.is_open()) {
        std::cerr << "Failed to open " << file_path << std::endl;
        return;
    }

    BufferedColumnWriter r_regionkey(output_dir + "/region/r_regionkey.bin");
    BufferedColumnWriter r_name(output_dir + "/region/r_name.bin");
    BufferedColumnWriter r_comment(output_dir + "/region/r_comment.bin");

    std::string line;
    size_t row_count = 0;

    while (std::getline(file, line)) {
        std::istringstream iss(line);
        std::string field;
        std::vector<std::string> fields;

        while (std::getline(iss, field, '|')) {
            fields.push_back(field);
        }

        if (fields.size() < 3) continue;

        int32_t r_regionkey_val;
        std::from_chars(fields[0].data(), fields[0].data() + fields[0].length(), r_regionkey_val);

        r_regionkey.write(r_regionkey_val);
        r_name.write_string(fields[1]);
        r_comment.write_string(fields[2]);

        row_count++;
    }

    r_regionkey.flush();
    r_name.flush();
    r_comment.flush();

    std::cout << "Region ingestion complete: " << row_count << " rows" << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string output_dir = argv[2];

    // Create output directories
    std::vector<std::string> tables = {"lineitem", "orders", "customer", "partsupp", "part", "supplier", "nation", "region"};
    for (const auto& table : tables) {
        fs::create_directories(output_dir + "/" + table);
    }

    std::cout << "Starting TPC-H data ingestion..." << std::endl;

    // Ingest tables in parallel
    std::thread t1([&]() { ingest_lineitem(data_dir + "/lineitem.tbl", output_dir); });
    std::thread t2([&]() { ingest_orders(data_dir + "/orders.tbl", output_dir); });
    std::thread t3([&]() { ingest_customer(data_dir + "/customer.tbl", output_dir); });
    std::thread t4([&]() { ingest_partsupp(data_dir + "/partsupp.tbl", output_dir); });
    std::thread t5([&]() { ingest_part(data_dir + "/part.tbl", output_dir); });
    std::thread t6([&]() { ingest_supplier(data_dir + "/supplier.tbl", output_dir); });
    std::thread t7([&]() { ingest_nation(data_dir + "/nation.tbl", output_dir); });
    std::thread t8([&]() { ingest_region(data_dir + "/region.tbl", output_dir); });

    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();
    t7.join();
    t8.join();

    std::cout << "TPC-H data ingestion complete!" << std::endl;

    return 0;
}
