#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>
#include <cstring>
#include <filesystem>
#include <map>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <memory>
#include <cstdint>
#include <algorithm>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <charconv>

namespace fs = std::filesystem;

struct IngestConfig {
    std::string data_dir;
    std::string output_dir;
    size_t chunk_size = 1024 * 1024 * 16;  // 16MB chunks
    int num_threads = std::thread::hardware_concurrency();
};

struct DictionaryBuilder {
    std::unordered_map<std::string, int32_t> value_to_code;
    std::vector<std::string> code_to_value;
    std::mutex lock;

    int32_t get_or_insert(const std::string& value) {
        std::lock_guard<std::mutex> g(lock);
        auto it = value_to_code.find(value);
        if (it != value_to_code.end()) return it->second;
        int32_t code = code_to_value.size();
        value_to_code[value] = code;
        code_to_value.push_back(value);
        return code;
    }

    void write(const std::string& path) {
        std::ofstream f(path);
        for (int32_t i = 0; i < (int32_t)code_to_value.size(); ++i) {
            f << i << "=" << code_to_value[i] << "\n";
        }
    }
};

// Parse date in YYYY-MM-DD format to days since 1970-01-01
int32_t parse_date(const char* s, size_t len) {
    if (len < 10) return 0;
    int32_t year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
    int32_t month = (s[5] - '0') * 10 + (s[6] - '0');
    int32_t day = (s[8] - '0') * 10 + (s[9] - '0');

    // Days since 1970-01-01
    int32_t days = 0;
    // Complete years from 1970 to year-1
    for (int32_t y = 1970; y < year; ++y) {
        days += ((y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)) ? 366 : 365;
    }
    // Complete months from January to month-1
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if ((year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)) days_in_month[1] = 29;
    for (int32_t m = 1; m < month; ++m) {
        days += days_in_month[m - 1];
    }
    // Days of this month
    days += day - 1;
    return days;
}

// Parse decimal with scale factor
int64_t parse_decimal(const char* s, size_t len, int scale_factor) {
    double val = 0.0;
    auto [ptr, ec] = std::from_chars(s, s + len, val);
    if (ec != std::errc()) return 0;
    return (int64_t)(val * scale_factor + 0.5);
}

struct FieldBuffer {
    std::vector<char> buffer;
    std::vector<size_t> offsets;  // Starting position of each field

    void parse_line(const std::string& line, char delimiter = '|') {
        buffer.clear();
        offsets.clear();
        buffer.insert(buffer.end(), line.begin(), line.end());

        offsets.push_back(0);
        for (size_t i = 0; i < line.size(); ++i) {
            if (line[i] == delimiter) {
                offsets.push_back(i + 1);
            }
        }
    }

    const char* get_field(int idx) const {
        if (idx >= (int)offsets.size()) return "";
        return buffer.data() + offsets[idx];
    }

    size_t get_field_len(int idx) const {
        if (idx >= (int)offsets.size()) return 0;
        size_t start = offsets[idx];
        size_t end = (idx + 1 < (int)offsets.size()) ? offsets[idx + 1] - 1 : buffer.size();
        return (end > start) ? end - start : 0;
    }
};

template <typename T>
void write_column(const std::string& path, const std::vector<T>& data) {
    std::ofstream f(path, std::ios::binary);
    f.write(reinterpret_cast<const char*>(data.data()), data.size() * sizeof(T));
    f.close();
}

void ingest_lineitem(const std::string& input_path, const std::string& output_dir) {
    std::cout << "Ingesting lineitem..." << std::endl;

    std::vector<int32_t> l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice,
                         l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate,
                         l_receiptdate, l_shipinstruct, l_shipmode;
    std::vector<std::string> l_comment;

    DictionaryBuilder dict_returnflag, dict_linestatus, dict_shipinstruct, dict_shipmode;

    std::ifstream infile(input_path);
    std::string line;
    FieldBuffer fields;
    size_t row_count = 0;

    while (std::getline(infile, line)) {
        if (line.empty() || line[0] == '\n') continue;
        fields.parse_line(line);

        int32_t ok, pk, sk, ln;
        std::from_chars(fields.get_field(0), fields.get_field(0) + fields.get_field_len(0), ok);
        std::from_chars(fields.get_field(1), fields.get_field(1) + fields.get_field_len(1), pk);
        std::from_chars(fields.get_field(2), fields.get_field(2) + fields.get_field_len(2), sk);
        std::from_chars(fields.get_field(3), fields.get_field(3) + fields.get_field_len(3), ln);

        l_orderkey.push_back(ok);
        l_partkey.push_back(pk);
        l_suppkey.push_back(sk);
        l_linenumber.push_back(ln);

        // Quantities and prices as DECIMAL(15,2) scaled by 100
        l_quantity.push_back(parse_decimal(fields.get_field(4), fields.get_field_len(4), 100));
        l_extendedprice.push_back(parse_decimal(fields.get_field(5), fields.get_field_len(5), 100));
        l_discount.push_back(parse_decimal(fields.get_field(6), fields.get_field_len(6), 100));
        l_tax.push_back(parse_decimal(fields.get_field(7), fields.get_field_len(7), 100));

        // Dictionary-encoded flags
        std::string rf(fields.get_field(8), fields.get_field_len(8));
        std::string ls(fields.get_field(9), fields.get_field_len(9));
        l_returnflag.push_back(dict_returnflag.get_or_insert(rf));
        l_linestatus.push_back(dict_linestatus.get_or_insert(ls));

        // Dates as epoch days
        l_shipdate.push_back(parse_date(fields.get_field(10), fields.get_field_len(10)));
        l_commitdate.push_back(parse_date(fields.get_field(11), fields.get_field_len(11)));
        l_receiptdate.push_back(parse_date(fields.get_field(12), fields.get_field_len(12)));

        // Dictionary-encoded ship instructions and modes
        std::string si(fields.get_field(13), fields.get_field_len(13));
        std::string sm(fields.get_field(14), fields.get_field_len(14));
        l_shipinstruct.push_back(dict_shipinstruct.get_or_insert(si));
        l_shipmode.push_back(dict_shipmode.get_or_insert(sm));

        l_comment.push_back(std::string(fields.get_field(15), fields.get_field_len(15)));

        if (++row_count % 1000000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }

    std::string table_dir = output_dir + "/lineitem";
    fs::create_directories(table_dir);

    write_column(table_dir + "/l_orderkey.bin", l_orderkey);
    write_column(table_dir + "/l_partkey.bin", l_partkey);
    write_column(table_dir + "/l_suppkey.bin", l_suppkey);
    write_column(table_dir + "/l_linenumber.bin", l_linenumber);
    write_column(table_dir + "/l_quantity.bin", l_quantity);
    write_column(table_dir + "/l_extendedprice.bin", l_extendedprice);
    write_column(table_dir + "/l_discount.bin", l_discount);
    write_column(table_dir + "/l_tax.bin", l_tax);
    write_column(table_dir + "/l_returnflag.bin", l_returnflag);
    write_column(table_dir + "/l_linestatus.bin", l_linestatus);
    write_column(table_dir + "/l_shipdate.bin", l_shipdate);
    write_column(table_dir + "/l_commitdate.bin", l_commitdate);
    write_column(table_dir + "/l_receiptdate.bin", l_receiptdate);
    write_column(table_dir + "/l_shipinstruct.bin", l_shipinstruct);
    write_column(table_dir + "/l_shipmode.bin", l_shipmode);

    dict_returnflag.write(table_dir + "/l_returnflag_dict.txt");
    dict_linestatus.write(table_dir + "/l_linestatus_dict.txt");
    dict_shipinstruct.write(table_dir + "/l_shipinstruct_dict.txt");
    dict_shipmode.write(table_dir + "/l_shipmode_dict.txt");

    // Write string column separately (variable length)
    std::ofstream comment_file(table_dir + "/l_comment.bin", std::ios::binary);
    for (const auto& c : l_comment) {
        uint32_t len = c.size();
        comment_file.write(reinterpret_cast<char*>(&len), 4);
        comment_file.write(c.data(), len);
    }
    comment_file.close();

    std::cout << "  lineitem: " << row_count << " rows" << std::endl;
}

void ingest_orders(const std::string& input_path, const std::string& output_dir) {
    std::cout << "Ingesting orders..." << std::endl;

    std::vector<int32_t> o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate,
                         o_orderpriority, o_clerk, o_shippriority;
    std::vector<std::string> o_comment;

    DictionaryBuilder dict_status, dict_priority, dict_clerk;

    std::ifstream infile(input_path);
    std::string line;
    FieldBuffer fields;
    size_t row_count = 0;

    while (std::getline(infile, line)) {
        if (line.empty()) continue;
        fields.parse_line(line);

        int32_t ok, ck, tp, sp;
        std::from_chars(fields.get_field(0), fields.get_field(0) + fields.get_field_len(0), ok);
        std::from_chars(fields.get_field(1), fields.get_field(1) + fields.get_field_len(1), ck);
        std::from_chars(fields.get_field(3), fields.get_field(3) + fields.get_field_len(3), tp);
        std::from_chars(fields.get_field(7), fields.get_field(7) + fields.get_field_len(7), sp);

        o_orderkey.push_back(ok);
        o_custkey.push_back(ck);

        std::string status(fields.get_field(2), fields.get_field_len(2));
        o_orderstatus.push_back(dict_status.get_or_insert(status));

        o_totalprice.push_back(parse_decimal(fields.get_field(3), fields.get_field_len(3), 100));
        o_orderdate.push_back(parse_date(fields.get_field(4), fields.get_field_len(4)));

        std::string priority(fields.get_field(5), fields.get_field_len(5));
        o_orderpriority.push_back(dict_priority.get_or_insert(priority));

        std::string clerk(fields.get_field(6), fields.get_field_len(6));
        o_clerk.push_back(dict_clerk.get_or_insert(clerk));

        o_shippriority.push_back(sp);
        o_comment.push_back(std::string(fields.get_field(8), fields.get_field_len(8)));

        if (++row_count % 1000000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }

    std::string table_dir = output_dir + "/orders";
    fs::create_directories(table_dir);

    write_column(table_dir + "/o_orderkey.bin", o_orderkey);
    write_column(table_dir + "/o_custkey.bin", o_custkey);
    write_column(table_dir + "/o_orderstatus.bin", o_orderstatus);
    write_column(table_dir + "/o_totalprice.bin", o_totalprice);
    write_column(table_dir + "/o_orderdate.bin", o_orderdate);
    write_column(table_dir + "/o_orderpriority.bin", o_orderpriority);
    write_column(table_dir + "/o_clerk.bin", o_clerk);
    write_column(table_dir + "/o_shippriority.bin", o_shippriority);

    dict_status.write(table_dir + "/o_orderstatus_dict.txt");
    dict_priority.write(table_dir + "/o_orderpriority_dict.txt");
    dict_clerk.write(table_dir + "/o_clerk_dict.txt");

    std::ofstream comment_file(table_dir + "/o_comment.bin", std::ios::binary);
    for (const auto& c : o_comment) {
        uint32_t len = c.size();
        comment_file.write(reinterpret_cast<char*>(&len), 4);
        comment_file.write(c.data(), len);
    }
    comment_file.close();

    std::cout << "  orders: " << row_count << " rows" << std::endl;
}

void ingest_customer(const std::string& input_path, const std::string& output_dir) {
    std::cout << "Ingesting customer..." << std::endl;

    std::vector<int32_t> c_custkey, c_nationkey, c_acctbal, c_mktsegment;
    std::vector<std::string> c_name, c_address, c_phone, c_comment;

    DictionaryBuilder dict_mktsegment;

    std::ifstream infile(input_path);
    std::string line;
    FieldBuffer fields;
    size_t row_count = 0;

    while (std::getline(infile, line)) {
        if (line.empty()) continue;
        fields.parse_line(line);

        int32_t ck, nk;
        std::from_chars(fields.get_field(0), fields.get_field(0) + fields.get_field_len(0), ck);
        std::from_chars(fields.get_field(3), fields.get_field(3) + fields.get_field_len(3), nk);

        c_custkey.push_back(ck);
        c_name.push_back(std::string(fields.get_field(1), fields.get_field_len(1)));
        c_address.push_back(std::string(fields.get_field(2), fields.get_field_len(2)));
        c_nationkey.push_back(nk);
        c_phone.push_back(std::string(fields.get_field(4), fields.get_field_len(4)));
        c_acctbal.push_back(parse_decimal(fields.get_field(5), fields.get_field_len(5), 100));

        std::string mktseg(fields.get_field(6), fields.get_field_len(6));
        c_mktsegment.push_back(dict_mktsegment.get_or_insert(mktseg));

        c_comment.push_back(std::string(fields.get_field(7), fields.get_field_len(7)));

        if (++row_count % 100000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }

    std::string table_dir = output_dir + "/customer";
    fs::create_directories(table_dir);

    write_column(table_dir + "/c_custkey.bin", c_custkey);
    write_column(table_dir + "/c_nationkey.bin", c_nationkey);
    write_column(table_dir + "/c_acctbal.bin", c_acctbal);
    write_column(table_dir + "/c_mktsegment.bin", c_mktsegment);

    dict_mktsegment.write(table_dir + "/c_mktsegment_dict.txt");

    // Write variable-length string columns
    std::ofstream name_file(table_dir + "/c_name.bin", std::ios::binary);
    for (const auto& n : c_name) {
        uint32_t len = n.size();
        name_file.write(reinterpret_cast<char*>(&len), 4);
        name_file.write(n.data(), len);
    }
    name_file.close();

    std::ofstream addr_file(table_dir + "/c_address.bin", std::ios::binary);
    for (const auto& a : c_address) {
        uint32_t len = a.size();
        addr_file.write(reinterpret_cast<char*>(&len), 4);
        addr_file.write(a.data(), len);
    }
    addr_file.close();

    std::ofstream phone_file(table_dir + "/c_phone.bin", std::ios::binary);
    for (const auto& p : c_phone) {
        uint32_t len = p.size();
        phone_file.write(reinterpret_cast<char*>(&len), 4);
        phone_file.write(p.data(), len);
    }
    phone_file.close();

    std::ofstream comment_file(table_dir + "/c_comment.bin", std::ios::binary);
    for (const auto& c : c_comment) {
        uint32_t len = c.size();
        comment_file.write(reinterpret_cast<char*>(&len), 4);
        comment_file.write(c.data(), len);
    }
    comment_file.close();

    std::cout << "  customer: " << row_count << " rows" << std::endl;
}

void ingest_part(const std::string& input_path, const std::string& output_dir) {
    std::cout << "Ingesting part..." << std::endl;

    std::vector<int32_t> p_partkey, p_size, p_mfgr, p_brand, p_container, p_retailprice;
    std::vector<std::string> p_name, p_type, p_comment;

    DictionaryBuilder dict_mfgr, dict_brand, dict_container;

    std::ifstream infile(input_path);
    std::string line;
    FieldBuffer fields;
    size_t row_count = 0;

    while (std::getline(infile, line)) {
        if (line.empty()) continue;
        fields.parse_line(line);

        int32_t pk, sz;
        std::from_chars(fields.get_field(0), fields.get_field(0) + fields.get_field_len(0), pk);
        std::from_chars(fields.get_field(5), fields.get_field(5) + fields.get_field_len(5), sz);

        p_partkey.push_back(pk);
        p_name.push_back(std::string(fields.get_field(1), fields.get_field_len(1)));

        std::string mfgr(fields.get_field(2), fields.get_field_len(2));
        p_mfgr.push_back(dict_mfgr.get_or_insert(mfgr));

        std::string brand(fields.get_field(3), fields.get_field_len(3));
        p_brand.push_back(dict_brand.get_or_insert(brand));

        p_type.push_back(std::string(fields.get_field(4), fields.get_field_len(4)));
        p_size.push_back(sz);

        std::string container(fields.get_field(6), fields.get_field_len(6));
        p_container.push_back(dict_container.get_or_insert(container));

        p_retailprice.push_back(parse_decimal(fields.get_field(7), fields.get_field_len(7), 100));
        p_comment.push_back(std::string(fields.get_field(8), fields.get_field_len(8)));

        if (++row_count % 100000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }

    std::string table_dir = output_dir + "/part";
    fs::create_directories(table_dir);

    write_column(table_dir + "/p_partkey.bin", p_partkey);
    write_column(table_dir + "/p_size.bin", p_size);
    write_column(table_dir + "/p_mfgr.bin", p_mfgr);
    write_column(table_dir + "/p_brand.bin", p_brand);
    write_column(table_dir + "/p_container.bin", p_container);
    write_column(table_dir + "/p_retailprice.bin", p_retailprice);

    dict_mfgr.write(table_dir + "/p_mfgr_dict.txt");
    dict_brand.write(table_dir + "/p_brand_dict.txt");
    dict_container.write(table_dir + "/p_container_dict.txt");

    std::ofstream name_file(table_dir + "/p_name.bin", std::ios::binary);
    for (const auto& n : p_name) {
        uint32_t len = n.size();
        name_file.write(reinterpret_cast<char*>(&len), 4);
        name_file.write(n.data(), len);
    }
    name_file.close();

    std::ofstream type_file(table_dir + "/p_type.bin", std::ios::binary);
    for (const auto& t : p_type) {
        uint32_t len = t.size();
        type_file.write(reinterpret_cast<char*>(&len), 4);
        type_file.write(t.data(), len);
    }
    type_file.close();

    std::ofstream comment_file(table_dir + "/p_comment.bin", std::ios::binary);
    for (const auto& c : p_comment) {
        uint32_t len = c.size();
        comment_file.write(reinterpret_cast<char*>(&len), 4);
        comment_file.write(c.data(), len);
    }
    comment_file.close();

    std::cout << "  part: " << row_count << " rows" << std::endl;
}

void ingest_partsupp(const std::string& input_path, const std::string& output_dir) {
    std::cout << "Ingesting partsupp..." << std::endl;

    std::vector<int32_t> ps_partkey, ps_suppkey, ps_availqty, ps_supplycost;
    std::vector<std::string> ps_comment;

    std::ifstream infile(input_path);
    std::string line;
    FieldBuffer fields;
    size_t row_count = 0;

    while (std::getline(infile, line)) {
        if (line.empty()) continue;
        fields.parse_line(line);

        int32_t pk, sk, aq;
        std::from_chars(fields.get_field(0), fields.get_field(0) + fields.get_field_len(0), pk);
        std::from_chars(fields.get_field(1), fields.get_field(1) + fields.get_field_len(1), sk);
        std::from_chars(fields.get_field(2), fields.get_field(2) + fields.get_field_len(2), aq);

        ps_partkey.push_back(pk);
        ps_suppkey.push_back(sk);
        ps_availqty.push_back(aq);
        ps_supplycost.push_back(parse_decimal(fields.get_field(3), fields.get_field_len(3), 100));
        ps_comment.push_back(std::string(fields.get_field(4), fields.get_field_len(4)));

        if (++row_count % 500000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }

    std::string table_dir = output_dir + "/partsupp";
    fs::create_directories(table_dir);

    write_column(table_dir + "/ps_partkey.bin", ps_partkey);
    write_column(table_dir + "/ps_suppkey.bin", ps_suppkey);
    write_column(table_dir + "/ps_availqty.bin", ps_availqty);
    write_column(table_dir + "/ps_supplycost.bin", ps_supplycost);

    std::ofstream comment_file(table_dir + "/ps_comment.bin", std::ios::binary);
    for (const auto& c : ps_comment) {
        uint32_t len = c.size();
        comment_file.write(reinterpret_cast<char*>(&len), 4);
        comment_file.write(c.data(), len);
    }
    comment_file.close();

    std::cout << "  partsupp: " << row_count << " rows" << std::endl;
}

void ingest_supplier(const std::string& input_path, const std::string& output_dir) {
    std::cout << "Ingesting supplier..." << std::endl;

    std::vector<int32_t> s_suppkey, s_nationkey, s_acctbal;
    std::vector<std::string> s_name, s_address, s_phone, s_comment;

    std::ifstream infile(input_path);
    std::string line;
    FieldBuffer fields;
    size_t row_count = 0;

    while (std::getline(infile, line)) {
        if (line.empty()) continue;
        fields.parse_line(line);

        int32_t sk, nk;
        std::from_chars(fields.get_field(0), fields.get_field(0) + fields.get_field_len(0), sk);
        std::from_chars(fields.get_field(3), fields.get_field(3) + fields.get_field_len(3), nk);

        s_suppkey.push_back(sk);
        s_name.push_back(std::string(fields.get_field(1), fields.get_field_len(1)));
        s_address.push_back(std::string(fields.get_field(2), fields.get_field_len(2)));
        s_nationkey.push_back(nk);
        s_phone.push_back(std::string(fields.get_field(4), fields.get_field_len(4)));
        s_acctbal.push_back(parse_decimal(fields.get_field(5), fields.get_field_len(5), 100));
        s_comment.push_back(std::string(fields.get_field(6), fields.get_field_len(6)));

        if (++row_count % 10000 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }

    std::string table_dir = output_dir + "/supplier";
    fs::create_directories(table_dir);

    write_column(table_dir + "/s_suppkey.bin", s_suppkey);
    write_column(table_dir + "/s_nationkey.bin", s_nationkey);
    write_column(table_dir + "/s_acctbal.bin", s_acctbal);

    std::ofstream name_file(table_dir + "/s_name.bin", std::ios::binary);
    for (const auto& n : s_name) {
        uint32_t len = n.size();
        name_file.write(reinterpret_cast<char*>(&len), 4);
        name_file.write(n.data(), len);
    }
    name_file.close();

    std::ofstream addr_file(table_dir + "/s_address.bin", std::ios::binary);
    for (const auto& a : s_address) {
        uint32_t len = a.size();
        addr_file.write(reinterpret_cast<char*>(&len), 4);
        addr_file.write(a.data(), len);
    }
    addr_file.close();

    std::ofstream phone_file(table_dir + "/s_phone.bin", std::ios::binary);
    for (const auto& p : s_phone) {
        uint32_t len = p.size();
        phone_file.write(reinterpret_cast<char*>(&len), 4);
        phone_file.write(p.data(), len);
    }
    phone_file.close();

    std::ofstream comment_file(table_dir + "/s_comment.bin", std::ios::binary);
    for (const auto& c : s_comment) {
        uint32_t len = c.size();
        comment_file.write(reinterpret_cast<char*>(&len), 4);
        comment_file.write(c.data(), len);
    }
    comment_file.close();

    std::cout << "  supplier: " << row_count << " rows" << std::endl;
}

void ingest_nation(const std::string& input_path, const std::string& output_dir) {
    std::cout << "Ingesting nation..." << std::endl;

    std::vector<int32_t> n_nationkey, n_regionkey;
    std::vector<std::string> n_name, n_comment;

    std::ifstream infile(input_path);
    std::string line;
    FieldBuffer fields;
    size_t row_count = 0;

    while (std::getline(infile, line)) {
        if (line.empty()) continue;
        fields.parse_line(line);

        int32_t nk, rk;
        std::from_chars(fields.get_field(0), fields.get_field(0) + fields.get_field_len(0), nk);
        std::from_chars(fields.get_field(2), fields.get_field(2) + fields.get_field_len(2), rk);

        n_nationkey.push_back(nk);
        n_name.push_back(std::string(fields.get_field(1), fields.get_field_len(1)));
        n_regionkey.push_back(rk);
        n_comment.push_back(std::string(fields.get_field(3), fields.get_field_len(3)));

        if (++row_count % 10 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }

    std::string table_dir = output_dir + "/nation";
    fs::create_directories(table_dir);

    write_column(table_dir + "/n_nationkey.bin", n_nationkey);
    write_column(table_dir + "/n_regionkey.bin", n_regionkey);

    std::ofstream name_file(table_dir + "/n_name.bin", std::ios::binary);
    for (const auto& n : n_name) {
        uint32_t len = n.size();
        name_file.write(reinterpret_cast<char*>(&len), 4);
        name_file.write(n.data(), len);
    }
    name_file.close();

    std::ofstream comment_file(table_dir + "/n_comment.bin", std::ios::binary);
    for (const auto& c : n_comment) {
        uint32_t len = c.size();
        comment_file.write(reinterpret_cast<char*>(&len), 4);
        comment_file.write(c.data(), len);
    }
    comment_file.close();

    std::cout << "  nation: " << row_count << " rows" << std::endl;
}

void ingest_region(const std::string& input_path, const std::string& output_dir) {
    std::cout << "Ingesting region..." << std::endl;

    std::vector<int32_t> r_regionkey;
    std::vector<std::string> r_name, r_comment;

    std::ifstream infile(input_path);
    std::string line;
    FieldBuffer fields;
    size_t row_count = 0;

    while (std::getline(infile, line)) {
        if (line.empty()) continue;
        fields.parse_line(line);

        int32_t rk;
        std::from_chars(fields.get_field(0), fields.get_field(0) + fields.get_field_len(0), rk);

        r_regionkey.push_back(rk);
        r_name.push_back(std::string(fields.get_field(1), fields.get_field_len(1)));
        r_comment.push_back(std::string(fields.get_field(2), fields.get_field_len(2)));

        if (++row_count % 5 == 0) {
            std::cout << "  " << row_count << " rows..." << std::endl;
        }
    }

    std::string table_dir = output_dir + "/region";
    fs::create_directories(table_dir);

    write_column(table_dir + "/r_regionkey.bin", r_regionkey);

    std::ofstream name_file(table_dir + "/r_name.bin", std::ios::binary);
    for (const auto& n : r_name) {
        uint32_t len = n.size();
        name_file.write(reinterpret_cast<char*>(&len), 4);
        name_file.write(n.data(), len);
    }
    name_file.close();

    std::ofstream comment_file(table_dir + "/r_comment.bin", std::ios::binary);
    for (const auto& c : r_comment) {
        uint32_t len = c.size();
        comment_file.write(reinterpret_cast<char*>(&len), 4);
        comment_file.write(c.data(), len);
    }
    comment_file.close();

    std::cout << "  region: " << row_count << " rows" << std::endl;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string output_dir = argv[2];

    fs::create_directories(output_dir);

    // Ingest all tables
    ingest_lineitem(data_dir + "/lineitem.tbl", output_dir);
    ingest_orders(data_dir + "/orders.tbl", output_dir);
    ingest_customer(data_dir + "/customer.tbl", output_dir);
    ingest_part(data_dir + "/part.tbl", output_dir);
    ingest_partsupp(data_dir + "/partsupp.tbl", output_dir);
    ingest_supplier(data_dir + "/supplier.tbl", output_dir);
    ingest_nation(data_dir + "/nation.tbl", output_dir);
    ingest_region(data_dir + "/region.tbl", output_dir);

    std::cout << "\nIngestion complete." << std::endl;
    return 0;
}
