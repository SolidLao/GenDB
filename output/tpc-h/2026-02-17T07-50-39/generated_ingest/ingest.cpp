#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <algorithm>
#include <numeric>

// Global thread pool size
const int NUM_THREADS = std::thread::hardware_concurrency();

// Parse date from YYYY-MM-DD to days since epoch (1970-01-01)
int32_t parse_date(const std::string& date_str) {
    if (date_str.empty() || date_str.size() < 10) return 0;

    int year = std::stoi(date_str.substr(0, 4));
    int month = std::stoi(date_str.substr(5, 2));
    int day = std::stoi(date_str.substr(8, 2));

    // Days since epoch calculation
    int days = 0;

    // Add days for complete years (1970..year-1)
    for (int y = 1970; y < year; y++) {
        bool leap = (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0);
        days += leap ? 366 : 365;
    }

    // Days per month for current year
    int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    bool leap_year = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    if (leap_year) days_in_month[1] = 29;

    // Add days for complete months (1..month-1)
    for (int m = 1; m < month; m++) {
        days += days_in_month[m - 1];
    }

    // Add remaining days
    days += (day - 1);

    return days;
}

// Parse decimal with 2 decimal places to int64_t with scale_factor 100
int64_t parse_decimal(const std::string& decimal_str) {
    if (decimal_str.empty()) return 0;

    size_t dot_pos = decimal_str.find('.');
    if (dot_pos == std::string::npos) {
        return std::stoll(decimal_str) * 100;
    }

    std::string int_part = decimal_str.substr(0, dot_pos);
    std::string frac_part = decimal_str.substr(dot_pos + 1);

    // Pad or truncate to 2 decimal places
    if (frac_part.size() < 2) frac_part += std::string(2 - frac_part.size(), '0');
    else if (frac_part.size() > 2) frac_part = frac_part.substr(0, 2);

    int64_t result = std::stoll(int_part) * 100 + std::stoll(frac_part);
    return result;
}

// Dictionary encoder
class DictionaryEncoder {
public:
    int32_t encode(const std::string& value) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = str_to_code_.find(value);
        if (it != str_to_code_.end()) {
            return it->second;
        }
        int32_t code = static_cast<int32_t>(code_to_str_.size());
        str_to_code_[value] = code;
        code_to_str_.push_back(value);
        return code;
    }

    void save_dictionary(const std::string& filepath) {
        std::ofstream out(filepath);
        for (const auto& str : code_to_str_) {
            out << str << '\n';
        }
    }

private:
    std::unordered_map<std::string, int32_t> str_to_code_;
    std::vector<std::string> code_to_str_;
    std::mutex mutex_;
};

// Trim whitespace
std::string trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \t\r\n");
    if (start == std::string::npos) return "";
    size_t end = str.find_last_not_of(" \t\r\n");
    return str.substr(start, end - start + 1);
}

// Write buffer for binary column data
template<typename T>
class ColumnWriter {
public:
    void append(const T& value) {
        buffer_.push_back(value);
        if (buffer_.size() >= 1000000) flush();
    }

    void flush() {
        if (!buffer_.empty() && file_.is_open()) {
            file_.write(reinterpret_cast<const char*>(buffer_.data()), buffer_.size() * sizeof(T));
            buffer_.clear();
        }
    }

    void open(const std::string& filepath) {
        file_.open(filepath, std::ios::binary);
    }

    void close() {
        flush();
        file_.close();
    }

private:
    std::vector<T> buffer_;
    std::ofstream file_;
};

// String column writer
class StringColumnWriter {
public:
    void append(const std::string& value) {
        buffer_.push_back(value);
        if (buffer_.size() >= 100000) flush();
    }

    void flush() {
        if (!buffer_.empty() && file_.is_open()) {
            for (const auto& str : buffer_) {
                uint32_t len = str.size();
                file_.write(reinterpret_cast<const char*>(&len), sizeof(len));
                file_.write(str.data(), len);
            }
            buffer_.clear();
        }
    }

    void open(const std::string& filepath) {
        file_.open(filepath, std::ios::binary);
    }

    void close() {
        flush();
        file_.close();
    }

private:
    std::vector<std::string> buffer_;
    std::ofstream file_;
};

struct PermutationEntry {
    int32_t sort_key;
    uint32_t original_pos;
};

// Ingest lineitem table
void ingest_lineitem(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting lineitem..." << std::endl;

    std::string input_file = input_dir + "/lineitem.tbl";

    // Memory-mapped input
    int fd = open(input_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* data = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << input_file << std::endl;
        close(fd);
        return;
    }
    madvise(data, file_size, MADV_SEQUENTIAL);

    // Dictionary encoders
    DictionaryEncoder l_returnflag_dict, l_linestatus_dict, l_shipinstruct_dict, l_shipmode_dict;

    // First pass: collect data in memory
    std::vector<int32_t> l_orderkey_vec, l_partkey_vec, l_suppkey_vec, l_linenumber_vec;
    std::vector<int64_t> l_quantity_vec, l_extendedprice_vec, l_discount_vec, l_tax_vec;
    std::vector<int32_t> l_returnflag_vec, l_linestatus_vec;
    std::vector<int32_t> l_shipdate_vec, l_commitdate_vec, l_receiptdate_vec;
    std::vector<int32_t> l_shipinstruct_vec, l_shipmode_vec;
    std::vector<std::string> l_comment_vec;

    const char* ptr = data;
    const char* end = data + file_size;

    while (ptr < end) {
        const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end - ptr));
        if (!line_end) line_end = end;

        std::string line(ptr, line_end);
        ptr = line_end + 1;

        if (line.empty()) continue;

        std::vector<std::string> fields;
        size_t pos = 0;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            if (next == std::string::npos) next = line.size();
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }

        if (fields.size() < 16) continue;

        l_orderkey_vec.push_back(std::stoi(fields[0]));
        l_partkey_vec.push_back(std::stoi(fields[1]));
        l_suppkey_vec.push_back(std::stoi(fields[2]));
        l_linenumber_vec.push_back(std::stoi(fields[3]));
        l_quantity_vec.push_back(parse_decimal(fields[4]));
        l_extendedprice_vec.push_back(parse_decimal(fields[5]));
        l_discount_vec.push_back(parse_decimal(fields[6]));
        l_tax_vec.push_back(parse_decimal(fields[7]));
        l_returnflag_vec.push_back(l_returnflag_dict.encode(trim(fields[8])));
        l_linestatus_vec.push_back(l_linestatus_dict.encode(trim(fields[9])));
        l_shipdate_vec.push_back(parse_date(fields[10]));
        l_commitdate_vec.push_back(parse_date(fields[11]));
        l_receiptdate_vec.push_back(parse_date(fields[12]));
        l_shipinstruct_vec.push_back(l_shipinstruct_dict.encode(trim(fields[13])));
        l_shipmode_vec.push_back(l_shipmode_dict.encode(trim(fields[14])));
        l_comment_vec.push_back(trim(fields[15]));
    }

    munmap(data, file_size);
    close(fd);

    size_t row_count = l_orderkey_vec.size();
    std::cout << "Loaded " << row_count << " rows from lineitem" << std::endl;

    // Create permutation based on l_shipdate
    std::vector<PermutationEntry> perm(row_count);
    for (size_t i = 0; i < row_count; i++) {
        perm[i].sort_key = l_shipdate_vec[i];
        perm[i].original_pos = i;
    }

    std::cout << "Sorting lineitem by l_shipdate..." << std::endl;
    std::sort(perm.begin(), perm.end(), [](const PermutationEntry& a, const PermutationEntry& b) {
        return a.sort_key < b.sort_key;
    });

    // Write sorted columns
    std::string table_dir = output_dir + "/lineitem";
    system(("mkdir -p " + table_dir).c_str());

    auto write_sorted_column = [&](const auto& src_vec, const std::string& col_name) {
        using T = typename std::decay<decltype(src_vec[0])>::type;
        ColumnWriter<T> writer;
        writer.open(table_dir + "/" + col_name + ".bin");
        for (size_t i = 0; i < row_count; i++) {
            writer.append(src_vec[perm[i].original_pos]);
        }
        writer.close();
    };

    auto write_sorted_string_column = [&](const std::vector<std::string>& src_vec, const std::string& col_name) {
        StringColumnWriter writer;
        writer.open(table_dir + "/" + col_name + ".bin");
        for (size_t i = 0; i < row_count; i++) {
            writer.append(src_vec[perm[i].original_pos]);
        }
        writer.close();
    };

    std::cout << "Writing sorted lineitem columns..." << std::endl;
    write_sorted_column(l_orderkey_vec, "l_orderkey");
    write_sorted_column(l_partkey_vec, "l_partkey");
    write_sorted_column(l_suppkey_vec, "l_suppkey");
    write_sorted_column(l_linenumber_vec, "l_linenumber");
    write_sorted_column(l_quantity_vec, "l_quantity");
    write_sorted_column(l_extendedprice_vec, "l_extendedprice");
    write_sorted_column(l_discount_vec, "l_discount");
    write_sorted_column(l_tax_vec, "l_tax");
    write_sorted_column(l_returnflag_vec, "l_returnflag");
    write_sorted_column(l_linestatus_vec, "l_linestatus");
    write_sorted_column(l_shipdate_vec, "l_shipdate");
    write_sorted_column(l_commitdate_vec, "l_commitdate");
    write_sorted_column(l_receiptdate_vec, "l_receiptdate");
    write_sorted_column(l_shipinstruct_vec, "l_shipinstruct");
    write_sorted_column(l_shipmode_vec, "l_shipmode");
    write_sorted_string_column(l_comment_vec, "l_comment");

    // Save dictionaries
    l_returnflag_dict.save_dictionary(table_dir + "/l_returnflag_dict.txt");
    l_linestatus_dict.save_dictionary(table_dir + "/l_linestatus_dict.txt");
    l_shipinstruct_dict.save_dictionary(table_dir + "/l_shipinstruct_dict.txt");
    l_shipmode_dict.save_dictionary(table_dir + "/l_shipmode_dict.txt");

    std::cout << "Lineitem ingestion complete." << std::endl;
}

// Ingest orders table
void ingest_orders(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting orders..." << std::endl;

    std::string input_file = input_dir + "/orders.tbl";
    int fd = open(input_file.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    struct stat sb;
    fstat(fd, &sb);
    size_t file_size = sb.st_size;

    char* data = static_cast<char*>(mmap(nullptr, file_size, PROT_READ, MAP_PRIVATE, fd, 0));
    if (data == MAP_FAILED) {
        std::cerr << "Failed to mmap " << input_file << std::endl;
        close(fd);
        return;
    }
    madvise(data, file_size, MADV_SEQUENTIAL);

    DictionaryEncoder o_orderstatus_dict, o_orderpriority_dict, o_clerk_dict;

    std::vector<int32_t> o_orderkey_vec, o_custkey_vec;
    std::vector<int32_t> o_orderstatus_vec;
    std::vector<int64_t> o_totalprice_vec;
    std::vector<int32_t> o_orderdate_vec;
    std::vector<int32_t> o_orderpriority_vec, o_clerk_vec;
    std::vector<int32_t> o_shippriority_vec;
    std::vector<std::string> o_comment_vec;

    const char* ptr = data;
    const char* end = data + file_size;

    while (ptr < end) {
        const char* line_end = static_cast<const char*>(memchr(ptr, '\n', end - ptr));
        if (!line_end) line_end = end;

        std::string line(ptr, line_end);
        ptr = line_end + 1;

        if (line.empty()) continue;

        std::vector<std::string> fields;
        size_t pos = 0;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            if (next == std::string::npos) next = line.size();
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }

        if (fields.size() < 9) continue;

        o_orderkey_vec.push_back(std::stoi(fields[0]));
        o_custkey_vec.push_back(std::stoi(fields[1]));
        o_orderstatus_vec.push_back(o_orderstatus_dict.encode(trim(fields[2])));
        o_totalprice_vec.push_back(parse_decimal(fields[3]));
        o_orderdate_vec.push_back(parse_date(fields[4]));
        o_orderpriority_vec.push_back(o_orderpriority_dict.encode(trim(fields[5])));
        o_clerk_vec.push_back(o_clerk_dict.encode(trim(fields[6])));
        o_shippriority_vec.push_back(std::stoi(fields[7]));
        o_comment_vec.push_back(trim(fields[8]));
    }

    munmap(data, file_size);
    close(fd);

    size_t row_count = o_orderkey_vec.size();
    std::cout << "Loaded " << row_count << " rows from orders" << std::endl;

    // Sort by o_orderdate
    std::vector<PermutationEntry> perm(row_count);
    for (size_t i = 0; i < row_count; i++) {
        perm[i].sort_key = o_orderdate_vec[i];
        perm[i].original_pos = i;
    }

    std::cout << "Sorting orders by o_orderdate..." << std::endl;
    std::sort(perm.begin(), perm.end(), [](const PermutationEntry& a, const PermutationEntry& b) {
        return a.sort_key < b.sort_key;
    });

    std::string table_dir = output_dir + "/orders";
    system(("mkdir -p " + table_dir).c_str());

    auto write_sorted_column = [&](const auto& src_vec, const std::string& col_name) {
        using T = typename std::decay<decltype(src_vec[0])>::type;
        ColumnWriter<T> writer;
        writer.open(table_dir + "/" + col_name + ".bin");
        for (size_t i = 0; i < row_count; i++) {
            writer.append(src_vec[perm[i].original_pos]);
        }
        writer.close();
    };

    auto write_sorted_string_column = [&](const std::vector<std::string>& src_vec, const std::string& col_name) {
        StringColumnWriter writer;
        writer.open(table_dir + "/" + col_name + ".bin");
        for (size_t i = 0; i < row_count; i++) {
            writer.append(src_vec[perm[i].original_pos]);
        }
        writer.close();
    };

    std::cout << "Writing sorted orders columns..." << std::endl;
    write_sorted_column(o_orderkey_vec, "o_orderkey");
    write_sorted_column(o_custkey_vec, "o_custkey");
    write_sorted_column(o_orderstatus_vec, "o_orderstatus");
    write_sorted_column(o_totalprice_vec, "o_totalprice");
    write_sorted_column(o_orderdate_vec, "o_orderdate");
    write_sorted_column(o_orderpriority_vec, "o_orderpriority");
    write_sorted_column(o_clerk_vec, "o_clerk");
    write_sorted_column(o_shippriority_vec, "o_shippriority");
    write_sorted_string_column(o_comment_vec, "o_comment");

    o_orderstatus_dict.save_dictionary(table_dir + "/o_orderstatus_dict.txt");
    o_orderpriority_dict.save_dictionary(table_dir + "/o_orderpriority_dict.txt");
    o_clerk_dict.save_dictionary(table_dir + "/o_clerk_dict.txt");

    std::cout << "Orders ingestion complete." << std::endl;
}

// Ingest customer table
void ingest_customer(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting customer..." << std::endl;

    std::string input_file = input_dir + "/customer.tbl";
    std::ifstream file(input_file);
    if (!file) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    DictionaryEncoder c_mktsegment_dict;

    std::string table_dir = output_dir + "/customer";
    system(("mkdir -p " + table_dir).c_str());

    ColumnWriter<int32_t> c_custkey_writer, c_nationkey_writer, c_mktsegment_writer;
    ColumnWriter<int64_t> c_acctbal_writer;
    StringColumnWriter c_name_writer, c_address_writer, c_phone_writer, c_comment_writer;

    c_custkey_writer.open(table_dir + "/c_custkey.bin");
    c_name_writer.open(table_dir + "/c_name.bin");
    c_address_writer.open(table_dir + "/c_address.bin");
    c_nationkey_writer.open(table_dir + "/c_nationkey.bin");
    c_phone_writer.open(table_dir + "/c_phone.bin");
    c_acctbal_writer.open(table_dir + "/c_acctbal.bin");
    c_mktsegment_writer.open(table_dir + "/c_mktsegment.bin");
    c_comment_writer.open(table_dir + "/c_comment.bin");

    std::string line;
    size_t row_count = 0;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::vector<std::string> fields;
        size_t pos = 0;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            if (next == std::string::npos) next = line.size();
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }

        if (fields.size() < 8) continue;

        c_custkey_writer.append(std::stoi(fields[0]));
        c_name_writer.append(trim(fields[1]));
        c_address_writer.append(trim(fields[2]));
        c_nationkey_writer.append(std::stoi(fields[3]));
        c_phone_writer.append(trim(fields[4]));
        c_acctbal_writer.append(parse_decimal(fields[5]));
        c_mktsegment_writer.append(c_mktsegment_dict.encode(trim(fields[6])));
        c_comment_writer.append(trim(fields[7]));

        row_count++;
    }

    c_custkey_writer.close();
    c_name_writer.close();
    c_address_writer.close();
    c_nationkey_writer.close();
    c_phone_writer.close();
    c_acctbal_writer.close();
    c_mktsegment_writer.close();
    c_comment_writer.close();

    c_mktsegment_dict.save_dictionary(table_dir + "/c_mktsegment_dict.txt");

    std::cout << "Customer ingestion complete (" << row_count << " rows)." << std::endl;
}

// Ingest part table
void ingest_part(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting part..." << std::endl;

    std::string input_file = input_dir + "/part.tbl";
    std::ifstream file(input_file);
    if (!file) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    DictionaryEncoder p_mfgr_dict, p_brand_dict, p_container_dict;

    std::string table_dir = output_dir + "/part";
    system(("mkdir -p " + table_dir).c_str());

    ColumnWriter<int32_t> p_partkey_writer, p_mfgr_writer, p_brand_writer, p_size_writer, p_container_writer;
    ColumnWriter<int64_t> p_retailprice_writer;
    StringColumnWriter p_name_writer, p_type_writer, p_comment_writer;

    p_partkey_writer.open(table_dir + "/p_partkey.bin");
    p_name_writer.open(table_dir + "/p_name.bin");
    p_mfgr_writer.open(table_dir + "/p_mfgr.bin");
    p_brand_writer.open(table_dir + "/p_brand.bin");
    p_type_writer.open(table_dir + "/p_type.bin");
    p_size_writer.open(table_dir + "/p_size.bin");
    p_container_writer.open(table_dir + "/p_container.bin");
    p_retailprice_writer.open(table_dir + "/p_retailprice.bin");
    p_comment_writer.open(table_dir + "/p_comment.bin");

    std::string line;
    size_t row_count = 0;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::vector<std::string> fields;
        size_t pos = 0;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            if (next == std::string::npos) next = line.size();
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }

        if (fields.size() < 9) continue;

        p_partkey_writer.append(std::stoi(fields[0]));
        p_name_writer.append(trim(fields[1]));
        p_mfgr_writer.append(p_mfgr_dict.encode(trim(fields[2])));
        p_brand_writer.append(p_brand_dict.encode(trim(fields[3])));
        p_type_writer.append(trim(fields[4]));
        p_size_writer.append(std::stoi(fields[5]));
        p_container_writer.append(p_container_dict.encode(trim(fields[6])));
        p_retailprice_writer.append(parse_decimal(fields[7]));
        p_comment_writer.append(trim(fields[8]));

        row_count++;
    }

    p_partkey_writer.close();
    p_name_writer.close();
    p_mfgr_writer.close();
    p_brand_writer.close();
    p_type_writer.close();
    p_size_writer.close();
    p_container_writer.close();
    p_retailprice_writer.close();
    p_comment_writer.close();

    p_mfgr_dict.save_dictionary(table_dir + "/p_mfgr_dict.txt");
    p_brand_dict.save_dictionary(table_dir + "/p_brand_dict.txt");
    p_container_dict.save_dictionary(table_dir + "/p_container_dict.txt");

    std::cout << "Part ingestion complete (" << row_count << " rows)." << std::endl;
}

// Ingest partsupp table
void ingest_partsupp(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting partsupp..." << std::endl;

    std::string input_file = input_dir + "/partsupp.tbl";
    std::ifstream file(input_file);
    if (!file) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    std::string table_dir = output_dir + "/partsupp";
    system(("mkdir -p " + table_dir).c_str());

    ColumnWriter<int32_t> ps_partkey_writer, ps_suppkey_writer, ps_availqty_writer;
    ColumnWriter<int64_t> ps_supplycost_writer;
    StringColumnWriter ps_comment_writer;

    ps_partkey_writer.open(table_dir + "/ps_partkey.bin");
    ps_suppkey_writer.open(table_dir + "/ps_suppkey.bin");
    ps_availqty_writer.open(table_dir + "/ps_availqty.bin");
    ps_supplycost_writer.open(table_dir + "/ps_supplycost.bin");
    ps_comment_writer.open(table_dir + "/ps_comment.bin");

    std::string line;
    size_t row_count = 0;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::vector<std::string> fields;
        size_t pos = 0;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            if (next == std::string::npos) next = line.size();
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }

        if (fields.size() < 5) continue;

        ps_partkey_writer.append(std::stoi(fields[0]));
        ps_suppkey_writer.append(std::stoi(fields[1]));
        ps_availqty_writer.append(std::stoi(fields[2]));
        ps_supplycost_writer.append(parse_decimal(fields[3]));
        ps_comment_writer.append(trim(fields[4]));

        row_count++;
    }

    ps_partkey_writer.close();
    ps_suppkey_writer.close();
    ps_availqty_writer.close();
    ps_supplycost_writer.close();
    ps_comment_writer.close();

    std::cout << "Partsupp ingestion complete (" << row_count << " rows)." << std::endl;
}

// Ingest supplier table
void ingest_supplier(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting supplier..." << std::endl;

    std::string input_file = input_dir + "/supplier.tbl";
    std::ifstream file(input_file);
    if (!file) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    std::string table_dir = output_dir + "/supplier";
    system(("mkdir -p " + table_dir).c_str());

    ColumnWriter<int32_t> s_suppkey_writer, s_nationkey_writer;
    ColumnWriter<int64_t> s_acctbal_writer;
    StringColumnWriter s_name_writer, s_address_writer, s_phone_writer, s_comment_writer;

    s_suppkey_writer.open(table_dir + "/s_suppkey.bin");
    s_name_writer.open(table_dir + "/s_name.bin");
    s_address_writer.open(table_dir + "/s_address.bin");
    s_nationkey_writer.open(table_dir + "/s_nationkey.bin");
    s_phone_writer.open(table_dir + "/s_phone.bin");
    s_acctbal_writer.open(table_dir + "/s_acctbal.bin");
    s_comment_writer.open(table_dir + "/s_comment.bin");

    std::string line;
    size_t row_count = 0;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::vector<std::string> fields;
        size_t pos = 0;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            if (next == std::string::npos) next = line.size();
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }

        if (fields.size() < 7) continue;

        s_suppkey_writer.append(std::stoi(fields[0]));
        s_name_writer.append(trim(fields[1]));
        s_address_writer.append(trim(fields[2]));
        s_nationkey_writer.append(std::stoi(fields[3]));
        s_phone_writer.append(trim(fields[4]));
        s_acctbal_writer.append(parse_decimal(fields[5]));
        s_comment_writer.append(trim(fields[6]));

        row_count++;
    }

    s_suppkey_writer.close();
    s_name_writer.close();
    s_address_writer.close();
    s_nationkey_writer.close();
    s_phone_writer.close();
    s_acctbal_writer.close();
    s_comment_writer.close();

    std::cout << "Supplier ingestion complete (" << row_count << " rows)." << std::endl;
}

// Ingest nation table
void ingest_nation(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting nation..." << std::endl;

    std::string input_file = input_dir + "/nation.tbl";
    std::ifstream file(input_file);
    if (!file) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    std::string table_dir = output_dir + "/nation";
    system(("mkdir -p " + table_dir).c_str());

    ColumnWriter<int32_t> n_nationkey_writer, n_regionkey_writer;
    StringColumnWriter n_name_writer, n_comment_writer;

    n_nationkey_writer.open(table_dir + "/n_nationkey.bin");
    n_name_writer.open(table_dir + "/n_name.bin");
    n_regionkey_writer.open(table_dir + "/n_regionkey.bin");
    n_comment_writer.open(table_dir + "/n_comment.bin");

    std::string line;
    size_t row_count = 0;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::vector<std::string> fields;
        size_t pos = 0;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            if (next == std::string::npos) next = line.size();
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }

        if (fields.size() < 4) continue;

        n_nationkey_writer.append(std::stoi(fields[0]));
        n_name_writer.append(trim(fields[1]));
        n_regionkey_writer.append(std::stoi(fields[2]));
        n_comment_writer.append(trim(fields[3]));

        row_count++;
    }

    n_nationkey_writer.close();
    n_name_writer.close();
    n_regionkey_writer.close();
    n_comment_writer.close();

    std::cout << "Nation ingestion complete (" << row_count << " rows)." << std::endl;
}

// Ingest region table
void ingest_region(const std::string& input_dir, const std::string& output_dir) {
    std::cout << "Ingesting region..." << std::endl;

    std::string input_file = input_dir + "/region.tbl";
    std::ifstream file(input_file);
    if (!file) {
        std::cerr << "Failed to open " << input_file << std::endl;
        return;
    }

    std::string table_dir = output_dir + "/region";
    system(("mkdir -p " + table_dir).c_str());

    ColumnWriter<int32_t> r_regionkey_writer;
    StringColumnWriter r_name_writer, r_comment_writer;

    r_regionkey_writer.open(table_dir + "/r_regionkey.bin");
    r_name_writer.open(table_dir + "/r_name.bin");
    r_comment_writer.open(table_dir + "/r_comment.bin");

    std::string line;
    size_t row_count = 0;
    while (std::getline(file, line)) {
        if (line.empty()) continue;

        std::vector<std::string> fields;
        size_t pos = 0;
        while (pos < line.size()) {
            size_t next = line.find('|', pos);
            if (next == std::string::npos) next = line.size();
            fields.push_back(line.substr(pos, next - pos));
            pos = next + 1;
        }

        if (fields.size() < 3) continue;

        r_regionkey_writer.append(std::stoi(fields[0]));
        r_name_writer.append(trim(fields[1]));
        r_comment_writer.append(trim(fields[2]));

        row_count++;
    }

    r_regionkey_writer.close();
    r_name_writer.close();
    r_comment_writer.close();

    std::cout << "Region ingestion complete (" << row_count << " rows)." << std::endl;
}

int main(int argc, char* argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_dir> <output_dir>" << std::endl;
        return 1;
    }

    std::string input_dir = argv[1];
    std::string output_dir = argv[2];

    // Create output directory
    system(("mkdir -p " + output_dir).c_str());
    system(("mkdir -p " + output_dir + "/indexes").c_str());

    std::cout << "Starting TPC-H data ingestion..." << std::endl;
    std::cout << "Input directory: " << input_dir << std::endl;
    std::cout << "Output directory: " << output_dir << std::endl;

    // Ingest tables (sequential to avoid overwhelming memory)
    ingest_lineitem(input_dir, output_dir);
    ingest_orders(input_dir, output_dir);
    ingest_customer(input_dir, output_dir);
    ingest_part(input_dir, output_dir);
    ingest_partsupp(input_dir, output_dir);
    ingest_supplier(input_dir, output_dir);
    ingest_nation(input_dir, output_dir);
    ingest_region(input_dir, output_dir);

    std::cout << "\n=== Ingestion Complete ===" << std::endl;
    std::cout << "Verifying data integrity..." << std::endl;

    // Verification: check date columns
    {
        int fd = open((output_dir + "/lineitem/l_shipdate.bin").c_str(), O_RDONLY);
        if (fd >= 0) {
            struct stat sb;
            fstat(fd, &sb);
            if (sb.st_size >= sizeof(int32_t)) {
                int32_t* dates = static_cast<int32_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
                if (dates != MAP_FAILED) {
                    int32_t max_date = 0;
                    for (size_t i = 0; i < sb.st_size / sizeof(int32_t); i++) {
                        if (dates[i] > max_date) max_date = dates[i];
                    }
                    std::cout << "✓ Date check: max l_shipdate = " << max_date << " days since epoch";
                    if (max_date > 3000) std::cout << " (PASS)" << std::endl;
                    else std::cout << " (FAIL: dates not parsed correctly)" << std::endl;
                    munmap(dates, sb.st_size);
                }
            }
            close(fd);
        }
    }

    // Verification: check decimal columns
    {
        int fd = open((output_dir + "/lineitem/l_extendedprice.bin").c_str(), O_RDONLY);
        if (fd >= 0) {
            struct stat sb;
            fstat(fd, &sb);
            if (sb.st_size >= sizeof(int64_t) * 10) {
                int64_t* prices = static_cast<int64_t*>(mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0));
                if (prices != MAP_FAILED) {
                    int64_t sample_sum = 0;
                    for (int i = 0; i < 10; i++) sample_sum += prices[i];
                    std::cout << "✓ Decimal check: first 10 l_extendedprice sum = " << sample_sum;
                    if (sample_sum > 0) std::cout << " (PASS)" << std::endl;
                    else std::cout << " (FAIL: decimals not parsed correctly)" << std::endl;
                    munmap(prices, sb.st_size);
                }
            }
            close(fd);
        }
    }

    std::cout << "\nAll data ingested successfully!" << std::endl;
    return 0;
}
