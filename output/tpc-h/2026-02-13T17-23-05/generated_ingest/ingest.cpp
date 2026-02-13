#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <charconv>
#include <cstring>
#include <cstdint>
#include <thread>
#include <mutex>
#include <numeric>
#include <algorithm>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sstream>

// Constants
const int BLOCK_SIZE = 150000;
const int DATE_EPOCH = 719162;  // Days from 0001-01-01 to 1970-01-01
const int NUM_THREADS = 64;
const size_t WRITE_BUFFER_SIZE = 1024 * 1024 * 2;  // 2MB per thread

struct ParsedDate {
    int32_t days_since_epoch;
};

// Fast date parser: YYYY-MM-DD -> days since 1970-01-01
ParsedDate parse_date(const char* str, size_t len) {
    if (len != 10 || str[4] != '-' || str[7] != '-') {
        return {0};
    }

    int year, month, day;
    auto res_year = std::from_chars(str, str + 4, year);
    auto res_month = std::from_chars(str + 5, str + 7, month);
    auto res_day = std::from_chars(str + 8, str + 10, day);

    if (res_year.ec != std::errc() || res_month.ec != std::errc() || res_day.ec != std::errc()) {
        return {0};
    }

    // Calculate days since 1970-01-01
    int32_t days = 0;

    // Count days from 1970 to year
    for (int y = 1970; y < year; y++) {
        days += (y % 4 == 0 && (y % 100 != 0 || y % 400 == 0)) ? 366 : 365;
    }

    // Days in months before this month
    int days_in_month[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)) {
        days_in_month[2] = 29;
    }
    for (int m = 1; m < month; m++) {
        days += days_in_month[m];
    }

    days += day - 1;  // -1 because January 1 is day 0

    return {days};
}

// Dictionary builder for low-cardinality string columns
class DictionaryBuilder {
public:
    std::vector<std::string> unique_values;
    std::unordered_map<std::string, uint8_t> value_to_code;

    uint8_t add_or_get(const std::string& value) {
        auto it = value_to_code.find(value);
        if (it != value_to_code.end()) {
            return it->second;
        }
        uint8_t code = unique_values.size();
        unique_values.push_back(value);
        value_to_code[value] = code;
        return code;
    }
};

// Column buffers for lineitem
struct ColumnBuffers {
    std::vector<int32_t> l_orderkey;
    std::vector<int32_t> l_partkey;
    std::vector<int32_t> l_suppkey;
    std::vector<int32_t> l_linenumber;
    std::vector<double> l_quantity;
    std::vector<double> l_extendedprice;
    std::vector<double> l_discount;
    std::vector<double> l_tax;
    std::vector<uint8_t> l_returnflag;
    std::vector<uint8_t> l_linestatus;
    std::vector<int32_t> l_shipdate;
    std::vector<int32_t> l_commitdate;
    std::vector<int32_t> l_receiptdate;
    std::vector<uint8_t> l_shipinstruct;
    std::vector<uint8_t> l_shipmode;
    std::vector<std::string> l_comment;

    size_t row_count() const {
        return l_orderkey.size();
    }
};

class LineitemIngester {
public:
    LineitemIngester(const std::string& input_file) : input_file_(input_file) {}

    void ingest(ColumnBuffers& buffers) {
        // mmap the input file
        int fd = open(input_file_.c_str(), O_RDONLY);
        if (fd < 0) {
            std::cerr << "Failed to open " << input_file_ << std::endl;
            return;
        }

        off_t file_size = lseek(fd, 0, SEEK_END);
        lseek(fd, 0, SEEK_SET);

        char* data = (char*)mmap(nullptr, file_size, PROT_READ, MAP_SHARED, fd, 0);
        if (data == MAP_FAILED) {
            std::cerr << "mmap failed" << std::endl;
            close(fd);
            return;
        }

        madvise(data, file_size, MADV_SEQUENTIAL);

        // Dictionaries for low-cardinality columns
        DictionaryBuilder returnflag_dict, linestatus_dict, shipinstruct_dict, shipmode_dict;

        // Parse the entire file into buffers
        const char* ptr = data;
        const char* end = data + file_size;

        while (ptr < end) {
            // Find line end
            const char* line_end = ptr;
            while (line_end < end && *line_end != '\n') line_end++;

            // Parse the line
            parse_line(ptr, line_end - ptr, buffers,
                      returnflag_dict, linestatus_dict, shipinstruct_dict, shipmode_dict);

            ptr = line_end + 1;
        }

        munmap(data, file_size);
        close(fd);
    }

private:
    std::string input_file_;

    void parse_line(const char* line, size_t len, ColumnBuffers& buffers,
                   DictionaryBuilder& returnflag_dict, DictionaryBuilder& linestatus_dict,
                   DictionaryBuilder& shipinstruct_dict, DictionaryBuilder& shipmode_dict) {
        const char* ptr = line;
        const char* end = line + len;

        // Helper lambda to extract field
        auto extract_field = [&](const char*& ptr, const char* end) -> std::pair<const char*, size_t> {
            const char* start = ptr;
            while (ptr < end && *ptr != '|') ptr++;
            size_t field_len = ptr - start;
            if (ptr < end) ptr++;  // Skip delimiter
            return {start, field_len};
        };

        // Parse fields - all 16 columns of lineitem
        auto [f1, len1] = extract_field(ptr, end);
        int32_t l_orderkey = 0;
        std::from_chars(f1, f1 + len1, l_orderkey);

        auto [f2, len2] = extract_field(ptr, end);
        int32_t l_partkey = 0;
        std::from_chars(f2, f2 + len2, l_partkey);

        auto [f3, len3] = extract_field(ptr, end);
        int32_t l_suppkey = 0;
        std::from_chars(f3, f3 + len3, l_suppkey);

        auto [f4, len4] = extract_field(ptr, end);
        int32_t l_linenumber = 0;
        std::from_chars(f4, f4 + len4, l_linenumber);

        auto [f5, len5] = extract_field(ptr, end);
        double l_quantity = 0.0;
        std::from_chars(f5, f5 + len5, l_quantity);

        auto [f6, len6] = extract_field(ptr, end);
        double l_extendedprice = 0.0;
        std::from_chars(f6, f6 + len6, l_extendedprice);

        auto [f7, len7] = extract_field(ptr, end);
        double l_discount = 0.0;
        std::from_chars(f7, f7 + len7, l_discount);

        auto [f8, len8] = extract_field(ptr, end);
        double l_tax = 0.0;
        std::from_chars(f8, f8 + len8, l_tax);

        auto [f9, len9] = extract_field(ptr, end);
        std::string l_returnflag_str(f9, len9);
        uint8_t l_returnflag_code = returnflag_dict.add_or_get(l_returnflag_str);

        auto [f10, len10] = extract_field(ptr, end);
        std::string l_linestatus_str(f10, len10);
        uint8_t l_linestatus_code = linestatus_dict.add_or_get(l_linestatus_str);

        auto [f11, len11] = extract_field(ptr, end);
        ParsedDate l_shipdate = parse_date(f11, len11);

        auto [f12, len12] = extract_field(ptr, end);
        ParsedDate l_commitdate = parse_date(f12, len12);

        auto [f13, len13] = extract_field(ptr, end);
        ParsedDate l_receiptdate = parse_date(f13, len13);

        auto [f14, len14] = extract_field(ptr, end);
        std::string l_shipinstruct_str(f14, len14);
        uint8_t l_shipinstruct_code = shipinstruct_dict.add_or_get(l_shipinstruct_str);

        auto [f15, len15] = extract_field(ptr, end);
        std::string l_shipmode_str(f15, len15);
        uint8_t l_shipmode_code = shipmode_dict.add_or_get(l_shipmode_str);

        auto [f16, len16] = extract_field(ptr, end);
        std::string l_comment(f16, len16);

        // Append to buffers
        buffers.l_orderkey.push_back(l_orderkey);
        buffers.l_partkey.push_back(l_partkey);
        buffers.l_suppkey.push_back(l_suppkey);
        buffers.l_linenumber.push_back(l_linenumber);
        buffers.l_quantity.push_back(l_quantity);
        buffers.l_extendedprice.push_back(l_extendedprice);
        buffers.l_discount.push_back(l_discount);
        buffers.l_tax.push_back(l_tax);
        buffers.l_returnflag.push_back(l_returnflag_code);
        buffers.l_linestatus.push_back(l_linestatus_code);
        buffers.l_shipdate.push_back(l_shipdate.days_since_epoch);
        buffers.l_commitdate.push_back(l_commitdate.days_since_epoch);
        buffers.l_receiptdate.push_back(l_receiptdate.days_since_epoch);
        buffers.l_shipinstruct.push_back(l_shipinstruct_code);
        buffers.l_shipmode.push_back(l_shipmode_code);
        buffers.l_comment.push_back(l_comment);
    }
};

void write_column_file(const std::string& filename, const void* data, size_t num_elements, size_t element_size) {
    std::ofstream out(filename, std::ios::binary);
    if (!out.is_open()) {
        std::cerr << "Failed to create " << filename << std::endl;
        return;
    }
    out.write((const char*)data, num_elements * element_size);
    out.close();
    std::cout << "  Wrote " << filename << " (" << (num_elements * element_size / 1024 / 1024) << " MB)" << std::endl;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: " << argv[0] << " <data_dir> <gendb_dir>" << std::endl;
        return 1;
    }

    std::string data_dir = argv[1];
    std::string gendb_dir = argv[2];

    // Create output directory
    system(("mkdir -p " + gendb_dir).c_str());

    // Ingest lineitem
    std::cout << "Starting lineitem ingestion..." << std::endl;
    ColumnBuffers lineitem_buffers;
    LineitemIngester lineitem_ingester(data_dir + "/lineitem.tbl");
    lineitem_ingester.ingest(lineitem_buffers);

    size_t lineitem_rows = lineitem_buffers.row_count();
    std::cout << "Ingested " << lineitem_rows << " lineitem rows" << std::endl;

    // Write binary column files
    std::cout << "Writing binary column files..." << std::endl;
    write_column_file(gendb_dir + "/lineitem.l_orderkey", lineitem_buffers.l_orderkey.data(),
                      lineitem_rows, sizeof(int32_t));
    write_column_file(gendb_dir + "/lineitem.l_partkey", lineitem_buffers.l_partkey.data(),
                      lineitem_rows, sizeof(int32_t));
    write_column_file(gendb_dir + "/lineitem.l_suppkey", lineitem_buffers.l_suppkey.data(),
                      lineitem_rows, sizeof(int32_t));
    write_column_file(gendb_dir + "/lineitem.l_linenumber", lineitem_buffers.l_linenumber.data(),
                      lineitem_rows, sizeof(int32_t));
    write_column_file(gendb_dir + "/lineitem.l_quantity", lineitem_buffers.l_quantity.data(),
                      lineitem_rows, sizeof(double));
    write_column_file(gendb_dir + "/lineitem.l_extendedprice", lineitem_buffers.l_extendedprice.data(),
                      lineitem_rows, sizeof(double));
    write_column_file(gendb_dir + "/lineitem.l_discount", lineitem_buffers.l_discount.data(),
                      lineitem_rows, sizeof(double));
    write_column_file(gendb_dir + "/lineitem.l_tax", lineitem_buffers.l_tax.data(),
                      lineitem_rows, sizeof(double));
    write_column_file(gendb_dir + "/lineitem.l_returnflag", lineitem_buffers.l_returnflag.data(),
                      lineitem_rows, sizeof(uint8_t));
    write_column_file(gendb_dir + "/lineitem.l_linestatus", lineitem_buffers.l_linestatus.data(),
                      lineitem_rows, sizeof(uint8_t));
    write_column_file(gendb_dir + "/lineitem.l_shipdate", lineitem_buffers.l_shipdate.data(),
                      lineitem_rows, sizeof(int32_t));
    write_column_file(gendb_dir + "/lineitem.l_commitdate", lineitem_buffers.l_commitdate.data(),
                      lineitem_rows, sizeof(int32_t));
    write_column_file(gendb_dir + "/lineitem.l_receiptdate", lineitem_buffers.l_receiptdate.data(),
                      lineitem_rows, sizeof(int32_t));
    write_column_file(gendb_dir + "/lineitem.l_shipinstruct", lineitem_buffers.l_shipinstruct.data(),
                      lineitem_rows, sizeof(uint8_t));
    write_column_file(gendb_dir + "/lineitem.l_shipmode", lineitem_buffers.l_shipmode.data(),
                      lineitem_rows, sizeof(uint8_t));

    // Write comment column separately (variable-length strings)
    std::ofstream comment_out(gendb_dir + "/lineitem.l_comment.txt");
    for (const auto& comment : lineitem_buffers.l_comment) {
        comment_out << comment << "\n";
    }
    comment_out.close();

    // Write metadata file
    std::ofstream meta_out(gendb_dir + "/metadata.txt");
    meta_out << "lineitem\n";
    meta_out << lineitem_rows << "\n";
    meta_out << "l_orderkey l_partkey l_suppkey l_linenumber l_quantity l_extendedprice l_discount l_tax "
             << "l_returnflag l_linestatus l_shipdate l_commitdate l_receiptdate l_shipinstruct l_shipmode l_comment\n";
    meta_out.close();

    std::cout << "Ingestion complete! Data written to " << gendb_dir << std::endl;

    return 0;
}
