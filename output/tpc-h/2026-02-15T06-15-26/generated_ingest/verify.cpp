#include <iostream>
#include <fstream>
#include <vector>
#include <cstring>

int main(int argc, char* argv[]) {
    if (argc < 2) {
        std::cerr << "Usage: " << argv[0] << " <gendb_dir>" << std::endl;
        return 1;
    }

    std::string gendb_dir = argv[1];

    // Verify DATE columns: should be > 0 and < 20000 (epoch days)
    std::cout << "=== DATE Column Verification ===" << std::endl;

    auto verify_dates = [&](const std::string& col_path, const std::string& col_name) {
        std::ifstream file(col_path, std::ios::binary);
        if (!file) return;

        file.seekg(0, std::ios::end);
        size_t num_rows = file.tellg() / sizeof(int32_t);
        file.seekg(0, std::ios::beg);

        std::vector<int32_t> dates(std::min(size_t(10), num_rows));
        file.read(reinterpret_cast<char*>(dates.data()), dates.size() * sizeof(int32_t));
        file.close();

        std::cout << col_name << " (first 10 values):" << std::endl;
        for (auto d : dates) {
            if (d > 0 && d < 20000) {
                std::cout << "  " << d << " ✓" << std::endl;
            } else {
                std::cout << "  " << d << " ✗ INVALID" << std::endl;
            }
        }
    };

    verify_dates(gendb_dir + "/lineitem/l_shipdate.bin", "l_shipdate");
    verify_dates(gendb_dir + "/orders/o_orderdate.bin", "o_orderdate");

    // Verify DECIMAL columns: should be non-zero for many rows
    std::cout << "\n=== DECIMAL Column Verification ===" << std::endl;

    auto verify_decimals = [&](const std::string& col_path, const std::string& col_name) {
        std::ifstream file(col_path, std::ios::binary);
        if (!file) return;

        file.seekg(0, std::ios::end);
        size_t num_rows = file.tellg() / sizeof(int64_t);
        file.seekg(0, std::ios::beg);

        std::vector<int64_t> vals(std::min(size_t(10), num_rows));
        file.read(reinterpret_cast<char*>(vals.data()), vals.size() * sizeof(int64_t));
        file.close();

        std::cout << col_name << " (first 10 values):" << std::endl;
        for (auto v : vals) {
            if (v != 0) {
                std::cout << "  " << v << " ✓" << std::endl;
            } else {
                std::cout << "  " << v << " (zero)" << std::endl;
            }
        }
    };

    verify_decimals(gendb_dir + "/lineitem/l_quantity.bin", "l_quantity");
    verify_decimals(gendb_dir + "/lineitem/l_discount.bin", "l_discount");
    verify_decimals(gendb_dir + "/orders/o_totalprice.bin", "o_totalprice");

    std::cout << "\n=== Data files created ===" << std::endl;
    std::cout << "Binary columnar data stored in: " << gendb_dir << std::endl;
    std::cout << "Indexes stored in: " << gendb_dir << "/indexes" << std::endl;

    return 0;
}
