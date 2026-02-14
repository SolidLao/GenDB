#include <iostream>
#include <vector>
#include <unordered_map>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <iomanip>

inline const void* mmapFile(const std::string& filename, size_t& filesize) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) return nullptr;
    struct stat sb;
    if (fstat(fd, &sb) < 0) { close(fd); return nullptr; }
    filesize = sb.st_size;
    void* ptr = mmap(nullptr, filesize, PROT_READ, MAP_SHARED, fd, 0);
    close(fd);
    if (ptr == MAP_FAILED) return nullptr;
    madvise(ptr, filesize, MADV_SEQUENTIAL);
    return ptr;
}

int main(int argc, char* argv[]) {
    std::string gendb_dir = argv[1];
    
    int32_t shipdate_cutoff = 9204;
    
    size_t lineitem_size = 0, l_shipdate_idx_size = 0;
    
    const int32_t* l_orderkey = (const int32_t*)mmapFile(gendb_dir + "/lineitem.l_orderkey.col", lineitem_size);
    const int32_t* l_shipdate_idx = (const int32_t*)mmapFile(gendb_dir + "/lineitem.l_shipdate.sorted_idx", l_shipdate_idx_size);
    
    size_t num_lineitems = lineitem_size / sizeof(int32_t);
    
    // Build map from row index to shipdate
    std::unordered_map<int32_t, int32_t> l_shipdate_map;
    size_t num_l_idx_entries = l_shipdate_idx_size / (2 * sizeof(int32_t));
    for (size_t i = 0; i < num_l_idx_entries; ++i) {
        int32_t date_val = l_shipdate_idx[2 * i];
        int32_t row_id = l_shipdate_idx[2 * i + 1];
        l_shipdate_map[row_id] = date_val;
    }
    
    std::cout << "Lineitem rows: " << num_lineitems << "\n";
    std::cout << "Shipdate index entries: " << num_l_idx_entries << "\n\n";
    
    // Check row 0
    std::cout << "Row 0:\n";
    std::cout << "  orderkey: " << l_orderkey[0] << "\n";
    std::cout << "  shipdate from map: " << (l_shipdate_map.count(0) > 0 ? l_shipdate_map[0] : -1) << "\n\n";
    
    // Count rows with shipdate > 9204
    int count_after_cutoff = 0;
    for (size_t i = 0; i < num_lineitems; ++i) {
        int32_t shipdate = l_shipdate_map.count(i) > 0 ? l_shipdate_map[i] : 0;
        if (shipdate > shipdate_cutoff) {
            count_after_cutoff++;
        }
    }
    
    std::cout << "Lineitem rows with shipdate > 9204 (1995-03-15): " << count_after_cutoff << "\n";
    std::cout << "Percentage: " << std::fixed << std::setprecision(2) 
              << (100.0 * count_after_cutoff / num_lineitems) << "%\n";
    
    return 0;
}
