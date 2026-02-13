#include "storage.h"
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <cstring>
#include <fstream>
#include <stdexcept>

namespace gendb {

// Dictionary implementation
uint8_t Dictionary::encode(const std::string& value) {
    auto it = code_map_.find(value);
    if (it != code_map_.end()) {
        return it->second;
    }
    uint8_t code = static_cast<uint8_t>(values_.size());
    values_.push_back(value);
    code_map_[value] = code;
    return code;
}

const std::string& Dictionary::decode(uint8_t code) const {
    return values_[code];
}

// ColumnWriter implementation
void ColumnWriter::write_int32(const std::string& path, const std::vector<int32_t>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open file: " + path);
    fwrite(data.data(), sizeof(int32_t), data.size(), f);
    fclose(f);
}

void ColumnWriter::write_int64(const std::string& path, const std::vector<int64_t>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open file: " + path);
    fwrite(data.data(), sizeof(int64_t), data.size(), f);
    fclose(f);
}

void ColumnWriter::write_uint8(const std::string& path, const std::vector<uint8_t>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open file: " + path);
    fwrite(data.data(), sizeof(uint8_t), data.size(), f);
    fclose(f);
}

void ColumnWriter::write_string(const std::string& path, const std::vector<std::string>& data) {
    std::ofstream out(path, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot open file: " + path);

    // Write count, then each string length + data
    size_t count = data.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(count));

    for (const auto& s : data) {
        size_t len = s.length();
        out.write(reinterpret_cast<const char*>(&len), sizeof(len));
        out.write(s.data(), len);
    }
}

void ColumnWriter::write_dictionary(const std::string& path, const Dictionary& dict) {
    std::ofstream out(path, std::ios::binary);
    if (!out) throw std::runtime_error("Cannot open file: " + path);

    size_t count = dict.size();
    out.write(reinterpret_cast<const char*>(&count), sizeof(count));

    for (size_t i = 0; i < count; ++i) {
        const std::string& s = dict.decode(static_cast<uint8_t>(i));
        size_t len = s.length();
        out.write(reinterpret_cast<const char*>(&len), sizeof(len));
        out.write(s.data(), len);
    }
}

// ColumnReader implementation
int32_t* ColumnReader::mmap_int32(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open file: " + path);

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        throw std::runtime_error("Cannot stat file: " + path);
    }

    count = sb.st_size / sizeof(int32_t);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        throw std::runtime_error("mmap failed for: " + path);
    }

    madvise(addr, sb.st_size, MADV_SEQUENTIAL);
    return static_cast<int32_t*>(addr);
}

int64_t* ColumnReader::mmap_int64(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open file: " + path);

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        throw std::runtime_error("Cannot stat file: " + path);
    }

    count = sb.st_size / sizeof(int64_t);
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        throw std::runtime_error("mmap failed for: " + path);
    }

    madvise(addr, sb.st_size, MADV_SEQUENTIAL);
    return static_cast<int64_t*>(addr);
}

uint8_t* ColumnReader::mmap_uint8(const std::string& path, size_t& count) {
    int fd = open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open file: " + path);

    struct stat sb;
    if (fstat(fd, &sb) < 0) {
        close(fd);
        throw std::runtime_error("Cannot stat file: " + path);
    }

    count = sb.st_size;
    void* addr = mmap(nullptr, sb.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (addr == MAP_FAILED) {
        throw std::runtime_error("mmap failed for: " + path);
    }

    madvise(addr, sb.st_size, MADV_SEQUENTIAL);
    return static_cast<uint8_t*>(addr);
}

std::vector<std::string> ColumnReader::read_strings(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Cannot open file: " + path);

    size_t count;
    in.read(reinterpret_cast<char*>(&count), sizeof(count));

    std::vector<std::string> result;
    result.reserve(count);

    for (size_t i = 0; i < count; ++i) {
        size_t len;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        std::string s(len, '\0');
        in.read(&s[0], len);
        result.push_back(std::move(s));
    }

    return result;
}

Dictionary ColumnReader::read_dictionary(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("Cannot open file: " + path);

    size_t count;
    in.read(reinterpret_cast<char*>(&count), sizeof(count));

    Dictionary dict;
    for (size_t i = 0; i < count; ++i) {
        size_t len;
        in.read(reinterpret_cast<char*>(&len), sizeof(len));
        std::string s(len, '\0');
        in.read(&s[0], len);
        dict.encode(s);
    }

    return dict;
}

void ColumnReader::unmap(void* addr, size_t length) {
    munmap(addr, length);
}

// Metadata functions
void write_metadata(const std::string& gendb_dir, const std::vector<TableMetadata>& tables) {
    std::string path = gendb_dir + "/metadata.txt";
    std::ofstream out(path);
    if (!out) throw std::runtime_error("Cannot write metadata");

    for (const auto& table : tables) {
        out << table.name << " " << table.row_count << "\n";
    }
}

std::vector<TableMetadata> read_metadata(const std::string& gendb_dir) {
    std::string path = gendb_dir + "/metadata.txt";
    std::ifstream in(path);
    if (!in) throw std::runtime_error("Cannot read metadata");

    std::vector<TableMetadata> tables;
    std::string name;
    size_t row_count;

    while (in >> name >> row_count) {
        tables.push_back({name, row_count, {}});
    }

    return tables;
}

} // namespace gendb
