#include "storage.h"
#include <fstream>
#include <cstring>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdexcept>

namespace storage {

// Write int32_t column
void write_column_int32(const std::string& path, const std::vector<int32_t>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open file for writing: " + path);

    size_t count = data.size();
    fwrite(&count, sizeof(size_t), 1, f);
    fwrite(data.data(), sizeof(int32_t), count, f);
    fclose(f);
}

// Write int64_t column
void write_column_int64(const std::string& path, const std::vector<int64_t>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open file for writing: " + path);

    size_t count = data.size();
    fwrite(&count, sizeof(size_t), 1, f);
    fwrite(data.data(), sizeof(int64_t), count, f);
    fclose(f);
}

// Write uint8_t column
void write_column_uint8(const std::string& path, const std::vector<uint8_t>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open file for writing: " + path);

    size_t count = data.size();
    fwrite(&count, sizeof(size_t), 1, f);
    fwrite(data.data(), sizeof(uint8_t), count, f);
    fclose(f);
}

// Write string column (length-prefixed)
void write_column_string(const std::string& path, const std::vector<std::string>& data) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open file for writing: " + path);

    size_t count = data.size();
    fwrite(&count, sizeof(size_t), 1, f);

    for (const auto& s : data) {
        uint32_t len = static_cast<uint32_t>(s.size());
        fwrite(&len, sizeof(uint32_t), 1, f);
        fwrite(s.data(), 1, len, f);
    }
    fclose(f);
}

// Write dictionary
void write_dictionary(const std::string& path, const Dictionary& dict) {
    FILE* f = fopen(path.c_str(), "wb");
    if (!f) throw std::runtime_error("Cannot open dictionary file for writing: " + path);

    size_t count = dict.values.size();
    fwrite(&count, sizeof(size_t), 1, f);

    for (const auto& s : dict.values) {
        uint32_t len = static_cast<uint32_t>(s.size());
        fwrite(&len, sizeof(uint32_t), 1, f);
        fwrite(s.data(), 1, len, f);
    }
    fclose(f);
}

// Read int32_t column
std::vector<int32_t> read_column_int32(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("Cannot open file for reading: " + path);

    size_t count;
    fread(&count, sizeof(size_t), 1, f);

    std::vector<int32_t> data(count);
    fread(data.data(), sizeof(int32_t), count, f);
    fclose(f);

    return data;
}

// Read int64_t column
std::vector<int64_t> read_column_int64(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("Cannot open file for reading: " + path);

    size_t count;
    fread(&count, sizeof(size_t), 1, f);

    std::vector<int64_t> data(count);
    fread(data.data(), sizeof(int64_t), count, f);
    fclose(f);

    return data;
}

// Read uint8_t column
std::vector<uint8_t> read_column_uint8(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("Cannot open file for reading: " + path);

    size_t count;
    fread(&count, sizeof(size_t), 1, f);

    std::vector<uint8_t> data(count);
    fread(data.data(), sizeof(uint8_t), count, f);
    fclose(f);

    return data;
}

// Read string column
std::vector<std::string> read_column_string(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("Cannot open file for reading: " + path);

    size_t count;
    fread(&count, sizeof(size_t), 1, f);

    std::vector<std::string> data;
    data.reserve(count);

    for (size_t i = 0; i < count; i++) {
        uint32_t len;
        fread(&len, sizeof(uint32_t), 1, f);

        std::string s(len, '\0');
        fread(&s[0], 1, len, f);
        data.push_back(std::move(s));
    }
    fclose(f);

    return data;
}

// Read dictionary
Dictionary read_dictionary(const std::string& path) {
    FILE* f = fopen(path.c_str(), "rb");
    if (!f) throw std::runtime_error("Cannot open dictionary file for reading: " + path);

    Dictionary dict;
    size_t count;
    fread(&count, sizeof(size_t), 1, f);

    dict.values.reserve(count);

    for (size_t i = 0; i < count; i++) {
        uint32_t len;
        fread(&len, sizeof(uint32_t), 1, f);

        std::string s(len, '\0');
        fread(&s[0], 1, len, f);
        dict.values.push_back(s);
        dict.value_to_code[s] = static_cast<uint8_t>(i);
    }
    fclose(f);

    return dict;
}

// Memory-mapped column destructor
MappedColumn::~MappedColumn() {
    close();
}

// Open memory-mapped column
void MappedColumn::open(const std::string& path) {
    close();

    fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) throw std::runtime_error("Cannot open file for mmap: " + path);

    struct stat st;
    if (fstat(fd, &st) < 0) {
        ::close(fd);
        throw std::runtime_error("Cannot stat file: " + path);
    }

    size = st.st_size;
    data = mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0);
    if (data == MAP_FAILED) {
        ::close(fd);
        throw std::runtime_error("Cannot mmap file: " + path);
    }

    // Advise sequential access
    madvise(data, size, MADV_SEQUENTIAL);
}

// Close memory-mapped column
void MappedColumn::close() {
    if (data && data != MAP_FAILED) {
        munmap(data, size);
        data = nullptr;
    }
    if (fd >= 0) {
        ::close(fd);
        fd = -1;
    }
    size = 0;
}

} // namespace storage
