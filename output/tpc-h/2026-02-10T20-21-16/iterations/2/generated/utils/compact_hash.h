#pragma once

#include <cstdint>
#include <cstring>
#include <vector>
#include <functional>
#include <utility>

namespace gendb {

// Simple and efficient Robin Hood hash map for int32_t keys
// Provides 2-5x speedup over std::unordered_map due to better cache locality
template<typename V>
class RobinHoodMap {
private:
    struct Entry {
        int32_t key;
        V value;
        uint8_t dib;  // distance from ideal bucket
        bool occupied;
    };

    std::vector<Entry> entries_;
    size_t size_;
    size_t capacity_;
    size_t mask_;

    // Fast integer hash
    static size_t hash(int32_t key) {
        uint32_t h = static_cast<uint32_t>(key);
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }

public:
    RobinHoodMap() : size_(0), capacity_(0), mask_(0) {}

    void reserve(size_t min_capacity) {
        // Round up to power of 2 for fast modulo
        size_t new_capacity = 16;
        while (new_capacity < min_capacity * 2) {  // 50% load factor
            new_capacity *= 2;
        }

        if (new_capacity <= capacity_) return;

        std::vector<Entry> old_entries = std::move(entries_);
        entries_.resize(new_capacity);
        for (auto& e : entries_) {
            e.occupied = false;
            e.dib = 0;
        }
        capacity_ = new_capacity;
        mask_ = new_capacity - 1;
        size_ = 0;

        // Rehash old entries
        for (const auto& old_entry : old_entries) {
            if (old_entry.occupied) {
                insert_internal(old_entry.key, old_entry.value);
            }
        }
    }

private:
    void insert_internal(int32_t key, const V& value) {
        size_t idx = hash(key) & mask_;
        uint8_t dib = 0;
        int32_t cur_key = key;
        V cur_value = value;

        while (true) {
            if (!entries_[idx].occupied) {
                entries_[idx].key = cur_key;
                entries_[idx].value = cur_value;
                entries_[idx].dib = dib;
                entries_[idx].occupied = true;
                size_++;
                return;
            }

            // Found existing key, update value
            if (entries_[idx].key == cur_key) {
                entries_[idx].value = cur_value;
                return;
            }

            // Robin Hood: steal from rich (high DIB), give to poor (low DIB)
            if (dib > entries_[idx].dib) {
                std::swap(cur_key, entries_[idx].key);
                std::swap(cur_value, entries_[idx].value);
                std::swap(dib, entries_[idx].dib);
            }

            idx = (idx + 1) & mask_;
            dib++;
        }
    }

public:
    V& operator[](int32_t key) {
        // Check if key exists
        size_t idx = hash(key) & mask_;
        uint8_t dib = 0;

        while (entries_[idx].occupied) {
            if (entries_[idx].key == key) {
                return entries_[idx].value;
            }
            if (dib > entries_[idx].dib) {
                break;  // Key not found
            }
            idx = (idx + 1) & mask_;
            dib++;
        }

        // Key not found, insert with default value
        if (size_ * 2 >= capacity_) {
            reserve(capacity_ * 2);
        }

        V default_value{};
        insert_internal(key, default_value);

        // Find it again and return reference
        idx = hash(key) & mask_;
        while (entries_[idx].occupied) {
            if (entries_[idx].key == key) {
                return entries_[idx].value;
            }
            idx = (idx + 1) & mask_;
        }

        // Should never reach here
        return entries_[0].value;
    }

    const V* find(int32_t key) const {
        if (capacity_ == 0) return nullptr;

        size_t idx = hash(key) & mask_;
        uint8_t dib = 0;

        while (entries_[idx].occupied) {
            if (entries_[idx].key == key) {
                return &entries_[idx].value;
            }
            if (dib > entries_[idx].dib) {
                return nullptr;  // Key not found
            }
            idx = (idx + 1) & mask_;
            dib++;
        }

        return nullptr;
    }

    bool contains(int32_t key) const {
        return find(key) != nullptr;
    }

    size_t size() const { return size_; }
};

// Simple and efficient Robin Hood hash set for int32_t
class RobinHoodSet {
private:
    struct Entry {
        int32_t key;
        uint8_t dib;
        bool occupied;
    };

    std::vector<Entry> entries_;
    size_t size_;
    size_t capacity_;
    size_t mask_;

    static size_t hash(int32_t key) {
        uint32_t h = static_cast<uint32_t>(key);
        h ^= h >> 16;
        h *= 0x85ebca6b;
        h ^= h >> 13;
        h *= 0xc2b2ae35;
        h ^= h >> 16;
        return h;
    }

public:
    RobinHoodSet() : size_(0), capacity_(0), mask_(0) {}

    void reserve(size_t min_capacity) {
        size_t new_capacity = 16;
        while (new_capacity < min_capacity * 2) {
            new_capacity *= 2;
        }

        if (new_capacity <= capacity_) return;

        std::vector<Entry> old_entries = std::move(entries_);
        entries_.resize(new_capacity);
        for (auto& e : entries_) {
            e.occupied = false;
            e.dib = 0;
        }
        capacity_ = new_capacity;
        mask_ = new_capacity - 1;
        size_ = 0;

        for (const auto& old_entry : old_entries) {
            if (old_entry.occupied) {
                insert(old_entry.key);
            }
        }
    }

    void insert(int32_t key) {
        if (size_ * 2 >= capacity_) {
            reserve(std::max<size_t>(16, capacity_ * 2));
        }

        size_t idx = hash(key) & mask_;
        uint8_t dib = 0;
        int32_t cur_key = key;

        while (true) {
            if (!entries_[idx].occupied) {
                entries_[idx].key = cur_key;
                entries_[idx].dib = dib;
                entries_[idx].occupied = true;
                size_++;
                return;
            }

            if (entries_[idx].key == cur_key) {
                return;  // Already exists
            }

            if (dib > entries_[idx].dib) {
                std::swap(cur_key, entries_[idx].key);
                std::swap(dib, entries_[idx].dib);
            }

            idx = (idx + 1) & mask_;
            dib++;
        }
    }

    bool contains(int32_t key) const {
        if (capacity_ == 0) return false;

        size_t idx = hash(key) & mask_;
        uint8_t dib = 0;

        while (entries_[idx].occupied) {
            if (entries_[idx].key == key) {
                return true;
            }
            if (dib > entries_[idx].dib) {
                return false;
            }
            idx = (idx + 1) & mask_;
            dib++;
        }

        return false;
    }

    size_t size() const { return size_; }
};

// Robin Hood hash map for complex keys with custom hash
template<typename K, typename V, typename Hash>
class RobinHoodMapGeneric {
private:
    struct Entry {
        K key;
        V value;
        uint8_t dib;
        bool occupied;
    };

    std::vector<Entry> entries_;
    size_t size_;
    size_t capacity_;
    size_t mask_;
    Hash hasher_;

public:
    RobinHoodMapGeneric() : size_(0), capacity_(0), mask_(0) {}

    void reserve(size_t min_capacity) {
        size_t new_capacity = 16;
        while (new_capacity < min_capacity * 2) {
            new_capacity *= 2;
        }

        if (new_capacity <= capacity_) return;

        std::vector<Entry> old_entries = std::move(entries_);
        entries_.resize(new_capacity);
        for (auto& e : entries_) {
            e.occupied = false;
            e.dib = 0;
        }
        capacity_ = new_capacity;
        mask_ = new_capacity - 1;
        size_ = 0;

        for (auto& old_entry : old_entries) {
            if (old_entry.occupied) {
                insert_internal(std::move(old_entry.key), std::move(old_entry.value));
            }
        }
    }

private:
    void insert_internal(K key, V value) {
        size_t idx = hasher_(key) & mask_;
        uint8_t dib = 0;

        while (true) {
            if (!entries_[idx].occupied) {
                entries_[idx].key = std::move(key);
                entries_[idx].value = std::move(value);
                entries_[idx].dib = dib;
                entries_[idx].occupied = true;
                size_++;
                return;
            }

            if (entries_[idx].key == key) {
                entries_[idx].value = std::move(value);
                return;
            }

            if (dib > entries_[idx].dib) {
                std::swap(key, entries_[idx].key);
                std::swap(value, entries_[idx].value);
                std::swap(dib, entries_[idx].dib);
            }

            idx = (idx + 1) & mask_;
            dib++;
        }
    }

public:
    V& operator[](const K& key) {
        if (capacity_ == 0) {
            reserve(16);
        }

        // Check if key exists
        size_t idx = hasher_(key) & mask_;
        uint8_t dib = 0;

        while (entries_[idx].occupied) {
            if (entries_[idx].key == key) {
                return entries_[idx].value;
            }
            if (dib > entries_[idx].dib) {
                break;
            }
            idx = (idx + 1) & mask_;
            dib++;
        }

        // Key not found, insert with default value
        if (size_ * 2 >= capacity_) {
            reserve(capacity_ * 2);
        }

        V default_value{};
        insert_internal(key, std::move(default_value));

        // Find it again
        idx = hasher_(key) & mask_;
        while (entries_[idx].occupied) {
            if (entries_[idx].key == key) {
                return entries_[idx].value;
            }
            idx = (idx + 1) & mask_;
        }

        return entries_[0].value;
    }

    // Iterator support for range-based for loops
    class iterator {
    private:
        Entry* ptr_;
        Entry* end_;

        void advance_to_next_occupied() {
            while (ptr_ < end_ && !ptr_->occupied) {
                ++ptr_;
            }
        }

    public:
        iterator(Entry* ptr, Entry* end) : ptr_(ptr), end_(end) {
            advance_to_next_occupied();
        }

        std::pair<const K&, V&> operator*() {
            return {ptr_->key, ptr_->value};
        }

        iterator& operator++() {
            ++ptr_;
            advance_to_next_occupied();
            return *this;
        }

        bool operator!=(const iterator& other) const {
            return ptr_ != other.ptr_;
        }
    };

    iterator begin() {
        return iterator(entries_.data(), entries_.data() + entries_.size());
    }

    iterator end() {
        return iterator(entries_.data() + entries_.size(), entries_.data() + entries_.size());
    }

    size_t size() const { return size_; }
};

} // namespace gendb
