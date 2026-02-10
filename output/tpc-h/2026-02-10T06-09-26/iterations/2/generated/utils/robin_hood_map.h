#pragma once

#include <vector>
#include <cstdint>
#include <cstring>
#include <functional>

namespace gendb {

// Robin Hood hash table with linear probing
// Optimized for cache locality and low memory overhead
template<typename K, typename V, typename Hash = std::hash<K>>
class RobinHoodMap {
private:
    struct Entry {
        K key;
        V value;
        uint8_t dib;      // distance from ideal bucket
        bool occupied;

        Entry() : key{}, value{}, dib(0), occupied(false) {}
    };

    std::vector<Entry> entries_;
    size_t size_;
    size_t capacity_;
    Hash hasher_;

    // Returns next power of 2 >= n
    static size_t next_power_of_2(size_t n) {
        if (n == 0) return 1;
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }

    void rehash(size_t new_capacity) {
        std::vector<Entry> old_entries = std::move(entries_);
        entries_.resize(new_capacity);
        capacity_ = new_capacity;
        size_ = 0;

        for (auto& entry : old_entries) {
            if (entry.occupied) {
                insert_internal(entry.key, std::move(entry.value));
            }
        }
    }

    void insert_internal(const K& key, V&& value) {
        size_t idx = hasher_(key) & (capacity_ - 1);
        uint8_t dib = 0;
        K current_key = key;
        V current_value = std::move(value);

        while (true) {
            if (!entries_[idx].occupied) {
                entries_[idx].key = current_key;
                entries_[idx].value = std::move(current_value);
                entries_[idx].dib = dib;
                entries_[idx].occupied = true;
                size_++;
                return;
            }

            // Robin Hood: if current entry has traveled further than existing entry, swap
            if (dib > entries_[idx].dib) {
                std::swap(current_key, entries_[idx].key);
                std::swap(current_value, entries_[idx].value);
                std::swap(dib, entries_[idx].dib);
            }

            idx = (idx + 1) & (capacity_ - 1);
            dib++;
        }
    }

public:
    RobinHoodMap() : size_(0), capacity_(16) {
        entries_.resize(capacity_);
    }

    explicit RobinHoodMap(size_t reserve_size) : size_(0) {
        // Target 75% load factor for good performance
        capacity_ = next_power_of_2((reserve_size * 4) / 3);
        if (capacity_ < 16) capacity_ = 16;
        entries_.resize(capacity_);
    }

    void reserve(size_t n) {
        size_t required_capacity = next_power_of_2((n * 4) / 3);
        if (required_capacity > capacity_) {
            rehash(required_capacity);
        }
    }

    // Insert or update
    V& operator[](const K& key) {
        // Check if we need to resize
        if (size_ * 8 > capacity_ * 7) {  // 87.5% load factor
            rehash(capacity_ * 2);
        }

        size_t idx = hasher_(key) & (capacity_ - 1);
        uint8_t dib = 0;

        // First, try to find existing key
        size_t search_idx = idx;
        uint8_t search_dib = 0;
        while (entries_[search_idx].occupied && search_dib <= entries_[search_idx].dib) {
            if (entries_[search_idx].key == key) {
                return entries_[search_idx].value;
            }
            search_idx = (search_idx + 1) & (capacity_ - 1);
            search_dib++;
        }

        // Key not found, insert new entry
        K current_key = key;
        V current_value{};
        V* result_ptr = nullptr;

        while (true) {
            if (!entries_[idx].occupied) {
                entries_[idx].key = current_key;
                entries_[idx].value = std::move(current_value);
                entries_[idx].dib = dib;
                entries_[idx].occupied = true;
                size_++;
                if (result_ptr == nullptr) {
                    result_ptr = &entries_[idx].value;
                }
                return *result_ptr;
            }

            if (dib > entries_[idx].dib) {
                std::swap(current_key, entries_[idx].key);
                std::swap(current_value, entries_[idx].value);
                std::swap(dib, entries_[idx].dib);
                if (result_ptr == nullptr) {
                    result_ptr = &entries_[idx].value;
                }
            }

            idx = (idx + 1) & (capacity_ - 1);
            dib++;
        }
    }

    // Find (returns pointer to value, or nullptr if not found)
    V* find(const K& key) {
        size_t idx = hasher_(key) & (capacity_ - 1);
        uint8_t dib = 0;

        while (entries_[idx].occupied && dib <= entries_[idx].dib) {
            if (entries_[idx].key == key) {
                return &entries_[idx].value;
            }
            idx = (idx + 1) & (capacity_ - 1);
            dib++;
        }

        return nullptr;
    }

    const V* find(const K& key) const {
        size_t idx = hasher_(key) & (capacity_ - 1);
        uint8_t dib = 0;

        while (entries_[idx].occupied && dib <= entries_[idx].dib) {
            if (entries_[idx].key == key) {
                return &entries_[idx].value;
            }
            idx = (idx + 1) & (capacity_ - 1);
            dib++;
        }

        return nullptr;
    }

    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    // Iterator support for range-based for loops
    class iterator {
        Entry* ptr_;
        Entry* end_;

        void advance() {
            while (ptr_ != end_ && !ptr_->occupied) {
                ++ptr_;
            }
        }

    public:
        iterator(Entry* ptr, Entry* end) : ptr_(ptr), end_(end) {
            advance();
        }

        std::pair<const K&, V&> operator*() {
            return {ptr_->key, ptr_->value};
        }

        iterator& operator++() {
            ++ptr_;
            advance();
            return *this;
        }

        bool operator!=(const iterator& other) const {
            return ptr_ != other.ptr_;
        }
    };

    iterator begin() {
        return iterator(entries_.data(), entries_.data() + capacity_);
    }

    iterator end() {
        return iterator(entries_.data() + capacity_, entries_.data() + capacity_);
    }
};

} // namespace gendb
