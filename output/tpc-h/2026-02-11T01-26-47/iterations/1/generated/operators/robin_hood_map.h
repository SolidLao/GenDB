#pragma once

#include <vector>
#include <cstdint>
#include <functional>
#include <cstring>
#include <algorithm>
#include <iterator>

namespace gendb {

// Robin Hood hash table with open addressing
// Faster than std::unordered_map due to better cache locality
// Uses linear probing with "distance from ideal bucket" (DIB) tracking
template<typename K, typename V, typename Hash = std::hash<K>>
class RobinHoodMap {
private:
    struct Entry {
        K key;
        V value;
        uint16_t dib;  // distance from ideal bucket
        bool occupied;

        Entry() : dib(0), occupied(false) {}
    };

    std::vector<Entry> entries_;
    size_t size_;
    size_t capacity_;
    Hash hasher_;

    // Power-of-2 capacity for fast modulo using bitwise AND
    static size_t next_power_of_2(size_t n) {
        if (n == 0) return 16;
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n |= n >> 32;
        return n + 1;
    }

    size_t ideal_index(const K& key) const {
        return hasher_(key) & (capacity_ - 1);
    }

    void rehash(size_t new_capacity) {
        std::vector<Entry> old_entries = std::move(entries_);
        size_t old_capacity = capacity_;

        capacity_ = new_capacity;
        entries_.clear();
        entries_.resize(capacity_);
        size_ = 0;

        for (size_t i = 0; i < old_capacity; ++i) {
            if (old_entries[i].occupied) {
                insert_internal(std::move(old_entries[i].key),
                              std::move(old_entries[i].value));
            }
        }
    }

    void insert_internal(K key, V value) {
        size_t idx = ideal_index(key);
        uint16_t dib = 0;

        while (true) {
            if (!entries_[idx].occupied) {
                entries_[idx].key = std::move(key);
                entries_[idx].value = std::move(value);
                entries_[idx].dib = dib;
                entries_[idx].occupied = true;
                size_++;
                return;
            }

            // Check if key already exists
            if (entries_[idx].key == key) {
                entries_[idx].value = std::move(value);
                return;
            }

            // Robin Hood: steal from rich (high DIB), give to poor (low DIB)
            if (dib > entries_[idx].dib) {
                std::swap(key, entries_[idx].key);
                std::swap(value, entries_[idx].value);
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
        // Use load factor of 0.875 (7/8)
        capacity_ = next_power_of_2((reserve_size * 8) / 7);
        entries_.resize(capacity_);
    }

    void reserve(size_t n) {
        if (n > capacity_ * 7 / 8) {
            size_t new_capacity = next_power_of_2((n * 8) / 7);
            if (new_capacity > capacity_) {
                rehash(new_capacity);
            }
        }
    }

    void clear() {
        entries_.clear();
        entries_.resize(capacity_);
        size_ = 0;
    }

    V& operator[](const K& key) {
        // Check if we need to rehash
        if (size_ >= capacity_ * 7 / 8) {
            rehash(capacity_ * 2);
        }

        size_t idx = ideal_index(key);
        uint16_t dib = 0;
        K insert_key = key;
        V insert_value{};
        bool inserting = true;

        while (true) {
            if (!entries_[idx].occupied) {
                entries_[idx].key = insert_key;
                entries_[idx].value = insert_value;
                entries_[idx].dib = dib;
                entries_[idx].occupied = true;
                size_++;
                return entries_[idx].value;
            }

            if (entries_[idx].key == insert_key) {
                return entries_[idx].value;
            }

            if (inserting && dib > entries_[idx].dib) {
                std::swap(insert_key, entries_[idx].key);
                std::swap(insert_value, entries_[idx].value);
                std::swap(dib, entries_[idx].dib);
            }

            idx = (idx + 1) & (capacity_ - 1);
            dib++;
        }
    }

    const V* find(const K& key) const {
        size_t idx = ideal_index(key);
        uint16_t dib = 0;

        while (entries_[idx].occupied) {
            if (entries_[idx].key == key) {
                return &entries_[idx].value;
            }

            // If our DIB exceeds the entry's DIB, key doesn't exist
            if (dib > entries_[idx].dib) {
                return nullptr;
            }

            idx = (idx + 1) & (capacity_ - 1);
            dib++;
        }

        return nullptr;
    }

    V* find(const K& key) {
        return const_cast<V*>(const_cast<const RobinHoodMap*>(this)->find(key));
    }

    bool contains(const K& key) const {
        return find(key) != nullptr;
    }

    size_t size() const { return size_; }
    bool empty() const { return size_ == 0; }

    // Iterator support for range-based for loops and STL algorithms
    class iterator {
    private:
        Entry* ptr_;
        Entry* end_;

        void advance_to_next_occupied() {
            while (ptr_ != end_ && !ptr_->occupied) {
                ++ptr_;
            }
        }

    public:
        using iterator_category = std::forward_iterator_tag;
        using value_type = std::pair<K, V>;
        using difference_type = std::ptrdiff_t;
        using pointer = value_type*;
        using reference = value_type&;

        iterator(Entry* ptr, Entry* end) : ptr_(ptr), end_(end) {
            advance_to_next_occupied();
        }

        value_type operator*() const {
            return {ptr_->key, ptr_->value};
        }

        iterator& operator++() {
            ++ptr_;
            advance_to_next_occupied();
            return *this;
        }

        iterator operator++(int) {
            iterator tmp = *this;
            ++(*this);
            return tmp;
        }

        bool operator!=(const iterator& other) const {
            return ptr_ != other.ptr_;
        }

        bool operator==(const iterator& other) const {
            return ptr_ == other.ptr_;
        }
    };

    iterator begin() {
        return iterator(entries_.data(), entries_.data() + capacity_);
    }

    iterator end() {
        return iterator(entries_.data() + capacity_, entries_.data() + capacity_);
    }

    iterator begin() const {
        return iterator(const_cast<Entry*>(entries_.data()),
                       const_cast<Entry*>(entries_.data() + capacity_));
    }

    iterator end() const {
        return iterator(const_cast<Entry*>(entries_.data() + capacity_),
                       const_cast<Entry*>(entries_.data() + capacity_));
    }
};

} // namespace gendb
