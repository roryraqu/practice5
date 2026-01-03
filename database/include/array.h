#pragma once

#include <initializer_list>
#include <iostream>
#include <iterator>

template <class T> class Array {
  public:
    T* head;
    size_t size;
    size_t capacity;

    Array(size_t cap = 1) : head(new T[cap]), size(0), capacity(cap) {}

    Array(const Array& other)
        : head(new T[other.capacity]), size(other.size), capacity(other.capacity) {
        for (size_t i = 0; i < size; i++) {
            head[i] = other[i];
        }
    }

    Array& operator=(const Array& other) {
        if (this != &other) {
            delete[] head;
            head = new T[other.capacity];
            size = other.size;
            capacity = other.capacity;
            for (size_t i = 0; i < size; i++) {
                head[i] = other[i];
            }
        }
        return *this;
    }

    Array(const std::initializer_list<T>& list)
        : size(list.size()), capacity(2 * list.size()), head(new T[2 * list.size()]) {
        int i = 0;
        for (auto& item : list) {
            head[i++] = item;
        }
    }

    ~Array() { delete[] head; }

    void recap() {
        capacity = (capacity == 0) ? 1 : capacity * 2;
        T* newArray = new T[capacity];
        for (size_t i = 0; i < size; i++) {
            newArray[i] = head[i];
        }
        delete[] head;
        head = newArray;
    }

    void pushBack(const T& item) {
        if (size == capacity) {
            recap();
        }
        head[size] = item;
        size++;
    }

    void deleteByValue(const T& item) {
        for (size_t i = 0; i < size; i++) {
            if (head[i] == item) {
                for (int j = i; j < size - 1; j++) {
                    head[j] = head[j + 1];
                }
                size--;
                break;
            }
        }
    }

    void deleteByIndex(int index) {
        for (size_t i = index; i < size - 1; i++) {
            head[i] = head[i + 1];
        }
        size--;
    }

    void print() { std::cout << *this; }

    int indexByValue(const T& item) const {
        int index = -1;
        for (size_t i = 0; i < size; i++) {
            if (head[i] == item) {
                index = i;
            }
        }
        return index;
    }

    T& operator[](size_t index) { return head[index]; }

    const T& operator[](size_t index) const { return head[index]; }

    T* begin() const { return head; };
    T* end() const { return head + size; }
};

template <class T> std::ostream& operator<<(std::ostream& os, const Array<T>& arr) {
    for (size_t i = 0; i < arr.size; i++) {
        os << arr[i];
        if (i != arr.size - 1) {
            os << ",";
        }
    }
    os << "\n";
    return os;
}