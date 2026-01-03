#pragma once

#include "array.h"
#include "nlohmann/json.hpp"
#include "pair.h"
#include <stdexcept>
#include <string>

using nlohmann::json;
using std::string;

int operator%(const string& key, int div);

enum class SlotState { EMPTY, OCCUPIED, DELETED };

template <class T, class K> class Set {
  public:
    Array<Pair<T, Array<K>>> table;
    Array<SlotState> state;
    size_t count;

    Set(size_t sz = 10) : table(sz), state(sz), count(0) {
        table.size = table.capacity;
        state.size = state.capacity;
        for (size_t i = 0; i < state.size; ++i) {
            state[i] = SlotState::EMPTY;
        }
    }

    int hash1(const T& key) const {
        return (key % static_cast<int>(table.capacity) + static_cast<int>(table.capacity)) %
               static_cast<int>(table.capacity);
    }

    int hash2(const T& key) const {
        if (table.capacity <= 2)
            return 1;
        int h =
            (key % static_cast<int>(table.capacity - 1) + static_cast<int>(table.capacity - 1)) %
            static_cast<int>(table.capacity - 1);
        return (h % 2 == 0) ? h + 1 : h;
    }

    int findSlotIndex(const T& key) const {
        int h1 = hash1(key);
        int h2 = hash2(key);
        for (size_t i = 0; i < table.capacity; ++i) {
            int idx = (h1 + i * h2) % static_cast<int>(table.capacity);
            if (state[idx] == SlotState::EMPTY)
                return -1;
            if (state[idx] == SlotState::OCCUPIED && table[idx].first == key)
                return idx;
        }
        return -1;
    }

    void put(const T& key, const K& value) {
        int idx = findSlotIndex(key);
        if (idx != -1) {
            table[idx].second.pushBack(value);
            return;
        }

        int h1 = hash1(key);
        int h2 = hash2(key);
        int firstDeleted = -1;
        for (size_t i = 0; i < table.capacity; ++i) {
            int slot = (h1 + i * h2) % static_cast<int>(table.capacity);

            if (state[slot] == SlotState::OCCUPIED && table[slot].first == key) {
                table[slot].second.pushBack(value);
                return;
            }

            if (state[slot] == SlotState::DELETED && firstDeleted == -1)
                firstDeleted = slot;

            if (state[slot] == SlotState::EMPTY) {
                int target = (firstDeleted != -1) ? firstDeleted : slot;
                table[target].first = key;
                table[target].second = Array<K>();
                table[target].second.pushBack(value);
                state[target] = SlotState::OCCUPIED;
                ++count;

                return;
            }
        }

        resize(table.capacity * 2);
        put(key, value);
    }

    bool contains(const T& key) const { return findSlotIndex(key) != -1; }

    const Array<K>& get(const T& key) const {
        int idx = findSlotIndex(key);
        if (idx == -1)
            throw std::runtime_error("Key not found in Set");
        return table[idx].second;
    }

    void remove(const T& key, const K& value) {
        int idx = findSlotIndex(key);
        if (idx == -1)
            return;

        Array<K>& list = table[idx].second;
        for (size_t i = 0; i < list.size; ++i) {
            if (list[i] == value) {
                list.deleteByIndex(i);
                break;
            }
        }

        if (list.size == 0) {
            state[idx] = SlotState::DELETED;
            --count;
        }
    }

    void resize(size_t newSize) {
        if (newSize == 0)
            newSize = 1;
        size_t oldSizeBackup = table.size;
        table.size = table.capacity;
        state.size = state.capacity;

        Array<Pair<T, Array<K>>> oldTable = table;
        Array<SlotState> oldState = state;
        size_t oldCap = table.capacity;

        table = Array<Pair<T, Array<K>>>(newSize);
        state = Array<SlotState>(newSize);
        count = 0;

        table.size = table.capacity;
        state.size = state.capacity;

        for (size_t i = 0; i < state.capacity; ++i) {
            state[i] = SlotState::EMPTY;
        }

        for (size_t i = 0; i < oldCap; ++i) {
            if (oldState[i] == SlotState::OCCUPIED) {
                const T& k = oldTable[i].first;
                const Array<K>& values = oldTable[i].second;
                for (size_t j = 0; j < values.size; ++j) {
                    put(k, values[j]);
                }
            }
        }
    }
};

json serializeSet(const Set<string, Pair<int, int>>& s);

void deserializeSet(const json& j, Set<string, Pair<int, int>>& s);