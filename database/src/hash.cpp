#include "hash.h"

int operator%(const string& key, int div) {
    if (div <= 0)
        return 0;
    unsigned long long sum = 0;
    for (unsigned char c : key)
        sum = (sum << 5) + c;
    return static_cast<int>(sum % div);
}

json serializeSet(const Set<string, Pair<int, int>>& s) {
    json j = json::parse("{}");

    for (size_t i = 0; i < s.table.capacity; i++) {
        if (s.state[i] == SlotState::OCCUPIED) {
            const string& key = s.table[i].first;
            const Array<Pair<int, int>>& values = s.table[i].second;

            json valueArray = json::array();
            for (size_t k = 0; k < values.size; ++k) {
                valueArray.push_back({values.head[k].first, values.head[k].second});
            }

            j[key] = valueArray;
        }
    }
    return j;
}

void deserializeSet(const json& j, Set<string, Pair<int, int>>& s) {
    for (auto it = j.begin(); it != j.end(); ++it) {
        if (!it.value().is_array())
            continue;
        string key = it.key();
        const json& pairs = it.value();
        for (const auto& p : pairs) {
            if (p.is_array() && p.size() == 2) {
                int a = p[0].get<int>();
                int b = p[1].get<int>();
                s.put(key, Pair<int, int>{a, b});
            }
        }
    }
}