#pragma once
#include <iostream>

using namespace std;

template <class T1, class T2> class Pair {
  public:
    T1 first;
    T2 second;

    template <class L, class R>
    friend bool operator==(const Pair<L, R>& lhs, const Pair<L, R>& rhs);
};

template <class T1, class T2> bool operator==(const Pair<T1, T2>& lhs, const Pair<T1, T2>& rhs) {
    return lhs.first == rhs.first && lhs.second == rhs.second;
}

template <class T1, class T2> ostream& operator<<(ostream& os, const Pair<T1, T2>& p) {
    os << p.first << " " << p.second << "\n";
    return os;
}