#ifndef TABLE_H
#define TABLE_H

#include "array.h"
#include "hash.h"
#include <filesystem>
#include <nlohmann/json.hpp>
#include <shared_mutex>
#include <string>

using namespace std;
using json = nlohmann::json;

enum LockState { LOCKED, UNLOCKED };

struct Table {
    string name;
    filesystem::path path;
    Array<string> columns;
    int pk = 0;
    int tupleLimit = 1000;
    int lastFile = 1;
    int lastOffset = 0;
    int numOfLines = 0;

    LockState state = UNLOCKED;
    mutable std::shared_mutex mutex_; 

    Array<Set<string, Pair<int, int>>*> indexes;

    Table() = default;
    Table(const string& name, const json& schema);
    
    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    ~Table();

    void refresh();
    void updateIndex(const Array<string>& rowValues, int fileNum, int offset);
    void updatePrimaryKey();
    
    void lock();
    void unlock();
    void lockShared();
    void unlockShared();

    void persistIndexes();
    string readRowByLocation(int fileNum, int byteOffset);
    
    void splitCSV(const string& row, Array<string>& out);
};

#endif