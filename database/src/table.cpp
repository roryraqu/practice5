#include "table.h"
#include "filters.h"
#include "fsmanager.h"
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>

using namespace std;
namespace fs = std::filesystem;

Table::Table(const string& name, const json& schema) {
    this->name = name;
    this->path = fs::path(schema["name"].get<string>() + "/" + this->name);
    
    this->columns.pushBack(name + "_pk");
    for (auto& column : schema["structure"][name]) {
        this->columns.pushBack(column);
    }

    refresh();

    this->tupleLimit = schema["tuples_limit"];
    this->state = UNLOCKED;

    indexes = Array<Set<string, Pair<int, int>>*>(columns.size);
    for (size_t i = 0; i < indexes.capacity; ++i) {
        indexes.head[i] = new Set<string, Pair<int, int>>();
    }
    indexes.size = indexes.capacity;

    for (size_t i = 0; i < columns.size; i++) {
        filesystem::path indexPath(this->path.string() + "/idx/" + to_string(i) + ".idx");
        fstream idxIn(indexPath, ios::in);
        json j = json::object();
        if (idxIn.peek() != ifstream::traits_type::eof()) {
            try {
                idxIn >> j;
            } catch (...) {
                j = json::object();
            }
        }
        deserializeSet(j, *indexes.head[i]);
        idxIn.close();
    }
}

void Table::refresh() {
    string pkPath = this->path.string() + "/" + name + "_pk";
    ifstream pkIn(pkPath);
    if (pkIn.is_open()) {
        pkIn >> this->pk;
        pkIn.close();
    } else {
        this->pk = 0;
    }

    string metaPath = this->path.string() + "/" + name + ".meta";
    ifstream metaIn(metaPath);
    if (metaIn.is_open()) {
        json metaJson;
        try {
            metaIn >> metaJson;
            this->lastFile = metaJson.value("last_file", 0);
            this->lastOffset = metaJson.value("last_offset", 0);
            this->numOfLines = metaJson.value("num_of_lines", 0);
        } catch (...) {
            this->lastFile = 0;
            this->lastOffset = 0;
            this->numOfLines = 0;
        }
        metaIn.close();
    } else {
        this->lastFile = 0;
        this->lastOffset = 0;
        this->numOfLines = 0;
    }
}

void Table::updateIndex(const Array<string>& rowValues, int fileNum, int offset) {
    for (size_t i = 0; i < columns.size; i++) {
        string key = rowValues.head[i];
        Pair<int, int> loc{fileNum, offset};
        indexes.head[i]->put(key, loc);
    }
}

Table::~Table() {
    json metaJson;
    metaJson["last_file"] = lastFile;
    metaJson["last_offset"] = lastOffset;
    metaJson["num_of_lines"] = numOfLines;
    
    ofstream metaOut(this->path.string() + "/" + name + ".meta");
    if (metaOut.is_open()) {
        metaOut << metaJson.dump(4);
        metaOut.close();
    }

    for (size_t i = 0; i < columns.size; ++i) {
        if (indexes.head[i]) {
            json j = serializeSet(*indexes[i]);
            fs::path p(this->path.string() + "/idx/" + to_string(i) + ".idx");
            fstream out(p, ios::out);
            out << j.dump(4);
            out.close();
            delete indexes.head[i];
        }
    }
}

string Table::readRowByLocation(int fileNum, int byteOffset) {
    string path = this->path.string() + "/" + to_string(fileNum) + ".csv";
    ifstream file(path, ios::in);
    if (!file.is_open())
        return "";
    file.seekg(byteOffset);
    string line;
    getline(file, line);
    file.close();
    return line;
}

void Table::splitCSV(const string& row, Array<string>& out) {
    out.size = 0; 
    
    stringstream ss(row);
    string part;
    while (getline(ss, part, ','))
        out.pushBack(part);
}

void Table::updatePrimaryKey() {
    string path = this->path.string() + "/" + name + "_pk";
    fstream pkOut(path, std::ios::out);
    pkOut << this->pk;
    pkOut.close();
}

void Table::lock() {
    mutex_.lock();
    this->state = LOCKED;
}

void Table::unlock() {
    this->state = UNLOCKED;
    mutex_.unlock();
}

void Table::lockShared() {
    mutex_.lock_shared();
}

void Table::unlockShared() {
    mutex_.unlock_shared();
}

void Table::persistIndexes() {
    fs::path dir(this->path.string() + "/idx");
    fs::create_directories(dir);
    for (size_t i = 0; i < columns.size; ++i) {
        json j = serializeSet(*indexes[i]);
        fs::path p(this->path.string() + "/idx/" + to_string(i) + ".idx");
        fstream out(p, ios::out);
        out << j.dump(4);
        out.close();
    }
}