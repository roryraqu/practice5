#ifndef DBMANAGER_H
#define DBMANAGER_H

#include "array.h"
#include "table.h"
#include "filters.h"
#include <nlohmann/json.hpp>
#include <string>
#include <unordered_map>
#include <mutex>
#include <iostream>

using json = nlohmann::json;
using namespace std;

class DBManager {
private:
    std::unordered_map<std::string, Table*> tablesCache;
    std::mutex tablesCacheMutex;
    json schema;

public:
    DBManager(const json& schema);
    ~DBManager();

    Table& getTable(const string& tableName);

    void INSERT(Table& table, const Array<string>& line);
    void DELETE(Table& table, const Array<Array<condition>>& whereConditions);
    
    void SELECT(const Array<string>& selectedColumns, Array<Table*>& tables,
                const Array<Array<condition>>& whereConditions,
                std::ostream& out);
                
    void VACUUM(Table& table);
};

#endif