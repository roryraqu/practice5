#ifndef SQLPARSER_H
#define SQLPARSER_H

#include "array.h"
#include "dbmanager.h"
#include <nlohmann/json.hpp>
#include <string>
#include <iostream>

using json = nlohmann::json;
using namespace std;

class SQLParser {
private:
    json schema;
    DBManager& dbm;

    static string trim(string str);
    
    Array<string> split(const string& str, char delimiter);
    Array<string> parseColumns(const string& colsStr);
    condition parseSingleCondition(const string& conditionStr) const;
    Array<Array<condition>> parseWhere(const string& whereClause) const;

public:
    SQLParser(const json& schemaJson, DBManager& dbm);
    void executeQuery(const string& query, std::ostream& out);
};

#endif