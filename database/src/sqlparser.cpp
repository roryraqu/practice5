#include "sqlparser.h"
#include <algorithm>
#include <cctype>
#include <sstream>

using namespace std;

string SQLParser::trim(string str) {
    const auto start = str.find_first_not_of(" \t\r\n");
    if (start == string::npos) {
        return "";
    }
    const auto end = str.find_last_not_of(" \t\r\n");
    return str.substr(start, end - start + 1);
}

Array<string> SQLParser::split(const string& str, char delimiter) {
    Array<string> tokens;
    stringstream ss(str);
    string token;
    while (getline(ss, token, delimiter)) {
        tokens.pushBack(token);
    }
    return tokens;
}

Array<string> SQLParser::parseColumns(const string& colsStr) {
    Array<string> cols;
    stringstream ss(colsStr);
    string token;
    while (getline(ss, token, ',')) {
        token = trim(std::move(token));
        cols.pushBack(token);
    }
    return cols;
}

condition SQLParser::parseSingleCondition(const string& conditionStr) const {
    string cond = trim(conditionStr);
    const auto opPos = cond.find('=');
    if (opPos == string::npos || opPos == 0 || opPos == cond.length() - 1) {
        throw runtime_error("No '=' operator found or invalid format in condition: " +
                            conditionStr);
    }

    string leftPart = trim(cond.substr(0, opPos));
    string rightPart = trim(cond.substr(opPos + 1));

    auto dotPos = leftPart.find('.');
    if (dotPos == string::npos || dotPos == 0 || dotPos == leftPart.length() - 1) {
        throw runtime_error("No '.' in left part of condition: " + leftPart);
    }

    string leftTable = trim(leftPart.substr(0, dotPos));
    string leftColumn = trim(leftPart.substr(dotPos + 1));

    dotPos = rightPart.find('.');
    if (dotPos == 0 || dotPos == leftPart.length() - 1) {
        throw runtime_error("No '.' in left part of condition: " + leftPart);
    }
    if (dotPos == string::npos) {
        string literal = rightPart;
        if (literal.size() >= 2 && literal.front() == '\'' && literal.back() == '\'') {
            literal = literal.substr(1, literal.size() - 2);
        }
        return {leftTable, leftColumn, "", literal, false};
    }

    string rightTable = trim(rightPart.substr(0, dotPos));
    string rightColumn = trim(rightPart.substr(dotPos + 1));
    return {leftTable, leftColumn, rightTable, rightColumn, true};
}

Array<Array<condition>> SQLParser::parseWhere(const string& whereClause) const {
    if (whereClause.empty()) {
        return Array<Array<condition>>();
    }

    string clause = whereClause;

    size_t pos = 0;
    while ((pos = clause.find(" AND ", pos)) != string::npos) {
        clause.replace(pos, 5, "\001");
        pos += 1;
    }

    pos = 0;
    while ((pos = clause.find(" OR ", pos)) != string::npos) {
        clause.replace(pos, 4, "\002");
        pos += 1;
    }

    Array<string> orParts;
    stringstream ssOr(clause);
    string token;
    while (getline(ssOr, token, '\002')) {
        orParts.pushBack(trim(token));
    }

    Array<Array<condition>> result;
    for (const auto& orBlock : orParts) {
        if (orBlock.empty())
            continue;
        Array<condition> andConditions;
        stringstream ssAnd(orBlock);
        string andToken;
        while (getline(ssAnd, andToken, '\001')) {
            andToken = trim(andToken);
            if (!andToken.empty()) {
                andConditions.pushBack(parseSingleCondition(andToken));
            }
        }
        result.pushBack(andConditions);
    }

    return result;
}

SQLParser::SQLParser(const json& schemaJson, DBManager& dbm) : schema(schemaJson), dbm(dbm) {}

void SQLParser::executeQuery(const string& query, std::ostream& out) {
    auto q = query;
    const auto selectPos = q.find("SELECT ");
    const auto insertPos = q.find("INSERT INTO");
    const auto deletePos = q.find("DELETE FROM");
    const auto vacuumPos = q.find("VACUUM ");

    if (selectPos == 0) {
        const auto rest = q.substr(7);
        const auto fromPos = rest.find(" FROM ");
        if (fromPos == string::npos)
            throw runtime_error("Invalid SELECT: missing FROM");

        const auto colsStr = trim(rest.substr(0, fromPos));
        auto afterFrom = rest.substr(fromPos + 6);

        const auto wherePos = afterFrom.find(" WHERE ");
        string tablesStr;
        string whereStr;

        if (wherePos != string::npos) {
            tablesStr = trim(afterFrom.substr(0, wherePos));
            whereStr = trim(afterFrom.substr(wherePos + 7));
        } else {
            tablesStr = trim(std::move(afterFrom));
        }

        const auto selectedColumns = parseColumns(colsStr);

        Array<string> tableNames = split(tablesStr, ',');
        for (auto& name : tableNames) {
            name = trim(std::move(name));
        }

        Array<Array<condition>> whereCondition;
        if (!whereStr.empty()) {
            whereCondition = parseWhere(whereStr);
        }

        Array<Table*> tables;
        for (const auto& name : tableNames) {
            tables.pushBack(&dbm.getTable(name));
        }

        dbm.SELECT(selectedColumns, tables, whereCondition, out);

    } else if (insertPos == 0) {
        auto rest = q.substr(12);
        const auto valuesPos = rest.find(" VALUES(");
        if (valuesPos == string::npos)
            throw runtime_error("Invalid INSERT: missing VALUES");

        const auto tableName = trim(rest.substr(0, valuesPos));
        auto valuesPart = rest.substr(valuesPos + 7);

        if (valuesPart.size() < 2 || valuesPart.front() != '(' || valuesPart.back() != ')') {
            throw runtime_error("Invalid INSERT: malformed VALUES clause");
        }
        string valuesStr = valuesPart.substr(1, valuesPart.length() - 2);
        valuesStr = trim(valuesStr);

        Array<string> values;
        stringstream ss(valuesStr);
        string token;
        while (getline(ss, token, ',')) {
            if (!token.empty() && token[0] == ' ') {
                token.erase(0, 1);
            }
            if (token.size() < 2 || token.front() != '\'' || token.back() != '\'') {
                throw runtime_error("Invalid INSERT: malformed value: " + token);
            }
            string val = token.substr(1, token.length() - 2);
            values.pushBack(std::move(val));
        }

        if (values.size == 0) {
            throw runtime_error("Invalid INSERT: no values found");
        }

        Table& table = dbm.getTable(tableName);
        dbm.INSERT(table, values);

    } else if (deletePos == 0) {
        auto rest = q.substr(12);
        const auto wherePos = rest.find(" WHERE ");

        string tableName;
        string whereStr = "";
        if (wherePos != string::npos) {
            tableName = trim(rest.substr(0, wherePos));
            whereStr = trim(rest.substr(wherePos + 7));
        } else {
            tableName = trim(std::move(rest));
        }

        Table& table = dbm.getTable(tableName);

        Array<Array<condition>> cond =
            whereStr.empty() ? Array<Array<condition>>() : parseWhere(whereStr);
        dbm.DELETE(table, cond);

    } else if (vacuumPos == 0) {
        string tableName = trim(q.substr(7));
        if (tableName.empty()) {
            throw runtime_error("Invalid VACUUM: missing table name");
        }
        
        Table& table = dbm.getTable(tableName);
        dbm.VACUUM(table);

    } else {
        throw runtime_error("Unsupported query type.");
    }
}