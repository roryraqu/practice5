#include "dbmanager.h"
#include "sqlparser.h"
#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <sstream>
#include <stdexcept>

using json = nlohmann::json;
using namespace std;

unique_ptr<DBManager> globalDB = nullptr;
unique_ptr<json> globalSchema = nullptr;
std::once_flag initFlag;
string loadedSchemaName = "";

void initGlobalDB(const string& schemaFile) {
    ifstream schemaStream(schemaFile);
    if (!schemaStream.is_open()) {
        throw runtime_error("Error opening schema file: " + schemaFile);
    }
    
    globalSchema = make_unique<json>();
    try {
        schemaStream >> *globalSchema;
    } catch (const exception& e) {
        throw runtime_error("Error parsing schema JSON: " + string(e.what()));
    }

    globalDB = make_unique<DBManager>(*globalSchema);
    loadedSchemaName = schemaFile;
    
    cout << "Database initialized from " << schemaFile << endl;
}

bool extractValue(const string& input, const string& flag, string& outValue) {
    size_t pos = input.find(flag);
    if (pos == string::npos)
        return false;

    size_t start = pos + flag.length();
    while (start < input.size() && isspace(static_cast<unsigned char>(input[start])))
        ++start;
    if (start >= input.size() || input[start] != '"')
        return false;

    size_t contentStart = start + 1;
    size_t contentEnd = input.find('"', contentStart);
    if (contentEnd == string::npos)
        return false;

    outValue = input.substr(contentStart, contentEnd - contentStart);
    return true;
}

string processQuery(const string& fullCommand) {
    string schemaFile;
    string query;
    if (!extractValue(fullCommand, "-s", schemaFile)) {
        return "Error: missing or malformed -s \"<schema>\"\n";
    }
    if (!extractValue(fullCommand, "-q", query)) {
        return "Error: missing or malformed -q \"<query>\"\n";
    }

    try {
        std::call_once(initFlag, initGlobalDB, schemaFile);
    } catch (const exception& e) {
        return string(e.what()) + "\n";
    }

    if (loadedSchemaName != schemaFile) {
        return "Error: Changing schema at runtime is not supported in optimized mode.\n";
    }

    string trimmed = query;
    trimmed.erase(trimmed.begin(), find_if(trimmed.begin(), trimmed.end(),
                                           [](unsigned char c) { return !isspace(c); }));
    string lowerQuery = trimmed;
    transform(lowerQuery.begin(), lowerQuery.end(), lowerQuery.begin(), ::tolower);

    bool isSelect = lowerQuery.substr(0, 6) == "select";

    try {
        SQLParser parser(*globalSchema, *globalDB);

        stringstream buffer;

        if (isSelect) {
            parser.executeQuery(query, buffer);
            string result = buffer.str();
            return result.empty() ? "\n" : result;
        } else {
            parser.executeQuery(query, buffer);
            return "Success\n";
        }
    } catch (const exception& e) {
        return "Error executing query: " + string(e.what()) + "\n";
    }
}