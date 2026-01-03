#include "fsmanager.h"

#include <filesystem>
#include <fstream>
#include <iostream>
#include <stdexcept>

using namespace std::filesystem;
using namespace std;

bool directoryExists(const string& dirpath) {
    path dir(dirpath);
    return exists(dir) && is_directory(dir);
}

void FSManager::createDirectories(const json& schema) {
    const string schemaName = schema["name"];
    create_directory(schemaName);

    for (auto& table : schema["structure"].items()) {
        string tableName = table.key();
        path tablePath = path(schemaName + "/" + tableName);

        if (!exists(tablePath)) {
            create_directory(tablePath);
            {
                fstream pkFile(tablePath.string() + "/" + tableName + "_pk", ios::out);
                pkFile << 0;
                pkFile.close();
            }

            {
                fstream lockFile(tablePath.string() + "/" + tableName + "_lock", ios::out);
                lockFile << UNLOCKED;
                lockFile.close();
            }

            {
                fstream meta(tablePath.string() + "/" + tableName + ".meta", ios::out);
                json j = json::object();

                j["last_file"] = 1;
                j["num_of_lines"] = 0;

                string header = tableName + "_pk";
                for (const auto& columnName : table.value()) {
                    header += ",";
                    header += columnName.get<string>();
                }
                header += "\n";

                j["last_offset"] = static_cast<int>(header.length());

                meta << j;
                meta.close();
            }

            {
                fstream csvFile(tablePath.string() + "/1.csv", ios::out);
                if (!csvFile.is_open()) {
                    throw runtime_error("File is not open: " + tablePath.string() + "/1.csv");
                }

                csvFile << tableName << "_pk";

                for (const auto& columnName : table.value()) {
                    csvFile << "," << columnName.get<string>();
                }

                csvFile << "\n";
                csvFile.close();
            }

            {
                create_directory(path(tablePath.string() + "/idx"));

                ofstream idxPK(tablePath.string() + "/idx/" + to_string(0) + ".idx");

                idxPK << "{}";
                idxPK.close();

                int i = 1;
                for (const auto& columnName : table.value()) {
                    ofstream idxFile(tablePath.string() + "/idx/" + to_string(i++) + ".idx");

                    idxFile << "{}";
                    idxFile.close();
                }
            }
        }
    }
}