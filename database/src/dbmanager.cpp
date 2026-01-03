#include "dbmanager.h"
#include "filters.h"
#include "fsmanager.h"
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>
#include <chrono>

using namespace std;
using namespace filesystem;

DBManager::DBManager(const json& schema) : schema(schema) {
    FSManager fsm;
    fsm.createDirectories(schema);
}

DBManager::~DBManager() {
    for (auto& kv : tablesCache) {
        delete kv.second;
    }
}

Table& DBManager::getTable(const string& tableName) {
    std::lock_guard<std::mutex> lock(this->tablesCacheMutex);

    if (tablesCache.find(tableName) == tablesCache.end()) {
        tablesCache[tableName] = new Table(tableName, schema);
    }
    return *tablesCache[tableName];
}

void DBManager::INSERT(Table& table, const Array<string>& line) {
    table.lock();
    struct LockGuard {
        Table& t;
        ~LockGuard() { t.unlock(); }
    } guard{table};

    if (line.size != table.columns.size - 1) {
        throw invalid_argument("INSERT: number of values does not match schema");
    }

    table.pk++;

    stringstream ss;
    ss << table.pk;
    for (size_t i = 0; i < line.size; ++i) {
        ss << "," << line.head[i];
    }
    ss << "\n";
    string newRow = ss.str();

    int fileNum;
    int rowStartOffset;

    if (table.numOfLines >= table.tupleLimit) {
        table.lastFile++;
        table.numOfLines = 0;
        string newCsvPath = table.path.string() + "/" + to_string(table.lastFile) + ".csv";
        ofstream csvOut(newCsvPath);
        if (!csvOut.is_open()) {
            throw runtime_error("Failed to create new CSV file: " + newCsvPath);
        }

        csvOut << table.name << "_pk";
        for (size_t i = 1; i < table.columns.size; ++i) {
            csvOut << "," << table.columns.head[i];
        }
        csvOut << "\n";

        string header = table.name + "_pk";
        for (size_t i = 1; i < table.columns.size; ++i) {
            header += "," + table.columns.head[i];
        }
        header += "\n";
        rowStartOffset = static_cast<int>(header.length());

        csvOut << newRow;
        csvOut.close();
        table.lastOffset = rowStartOffset + static_cast<int>(newRow.length());
        fileNum = table.lastFile;

    } else {
        string currentCsv = table.path.string() + "/" + to_string(table.lastFile) + ".csv";
        fstream file(currentCsv, ios::in | ios::out | ios::ate);
        if (!file.is_open()) {
            throw runtime_error("Failed to open CSV for writing: " + currentCsv);
        }

        rowStartOffset = table.lastOffset;
        file.seekp(rowStartOffset);
        file << newRow;
        file.close();

        table.lastOffset += static_cast<int>(newRow.length());
        fileNum = table.lastFile;
    }

    table.numOfLines++;
    table.updatePrimaryKey();

    Array<string> lineWithPK;
    lineWithPK.pushBack(to_string(table.pk));
    for (const auto& l : line) {
        lineWithPK.pushBack(l);
    }

    table.updateIndex(lineWithPK, fileNum, rowStartOffset);
}

void DBManager::DELETE(Table& table, const Array<Array<condition>>& whereConditions) {
    table.lock();
    struct LockGuard {
        Table& t;
        ~LockGuard() { t.unlock(); }
    } guard{table};

    if (whereConditions.size == 0) {
        throw std::runtime_error("Full DELETE not supported (use WHERE)");
    }

    Array<Pair<int, int>> toDelete;
    for (size_t blockIdx = 0; blockIdx < whereConditions.size; ++blockIdx) {
        const Array<condition>& block = whereConditions[blockIdx];
        for (const condition& cond : block) {
            if (cond.isJoin || cond.leftTable != table.name) {
                throw std::runtime_error("Only single-table conditions in DELETE");
            }
        }

        Array<Pair<int, int>> candidates;
        if (block.size == 0)
            continue;
        const condition& firstCond = block[0];
        int colIndex = -1;
        for (size_t i = 0; i < table.columns.size; ++i) {
            if (table.columns[i] == firstCond.leftColumn) {
                colIndex = static_cast<int>(i);
                break;
            }
        }
        if (colIndex == -1)
            continue;
        if (table.indexes[colIndex]->contains(firstCond.rightColumn)) {
            const Array<Pair<int, int>>& locs = table.indexes[colIndex]->get(firstCond.rightColumn);
            for (size_t i = 0; i < locs.size; ++i) {
                candidates.pushBack(locs[i]);
            }
        } else {
            candidates = Array<Pair<int, int>>();
        }

        Array<Pair<int, int>> validForBlock;
        Array<string> rowValues;
        for (size_t i = 0; i < candidates.size; ++i) {
            Pair<int, int> loc = candidates[i];
            string line = table.readRowByLocation(loc.first, loc.second);
            if (line.empty())
                continue;
            
            table.splitCSV(line, rowValues);

            if (Filters::evaluateConditions(Array<Array<condition>>{block}, rowValues, &table)) {
                validForBlock.pushBack(loc);
            }
        }

        for (size_t i = 0; i < validForBlock.size; ++i) {
            bool found = false;
            for (size_t j = 0; j < toDelete.size; ++j) {
                if (toDelete[j].first == validForBlock[i].first &&
                    toDelete[j].second == validForBlock[i].second) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                toDelete.pushBack(validForBlock[i]);
            }
        }
    }

    Array<string> rowValues;
    for (size_t i = 0; i < toDelete.size; ++i) {
        int fileNum = toDelete[i].first;
        int offset = toDelete[i].second;

        string line = table.readRowByLocation(fileNum, offset);
        if (line.empty())
            continue;
        
        table.splitCSV(line, rowValues);

        for (size_t j = 0; j < table.columns.size; ++j) {
            table.indexes[j]->remove(rowValues[j], Pair<int, int>{fileNum, offset});
        }
    }
}

void DBManager::VACUUM(Table& table) {
    table.lock();
    struct LockGuard {
        Table& t;
        ~LockGuard() { t.unlock(); }
    } guard{table};

    Set<string, Pair<int, int>> liveLocations;
    for (size_t i = 0; i < table.indexes.size; ++i) {
        Set<string, Pair<int, int>>* indexSet = table.indexes.head[i];
        for (size_t j = 0; j < indexSet->table.capacity; ++j) {
            if (indexSet->state[j] == SlotState::OCCUPIED) {
                const Array<Pair<int, int>>& locations = indexSet->table[j].second;
                for (size_t k = 0; k < locations.size; ++k) {
                    const Pair<int, int>& loc = locations.head[k];
                    string key = to_string(loc.first) + ":" + to_string(loc.second);

                    if (!liveLocations.contains(key)) {
                        liveLocations.put(key, loc);
                    }
                }
            }
        }
    }

    for (size_t i = 0; i < table.indexes.size; ++i) {
        delete table.indexes.head[i];
        table.indexes.head[i] = new Set<string, Pair<int, int>>();
    }

    Array<Array<string>> allLiveRowsData;
    Array<string> tempRow;
    for (size_t j = 0; j < liveLocations.table.capacity; ++j) {
        if (liveLocations.state[j] == SlotState::OCCUPIED) {
            const Pair<int, int>& loc = liveLocations.table[j].second.head[0];
            string rowStr = table.readRowByLocation(loc.first, loc.second);
            if (!rowStr.empty()) {
                table.splitCSV(rowStr, tempRow);
                allLiveRowsData.pushBack(tempRow);
            }
        }
    }

    auto setupNewFile = [&](int fileNum) {
        string filePath = table.path.string() + "/" + to_string(fileNum) + ".csv";
        ofstream out(filePath);
        if (!out.is_open()) {
            throw std::runtime_error("Failed to create new CSV file during VACUUM: " + filePath);
        }

        string header = table.name + "_pk";
        for (size_t i = 1; i < table.columns.size; ++i) {
            header += "," + table.columns.head[i];
        }
        header += "\n";
        out << header;

        return std::make_pair(std::move(out), static_cast<int>(header.length()));
    };
    int currentFileNum = 1;
    int currentNumOfLines = 0;
    int currentLastOffset = 0;

    std::pair<std::ofstream, int> fileData = setupNewFile(currentFileNum);
    std::ofstream& currentOut = fileData.first;
    currentLastOffset = fileData.second;

    int totalLiveRows = allLiveRowsData.size;
    for (size_t i = 0; i < allLiveRowsData.size; ++i) {
        const Array<string>& rowData = allLiveRowsData.head[i];
        std::stringstream ss;
        ss << rowData.head[0];
        for (size_t j = 1; j < rowData.size; ++j) {
            ss << "," << rowData.head[j];
        }
        ss << "\n";
        std::string newRow = ss.str();
        if (currentNumOfLines >= table.tupleLimit) {
            currentOut.close();
            currentFileNum++;
            fileData = setupNewFile(currentFileNum);
            currentOut = std::move(fileData.first);
            currentLastOffset = fileData.second;
            currentNumOfLines = 0;
        }

        currentOut << newRow;
        int newRowOffset = currentLastOffset;
        table.updateIndex(rowData, currentFileNum, newRowOffset);
        currentLastOffset += static_cast<int>(newRow.length());
        currentNumOfLines++;
    }

    currentOut.close();

    for (int fileNum = currentFileNum + 1; fileNum <= table.lastFile; ++fileNum) {
        string oldPath = table.path.string() + "/" + to_string(fileNum) + ".csv";
        remove(oldPath.c_str());
    }

    table.lastFile = currentFileNum;
    table.numOfLines = currentNumOfLines;
    table.lastOffset = currentLastOffset;
    table.updatePrimaryKey();
    std::cout << "VACUUM completed. Total live rows: " << totalLiveRows << "\n";
}

void DBManager::SELECT(const Array<string>& selectedColumns, Array<Table*>& tables,
                       const Array<Array<condition>>& whereConditions,
                       std::ostream& out) {
    for (size_t i = 0; i < tables.size; ++i) {
        tables[i]->lockShared();
    }
    
    struct SharedLocksGuard {
        Array<Table*>& t_arr;
        ~SharedLocksGuard() {
            for (size_t i = 0; i < t_arr.size; ++i) {
                t_arr[i]->unlockShared();
            }
        }
    } guard{tables};

    if (tables.size == 0)
        return;

    struct TablePlan {
        Table* table = nullptr;
        Array<Pair<int, int>> candidateLocations;
        bool useIndex = false;
        int filterColIndex = -1;
        string filterValue;
    };

    Array<TablePlan> plans(tables.size);
    for (size_t ti = 0; ti < tables.size; ++ti) {
        Table* t = tables[ti];
        plans[ti].table = t;

        bool foundFilter = false;
        for (size_t blockIdx = 0; blockIdx < whereConditions.size && !foundFilter; ++blockIdx) {
            const Array<condition>& block = whereConditions[blockIdx];
            for (size_t ci = 0; ci < block.size; ++ci) {
                const condition& cond = block[ci];
                if (!cond.isJoin && cond.leftTable == t->name) {
                    for (size_t colIdx = 0; colIdx < t->columns.size; ++colIdx) {
                        if (t->columns[colIdx] == cond.leftColumn) {
                            string val = cond.rightColumn;
                            if (val.size() >= 2 && val.front() == '\'' && val.back() == '\'') {
                                val = val.substr(1, val.size() - 2);
                            }
                            plans[ti].filterColIndex = static_cast<int>(colIdx);
                            plans[ti].filterValue = val;
                            plans[ti].useIndex = true;
                            foundFilter = true;
                            break;
                        }
                    }
                    if (foundFilter)
                        break;
                }
            }
        }

        if (plans[ti].useIndex) {
            Set<string, Pair<int, int>>& idx = *t->indexes[plans[ti].filterColIndex];
            if (idx.contains(plans[ti].filterValue)) {
                plans[ti].candidateLocations = idx.get(plans[ti].filterValue);
            }
        } else {
            Set<string, Pair<int, int>>& pkIdx = *t->indexes[0];
            for (size_t slot = 0; slot < pkIdx.table.capacity; ++slot) {
                if (pkIdx.state[slot] == SlotState::OCCUPIED) {
                    const Array<Pair<int, int>>& locs = pkIdx.table[slot].second;
                    for (size_t i = 0; i < locs.size; ++i) {
                        plans[ti].candidateLocations.pushBack(locs[i]);
                    }
                }
            }
        }
    }

    Array<Array<string>> resultRows;
    
    Array<Array<string>*> currentRows; 
    for(size_t i=0; i<tables.size; ++i) {
        currentRows.pushBack(new Array<string>());
    }

    struct RowsCleaner {
        Array<Array<string>*>& rows;
        ~RowsCleaner() {
            for(size_t i=0; i<rows.size; ++i) {
                delete rows[i];
            }
        }
    } rowsCleaner{currentRows};

    std::function<void(size_t)> dfs = [&](size_t depth) {
        if (depth == tables.size) {
            bool match = false;
            if (whereConditions.size == 0) {
                match = true;
            } else if (tables.size == 1) {
                match = Filters::evaluateConditions(whereConditions, *currentRows[0], plans[0].table);
            } else {
                match = Filters::evaluateConditionsN(whereConditions, currentRows, tables);
            }
            if (match) {
                Array<string> proj;
                if (tables.size == 1) {
                    Filters::buildProjection(selectedColumns, plans[0].table, *currentRows[0], proj);
                } else {
                    Filters::buildProjection(selectedColumns, tables, currentRows, proj);
                }
                resultRows.pushBack(proj);
            }
            return;
        }

        TablePlan& plan = plans[depth];
        for (size_t i = 0; i < plan.candidateLocations.size; ++i) {
            Pair<int, int> loc = plan.candidateLocations[i];
            string line = plan.table->readRowByLocation(loc.first, loc.second);
            if (line.empty())
                continue;
            
            plan.table->splitCSV(line, *currentRows[depth]);
            dfs(depth + 1);
        }
    };

    dfs(0);
    Filters::printResults(resultRows, selectedColumns, out);
}