#include "filters.h"
#include "table.h"
#include <iostream>
#include <sstream>

using namespace std;

string getColValue(const string& tableName, const string& columnName, const Array<string>& row1,
                   const Array<string>& row2, Table* t1, Table* t2) {
    Table* tbl = nullptr;
    const Array<string>* row = nullptr;

    if (t1 && tableName == t1->name) {
        tbl = t1;
        row = &row1;
    } else if (t2 && tableName == t2->name) {
        tbl = t2;
        row = &row2;
    } else {
        return "";
    }

    for (size_t i = 0; i < tbl->columns.size; ++i) {
        if (tbl->columns[i] == columnName) {
            if (row->size > i) {
                return row->head[i];
            }
            break;
        }
    }
    return "";
}

string getColValueN(const string& tableName, const string& columnName,
                    const Array<Array<string>*>& rows, const Array<Table*>& tables) {
    for (size_t ti = 0; ti < tables.size; ++ti) {
        if (tables[ti]->name == tableName) {
            const Array<string>& row = *rows[ti];
            Table* tbl = tables[ti];
            for (size_t i = 0; i < tbl->columns.size; ++i) {
                if (tbl->columns[i] == columnName) {
                    if (row.size > i) {
                        return row[i];
                    }
                    break;
                }
            }
            break;
        }
    }
    return "";
}

bool Filters::evaluateConditionsN(const Array<Array<condition>>& whereConditions,
                                  const Array<Array<string>*>& rows, const Array<Table*>& tables) {
    if (whereConditions.size == 0)
        return true;
    for (size_t blockIdx = 0; blockIdx < whereConditions.size; ++blockIdx) {
        const Array<condition>& block = whereConditions[blockIdx];
        bool blockMatches = true;

        for (size_t i = 0; i < block.size; ++i) {
            const condition& cond = block[i];
            string leftValue = getColValueN(cond.leftTable, cond.leftColumn, rows, tables);
            string rightValue;

            if (cond.isJoin) {
                rightValue = getColValueN(cond.rightTable, cond.rightColumn, rows, tables);
            } else {
                rightValue = cond.rightColumn;
                if (rightValue.size() >= 2 && rightValue.front() == '\'' &&
                    rightValue.back() == '\'') {
                    rightValue = rightValue.substr(1, rightValue.size() - 2);
                }
            }

            if (leftValue != rightValue) {
                blockMatches = false;
                break;
            }
        }

        if (blockMatches) {
            return true;
        }
    }
    return false;
}

bool Filters::evaluateConditions(const Array<Array<condition>>& whereConditions,
                                 const Array<string>& row1, Table* t1) {
    if (whereConditions.size == 0)
        return true;
    for (size_t blockIdx = 0; blockIdx < whereConditions.size; ++blockIdx) {
        const Array<condition>& block = whereConditions[blockIdx];
        bool allTrue = true;

        for (size_t i = 0; i < block.size; ++i) {
            const condition& cond = block[i];
            if (cond.isJoin || cond.leftTable != t1->name) {
                allTrue = false;
                break;
            }

            string leftValue =
                getColValue(cond.leftTable, cond.leftColumn, row1, Array<string>{}, t1, nullptr);
            string rightValue = cond.rightColumn;
            if (rightValue.size() >= 2 && rightValue.front() == '\'' && rightValue.back() == '\'') {
                rightValue = rightValue.substr(1, rightValue.size() - 2);
            }

            if (leftValue != rightValue) {
                allTrue = false;
                break;
            }
        }

        if (allTrue) {
            return true;
        }
    }

    return false;
}

void Filters::buildProjection(const Array<string>& selectedColumns, Table* table,
                              const Array<string>& row, Array<string>& resultRow) {
    for (size_t i = 0; i < selectedColumns.size; ++i) {
        const string& full = selectedColumns[i];
        size_t dot = full.find('.');
        if (dot == string::npos) {
            resultRow.pushBack("[ERR]");
            continue;
        }
        string tbl = full.substr(0, dot);
        string col = full.substr(dot + 1);
        if (tbl != table->name) {
            resultRow.pushBack("[NO_TBL]");
            continue;
        }
        bool found = false;
        for (size_t j = 0; j < table->columns.size; ++j) {
            if (table->columns[j] == col) {
                if (row.size > j) {
                    resultRow.pushBack(row[j]);
                } else {
                    resultRow.pushBack("");
                }
                found = true;
                break;
            }
        }
        if (!found)
            resultRow.pushBack("[NO_COL]");
    }
}

void Filters::buildProjection(const Array<string>& selectedColumns, const Array<Table*>& tables,
                              const Array<Array<string>*>& rows, Array<string>& resultRow) {
    for (size_t i = 0; i < selectedColumns.size; ++i) {
        const string& full = selectedColumns[i];
        size_t dot = full.find('.');
        if (dot == string::npos) {
            resultRow.pushBack("[ERR]");
            continue;
        }
        string reqTable = full.substr(0, dot);
        string reqCol = full.substr(dot + 1);
        bool handled = false;
        for (size_t ti = 0; ti < tables.size; ++ti) {
            if (tables[ti]->name == reqTable) {
                Table* t = tables[ti];
                const Array<string>& row = *rows[ti];
                for (size_t j = 0; j < t->columns.size; ++j) {
                    if (t->columns[j] == reqCol) {
                        if (row.size > j) {
                            resultRow.pushBack(row[j]);
                        } else {
                            resultRow.pushBack("");
                        }
                        handled = true;
                        break;
                    }
                }
                if (handled)
                    break;
            }
        }
        if (!handled) {
            resultRow.pushBack("[MISSING]");
        }
    }
}

void Filters::printResults(const Array<Array<string>>& resultRows,
                           const Array<string>& selectedColumns,
                           std::ostream& out) {
    for (size_t i = 0; i < resultRows.size; ++i) {
        const Array<string>& row = resultRows[i];
        for (size_t j = 0; j < row.size; ++j) {
            out << row[j];
            if (j + 1 < row.size)
                out << " | ";
        }
        out << "\n";
    }
}