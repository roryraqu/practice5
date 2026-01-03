#pragma once

#include "array.h"
#include "table.h"
#include <iostream>

using std::string;

struct condition {
    string leftTable;
    string leftColumn;
    string rightTable;
    string rightColumn;
    bool isJoin;
};

struct ColumnInfo {
    string tableName;
    string columnName;
    int tableIndex;
    int colIndex;
};

class Filters {
public:
    static bool evaluateConditions(const Array<Array<condition>>& whereConditions,
                                   const Array<string>& row, Table* table);

    static bool evaluateConditionsN(const Array<Array<condition>>& whereConditions,
                                    const Array<Array<string>*>& rows, 
                                    const Array<Table*>& tables);

    static void buildProjection(const Array<string>& selectedColumns, Table* table,
                                const Array<string>& row, Array<string>& resultRow);

    static void buildProjection(const Array<string>& selectedColumns, 
                                const Array<Table*>& tables,
                                const Array<Array<string>*>& rows, 
                                Array<string>& resultRow);

    static void printResults(const Array<Array<string>>& resultRows,
                             const Array<string>& selectedColumns,
                             std::ostream& out);
};