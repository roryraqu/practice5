#pragma once

#include "dbmanager.h"
#include "sqlparser.h"
#include <fstream>
#include <iostream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

string processQuery(const string& query);