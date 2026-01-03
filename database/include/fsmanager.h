#pragma once

#include "hash.h"
#include "table.h"

class FSManager {
  public:
    void createDirectories(const json& schema);
};