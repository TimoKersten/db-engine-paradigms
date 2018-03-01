#pragma once
#include "Database.hpp"
#include <string>

namespace runtime {
   /// imports tpch relations from CSVs in dir into db
   void importTPCH(std::string dir, Database& db);
}
