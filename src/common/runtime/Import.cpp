#include "common/runtime/Import.hpp"
#include "common/runtime/Mmap.hpp"
#include "common/runtime/Types.hpp"
#include "errno.h"
#include "sys/stat.h"
#include <fstream>
#include <stdlib.h>

using namespace std;

enum RTType {
   Integer,
   Numeric_12_2,
   Date,
   Char_1,
   Char_10,
   Char_15,
   Char_25,
   Varchar_23,
   Varchar_25,
   Varchar_40,
   Varchar_44,
   Varchar_55,
   Varchar_79,
   Varchar_101,
   Varchar_117,
   Varchar_152,
   Varchar_199
};
RTType algebraToRTType(algebra::Type* t) {
   if (dynamic_cast<algebra::Integer*>(t)) {
      return RTType::Integer;
   } else if (auto e = dynamic_cast<algebra::Numeric*>(t)) {
      if (e->size == 12 and e->precision == 2)
         return Numeric_12_2;
      else
         throw runtime_error("Unknown Numeric precision");
   } else if (auto e = dynamic_cast<algebra::Char*>(t)) {
      switch (e->size) {
      case 1: return Char_1;
      case 10: return Char_10;
      case 15: return Char_15;
      case 25: return Char_25;
      default: throw runtime_error("Unknown Char size");
      }
   } else if (auto e = dynamic_cast<algebra::Varchar*>(t)) {
      switch (e->size) {
      case 23: return Varchar_23;
      case 25: return Varchar_25;
      case 40: return Varchar_40;
      case 44: return Varchar_44;
      case 55: return Varchar_55;
      case 79: return Varchar_79;
      case 101: return Varchar_101;
      case 117: return Varchar_117;
      case 152: return Varchar_152;
      case 199: return Varchar_199;

      default:
         throw runtime_error("Unknown Varchar size" + std::to_string(e->size));
      }
   } else if (dynamic_cast<algebra::Date*>(t)) {
      return Date;
   } else {
      throw runtime_error("Unknown type");
   }
}
struct ColumnConfigOwning {
   string name;
   unique_ptr<algebra::Type> type;
   ColumnConfigOwning(string n, unique_ptr<algebra::Type>&& t)
       : name(n), type(move(t)) {}
   ColumnConfigOwning(ColumnConfigOwning&&) = default;
};
struct ColumnConfig {
   string name;
   algebra::Type* type;
   ColumnConfig(string n, algebra::Type* t) : name(n), type(t) {}
};

#define COMMA ,
#define EACHTYPE                                                               \
   case Integer: D(types::Integer)                                             \
   case Numeric_12_2: D(types::Numeric<12 COMMA 2>)                            \
   case Date: D(types::Date)                                                   \
   case Char_1: D(types::Char<1>)                                              \
   case Char_10: D(types::Char<10>)                                            \
   case Char_15: D(types::Char<15>)                                            \
   case Char_25: D(types::Char<25>)                                            \
   case Varchar_23: D(types::Varchar<23>)                                      \
   case Varchar_25: D(types::Varchar<25>)                                      \
   case Varchar_40: D(types::Varchar<40>)                                      \
   case Varchar_44: D(types::Varchar<44>)                                      \
   case Varchar_55: D(types::Varchar<55>)                                      \
   case Varchar_79: D(types::Varchar<79>)                                      \
   case Varchar_101: D(types::Varchar<101>)                                    \
   case Varchar_117: D(types::Varchar<117>)                                    \
   case Varchar_152: D(types::Varchar<152>)                                    \
   case Varchar_199: D(types::Varchar<199>)

inline void parse(ColumnConfig& c, std::vector<void*>& col, std::string& line,
                  unsigned& begin, unsigned& end) {

   const char* start = line.data() + begin;
   end = line.find_first_of('|', begin);
   size_t size = end - begin;

#define D(type)                                                                \
   reinterpret_cast<std::vector<type>&>(col).emplace_back(                     \
       type::castString(start, size));                                         \
   break;

   switch (algebraToRTType(c.type)) { EACHTYPE }
#undef D
   begin = end + 1;
}

void writeBinary(ColumnConfig& col, std::vector<void*>& data,
                 std::string path) {
#define D(type)                                                                \
   {                                                                           \
      auto name = path + "_" + col.name;                                       \
      runtime::Vector<type>::writeBinary(                                      \
          name.data(), reinterpret_cast<std::vector<type>&>(data));            \
      break;                                                                   \
   }
   switch (algebraToRTType(col.type)) { EACHTYPE }
#undef D
}

size_t readBinary(runtime::Relation& r, ColumnConfig& col, std::string path) {
#define D(rt_type)                                                             \
   {                                                                           \
      auto name = path + "_" + col.name;                                       \
      auto& attr = r[col.name];                                                \
      /* attr.name = col.name;   */                                            \
      /*attr.type = col.type;  */                                              \
      auto& data = attr.typedAccessForChange<rt_type>();                       \
      data.readBinary(name.data());                                            \
      return data.size();                                                      \
   }
   switch (algebraToRTType(col.type)) {
      EACHTYPE default : throw runtime_error("Unknown type");
   }
#undef D
}

void parseColumns(runtime::Relation& r, std::vector<ColumnConfigOwning>& cols,
                  std::string dir, std::string fileName) {

   std::vector<ColumnConfig> colsC;
   for (auto& col : cols) {
      colsC.emplace_back(col.name, col.type.get());
      r.insert(col.name, move(col.type));
   }

   bool allColumnsMMaped = true;
   string cachedir = dir + "/cached/";
   if (!mkdir((dir + "/cached/").c_str(), 0777))
     throw runtime_error("Could not create dir 'cached'.");
   for (auto& col : colsC)
     if (!std::ifstream(cachedir + fileName + "_" + col.name))
       allColumnsMMaped = false;

   if (!allColumnsMMaped) {
      std::vector<std::vector<void*>> attributes;
      attributes.assign(colsC.size(), {});
      ifstream relationFile(dir + fileName + ".tbl");
      if (!relationFile.is_open())
         throw runtime_error("csv file not found: " + dir);
      string line;
      unsigned begin = 0, end;
      uint64_t count = 0;
      while (getline(relationFile, line)) {
         count++;
         unsigned i = 0;
         for (auto& col : colsC) parse(col, attributes[i++], line, begin, end);
         begin = 0;
      }
      count = 0;
      for (auto& col : colsC)
         writeBinary(col, attributes[count++], cachedir + fileName);
   }
   // load mmaped files
   size_t size = 0;
   size_t diffs = 0;
   for (auto& col : colsC) {
      auto oldSize = size;
      size = readBinary(r, col, cachedir + fileName);
      diffs += (oldSize != size);
   }
   if (diffs > 1)
      throw runtime_error("Columns of " + fileName + " differ in size.");
   r.nrTuples = size;
}

std::vector<ColumnConfigOwning>
configX(std::initializer_list<ColumnConfigOwning>&& l) {
   std::vector<ColumnConfigOwning> v;
   for (auto& e : l)
      v.emplace_back(e.name, move(const_cast<unique_ptr<Type>&>(e.type)));
   return v;
}

namespace runtime {
void importTPCH(std::string dir, Database& db) {

   //--------------------------------------------------------------------------------
   // part
   {
      auto& rel = db["part"];
      rel.name = "part";
      auto columns =
          configX({{"p_partkey", make_unique<algebra::Integer>()},
                   {"p_name", make_unique<algebra::Varchar>(55)},
                   {"p_mfgr", make_unique<algebra::Char>(25)},
                   {"p_brand", make_unique<algebra::Char>(10)},
                   {"p_type", make_unique<algebra::Varchar>(25)},
                   {"p_size", make_unique<algebra::Integer>()},
                   {"p_container", make_unique<algebra::Char>(10)},
                   {"p_retailprice", make_unique<algebra::Numeric>(12, 2)},
                   {"p_comment", make_unique<algebra::Varchar>(23)}});
      parseColumns(rel, columns, dir, "part");
   }
   //--------------------------------------------------------------------------------
   // supplier
   {
      auto& rel = db["supplier"];
      rel.name = "supplier";
      auto columns =
          configX({{"s_suppkey", make_unique<algebra::Integer>()},
                   {"s_name", make_unique<algebra::Char>(25)},
                   {"s_address", make_unique<algebra::Varchar>(40)},
                   {"s_nationkey", make_unique<algebra::Integer>()},
                   {"s_phone", make_unique<algebra::Char>(15)},
                   {"s_acctbal", make_unique<algebra::Numeric>(12, 2)},
                   {"s_comment", make_unique<algebra::Varchar>(101)}});
      parseColumns(rel, columns, dir, "supplier");
   }
   //--------------------------------------------------------------------------------
   // partsupp
   {
      auto& rel = db["partsupp"];
      rel.name = "partsupp";
      auto columns =
          configX({{"ps_partkey", make_unique<algebra::Integer>()},
                   {"ps_suppkey", make_unique<algebra::Integer>()},
                   {"ps_availqty", make_unique<algebra::Integer>()},
                   {"ps_supplycost", make_unique<algebra::Numeric>(12, 2)},
                   {"ps_comment", make_unique<algebra::Varchar>(199)}});
      parseColumns(rel, columns, dir, "partsupp");
   }
   //------------------------------------------------------------------------------
   // customer
   {
      auto& cu = db["customer"];
      cu.name = "customer";
      auto columns =
          configX({{"c_custkey", make_unique<algebra::Integer>()},
                   {"c_name", make_unique<algebra::Char>(25)},
                   {"c_address", make_unique<algebra::Varchar>(40)},
                   {"c_nationkey", make_unique<algebra::Integer>()},
                   {"c_phone", make_unique<algebra::Char>(15)},
                   {"c_acctbal", make_unique<algebra::Numeric>(12, 2)},
                   {"c_mktsegment", make_unique<algebra::Char>(10)},
                   {"c_comment", make_unique<algebra::Varchar>(117)}});

      parseColumns(cu, columns, dir, "customer");
   }

   //------------------------------------------------------------------------------
   // orders
   {
      auto& od = db["orders"];
      od.name = "orders";
      auto columns =
          configX({{"o_orderkey", make_unique<algebra::Integer>()},
                   {"o_custkey", make_unique<algebra::Integer>()},
                   {"o_orderstatus", make_unique<algebra::Char>(1)},
                   {"o_totalprice", make_unique<algebra::Numeric>(12, 2)},
                   {"o_orderdate", make_unique<algebra::Date>()},
                   {"o_orderpriority", make_unique<algebra::Char>(15)},
                   {"o_clerk", make_unique<algebra::Char>(15)},
                   {"o_shippriority", make_unique<algebra::Integer>()},
                   {"o_comment", make_unique<algebra::Varchar>(79)}});
      parseColumns(od, columns, dir, "orders");
   }
   //--------------------------------------------------------------------------------
   // lineitem
   {
      auto& li = db["lineitem"];
      li.name = "lineitem";
      auto columns =
          configX({{"l_orderkey", make_unique<algebra::Integer>()},
                   {"l_partkey", make_unique<algebra::Integer>()},
                   {"l_suppkey", make_unique<algebra::Integer>()},
                   {"l_linenumber", make_unique<algebra::Integer>()},
                   {"l_quantity", make_unique<algebra::Numeric>(12, 2)},
                   {"l_extendedprice", make_unique<algebra::Numeric>(12, 2)},
                   {"l_discount", make_unique<algebra::Numeric>(12, 2)},
                   {"l_tax", make_unique<algebra::Numeric>(12, 2)},
                   {"l_returnflag", make_unique<algebra::Char>(1)},
                   {"l_linestatus", make_unique<algebra::Char>(1)},
                   {"l_shipdate", make_unique<algebra::Date>()},
                   {"l_commitdate", make_unique<algebra::Date>()},
                   {"l_receiptdate", make_unique<algebra::Date>()},
                   {"l_shipinstruct", make_unique<algebra::Char>(25)},
                   {"l_shipmode", make_unique<algebra::Char>(10)},
                   {"l_comment", make_unique<algebra::Varchar>(44)}});

      parseColumns(li, columns, dir, "lineitem");
   }
   //--------------------------------------------------------------------------------
   // nation
   {
      auto& rel = db["nation"];
      rel.name = "nation";
      auto columns =
          configX({{"n_nationkey", make_unique<algebra::Integer>()},
                   {"n_name", make_unique<algebra::Char>(25)},
                   {"n_regionkey", make_unique<algebra::Integer>()},
                   {"n_comment", make_unique<algebra::Varchar>(152)}});
      parseColumns(rel, columns, dir, "nation");
   }
   //--------------------------------------------------------------------------------
   // region
   {
      auto& rel = db["region"];
      rel.name = "region";
      auto columns =
          configX({{"r_regionkey", make_unique<algebra::Integer>()},
                   {"r_name", make_unique<algebra::Char>(25)},
                   {"r_comment", make_unique<algebra::Varchar>(152)}});
      parseColumns(rel, columns, dir, "region");
   }
}
} // namespace runtime
