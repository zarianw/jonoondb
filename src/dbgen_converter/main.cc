#include <iostream>
#include <fstream>
#include <boost/tokenizer.hpp>
#include "C:\code\jonoondb\test\schemas\flatbuffers\region_generated.h"
#include "C:\code\jonoondb\test\schemas\flatbuffers\nation_generated.h"
#include "C:\code\jonoondb\test\schemas\flatbuffers\lineitem_generated.h"
#include "C:\code\jonoondb\test\schemas\flatbuffers\partsupp_generated.h"
#include "C:\code\jonoondb\test\schemas\flatbuffers\part_generated.h"
#include "C:\code\jonoondb\test\schemas\flatbuffers\orders_generated.h"
#include "flatbuffers/flatbuffers.h"

using namespace dbgen_loader;
using namespace std;
using namespace boost;
using namespace flatbuffers;

// Learn these concepts first
// - Read a text file line by line
// - How to write to a binary file
// Tutorial: http://www.cplusplus.com/doc/tutorial/files/
// Or google fstream

int main(int argc, char **argv) {

  cout << "DBGen Tool." << endl;


  // - Read csv (*.tbl) files line by line
  // - For each line (record) generate a corresponding flatbuffer object using the generated headers.
  // - (Inside the loop) Write the size of the object (which is a int) in a output file
  //    then write the object in the same file
  // Loop END
  {
    string filePath("C:/code/tpch_2_14_3/dbgen/region.tbl");

    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    std::remove("region.fb");
    ofstream outputFile("region.fb", ios::binary);
    if (!outputFile.is_open()) return 1;

    while (getline(file, line))
    {
      tokenizer<char_separator<char>> tok(line, sep);
      tokens.assign(tok.begin(), tok.end());

      // create region flatbuffer object here
      FlatBufferBuilder fbb;
      auto R_NAME = fbb.CreateString(tokens[1]);
      auto R_COMMENT = fbb.CreateString(tokens[2]);
      auto R_REGIONKEY = stoi(tokens[0]);
      auto region = CreateREGION(fbb, R_REGIONKEY, R_NAME, R_COMMENT);
      fbb.Finish(region);

      // Now write the object size std::int32_t (4 Bytes) and the object data
      int32_t size = static_cast<int32_t>(fbb.GetSize());
      outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
      outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);
    }
  }

    {
      // Nation
      string filePath("C:/code/tpch_2_14_3/dbgen/nation.tbl");
      ifstream file(filePath.c_str());
      if (!file.is_open()) return 1;

      vector<string> tokens;
      string line;
      char_separator<char> sep("|");

      std::remove("nation.fb");

      ofstream outputFile("nation.fb", ios::binary);

      if (!outputFile.is_open()) return 1;

      while (getline(file, line))
      {
        tokenizer<char_separator<char>> tok(line, sep);
        tokens.assign(tok.begin(), tok.end());

        FlatBufferBuilder fbb;
        auto N_NAME = fbb.CreateString(tokens[1]);
        auto N_COMMENT = fbb.CreateString(tokens[3]);
        auto nationKey = stoi(tokens[0]);
        auto regionKey = stoi(tokens[2]);
        auto nation = CreateNATION(fbb, nationKey, N_NAME, regionKey, N_COMMENT);
        fbb.Finish(nation);

        int32_t size = static_cast<int32_t>(fbb.GetSize());
        outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
        outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);

      }
    }
    {
      string filePath("C:/code/tpch_2_14_3/dbgen/lineitem.tbl");
      ifstream file(filePath.c_str());
      if (!file.is_open()) return 1;

      vector<string> tokens;
      string line;
      char_separator<char> sep("|");
      std::remove("lineitem.fb");

      ofstream outputFile("lineitem.fb", ios::binary);

      if (!outputFile.is_open()) return 1;

      while (getline(file, line))
      {
        tokenizer<char_separator<char>> tok(line, sep);
        tokens.assign(tok.begin(), tok.end());

        FlatBufferBuilder fbb;
        auto returnFlag = fbb.CreateString(tokens[8]);
        auto lineStatus = fbb.CreateString(tokens[9]);
        auto shipDate = fbb.CreateString(tokens[10]);
        auto commitDate = fbb.CreateString(tokens[11]);
        auto receiptDate = fbb.CreateString(tokens[12]);
        auto shipInstruct = fbb.CreateString(tokens[13]);
        auto shipMode = fbb.CreateString(tokens[14]);
        auto comment = fbb.CreateString(tokens[15]);
        auto orderKey = stoi(tokens[0]);
        auto partKey = stoi(tokens[1]);
        auto suppKey = stoi(tokens[2]);
        auto lineNumber = stoi(tokens[3]);
        auto quantity = stod(tokens[4]);
        auto extendedPrice = stod(tokens[5]);
        auto discount = stod(tokens[6]);
        auto tax = stod(tokens[7]);
        auto lineItem = CreateLINEITEM(fbb, orderKey, partKey, suppKey, lineNumber, quantity, extendedPrice,
          discount, tax, returnFlag, lineStatus, shipDate, commitDate, receiptDate, shipInstruct, shipMode, comment);

        fbb.Finish(lineItem);
        int32_t size = static_cast<int32_t>(fbb.GetSize());
        outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
        outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);

      }

    }
    {
      string filePath("C:/code/tpch_2_14_3/dbgen/partsupp.tbl");

      ifstream file(filePath.c_str());
      if (!file.is_open()) return 1;

      vector<string> tokens;
      string line;
      char_separator<char> sep("|");

      std::remove("partsupp.fb");
      ofstream outputFile("partsupp.fb", ios::binary);
      if (!outputFile.is_open()) return 1;

      while (getline(file, line))
      {
        tokenizer<char_separator<char>> tok(line, sep);
        tokens.assign(tok.begin(), tok.end());

        FlatBufferBuilder fbb;
        auto comment = fbb.CreateString(tokens[4]);
        auto partKey = stoi(tokens[0]);
        auto suppKey = stoi(tokens[1]);
        auto availQty = stoi(tokens[2]);
        auto supplyCost = stod(tokens[3]);
        auto partSupp = CreatePARTSUPP(fbb, partKey, suppKey, availQty, supplyCost, comment);
        fbb.Finish(partSupp);
      }
    }

    {
      string filePath("C:/code/tpch_2_14_3/dbgen/part.tbl");

      ifstream file(filePath.c_str());
      if (!file.is_open()) return 1;

      vector<string> tokens;
      string line;
      char_separator<char> sep("|");

      std::remove("part.fb");
      ofstream outputFile("part.fb", ios::binary);
      if (!outputFile.is_open()) return 1;

      while (getline(file, line))
      {
        tokenizer<char_separator<char>> tok(line, sep);
        tokens.assign(tok.begin(), tok.end());

        FlatBufferBuilder fbb;
        auto name = fbb.CreateString(tokens[1]);
        auto mfgr = fbb.CreateString(tokens[2]);
        auto brand = fbb.CreateString(tokens[3]);
        auto type = fbb.CreateString(tokens[4]);
        auto container = fbb.CreateString(tokens[6]);
        auto comment = fbb.CreateString(tokens[8]);
        auto partKey = stoi(tokens[0]);
        auto partSize = stoi(tokens[5]);
        auto retailPrice = stod(tokens[7]);
        auto part = CreatePART(fbb, partKey, name, mfgr, brand, type, partSize, container, retailPrice, comment);
        fbb.Finish(part);
        int32_t size = static_cast<int32_t>(fbb.GetSize()); // get size of the flat buffer
        outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
        outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);

      }
    }

    {
    string filePath("C:/code/tpch_2_14_3/dbgen/orders.tbl");
    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    std::remove("orders.fb");

    ofstream outputFile("orders.fb", ios::binary);

    if (!outputFile.is_open()) return 1;

    while (getline(file, line))
    {
      tokenizer<char_separator<char>> tok(line, sep);
      tokens.assign(tok.begin(), tok.end());

      FlatBufferBuilder fbb;

      auto orderStatus = fbb.CreateString(tokens[2]);
      auto orderDate = fbb.CreateString(tokens[4]);
      auto orderPriorty = fbb.CreateString(tokens[5]);
      auto clerk = fbb.CreateString(tokens[6]);
      auto comment = fbb.CreateString(tokens[8]);
      auto orderKey = stoi(tokens[0]);
      auto custKey = stoi(tokens[1]);
      auto totalPrice = stod(tokens[3]);
      auto shipPriority = stoi(tokens[7]);

      auto orders = CreateORDERS(fbb, orderKey, custKey, orderStatus, totalPrice, orderDate, orderPriorty, clerk, shipPriority, comment);
      fbb.Finish(orders);

      int32_t size = static_cast<int32_t>(fbb.GetSize());
      outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
      outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);

    }


  }
}