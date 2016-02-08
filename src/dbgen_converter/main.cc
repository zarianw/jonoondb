#include <iostream>
#include <fstream>
#include <boost/tokenizer.hpp>
#include "C:\code\jonoondb\test\schemas\flatbuffers\region_generated.h"
#include "C:\code\jonoondb\test\schemas\flatbuffers\nation_generated.h"
#include "C:\code\jonoondb\test\schemas\flatbuffers\lineitem_generated.h"
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
    }
    {
      string filePath("C:/code/tpch_2_14_3/dbgen/lineitem.tbl");
      ifstream file(filePath.c_str());
      if (!file.is_open()) return 1;

      vector<string> tokens;
      string line;
      char_separator<char> sep("|");

      while (getline(file, line))
      {
        tokenizer<char_separator<char>> tok(line, sep);
        tokens.assign(tok.begin(), tok.end());

        FlatBufferBuilder fbb;
        auto l_returnFlag = fbb.CreateString(tokens[8]);
        auto l_lineStatus = fbb.CreateString(tokens[9]);
        auto l_shipDate = fbb.CreateString(tokens[10]);
        auto l_commitDate = fbb.CreateString(tokens[11]);
        auto l_receiptDate = fbb.CreateString(tokens[12]);
        auto l_shipInstruct = fbb.CreateString(tokens[13]);
        auto l_shipMode = fbb.CreateString(tokens[14]);
        auto l_comment = fbb.CreateString(tokens[15]);
        auto l_orderKey = stoi(tokens[0]);
        auto l_partKey = stoi(tokens[1]);
        auto l_suppKey = stoi(tokens[2]);
        auto l_lineNumber = stoi(tokens[3]);
        auto l_quantity = stod(tokens[4]);
        auto l_extendedPrice = stod(tokens[5]);
        auto l_discount = stod(tokens[6]);
        auto l_tax = stod(tokens[7]);
        auto lineItem = CreateLINEITEM(fbb, l_orderKey, l_partKey, l_suppKey, l_lineNumber, l_quantity, l_extendedPrice, l_discount);
      }
    }
}