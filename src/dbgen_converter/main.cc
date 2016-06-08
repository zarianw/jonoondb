#include <iostream>
#include <fstream>
#include <boost/tokenizer.hpp>
#include "flatbuffers/flatbuffers.h"
#include "dbgen_converter/region_generated.h"
#include "dbgen_converter/nation_generated.h"
#include "dbgen_converter/lineitem_generated.h"
#include "dbgen_converter/partsupp_generated.h"
#include "dbgen_converter/part_generated.h"
#include "dbgen_converter/orders_generated.h"
#include "dbgen_converter/customer_generated.h"
#include "dbgen_converter/supplier_generated.h"

using namespace dbgen_converter;
using namespace std;
using namespace boost;
using namespace flatbuffers;

int main(int argc, char** argv) {
  cout << "DBGen Tool." << endl;

  if (argc != 3) {
    cout << "Usage: dbgen_converter IN_DIRECTORY_PATH OUT_DIRECTORY_PATH"
        << endl;
    return 1;
  }

  string directoryPath = argv[1];
  string outputDirectoryPath = argv[2];

  // - Read csv (*.tbl) files line by line
  // - For each line (record) generate a corresponding flatbuffer object using the generated headers.
  // - (Inside the loop) Write the size of the object (which is a int) in a output file
  //    then write the object in the same file
  // Loop END
  {
    string filePath(directoryPath + "region.tbl");
    cout << "Processing " << filePath << endl;

    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    string destFilePath = outputDirectoryPath + "region.fb";
    std::remove(destFilePath.c_str());
    ofstream outputFile(destFilePath, ios::binary);
    if (!outputFile.is_open()) return 1;

    while (getline(file, line)) {
      tokenizer<char_separator<char>> tok(line, sep);
      tokens.assign(tok.begin(), tok.end());

      // create region flatbuffer object here
      FlatBufferBuilder fbb;
      auto name = fbb.CreateString(tokens[1]);
      auto comment = fbb.CreateString(tokens[2]);
      auto regionKey = stoi(tokens[0]);
      auto region = CreateREGION(fbb, regionKey, name, comment);
      fbb.Finish(region);

      // Now write the object size std::int32_t (4 Bytes) and the object data
      int32_t size = static_cast<int32_t>(fbb.GetSize());
      outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
      outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);
    }
  }

  {
    // Nation
    string filePath(directoryPath + "nation.tbl");
    cout << "Processing " << filePath << endl;

    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    string destFilePath = outputDirectoryPath + "nation.fb";
    std::remove(destFilePath.c_str());
    ofstream outputFile(destFilePath, ios::binary);
    if (!outputFile.is_open()) return 1;

    while (getline(file, line)) {
      tokenizer<char_separator<char>> tok(line, sep);
      tokens.assign(tok.begin(), tok.end());

      FlatBufferBuilder fbb;
      auto name = fbb.CreateString(tokens[1]);
      auto comment = fbb.CreateString(tokens[3]);
      auto nationKey = stoi(tokens[0]);
      auto regionKey = stoi(tokens[2]);
      auto nation = CreateNATION(fbb, nationKey, name, regionKey, comment);
      fbb.Finish(nation);

      int32_t size = static_cast<int32_t>(fbb.GetSize());
      outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
      outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);
    }
  }

  {
    string filePath(directoryPath + "lineitem.tbl");
    cout << "Processing " << filePath << endl;

    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    string destFilePath = outputDirectoryPath + "lineitem.fb";
    std::remove(destFilePath.c_str());
    ofstream outputFile(destFilePath, ios::binary);
    if (!outputFile.is_open()) return 1;

    while (getline(file, line)) {
      tokenizer<char_separator<char>> tok(line, sep);
      tokens.assign(tok.begin(), tok.end());

      FlatBufferBuilder fbb;

      auto orderKey = stoi(tokens[0]);
      auto partKey = stoi(tokens[1]);
      auto suppKey = stoi(tokens[2]);
      auto lineNumber = stoi(tokens[3]);
      auto quantity = stod(tokens[4]);
      auto extendedPrice = stod(tokens[5]);
      auto discount = stod(tokens[6]);
      auto tax = stod(tokens[7]);

      auto returnFlag = fbb.CreateString(tokens[8]);
      auto lineStatus = fbb.CreateString(tokens[9]);
      auto shipDate = fbb.CreateString(tokens[10]);
      auto commitDate = fbb.CreateString(tokens[11]);
      auto receiptDate = fbb.CreateString(tokens[12]);
      auto shipInstruct = fbb.CreateString(tokens[13]);
      auto shipMode = fbb.CreateString(tokens[14]);
      auto comment = fbb.CreateString(tokens[15]);

      auto lineItem =
          CreateLINEITEM(fbb,
                         orderKey,
                         partKey,
                         suppKey,
                         lineNumber,
                         quantity,
                         extendedPrice,
                         discount,
                         tax,
                         returnFlag,
                         lineStatus,
                         shipDate,
                         commitDate,
                         receiptDate,
                         shipInstruct,
                         shipMode,
                         comment);

      fbb.Finish(lineItem);
      int32_t size = static_cast<int32_t>(fbb.GetSize());
      outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
      outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);
    }
  }

  {
    string filePath(directoryPath + "partsupp.tbl");
    cout << "Processing " << filePath << endl;

    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    string destFilePath = outputDirectoryPath + "partsupp.fb";
    std::remove(destFilePath.c_str());
    ofstream outputFile(destFilePath, ios::binary);
    if (!outputFile.is_open()) return 1;

    while (getline(file, line)) {
      tokenizer<char_separator<char>> tok(line, sep);
      tokens.assign(tok.begin(), tok.end());

      FlatBufferBuilder fbb;
      auto comment = fbb.CreateString(tokens[4]);
      auto partKey = stoi(tokens[0]);
      auto suppKey = stoi(tokens[1]);
      auto availQty = stoi(tokens[2]);
      auto supplyCost = stod(tokens[3]);
      auto partSupp =
          CreatePARTSUPP(fbb, partKey, suppKey, availQty, supplyCost, comment);
      fbb.Finish(partSupp);
    }
  }

  {
    string filePath(directoryPath + "part.tbl");
    cout << "Processing " << filePath << endl;

    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    string destFilePath = outputDirectoryPath + "part.fb";
    std::remove(destFilePath.c_str());
    ofstream outputFile(destFilePath, ios::binary);
    if (!outputFile.is_open()) return 1;

    while (getline(file, line)) {
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
      auto part = CreatePART(fbb,
                             partKey,
                             name,
                             mfgr,
                             brand,
                             type,
                             partSize,
                             container,
                             retailPrice,
                             comment);
      fbb.Finish(part);
      int32_t size =
          static_cast<int32_t>(fbb.GetSize()); // get size of the flat buffer
      outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
      outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);
    }
  }

  {
    string filePath(directoryPath + "orders.tbl");
    cout << "Processing " << filePath << endl;

    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    string destFilePath = outputDirectoryPath + "orders.fb";
    std::remove(destFilePath.c_str());
    ofstream outputFile(destFilePath, ios::binary);
    if (!outputFile.is_open()) return 1;

    while (getline(file, line)) {
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

      auto orders = CreateORDERS(fbb,
                                 orderKey,
                                 custKey,
                                 orderStatus,
                                 totalPrice,
                                 orderDate,
                                 orderPriorty,
                                 clerk,
                                 shipPriority,
                                 comment);
      fbb.Finish(orders);

      int32_t size = static_cast<int32_t>(fbb.GetSize());
      outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
      outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);
    }
  }

  {
    string filePath(directoryPath + "customer.tbl");
    cout << "Processing " << filePath << endl;

    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    string destFilePath = outputDirectoryPath + "customer.fb";
    std::remove(destFilePath.c_str());
    ofstream outputFile(destFilePath, ios::binary);
    if (!outputFile.is_open()) return 1;

    while (getline(file, line)) {
      tokenizer<char_separator<char>> tok(line, sep);
      tokens.assign(tok.begin(), tok.end());

      FlatBufferBuilder fbb;

      auto name = fbb.CreateString(tokens[1]);
      auto address = fbb.CreateString(tokens[2]);
      auto mktSegment = fbb.CreateString(tokens[6]);
      auto comment = fbb.CreateString(tokens[7]);
      auto custKey = stoi(tokens[0]);
      auto nationKey = stoi(tokens[3]);
      auto phone = stoi(tokens[4]);
      auto acctBal = stod(tokens[5]);
      auto customer = CreateCUSTOMER(fbb,
                                     custKey,
                                     name,
                                     address,
                                     nationKey,
                                     phone,
                                     acctBal,
                                     mktSegment,
                                     comment);

      fbb.Finish(customer);

      int32_t size = static_cast<int32_t>(fbb.GetSize());
      outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
      outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);
    }
  }

  {
    string filePath(directoryPath + "supplier.tbl");
    cout << "Processing " << filePath << endl;

    ifstream file(filePath.c_str());
    if (!file.is_open()) return 1;

    vector<string> tokens;
    string line;
    char_separator<char> sep("|");

    string destFilePath = outputDirectoryPath + "supplier.fb";
    std::remove(destFilePath.c_str());
    ofstream outputFile(destFilePath, ios::binary);
    if (!outputFile.is_open()) return 1;

    while (getline(file, line)) {
      tokenizer<char_separator<char>> tok(line, sep);
      tokens.assign(tok.begin(), tok.end());

      FlatBufferBuilder fbb;
      auto name = fbb.CreateString(tokens[1]);
      auto address = fbb.CreateString(tokens[2]);
      auto phone = fbb.CreateString(tokens[4]);
      auto comment = fbb.CreateString(tokens[6]);
      auto suppKey = stoi(tokens[0]);
      auto nationKey = stoi(tokens[3]);
      auto acctBal = stod(tokens[5]);

      auto supplier = CreateSUPPLIER(fbb,
                                     suppKey,
                                     name,
                                     address,
                                     nationKey,
                                     phone,
                                     acctBal,
                                     comment);
      fbb.Finish(supplier);

      int32_t size = static_cast<int32_t>(fbb.GetSize());
      outputFile.write(reinterpret_cast<char*>(&size), sizeof(int32_t));
      outputFile.write(reinterpret_cast<char*>(fbb.GetBufferPointer()), size);
    }
  }
}