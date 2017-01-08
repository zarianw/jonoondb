#include <iostream>
#include <iomanip>
#include <fstream>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
#include <boost/interprocess/file_mapping.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/endian/conversion.hpp>
#include "linenoise/linenoise.h"
#include "jonoondb_api/database_impl.h"
#include "jonoondb_api/buffer_impl.h"
#include "jonoondb_api/options_impl.h"
#include "jonoondb_api/index_info_impl.h"
#include "jonoondb_api/resultset_impl.h"
#include "jonoondb_utils/stopwatch.h"
#include "jonoondb_api/file.h"
#include "jonoondb_utils/varint.h"
#include "jonoondb_api/write_options_impl.h"

namespace po = boost::program_options;
using namespace std;
using namespace jonoondb_api;
using namespace jonoondb_utils;
using namespace boost::filesystem;

void PrintResultSet(ResultSetImpl& rs) {
  std::int32_t colCount = rs.GetColumnCount();
  vector<size_t> columnWidths(colCount, 0);

  // try to guess column width by reading few rows
  int count = 0;
  vector<vector<string>> readVals;
  while (count < 50 && rs.Next()) {
    readVals.push_back(vector<string>());
    for (std::int32_t i = 0; i < colCount; i++) {
      const string& val = rs.GetString(i);
      if (columnWidths[i] <= val.size()) {
        columnWidths[i] = val.size() + 1;
      }
      readVals.back().push_back(val);
    }
    count++;
  }

  // Print header
  const char fill = ' ';
  for (std::int32_t i = 0; i < colCount; i++) {
    if (columnWidths[i] < rs.GetColumnLabel(i).size()) {
      columnWidths[i] = rs.GetColumnLabel(i).size();
    }

    cout << left << std::setw(columnWidths[i]) << std::setfill(fill)
         << rs.GetColumnLabel(i) << "|";
  }
  cout << "\n";

  // Print values that were read while guessing
  for (auto& vals : readVals) {
    for (std::int32_t i = 0; i < colCount; i++) {
      cout << left << std::setw(columnWidths[i]) << std::setfill(fill)
           << vals[i] << "|";
    }
    cout << "\n";
  }

  // Print rest of the values
  while (rs.Next()) {
    for (std::int32_t i = 0; i < colCount; i++) {
      cout << left << std::setw(columnWidths[i]) << std::setfill(fill)
           << rs.GetString(i) << "|";
    }
    cout << "\n";
  }
}

void PrintTime(Stopwatch sw) {
  cout << "Run Time: " << sw.ElapsedMilliSeconds() << " millisecs." << endl;
}

void LoadHistory(const string& path) {
#if !defined(_WIN32)
  linenoiseHistoryLoad(path.c_str());
  linenoiseHistorySetMaxLen(30);
#endif
}

void SaveHistory(const string& path) {
#if !defined(_WIN32)
  /* Save the history on disk. */
  linenoiseHistorySave(path.c_str());
#endif
}

void GetLine(std::string& line, const string& historyFilePath) {
#if defined(_WIN32)
  cout << "JonoonDB> ";
  std::getline(std::cin, line);
#else
  static int counter = 0;
  char* input = linenoise("JonoonDB> ");
  if (input == nullptr) {
    line = "";
  } else {
    line = input;
    if (line.size() > 0) {
      linenoiseHistoryAdd(input);
      counter++;
      if (counter % 5 == 0) {
        // Save history every 5th command. This protects the history incase
        // we don't have a normal shutdown using ".exit" cmd
        SaveHistory(historyFilePath);
      }
    }
    linenoiseFree(input);
  }
#endif
}

int StartJonoonDBCLI(string dbName, string dbPath) {
  try {
    cout << "JonoonDB - Lets change things." << "\n";
    cout << "DBNAME: " << dbName << "\n";
    cout << "DBPATH: " << dbPath << "\n";
    cout << "Loading DB ..." << endl;

    OptionsImpl opt;
    //opt.SetMaxDataFileSize(1024 * 1024 * 128);
    //opt.SetMemoryCleanupThreshold(1024 * 1024 * 512);
    Stopwatch loadSW(true);
    DatabaseImpl db(dbPath, dbName, opt);
    loadSW.Stop();
    cout << "Loading completed in " << loadSW.ElapsedMilliSeconds()
         << " millisecs." << "\n";
    cout << "Enter \".help\" for usage hints." << endl;
    std::string cmd;
    boost::char_separator<char> sep(" ");
    bool isTimerOn = true;
    const string historyFilePath = ".jdb_cmd_history";
    LoadHistory(historyFilePath);

    while (true) {
      GetLine(cmd, historyFilePath);
      if (cmd.size() == 0)
        continue;

      try {
        if (boost::starts_with(cmd, "select ")
            || boost::starts_with(cmd, "SELECT ") ||
            boost::starts_with(cmd, "explain select ")
            || boost::starts_with(cmd, "EXPLAIN SELECT ")) {
          //select command or explain select command
          Stopwatch sw(true);
          auto rs = db.ExecuteSelect(cmd);
          PrintResultSet(rs);
          if (isTimerOn) {
            sw.Stop();
            PrintTime(sw);
          }
          continue;
        }

        boost::tokenizer<boost::char_separator<char>> tokenizer(cmd, sep);
        std::vector<std::string> tokens(tokenizer.begin(), tokenizer.end());
        if (tokens.size() == 0)
          continue;

        if (tokens[0] == ".help") {
          cout << ".cc COLLECTION_NAME SCHEMA_FILE [INDEX_FILE]      Create collection with name COLLECTION_NAME\n";
          cout << "                                                  and schema defined in file SCHEMA_FILE. If\n";
          cout << "                                                  INDEX_FILE is specified then indexes specified\n";
          cout << "                                                  in the file are created as well.\n";

          cout
              << ".i COLLECTION_NAME DATA_FILE [be]                 Import data into COLLECTION_NAME from DATA_FILE.\n";
          cout << "                                                  If \"be\" is specified then object sizes in the\n";
          cout << "                                                  DATA_FILE are treated as big endian otherwise\n";
          cout << "                                                  they are treated as little endian.\n";

          cout << ".timer on|off                                     Turn timer on or off.\n";
          cout << ".exit                                             Exit this program.\n";
        } else if (tokens[0] == ".cc") {
          // create collection command
          // make sure we have enough params
          if (tokens.size() < 3) {
            cout
                << "Not enough parameters. USAGE: .cc COLLECTION_NAME SCHEMA_FILE [INDEX_FILE]"
                << endl;
            continue;
          }

          vector<IndexInfoImpl> indexes;
          if (tokens.size() == 4) {
            // We have a index file lets read the indexes from it
            auto& indexFile = tokens[3];
            std::ifstream file(indexFile);
            if (!file.is_open()) {
              ostringstream ss;
              ss << "Filed to open INDEX_FILE " << indexFile << ".";
              throw std::runtime_error(ss.str());
            }

            string line;
            boost::char_separator<char> csvSep(",");
            while (getline(file, line)) {
              boost::tokenizer<boost::char_separator<char>>
                  idxTokenizer(line, csvSep);
              std::vector<std::string>
                  idxTokens(idxTokenizer.begin(), idxTokenizer.end());
              if (idxTokens.size() == 0)
                continue;

              // Get type of index
              if (idxTokens.size() < 4) {
                ostringstream ss;
                ss << "Not enough values specified for index in INDEX_FILE "
                   << indexFile << ". " <<
                   "Line: " << line << "." << endl;
                throw std::runtime_error(ss.str());
              }

              // trim all tokens
              for (auto& tok : idxTokens) {
                boost::trim(tok);
              }

              if (idxTokens[1] == "INVERTED_COMPRESSED_BITMAP") {
                bool isAscending = false;
                if (boost::iequals("ASC", idxTokens[3])) {
                  isAscending = true;
                }
                indexes.push_back(IndexInfoImpl(idxTokens[0],
                                                IndexType::INVERTED_COMPRESSED_BITMAP,
                                                idxTokens[2],
                                                isAscending));
              } else if (idxTokens[1] == "VECTOR") {
                bool isAscending = false;
                if (boost::iequals("ASC", idxTokens[3])) {
                  isAscending = true;
                }
                indexes.push_back(IndexInfoImpl(idxTokens[0], IndexType::VECTOR,
                                                idxTokens[2], isAscending));
              } else {
                ostringstream ss;
                ss << "Unknown index type \"" << idxTokens[1]
                   << "\" specified in INDEX_FILE " <<
                   indexFile << ". Line: " << line
                   << ". Index types are case sensitive." << endl;
                throw std::runtime_error(ss.str());
              }
            }
          }

          auto schema = File::Read(tokens[2]);
          vector<IndexInfoImpl*> idxs;
          for (auto& item : indexes) {
            idxs.push_back(&item);
          }
          db.CreateCollection(tokens[1],
                              SchemaType::FLAT_BUFFERS,
                              schema,
                              idxs);
        } else if (tokens[0] == ".i") {
          // import command
          // make sure we have enough params
          Stopwatch sw(true);
          if (tokens.size() < 3) {
            cout
                << "Not enough parameters. USAGE: .i COLLECTION_NAME DATA_FILE [be]"
                << endl;
            continue;
          }

          bool bigEndSize = false;
          if (tokens.size() == 4) {
            if (tokens[3] == "be") {
              bigEndSize = true;
            }
          }

          auto fileMapping =
              boost::interprocess::file_mapping(tokens[2].c_str(),
                                                boost::interprocess::read_only);
          auto mappedRegion = boost::interprocess::mapped_region(fileMapping,
                                                                 boost::interprocess::read_only);
          auto fileSize = mappedRegion.get_size();
          char* currentPostion =
              reinterpret_cast<char*>(mappedRegion.get_address());
          std::vector<BufferImpl> documents;
          std::size_t bytesRead = 0;
          std::int64_t bytesReadForCurrRegion = 0;
          std::uint32_t size = 0;
          std::size_t
              pageSize = boost::interprocess::mapped_region::get_page_size();
          bool swapEndianess = false;
          if ((Varint::OnLittleEndianMachine() && bigEndSize) ||
              (!Varint::OnLittleEndianMachine() && !bigEndSize)) {
            swapEndianess = true;
          }

          WriteOptionsImpl wo(true, true);
          while (bytesRead < fileSize) {
            memcpy(&size, currentPostion, sizeof(std::uint32_t));
            if (swapEndianess) {
              boost::endian::endian_reverse_inplace(size);
            }

            currentPostion += sizeof(std::uint32_t);
            documents.push_back(BufferImpl(currentPostion, size, size, nullptr));
            currentPostion += size;
            bytesReadForCurrRegion += sizeof(std::uint32_t) + size;
            bytesRead += sizeof(std::uint32_t) + size;
            if (bytesReadForCurrRegion > (1024 * 1024 * 1024)) {
              // Insert accumulated docs
              vector<const BufferImpl*> docs;
              for (auto& item : documents) {
                docs.push_back(&item);
              }
              gsl::span<const BufferImpl*> docSpan(docs.data(), docs.size());
              db.MultiInsert(tokens[1], docSpan, wo);
              documents.clear();

              // Remap to not use too much memory
              auto quotient = bytesRead / pageSize;
              auto remainder = bytesRead % pageSize;
              auto offset = pageSize * quotient;

              mappedRegion = boost::interprocess::mapped_region(
                  fileMapping, boost::interprocess::read_only,
                  offset);
              currentPostion =
                  reinterpret_cast<char*>(mappedRegion.get_address());
              currentPostion += remainder;
              bytesReadForCurrRegion = 0;
            }
          }

          if (documents.size() > 0) {
            vector<const BufferImpl*> docs;
            for (auto& item : documents) {
              docs.push_back(&item);
            }
            gsl::span<const BufferImpl*> docSpan(docs.data(), docs.size());
            db.MultiInsert(tokens[1], docSpan, wo);
          }

          if (isTimerOn) {
            sw.Stop();
            PrintTime(sw);
          }
        } else if (tokens[0] == ".timer") {
          // timer command
          // make sure we have enough params
          if (tokens.size() < 2) {
            cout << "Not enough parameters. USAGE: .timer on|off" << endl;
            continue;
          }

          if (boost::iequals(tokens[1], "on")) {
            isTimerOn = true;
          } else if (boost::iequals(tokens[1], "off")) {
            isTimerOn = false;
          } else {
            cout << "ERROR: Not a boolean value: \"" << tokens[1]
                 << "\". Assuming \"no\"." << endl;
          }
        } else if (tokens[0] == ".exit") {
          SaveHistory(historyFilePath);
          return 0;
        } else {
          cout << "Unknow command " << tokens[0] << "." << endl;
        }
      } catch (JonoonDBException& ex) {
        cout << ex.to_string() << endl;
      } catch (std::exception& ex) {
        cout << "Exception: " << ex.what() << endl;
      }
    }
  } catch (JonoonDBException& ex) {
    cout << ex.to_string() << endl;
    return 1;
  } catch (std::exception& ex) {
    cout << "Exception: " << ex.what() << endl;
    return 1;
  }
}

int main(int argc, char** argv) {
  try {
    // Declare the supported options.
    po::options_description desc("Usage: jonoondb_cli DBNAME [DBPATH] [OPTIONS]\n"
                                     "DBNAME is the name of the database.\n"
                                     "DBPATH is the path where database file exists. A new database is created\n"
                                     "if the file does not previously exist. Default is current directory.\n\n"
                                     "Allowed options");

    desc.add_options()
        ("help", "produce help message")
        ("db_name", po::value<string>(), "Name of the database. ")
        ("db_path", po::value<string>(), "Path where database file exists. "
            "A new database is created if the file does not previously exist. "
            "Default is current directory.");

    po::positional_options_description p;
    p.add("db_name", 1);
    p.add("db_path", 1);

    po::variables_map vm_positional;
    po::store(po::command_line_parser(argc, argv).
        options(desc).positional(p).run(), vm_positional);

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);
    po::notify(vm);

    if (vm.count("help")) {
      cout << desc << endl;
      return 1;
    }

    if (vm_positional["db_name"].empty()) {
      cout << "db_name not specified." << endl;
      cout << desc << endl;
      return 1;
    }
    string dbName = vm_positional["db_name"].as<string>();
    string dbPath;

    if (!vm_positional["db_path"].empty()) {
      dbPath = vm_positional["db_path"].as<string>();
    } else {
      dbPath = current_path().generic_string();
    }

    // ok at this point we have all our options  
    return StartJonoonDBCLI(dbName, dbPath);
  } catch (exception& ex) {
    cout << "Exception: " << ex.what() << endl;
    return 1;
  }
}

