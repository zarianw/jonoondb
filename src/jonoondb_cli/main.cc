#include <iostream>
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
#include "database.h"

namespace po = boost::program_options;
using namespace std;
using namespace jonoondb_api;
using namespace boost::filesystem;

string ReadTextFile(const std::string& path) {
  std::ifstream ifs(path);
  if (!ifs.is_open()) {
    ostringstream ss;
    ss << "Failed to open file at path " << path << ".";
    throw std::exception(ss.str().c_str());
  }

  std::string schema((std::istreambuf_iterator<char>(ifs)),
                     (std::istreambuf_iterator<char>()));

  return schema;
}

inline void DeleteNoOp(char *) {
}

void PrintResultSet(ResultSet& rs) {
  // Print header
  
  // Print values
  while (rs.Next()) {
    cout << rs.GetInteger(rs.GetColumnIndex("id")) << "|";
    cout << rs.GetString(rs.GetColumnIndex("text")).str() << "|";
    cout << rs.GetInteger(rs.GetColumnIndex("user.id")) << "|";
    cout << rs.GetString(rs.GetColumnIndex("user.name")).str() << endl;
  }
}

int StartJonoonDBCLI(string dbName, string dbPath) {
  try {
    cout << "JonoonDB - Lets change things." << "\n";
    cout << "DBNAME: " << dbName << endl;
    cout << "DBPATH: " << dbPath << endl;

    Options opt;
    Database db(dbPath, dbName, opt);
    std::string cmd;    
    boost::char_separator<char> sep(" ");  
    
    while (true) {
      cout << "JonoonDB> ";
      std::getline(std::cin, cmd);
      if (cmd.size() == 0)
        continue;
      
      try {
        if (boost::starts_with(cmd, "select ") || boost::starts_with(cmd, "SELECT ")) {
          //select command
          auto rs = db.ExecuteSelect(cmd);          
          PrintResultSet(rs);
          continue;
        }

        boost::tokenizer<boost::char_separator<char>> tokenizer(cmd, sep);
        std::vector<std::string> tokens(tokenizer.begin(), tokenizer.end());
        if (tokens.size() == 0)
          continue;

        if (tokens[0] == ".cc") {
          // create collection command
          // make sure we have enough params
          if (tokens.size() < 3) {
            cout << "Not enough parameters. USAGE: .cc COLLECTION_NAME SCHEMA_FILE" << endl;
            continue;
          }

          auto schema = ReadTextFile(tokens[2]);
          vector<IndexInfo> indexes;
          db.CreateCollection(tokens[1], SchemaType::FLAT_BUFFERS, schema, indexes);
        } else if (tokens[0] == ".i") {
          // import command
          // make sure we have enough params
          if (tokens.size() < 3) {
            cout << "Not enough parameters. USAGE: .i COLLECTION_NAME DATA_FILE" << endl;
            continue;
          }

          auto fileMapping = boost::interprocess::file_mapping(tokens[2].c_str(), boost::interprocess::read_only);
          auto mappedRegion = boost::interprocess::mapped_region(fileMapping, boost::interprocess::read_only);
          std::uint32_t size = 0;
          auto fileSize = mappedRegion.get_size();
          std::size_t bytesRead = 0;
          char* basePosition = reinterpret_cast<char*>(mappedRegion.get_address());
          char* currentPostion = reinterpret_cast<char*>(mappedRegion.get_address());
          std::vector<Buffer> documents;
          // We can use pointer subtraction safely because the size of the object
          // they point to is 1 byte (char).
          while ((currentPostion - basePosition) < fileSize) {
            memcpy(&size, currentPostion, sizeof(std::uint32_t));
            currentPostion += sizeof(std::uint32_t);
            documents.push_back(Buffer(currentPostion, size, size, DeleteNoOp));
            currentPostion += size;
          }

          db.MultiInsert(tokens[1], documents);          
        } else if (tokens[0] == "exit") {
          return 0;
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

  return 0;
}

int main(int argc, char **argv) {
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
}

