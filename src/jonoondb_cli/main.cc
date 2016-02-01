#include <iostream>
#include <fstream>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/filesystem.hpp>
#include <boost/tokenizer.hpp>
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

          std::ifstream file(tokens[1], ios::binary);
          if (!file.is_open()) {
            string msg = "Failed to open file ";
            msg.append(tokens[1]).append(".");
            cout << msg << endl;
            continue;
          }
          std::vector<Buffer> documents;

          while (!file.eof()) {
            std::uint32_t size = 0;
            file.read(reinterpret_cast<char*>(&size), sizeof(std::uint32_t));
          }
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

