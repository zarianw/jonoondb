#include <iostream>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/cmdline.hpp>
#include <boost/program_options/parsers.hpp>
#include <boost/filesystem.hpp>
#include "database.h"

namespace po = boost::program_options;
using namespace std;
using namespace jonoondb_api;
using namespace boost::filesystem;

int StartJonoonDBCLI(string dbName, string dbPath) {
  try {
    cout << "JonoonDB - Lets change things." << "\n";

    Options opt;
    Database db(dbPath, dbName, opt);

    std::string cmd;
    while (true) {
      cout << "JonoonDB> ";
      cin >> cmd;
      cout << "echo: " << cmd << endl;
    }


       
  } catch (JonoonDBException& ex) {
    cout << ex.to_string() << std::endl; 
    return 1;
  } catch (std::exception& ex) {
    cout << "Exception: " << ex.what() << std::endl;
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
    cout << desc << "\n";
    return 1;
  }

  // ok at this point we have all our options
  if (vm_positional["db_name"].empty()) {
    cout << "db_name not specified." << "\n";
    cout << desc << "\n";
    return 1;
  }
  string dbName = vm_positional["db_name"].as<string>();
  string dbPath;

  if (!vm_positional["db_path"].empty()) {
    dbPath = vm_positional["db_path"].as<string>();
  } else {
    dbPath = current_path().string();
  }

  return StartJonoonDBCLI(dbName, dbPath);  
}

