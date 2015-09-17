#pragma once

struct sqlite3;
namespace jonoondb_api {
class Field;

class GuardFuncs {
public:
  static void DisposeField(Field* field);
  static void SQLite3Close(sqlite3* db);

};
} // jonoondb_api