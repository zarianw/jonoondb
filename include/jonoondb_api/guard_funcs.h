#pragma once

struct sqlite3;
struct sqlite3_stmt;

namespace jonoondb_api {
class Field;

class GuardFuncs {
public:
  static void DisposeField(Field* field);
  static void SQLite3Close(sqlite3* db);
  static void SQLite3Finalize(sqlite3_stmt* stmt);

};
} // namespace jonoondb_api