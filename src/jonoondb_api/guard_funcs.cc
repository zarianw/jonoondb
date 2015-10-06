#include <assert.h>
#include "guard_funcs.h"
#include "field.h"
#include "sqlite3.h"

using namespace jonoondb_api;

void GuardFuncs::DisposeField(Field* field) {
  if (field != nullptr)
    field->Dispose();
}

void GuardFuncs::SQLite3Close(sqlite3* db) {
  if (db != nullptr) {
    int code = sqlite3_close(db);
    assert(code == SQLITE_OK);
    if (code == SQLITE_BUSY) {
      //Todo: Handle SQLITE_BUSY response here
    }    
  }
}