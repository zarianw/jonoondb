#include "jonoondb_api/delete_vector.h"
#include "jonoondb_api/document_collection.h"
#include "jonoondb_api/jonoondb_exceptions.h"
#include "jonoondb_api/sqlite_utils.h"
#include "jonoondb_api/guard_funcs.h"
#include "sqlite3.h"

using namespace std;
using namespace boost::filesystem;
using namespace gsl;
using namespace jonoondb_api;

DeleteVector::DeleteVector(const string& dbPath, const string& dbName,
                           const string& collectionName, bool
                           createDBIfMissing, uint64_t nextDocId)
    : m_collectionName(collectionName), m_nextDocumentId(nextDocId),
      m_dbConnection(nullptr, GuardFuncs::SQLite3Close),
      m_updateStmt(nullptr, GuardFuncs::SQLite3Finalize),
      m_selectStmt(nullptr, GuardFuncs::SQLite3Finalize) {
  path normalizedPath;
  m_dbConnection = SQLiteUtils::NormalizePathAndCreateDBConnection(
      dbPath, dbName, createDBIfMissing, normalizedPath);

  InitializeTableAndStatements();

  auto count = GetRowCount();

  if (count == 0) {
    InsertEmptyDeleteVector();
  } else {
    LoadBitmap();
  }

  m_isDirty = m_nextDocumentId > 0;
}

void DeleteVector::OnDocumentDeleted(uint64_t docId) {
  assert(docId < m_nextDocumentId);
  assert (m_deletedDocIds.find(docId) == m_deletedDocIds.end());

  m_deletedDocIds.insert(docId);

  if (*m_deletedDocIds.rbegin() == docId) {
    m_deleteVecSerializationBitmap.Add(docId);
  } else {
    m_deleteVecSerializationBitmap.Reset();
    for (auto id : m_deletedDocIds) {
      m_deleteVecSerializationBitmap.Add(id);
    }
  }

  try {
    StoreBitmap();
    m_isDirty = true;
  } catch (...) {
    // rollback in-memory changes
    m_deletedDocIds.erase(docId);
    m_deleteVecSerializationBitmap.Reset();
    for (auto id : m_deletedDocIds) {
      m_deleteVecSerializationBitmap.Add(id);
    }
    throw;
  }
}

const MamaJenniesBitmap& DeleteVector::GetDeleteVectorBitmap() {
  MaybeReconstructBitmap();
  return m_deleteVecBitmap;
}

void DeleteVector::OnDocumentsInserted(std::uint64_t nextDocId) {
  assert(nextDocId > m_nextDocumentId);
  m_nextDocumentId = nextDocId;
  m_isDirty = true;
}

void DeleteVector::MaybeReconstructBitmap() {
  if (m_isDirty) {
    m_deleteVecBitmap.Reset();
    for (auto& id : m_deletedDocIds) {
      m_deleteVecBitmap.Add(id);
    }
    // Its important to add a id that is last_doc_id + 1 at the end of the
    // bitmap so that the length of bitmap is correct
    m_deleteVecBitmap.Add(m_nextDocumentId);
    m_deleteVecBitmap.InPlaceLogicalNOT();
    m_isDirty = false;
  }
}

void DeleteVector::InsertEmptyDeleteVector() {
  sqlite3_stmt* stmt = nullptr;
  int sqliteCode = sqlite3_prepare_v2(m_dbConnection.get(),
                                      "INSERT INTO CollectionDeleteVector (CollectionName, "
                                      "DeleteVectorType, Version, DeleteVectorData) VALUES (?, ?, 1, NULL)",  // stmt
                                      -1,
                                      &stmt,
                                      0);
  std::unique_ptr<sqlite3_stmt, void (*)(sqlite3_stmt*)> statementGuard(
      stmt, GuardFuncs::SQLite3Finalize);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  sqliteCode = sqlite3_bind_text(stmt,
                                 1,  // Index of wildcard
                                 m_collectionName.c_str(),
                                 -1,  // -1 means go until NULL char
                                 SQLITE_STATIC);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  sqliteCode = sqlite3_bind_int(
      stmt,
      2,  // Index of wildcard
      static_cast<int32_t>(m_deleteVecSerializationBitmap.GetType()));
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  // Now insert the record
  sqliteCode = sqlite3_step(stmt);
  if (sqliteCode != SQLITE_DONE) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);
  }
}

void DeleteVector::StoreBitmap() {
  // store bitmap persistently
  m_deleteVecSerializationBitmap.Serialize(m_bitmapBuffer);
  std::unique_ptr<sqlite3_stmt, void (*)(sqlite3_stmt*)> statementGuard(
      m_updateStmt.get(), SQLiteUtils::ClearAndResetStatement);

  int sqliteCode = sqlite3_bind_int(
      m_updateStmt.get(),
      1,  // Index of wildcard
      static_cast<int32_t>(m_deleteVecSerializationBitmap.GetType()));
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  sqliteCode = sqlite3_bind_blob64(m_updateStmt.get(),
                                   2,  // Index of wildcard
                                   m_bitmapBuffer.GetData(),
                                   m_bitmapBuffer.GetLength(), SQLITE_STATIC);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  sqliteCode = sqlite3_bind_text(m_updateStmt.get(),
                                 3,  // Index of wildcard
                                 m_collectionName.c_str(),
                                 -1,  // -1 means go until NULL char
                                 SQLITE_STATIC);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  // Now update the record
  sqliteCode = sqlite3_step(m_updateStmt.get());
  if (sqliteCode != SQLITE_DONE) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);
  }
}

void DeleteVector::LoadBitmap() {
  std::unique_ptr<sqlite3_stmt, void (*)(sqlite3_stmt*)> statementGuard(
      m_selectStmt.get(), SQLiteUtils::ClearAndResetStatement);

  int sqliteCode = sqlite3_bind_text(m_selectStmt.get(), 1, m_collectionName.c_str(), -1, SQLITE_STATIC);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  // Now read the record
  sqliteCode = sqlite3_step(m_selectStmt.get());
  if (sqliteCode == SQLITE_ROW) {
    BitmapType type = static_cast<BitmapType>(sqlite3_column_int(m_selectStmt.get(), 0));
    int version = sqlite3_column_int(m_selectStmt.get(), 1);

    const char* data = reinterpret_cast<const char*>(sqlite3_column_blob(m_selectStmt.get(), 2));
    if (data != nullptr) {
      int length = sqlite3_column_bytes(m_selectStmt.get(), 2);
      span<const char> s(data, length);
      m_deleteVecSerializationBitmap.Deserialize(type, version, s);
      for (auto docId : m_deleteVecSerializationBitmap) {
        m_deletedDocIds.insert(docId);
      }
    }
  } else if (sqliteCode == SQLITE_DONE) {
    ostringstream ss;
    ss << "Bitmap not found for collection " << m_collectionName << ".";
    throw JonoonDBException(ss.str(), __FILE__, __func__,
                            __LINE__);
  } else {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);
  }
}

void DeleteVector::InitializeTableAndStatements() {
  // create the necessary table if it does not exist
  string sql =
      "CREATE TABLE IF NOT EXISTS CollectionDeleteVector ("
      "CollectionName TEXT PRIMARY KEY,"
      "DeleteVectorType INT,"
      "Version INT,"
      "DeleteVectorData BLOB)";

  int sqliteCode = sqlite3_exec(m_dbConnection.get(), sql.c_str(), nullptr,
                                nullptr, nullptr);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  sqlite3_stmt* stmt = nullptr;
  sqliteCode = sqlite3_prepare_v2(m_dbConnection.get(),
                                  "UPDATE CollectionDeleteVector "
                                  "SET DeleteVectorType = ?, "
                                  "DeleteVectorData = ? "
                                  "WHERE CollectionName = ?",
                                  -1,
                                  &stmt,
                                  0);
  m_updateStmt.reset(stmt);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  stmt = nullptr;
  sqliteCode = sqlite3_prepare_v2(m_dbConnection.get(),
                                  "SELECT DeleteVectorType, Version, DeleteVectorData "
                                  "FROM CollectionDeleteVector "
                                  "WHERE CollectionName = ?",
                                  -1,
                                  &stmt,
                                  0);
  m_selectStmt.reset(stmt);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);
}

int64_t DeleteVector::GetRowCount() {
  sqlite3_stmt* stmt = nullptr;
  int sqliteCode = sqlite3_prepare_v2(m_dbConnection.get(),
                                      "SELECT COUNT(*) FROM CollectionDeleteVector "
                                      "WHERE CollectionName = ?",  // stmt
                                      -1,
                                      &stmt,
                                      0);
  std::unique_ptr<sqlite3_stmt, void (*)(sqlite3_stmt*)> statementGuard(
      stmt, GuardFuncs::SQLite3Finalize);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  sqliteCode = sqlite3_bind_text(stmt, 1, m_collectionName.c_str(), -1, SQLITE_STATIC);
  SQLiteUtils::HandleSQLiteCode(sqliteCode);

  sqliteCode = sqlite3_step(stmt);
  if (sqliteCode != SQLITE_ROW) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);
  }

  return sqlite3_column_int64(stmt, 0);
}


