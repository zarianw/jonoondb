#include "sqlite3.h"
#include "jonoondb_api/delete_vector.h"
#include "jonoondb_api/document_collection.h"
#include "jonoondb_api/jonoondb_exceptions.h"
#include "jonoondb_api/sqlite_utils.h"

using namespace std;
using namespace jonoondb_api;
using namespace boost::filesystem;

DeleteVector::DeleteVector(const string& dbPath, const string& dbName,
                           const string& collectionName, bool createDBIfMissing)
    : m_collectionName(collectionName),
      m_dbConnection(nullptr, GuardFuncs::SQLite3Close),
      m_insertStmt(nullptr, GuardFuncs::SQLite3Finalize),
      m_selectStmt(nullptr, GuardFuncs::SQLite3Finalize) {
  path normalizedPath;
  m_dbConnection = SQLiteUtils::NormalizePathAndCreateDBConnection(
      dbPath, dbName, createDBIfMissing, normalizedPath);

  // create the necessary table if it does not exist
  string sql =
      "CREATE TABLE IF NOT EXISTS CollectionDeleteVector ("
      "CollectionName TEXT PRIMARY KEY,"
      "DeleteVectorType INT,"
      "DeleteVector BLOB)";

  int sqliteCode = sqlite3_exec(m_dbConnection.get(), sql.c_str(), nullptr,
                                nullptr, nullptr);
  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);

  sqlite3_stmt* stmt = nullptr;
  sqliteCode = sqlite3_prepare_v2(
      m_dbConnection.get(),
      "INSERT INTO CollectionDeleteVector (CollectionName, "
      "DeleteVectorType, DeleteVector) VALUES (?, ?, ?)",  // stmt
      -1,     // If greater than zero, then stmt is read up to
              // the first null terminator
      &stmt,  // Statement that is to be prepared
      0       // Pointer to unused portion of stmt
  );
  m_insertStmt.reset(stmt);

  if (sqliteCode != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);
  }

  stmt = nullptr;
  sqliteCode = sqlite3_prepare_v2(
      m_dbConnection.get(),
      "SELECT DeleteVectorType, DeleteVectorType FROM CollectionDeleteVector "
      "WHERE CollectionName = ?",  // stmt
      -1,     // If greater than zero, then stmt is read up to
              // the first null terminator
      &stmt,  // Statement that is to be prepared
      0       // Pointer to unused portion of stmt
  );
  m_selectStmt.reset(stmt);

  if (sqliteCode != SQLITE_OK) {
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);
  }
}

void DeleteVector::Add(uint64_t docId) {
	assert(m_deletedDocIds.find(docId) == m_deletedDocIds.end());
	m_deletedDocIds.insert(docId);
	if (*m_deletedDocIds.rbegin() == docId) {
		m_deleteVecSerializationBitmap.Add(docId);
	} else {
		m_deleteVecSerializationBitmap.Reset();
		for (auto id : m_deletedDocIds) {
			m_deleteVecSerializationBitmap.Add(id);
		}
	}
	StoreBitmap();
	m_isDirty = true;
}

const MamaJenniesBitmap& DeleteVector::GetDeleteVectorBitmap() {
  MaybeReconstructBitmap();
  return m_deleteVecBitmap;
}

bool DeleteVector::Empty() { return m_deletedDocIds.empty(); }

void DeleteVector::UpdateNextDocumentId(std::uint64_t docId) {
  m_nextDocumentId = docId;
  m_isDirty = true;
}

void DeleteVector::MaybeReconstructBitmap() {
  if (m_isDirty) {
    m_deleteVecBitmap.Reset();
    for (auto& id : m_deletedDocIds) {
      m_deleteVecBitmap.Add(id);
    }
    // Its important to add a id that is last_doc_id + 1 at the end of the
    // bitmap so that the size of the total length of bitmap is correct
    m_deleteVecBitmap.Add(m_nextDocumentId);
		m_deleteVecBitmap.InPlaceLogicalNOT();
    m_isDirty = false;
  }
}

void DeleteVector::StoreBitmap() {
	m_deleteVecSerializationBitmap.Serialize(m_bitmapBuffer);
	// store this buffer persistently
  // statement guard will make sure that the statement is cleared and reset when
  // statementGuard object goes out of scope
  std::unique_ptr<sqlite3_stmt, void (*)(sqlite3_stmt*)> statementGuard(
      m_insertStmt.get(), SQLiteUtils::ClearAndResetStatement);

  /*int sqliteCode = sqlite3_bind_text(m_putStatement,
                                     1,  // Index of wildcard
                                     m_collectionName.c_str(),
                                     -1,  // -1 means go until NULL char
                                     SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);

  sqliteCode = sqlite3_bind_int(m_putStatement,
                                2,  // Index of wildcard
                                fileKey);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);

  sqliteCode = sqlite3_bind_text(m_putStatement,
                                 3,  // Index of wildcard
                                 fileName.c_str(),
                                 -1,  // -1 means go until NULL char
                                 SQLITE_STATIC);

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);

  sqliteCode = sqlite3_bind_int64(
      m_putStatement,
      4,    // Index of wildcard
      -1);  // Default value, -1 represents that the length has not been set

  if (sqliteCode != SQLITE_OK)
    throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                       __LINE__);

  // Now insert the record
  sqliteCode = sqlite3_step(m_putStatement);
  if (sqliteCode != SQLITE_DONE) {
    if (sqliteCode == SQLITE_CONSTRAINT) {
      // Key already exists
      std::ostringstream ss;
      ss << "The specified file key '" << fileKey
         << "' already exists for collection '" << m_collectionName << "'.";
      throw JonoonDBException(ss.str(), __FILE__, __func__, __LINE__);
    } else {
      throw SQLException(sqlite3_errstr(sqliteCode), __FILE__, __func__,
                         __LINE__);
    }
  }*/
}
