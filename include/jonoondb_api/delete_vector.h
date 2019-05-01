#pragma once

#include <set>
#include <cstdint>
#include "jonoondb_api/mama_jennies_bitmap.h"
#include "jonoondb_api/buffer_impl.h"
#include "jonoondb_api/guard_funcs.h"

// Forward declarations
struct sqlite3;
struct sqlite3_stmt;

namespace jonoondb_api {
class DocumentCollection;

class DeleteVector final {
 public:
  DeleteVector(const std::string& dbPath, const std::string& dbName,
               const std::string& collectionName, bool createDBIfMissing,
               std::uint64_t nextDocId);
  DeleteVector(const DeleteVector&) = delete;
  DeleteVector(DeleteVector&&) = delete;
  DeleteVector& operator=(const DeleteVector&) = delete;
  DeleteVector& operator=(DeleteVector&&) = delete;

  void OnDocumentDeleted(std::uint64_t docId);
  const MamaJenniesBitmap& GetDeleteVectorBitmap();
  void OnDocumentsInserted(std::uint64_t nextDocId);
 private:
  void MaybeReconstructBitmap();
  void InsertEmptyDeleteVector();
  void StoreBitmap();
  void LoadBitmap();
  void InitializeTableAndStatements();
  std::int64_t GetRowCount();

  // keep m_dbConnection as first member so it gets destructed in the end, stmts should be destructed first
  std::unique_ptr<sqlite3, void (*)(sqlite3*)> m_dbConnection;
  std::set<std::uint64_t> m_deletedDocIds;
  MamaJenniesBitmap m_deleteVecBitmap;
  MamaJenniesBitmap m_deleteVecSerializationBitmap;
  bool m_isDirty;
  // m_nextDocumentId is equal to next id that will be assigned to a new document inserted into collection
  std::uint64_t m_nextDocumentId;
  std::unique_ptr<sqlite3_stmt, void (*)(sqlite3_stmt*)> m_updateStmt;
  std::unique_ptr<sqlite3_stmt, void (*)(sqlite3_stmt*)> m_selectStmt;
  std::string m_collectionName;
  BufferImpl m_bitmapBuffer;
};
} // namespace jonoondb_api
