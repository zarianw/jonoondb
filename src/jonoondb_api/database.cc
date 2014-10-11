#include "database.h"
#include "status.h"
#include "database_impl.h"
#include "buffer.h"

using namespace jonoondb_api;

Database::Database(DatabaseImpl* databaseImpl) : m_databaseImpl(databaseImpl)
{	
}

Status Database::Open(const char* dbPath, const char* dbName, const Options& options, Database** db)
{
	DatabaseImpl* dbImpl;
	Status status = DatabaseImpl::Open(dbPath, dbName, options, &dbImpl);
	if (!status.OK())
	{
		return status;
	}

	*db = new Database(dbImpl);

	return status;
}

Status Database::Insert(const char* collectionName, Buffer& documentData)
{
  return Status();
}

