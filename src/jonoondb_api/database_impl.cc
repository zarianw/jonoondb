#include "database_impl.h"
#include "status.h"
#include "database_metadata_manager.h"
#include "options.h"

using namespace jonoondb_api;

DatabaseImpl::DatabaseImpl()
{	
}

Status DatabaseImpl::Open(const char* dbPath, const char* dbName, const Options& options, DatabaseImpl** db)
{
	// Initialize DatabaseMetadataManager
	DatabaseMetadataManager* databaseMetadataManager;
	Status status = DatabaseMetadataManager::Open(dbPath, dbName, options.GetCreateDBIfMissing(), databaseMetadataManager);
	if (!status.OK())
	{
		return status;
	}	

	return status;
}

Status DatabaseImpl::CreateCollection(const char* name, int schemaType, const char* schema)
{
	// Add collection info in the database metadata

	return Status();
}

Status DatabaseImpl::Insert(const char* collectionName, Buffer& documentData)
{
  return Status();
}

