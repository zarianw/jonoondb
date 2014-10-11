#include <string>
#include "boost/filesystem.hpp"
#include "sqlite3.h"
#include "document_collection.h"
#include "status.h"
#include "string_utils.h"
#include "exception_utils.h"
#include "sqlite_utils.h"

using namespace jonoondb_api;
using namespace jonoon_utils;
using namespace std;

DocumentCollection::DocumentCollection(sqlite3* dbConnection) : m_dbConnection(dbConnection)
{
}

DocumentCollection::~DocumentCollection()
{
}

Status DocumentCollection::Construct(
	const char* databaseMetadataFilePath,
	const char* name,
	int schemaType,
	const char* schema,
	const IndexInfo indexes[],
	int indexesLength,
	DocumentCollection*& documentCollection)
{
	string errorMessage;
	// Validate function arguments
	if (StringUtils::IsNullOrEmpty(databaseMetadataFilePath))
	{
		errorMessage = "Argument databaseMetadataFilePath is null or empty.";
		return Status(Status::InvalidArgumentCode, errorMessage.c_str(), (int32_t)errorMessage.length());
	}	

	// databaseMetadataFile should exist and all the tables should exist in it
	if (!boost::filesystem::exists(databaseMetadataFilePath))
	{
		return Status(Status::MissingDatabaseFileCode, errorMessage.c_str(), (int32_t)errorMessage.length());
	}

	sqlite3* dbConnection;
	int sqliteCode = sqlite3_open(databaseMetadataFilePath, &dbConnection);//, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, nullptr);	

	if (sqliteCode != SQLITE_OK)
	{
		errorMessage = ExceptionUtils::GetSQLiteErrorFromSQLiteErrorCode(sqliteCode);
		SQLiteUtils::CloseSQLiteConnection(dbConnection);
		return Status(Status::FailedToOpenMetadataDatabaseFileCode, errorMessage.c_str(), errorMessage.length());
	}	

	documentCollection = new DocumentCollection(dbConnection);

	return Status();
}

Status DocumentCollection::Insert(Buffer& documentData)
{
  return Status();
}
