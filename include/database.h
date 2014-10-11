#pragma once

#include <stdint.h>

namespace jonoondb_api
{
	//Forward Declarations
	class DatabaseImpl;
	class Status;
	class Options;
	class IndexInfo;
  class Buffer;

	class Database
	{
	public:
		~Database();

		static Status Open(const char* dbPath, const char* dbName, const Options& options, Database** db);
		Status CreateCollection(const char* name, int schemaType, const char* schema, const IndexInfo indexes[], int indexesLength);
    Status Insert(const char* collectionName, Buffer& documentData);

		//Status Close();			

	private:
		Database(DatabaseImpl* databaseImpl);
		DatabaseImpl* m_databaseImpl;
	};

} // jonoondb_api