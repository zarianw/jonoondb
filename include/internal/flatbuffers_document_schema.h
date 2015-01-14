#pragma once

#include <cstdint>
#include "document_schema.h"

namespace jonoondb_api {
//Forward declaration
class Status;
enum class ColumnType : std::int16_t;

class FlatbuffersDocumentSchema final : public DocumentSchema {
public:
  Status Construct(const char* schemaText);
  virtual const char* GetSchemaText() override;
  virtual Status GetColumnType(const char* columnName, ColumnType& columnType) override;
private:
  FlatbuffersDocumentSchema();
  FlatbuffersDocumentSchema(const FlatbuffersDocumentSchema&) = delete;
  FlatbuffersDocumentSchema(FlatbuffersDocumentSchema&&) = delete;
};
} // jonoondb_api