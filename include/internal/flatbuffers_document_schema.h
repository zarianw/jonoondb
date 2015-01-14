#pragma once

#include <cstdint>
#include "document_schema.h"

namespace jonoondb_api {
//Forward declaration
class Status;
enum class ColumnType : std::int32_t;

class FlatbuffersDocumentSchema final : public DocumentSchema {
public:
  ~FlatbuffersDocumentSchema() override;
  static Status Construct(const char* schemaText,
                   FlatbuffersDocumentSchema*& documentSchema);
  virtual const char* GetSchemaText() const override;
  virtual Status GetColumnType(const char* columnName, ColumnType& columnType) const override;
private:
  FlatbuffersDocumentSchema(const char* schemaText);
  FlatbuffersDocumentSchema(const FlatbuffersDocumentSchema&) = delete;
  FlatbuffersDocumentSchema(FlatbuffersDocumentSchema&&) = delete;
};
} // jonoondb_api