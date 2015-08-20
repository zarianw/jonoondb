#pragma once
#include <string>
#include "flatbuffers/idl.h"
#include "field.h"
#include "enums.h"

namespace jonoondb_api {
class FlatbuffersField final : public Field {
 public:
  const char* GetName() const override;
  FieldType GetType() const override;
  std::size_t GetSubFieldCount() const override;
  Status GetSubField(size_t index, Field*& field) const override;
  Status AllocateField(Field*& field) const override;
  void Dispose() override;
  void SetFieldDef(flatbuffers::FieldDef* val);
 private:
  flatbuffers::FieldDef* m_fieldDef = nullptr;
};
}  // jonoondb_api
