#pragma once
#include <string>
#include "enums.h"
#include "field.h"
#include "flatbuffers/reflection.h"

namespace jonoondb_api {
class FlatbuffersField final : public Field {
 public:
  const std::string GetName() const override;
  FieldType GetType() const override;
  FieldType GetElementType() const override;
  std::size_t GetSubFieldCount() const override;
  void GetSubField(size_t index, Field*& field) const override;
  Field* AllocateField() const override;
  void Dispose() override;
  void SetMembers(reflection::Field* val, reflection::Schema* valSch);

 private:
  reflection::Field* m_fieldDef = nullptr;
  reflection::Schema* m_schema = nullptr;
};
}  // namespace jonoondb_api
