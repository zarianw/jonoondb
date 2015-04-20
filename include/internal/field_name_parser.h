#pragma once

#include <string>

namespace jonoondb_api {
class FieldNameParser {
public:
  FieldNameParser(const char* fieldName);
  bool MoveNext();
  const char* GetCurrentFieldName();
  bool IsCurrentFieldOfTypeVector();

private:
  std::string m_fieldName;
};
} // jonoondb_api