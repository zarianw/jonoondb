#pragma once

#include <cstddef>
#include <cstdint>

namespace jonoondb_api {
class Status;

class ResultSet {
public:
  Status Next();
  Status GetInt8(const char* columnName, std::int8_t& val) const;    
  Status GetInt16(const char* fieldName, std::int16_t& val) const;    
  Status GetInt32(const char* fieldName, std::int32_t& val) const;    
  Status GetInt64(const char* fieldName, std::int64_t& val) const;    

  Status GetUInt8(const char* fieldName, uint8_t& val) const;    
  Status GetUInt16(const char* fieldName, uint16_t& val) const;    
  Status GetUInt32(const char* fieldName, uint32_t& val) const;    
  Status GetUInt64(const char* fieldName, uint64_t& val) const;    

  Status GetFloat(const char* fieldName, float& val) const;    
  Status GetDouble(const char* fieldName, double& val) const;    

  Status GetStringValue(const char* fieldName, char*& val) const;   
};
} // jonoondb_api