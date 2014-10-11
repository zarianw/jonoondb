#pragma once

namespace jonoondb_api
{
  inline void StandardDelete(char* ptr)
  {
    delete ptr;
  }

  inline void StandardDeleteNoOp(char* ptr)
  {
  }
} // jonoondb_api