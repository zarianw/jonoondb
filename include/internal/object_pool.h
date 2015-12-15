#pragma once

#include <stdlib.h>
#include <list>
#include <vector>
#include <mutex>
#include "jonoondb_exceptions.h"

namespace jonoondb_api {
template<class ObjectType> class ObjectPool {
public:
  typedef ObjectType*(*ObjectAllocatorFunc)();
  typedef void(*ObjectDeallocatorFunc)(ObjectType*);
  typedef void(*ObjectResetFunc)(ObjectType*);

  ObjectPool(int poolInitialiSize, int poolCapacity, ObjectAllocatorFunc objectAllocatorFunc, ObjectDeallocatorFunc objectDeallocatorFunc, ObjectResetFunc objectResetFunc) :
    m_poolInitialiSize(poolInitialiSize), m_poolCapacity(poolCapacity), m_objectAllocatorFunc(objectAllocatorFunc),
    m_objectDeallocatorFunc(objectDeallocatorFunc), m_objectResetFunc(objectResetFunc), m_initialized(false), m_currentObjectIndex(-1) {
    if (m_poolCapacity == 0 || m_poolCapacity < m_poolInitialiSize) {
      throw InvalidArgumentException("Argument poolCapacity cannot be 0 or less than initialPoolSize.",
        __FILE__, "", __LINE__);
    }

    m_objects.resize(m_poolCapacity);

    try {
      if (m_objectAllocatorFunc != nullptr) {
        for (int i = 0; i < m_poolInitialiSize; i++) {
          m_objects[i] = InvokeObjectAllocatorFunc();
          if (m_objects[i] == nullptr) {
            throw JonoonDBException("Object allocation failed.");
          }
        }
      }

      m_currentObjectIndex = m_poolInitialiSize - 1;
    } catch {
      for (int i = 0; i <= m_currentObjectIndex; i++) {
        if (m_objects[i] != nullptr) {
          InvokeObjectDeallocatorFunc(m_objects[i]);
        }
      }

      throw;
    }
  }

  ObjectType* Take() {
    {
      //The lock will be released when lock goes out of scope
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_currentObjectIndex > -1) {
        ObjectType* obj = m_objects[m_currentObjectIndex];
        m_objects[m_currentObjectIndex] = nullptr;
        m_currentObjectIndex--;
        return obj;
      }
    }

    //If we are here the we have exhausted all objects of the pool.
    //Construct a new object and hand it to the consumer
    return InvokeObjectAllocatorFunc();
  }

  void Return(ObjectType* object) {
    {
      //The lock will be released when lock goes out of scope
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_currentObjectIndex < (m_poolCapacity - 1)) {
        InvokeObjectResetFunc(object);
        m_currentObjectIndex++;
        m_objects[m_currentObjectIndex] = object;
        return;
      }
    }

    //Drop this object to the floor and continue.
    //This will only happen if pool is at max capacity. 
    InvokeObjectDeallocatorFunc(object);
  }

  ~ObjectPool() {
    for (int i = 0; i <= m_currentObjectIndex; i++) {
      InvokeObjectDeallocatorFunc(m_objects[i]);
    }
  }

private:
  std::vector<ObjectType*> m_objects;
  int m_poolInitialiSize;
  int m_poolCapacity;
  ObjectAllocatorFunc m_objectAllocatorFunc;
  ObjectDeallocatorFunc m_objectDeallocatorFunc;
  ObjectResetFunc m_objectResetFunc;
  int m_currentObjectIndex;
  std::mutex m_mutex;

  inline ObjectType* InvokeObjectAllocatorFunc() {
    if (m_objectAllocatorFunc == nullptr) {
      return nullptr;
    }

    return m_objectAllocatorFunc();
  }

  inline void InvokeObjectDeallocatorFunc(ObjectType* obj) {
    if (m_objectDeallocatorFunc != nullptr) {
      m_objectDeallocatorFunc(obj);
    }
  }

  inline void InvokeObjectResetFunc(ObjectType* obj) {
    if (m_objectResetFunc != nullptr) {
      m_objectResetFunc(obj);
    }
  }
};
} // namespace jonoondb_api