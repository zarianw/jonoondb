#pragma once

#include <stdlib.h>
#include <functional>
#include <list>
#include <mutex>
#include <vector>
#include "jonoondb_exceptions.h"

namespace jonoondb_api {
template <typename ObjectType>
class ObjectPool final {
 public:
  ObjectPool(int poolInitialiSize, int poolCapacity,
             std::function<ObjectType*()> objectAllocatorFunc,
             std::function<void(ObjectType*)> objectDeallocatorFunc,
             std::function<void(ObjectType*)> objectResetFunc =
                 std::function<void(ObjectType*)>())
      : m_poolInitialiSize(poolInitialiSize),
        m_poolCapacity(poolCapacity),
        m_objectAllocatorFunc(objectAllocatorFunc),
        m_objectDeallocatorFunc(objectDeallocatorFunc),
        m_currentObjectIndex(-1) {
    if (m_poolCapacity == 0 || m_poolCapacity < m_poolInitialiSize) {
      throw InvalidArgumentException(
          "Argument poolCapacity cannot be 0 or less than initialPoolSize.",
          __FILE__, __func__, __LINE__);
    }

    if (!m_objectAllocatorFunc) {
      throw InvalidArgumentException(
          "Argument objectAllocatorFunc cannot be empty.", __FILE__, __func__,
          __LINE__);
    }

    if (!m_objectDeallocatorFunc) {
      throw InvalidArgumentException(
          "Argument objectDeallocatorFunc cannot be empty.", __FILE__, __func__,
          __LINE__);
    }

    if (objectResetFunc) {
      m_objectResetFunc = objectResetFunc;
    }

    m_objects.resize(m_poolCapacity, nullptr);

    try {
      for (int i = 0; i < m_poolInitialiSize; i++) {
        m_objects[i] = InvokeObjectAllocatorFunc();
        if (m_objects[i] == nullptr) {
          throw JonoonDBException(
              "Object allocation failed. ObjectAllocatorFunc returned nullptr.",
              __FILE__, __func__, __LINE__);
        }
      }
      m_currentObjectIndex = m_poolInitialiSize - 1;
    } catch (...) {
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
      // The lock will be released when lock goes out of scope
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_currentObjectIndex > -1) {
        ObjectType* obj = m_objects[m_currentObjectIndex];
        m_objects[m_currentObjectIndex] = nullptr;
        m_currentObjectIndex--;
        return obj;
      }
    }

    // If we are here the we have exhausted all objects of the pool.
    // Construct a new object and hand it to the consumer
    return InvokeObjectAllocatorFunc();
  }

  void Return(ObjectType* object) {
    {
      // The lock will be released when lock goes out of scope
      std::lock_guard<std::mutex> lock(m_mutex);
      if (m_currentObjectIndex < (m_poolCapacity - 1)) {
        InvokeObjectResetFunc(object);
        m_currentObjectIndex++;
        m_objects[m_currentObjectIndex] = object;
        return;
      }
    }

    // Drop this object to the floor and continue.
    // This will only happen if pool is at max capacity.
    InvokeObjectDeallocatorFunc(object);
  }

  ~ObjectPool() {
    for (int i = 0; i <= m_currentObjectIndex; i++) {
      InvokeObjectDeallocatorFunc(m_objects[i]);
      m_objects[i] = nullptr;
    }
  }

 private:
  inline ObjectType* InvokeObjectAllocatorFunc() {
    return m_objectAllocatorFunc();
  }

  inline void InvokeObjectDeallocatorFunc(ObjectType* obj) {
    m_objectDeallocatorFunc(obj);
  }

  inline void InvokeObjectResetFunc(ObjectType* obj) {
    if (m_objectResetFunc) {
      m_objectResetFunc(obj);
    }
  }

  std::vector<ObjectType*> m_objects;
  int m_poolInitialiSize;
  int m_poolCapacity;
  std::function<ObjectType*()> m_objectAllocatorFunc;
  std::function<void(ObjectType*)> m_objectDeallocatorFunc;
  std::function<void(ObjectType*)> m_objectResetFunc;
  int m_currentObjectIndex;
  std::mutex m_mutex;
};

template <typename ObjectType>
class ObjectPoolGuard {
 public:
  ObjectPoolGuard() : m_pool(nullptr), m_obj(nullptr) {}

  ObjectPoolGuard(ObjectPool<ObjectType>* pool, ObjectType* obj)
      : m_pool(pool), m_obj(obj) {}

  ObjectPoolGuard(ObjectPoolGuard&& other) {
    if (this != &other) {
      this->m_pool = other.m_pool;
      this->m_obj = other.m_obj;

      other.m_pool = nullptr;
      other.m_obj = nullptr;
    }
  }

  ObjectPoolGuard& operator=(ObjectPoolGuard&& other) {
    if (this != &other) {
      if (m_pool && m_obj) {
        m_pool->Return(m_obj);
      }

      this->m_pool = other.m_pool;
      this->m_obj = other.m_obj;

      other.m_pool = nullptr;
      other.m_obj = nullptr;
    }

    return *this;
  }

  ObjectPoolGuard(const ObjectPoolGuard&) = delete;
  ObjectPoolGuard& operator=(const ObjectPoolGuard&) = delete;

  ~ObjectPoolGuard() {
    if (m_pool && m_obj) {
      m_pool->Return(m_obj);
    }
  }

  // implicit conversion from ObjectPoolGuard to Object
  operator ObjectType*() {
    return m_obj;
  }

 private:
  ObjectPool<ObjectType>* m_pool;
  ObjectType* m_obj;
};
}  // namespace jonoondb_api