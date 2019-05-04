#include <gtest/gtest.h>
#include <algorithm>
#include <functional>
#include "object_pool.h"

using namespace jonoondb_api;

class ObjectPoolTestObject {
 public:
  int Data;

  ObjectPoolTestObject* AllocateObjectPoolTestObject() {
    return new ObjectPoolTestObject();
  }

  ObjectPoolTestObject* AllocateNullObjectPoolTestObject() {
    return nullptr;
  }

  void DeallocateObjectPoolTestObject(ObjectPoolTestObject* obj) {
    delete obj;
  }
};

TEST(ObjectPool, Ctor_EmptyAllocationFunction) {
  ObjectPoolTestObject obj;
  // empty allocation function
  ASSERT_THROW(
      ObjectPool<ObjectPoolTestObject> pool(
          5, 10, std::function<ObjectPoolTestObject*()>(),
          std::bind(&ObjectPoolTestObject::DeallocateObjectPoolTestObject, obj,
                    std::placeholders::_1)),
      InvalidArgumentException);
}

TEST(ObjectPool, Ctor_EmptyDeallocationFunction) {
  ObjectPoolTestObject obj;
  // empty deallocation function
  ASSERT_THROW(
      ObjectPool<ObjectPoolTestObject> pool(
          5, 10,
          std::bind(&ObjectPoolTestObject::AllocateObjectPoolTestObject, obj),
          std::function<void(ObjectPoolTestObject*)>()),
      InvalidArgumentException);
}

TEST(ObjectPool, Ctor_PoolCapacityZero) {
  ObjectPoolTestObject obj;
  // max cap == 0 so throw
  ASSERT_THROW(
      ObjectPool<ObjectPoolTestObject> pool(
          5, 0,
          std::bind(&ObjectPoolTestObject::AllocateObjectPoolTestObject, obj),
          std::bind(&ObjectPoolTestObject::DeallocateObjectPoolTestObject, obj,
                    std::placeholders::_1)),
      InvalidArgumentException);
}

TEST(ObjectPool, Ctor_MaxCapLessThanInitCap) {
  ObjectPoolTestObject obj;
  // max cap < initCap so returns false
  ASSERT_THROW(
      ObjectPool<ObjectPoolTestObject> pool(
          10, 5,
          std::bind(&ObjectPoolTestObject::AllocateObjectPoolTestObject, obj),
          std::bind(&ObjectPoolTestObject::DeallocateObjectPoolTestObject, obj,
                    std::placeholders::_1)),
      InvalidArgumentException);
}

TEST(ObjectPool, Ctor_ValidConstruction) {
  ObjectPoolTestObject obj;
  ASSERT_NO_THROW(ObjectPool<ObjectPoolTestObject> pool(
      5, 10,
      std::bind(&ObjectPoolTestObject::AllocateObjectPoolTestObject, obj),
      std::bind(&ObjectPoolTestObject::DeallocateObjectPoolTestObject, obj,
                std::placeholders::_1)));
}

TEST(ObjectPool, Ctor_FaultyAllocator) {
  ObjectPoolTestObject obj;
  ASSERT_THROW(
      ObjectPool<ObjectPoolTestObject> pool(
          5, 10,
          std::bind(&ObjectPoolTestObject::AllocateNullObjectPoolTestObject,
                    obj),
          std::bind(&ObjectPoolTestObject::DeallocateObjectPoolTestObject, obj,
                    std::placeholders::_1)),
      JonoonDBException);
}

TEST(ObjectPool, Take) {
  ObjectPoolTestObject obj;
  ObjectPool<ObjectPoolTestObject> pool(
      5, 10,
      std::bind(&ObjectPoolTestObject::AllocateObjectPoolTestObject, obj),
      std::bind(&ObjectPoolTestObject::DeallocateObjectPoolTestObject, obj,
                std::placeholders::_1));

  for (size_t i = 0; i < 10; i++) {
    auto val = pool.Take();
    ASSERT_NE(val, nullptr);
  }
}

TEST(ObjectPool, Take_BeyondCapacity) {
  ObjectPoolTestObject obj;
  ObjectPool<ObjectPoolTestObject> pool(
      5, 10,
      std::bind(&ObjectPoolTestObject::AllocateObjectPoolTestObject, obj),
      std::bind(&ObjectPoolTestObject::DeallocateObjectPoolTestObject, obj,
                std::placeholders::_1));

  for (size_t i = 0; i < 20; i++) {
    auto val = pool.Take();
    ASSERT_NE(val, nullptr);
  }
}

TEST(ObjectPool, Return) {
  ObjectPoolTestObject obj;
  ObjectPool<ObjectPoolTestObject> pool(
      5, 10,
      std::bind(&ObjectPoolTestObject::AllocateObjectPoolTestObject, obj),
      std::bind(&ObjectPoolTestObject::DeallocateObjectPoolTestObject, obj,
                std::placeholders::_1));
  std::vector<ObjectPoolTestObject*> objects;
  for (size_t i = 0; i < 10; i++) {
    auto val = pool.Take();
    objects.push_back(val);
  }

  for (size_t i = 0; i < 10; i++) {
    pool.Return(objects[i]);
  }

  // Now again take these objects and make sure they are one of the objects
  // returned the first time around.
  for (size_t i = 0; i < 10; i++) {
    auto val = pool.Take();
    auto iter = std::find(objects.begin(), objects.end(), val);
    ASSERT_NE(std::find(objects.begin(), objects.end(), val), objects.end());
  }
}
