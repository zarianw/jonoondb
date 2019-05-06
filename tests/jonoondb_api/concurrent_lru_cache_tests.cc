#include <memory>
#include "concurrent_lru_cache.h"
#include "gtest/gtest.h"

using namespace std;
using namespace jonoondb_api;

class CacheObject {
 public:
  int Data;
};

TEST(ConcurrentLRUCache, AddAndFind) {
  const int arraySize = 5;
  shared_ptr<CacheObject> cacheObjectPtrs[arraySize];

  {
    ConcurrentLRUCache<int, CacheObject> lruCache(10);
    for (int i = 0; i < arraySize; i++) {
      cacheObjectPtrs[i].reset(new CacheObject());
      cacheObjectPtrs[i]->Data = i;
      lruCache.Add(cacheObjectPtrs[i]->Data,
                   shared_ptr<CacheObject>(cacheObjectPtrs[i]), true);
    }

    shared_ptr<CacheObject> foundObject;
    for (int i = 0; i < arraySize; i++) {
      ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
      ASSERT_TRUE(cacheObjectPtrs[i]->Data == foundObject->Data);
    }
  }
}

TEST(ConcurrentLRUCache, PerformEviction_Sequential) {
  const int arraySize = 20;
  shared_ptr<CacheObject> cacheObjectPtrs[arraySize];

  {
    int maxCacheSize = 10;
    ConcurrentLRUCache<int, CacheObject> lruCache(maxCacheSize);
    for (int i = 0; i < arraySize; i++) {
      cacheObjectPtrs[i].reset(new CacheObject());
      cacheObjectPtrs[i]->Data = i;
      lruCache.Add(cacheObjectPtrs[i]->Data,
                   shared_ptr<CacheObject>(cacheObjectPtrs[i]), true);
    }

    // Now perform eviction
    lruCache.PerformEviction();

    // Now try to get the items that should not be there
    shared_ptr<CacheObject> foundObject;
    for (int i = 0; i < maxCacheSize; i++) {
      ASSERT_TRUE(!lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
    }

    // Now try to get the items that should be there
    for (int i = maxCacheSize; i < arraySize; i++) {
      ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
      ASSERT_TRUE(cacheObjectPtrs[i]->Data == foundObject->Data);
    }
  }
}

TEST(ConcurrentLRUCache, PerformEviction_Random) {
  const int arraySize = 20;
  shared_ptr<CacheObject> cacheObjectPtrs[arraySize];

  {
    int maxCacheSize = 10;
    ConcurrentLRUCache<int, CacheObject> lruCache(maxCacheSize);
    for (int i = 0; i < arraySize; i++) {
      cacheObjectPtrs[i].reset(new CacheObject());
      cacheObjectPtrs[i]->Data = i;
      lruCache.Add(cacheObjectPtrs[i]->Data,
                   shared_ptr<CacheObject>(cacheObjectPtrs[i]), true);
    }

    // Now access all the even keys
    shared_ptr<CacheObject> foundObject;
    for (int i = 0; i < arraySize; i++) {
      if (i % 2 == 0) {
        ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
      }
    }

    // Now perform eviction
    lruCache.PerformEviction();

    // Now try to get the items that should and should not be there
    for (int i = 0; i < arraySize; i++) {
      if (i % 2 != 0) {
        ASSERT_TRUE(!lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
      } else {
        ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
        ASSERT_TRUE(cacheObjectPtrs[i]->Data == foundObject->Data);
      }
    }
  }
}

TEST(ConcurrentLRUCache, PerformEviction_Reverse) {
  const int arraySize = 20;
  shared_ptr<CacheObject> cacheObjectPtrs[arraySize];

  {
    int maxCacheSize = 10;
    ConcurrentLRUCache<int, CacheObject> lruCache(maxCacheSize);
    for (int i = 0; i < arraySize; i++) {
      cacheObjectPtrs[i].reset(new CacheObject());
      cacheObjectPtrs[i]->Data = i;
      lruCache.Add(cacheObjectPtrs[i]->Data,
                   shared_ptr<CacheObject>(cacheObjectPtrs[i]), true);
    }

    // Now access all the in reverse order keys
    shared_ptr<CacheObject> foundObject;
    for (int i = arraySize - 1; i > -1; i--) {
      ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
    }

    // Now perform eviction
    lruCache.PerformEviction();

    // Now try to get the items that should be there
    for (int i = 0; i < maxCacheSize; i++) {
      ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
      ASSERT_TRUE(cacheObjectPtrs[i]->Data == foundObject->Data);
    }

    // Now try to get the items that should not be there
    for (int i = maxCacheSize; i < arraySize; i++) {
      ASSERT_TRUE(!lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
    }
  }
}

TEST(ConcurrentLRUCache, PerformEviction_EvictableFlag) {
  const int arraySize = 20;
  shared_ptr<CacheObject> cacheObjectPtrs[arraySize];

  {
    int maxCacheSize = 10;
    ConcurrentLRUCache<int, CacheObject> lruCache(maxCacheSize);
    for (int i = 0; i < arraySize; i++) {
      cacheObjectPtrs[i].reset(new CacheObject());
      cacheObjectPtrs[i]->Data = i;
      if ((i + 1) % 5 == 0) {
        lruCache.Add(cacheObjectPtrs[i]->Data,
                     shared_ptr<CacheObject>(cacheObjectPtrs[i]), false);
      } else {
        lruCache.Add(cacheObjectPtrs[i]->Data,
                     shared_ptr<CacheObject>(cacheObjectPtrs[i]), true);
      }
    }

    // Now perform eviction
    lruCache.PerformEviction();

    // Now try to get the items that should not have been evicted
    shared_ptr<CacheObject> foundObject;
    for (int i = 0; i < maxCacheSize + 2; i++) {
      if ((i + 1) % 5 == 0) {
        ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
        ASSERT_TRUE(cacheObjectPtrs[i]->Data == foundObject->Data);
      } else {
        ASSERT_TRUE(!lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
      }
    }

    // Now try to get the items that should be there
    for (int i = maxCacheSize + 2; i < arraySize; i++) {
      ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
      ASSERT_TRUE(cacheObjectPtrs[i]->Data == foundObject->Data);
    }
  }
}

TEST(ConcurrentLRUCache, PerformEviction_EvictableFlagReset) {
  const int arraySize = 20;
  shared_ptr<CacheObject> cacheObjectPtrs[arraySize];

  {
    int maxCacheSize = 1;
    ConcurrentLRUCache<int, CacheObject> lruCache(maxCacheSize);
    for (int i = 0; i < arraySize; i++) {
      cacheObjectPtrs[i].reset(new CacheObject());
      cacheObjectPtrs[i]->Data = i;
      if ((i + 1) % 5 == 0 || i == arraySize - 2) {
        lruCache.Add(cacheObjectPtrs[i]->Data,
                     shared_ptr<CacheObject>(cacheObjectPtrs[i]), false);
      } else {
        lruCache.Add(cacheObjectPtrs[i]->Data,
                     shared_ptr<CacheObject>(cacheObjectPtrs[i]), true);
      }
    }

    // Now perform eviction
    lruCache.PerformEviction();

    // Now try to get the items that should not have been evicted
    shared_ptr<CacheObject> foundObject;
    for (int i = 0; i < arraySize; i++) {
      if ((i + 1) % 5 == 0 || i == arraySize - 2) {
        ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
        ASSERT_TRUE(cacheObjectPtrs[i]->Data == foundObject->Data);
      } else {
        ASSERT_TRUE(!lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
      }
    }

    // Now reset the evictable flag
    for (int i = 0; i < 20; i++) {
      if ((i + 1) % 5 == 0) {
        ASSERT_TRUE(lruCache.SetEvictable(cacheObjectPtrs[i]->Data, true));
      }
    }

    // Now perform eviction
    lruCache.PerformEviction();

    // Now see if these entries were removed
    for (int i = 0; i < arraySize; i++) {
      if ((i + 1) % 5 == 0) {
        ASSERT_TRUE(!lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
      } else if (i == arraySize - 2) {
        ASSERT_TRUE(lruCache.Find(cacheObjectPtrs[i]->Data, foundObject));
        ASSERT_TRUE(cacheObjectPtrs[i]->Data == foundObject->Data);
      }
    }
  }
}
