#pragma once

#include <unordered_map>
#include <memory>
#include <atomic>
#include <list>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lock_types.hpp>

namespace jonoondb_api {
template<class T1, class T2> class ConcurrentLRUCache {
 private:
  template<class U1> class LRUCacheEntry {
   public:
    std::shared_ptr<U1> Value;
    int64_t LastUsed;bool Evictable;

    LRUCacheEntry(const std::shared_ptr<U1>& value, int64_t lastUsed,
                  bool evictable)
        : Value(value),
          LastUsed(lastUsed),
          Evictable(evictable) {
    }
  };

 public:

  ConcurrentLRUCache(size_t maxCacheSize)
      : m_maxCacheSize(maxCacheSize) {
  }

  void Add(const T1& key, const std::shared_ptr<T2>& value, bool evictable) {
    boost::unique_lock<boost::shared_mutex> lock(m_mutex);

    m_lastUsed++;

    m_map[key] = std::unique_ptr < LRUCacheEntry
        < T2 >> (new LRUCacheEntry<T2>(value, m_lastUsed, evictable));
  }

  bool Find(const T1& key, std::shared_ptr<T2>& value) {
    boost::shared_lock<boost::shared_mutex> lock(m_mutex);

    auto c_iter = m_map.find(key);

    if (c_iter != m_map.end()) {
      m_lastUsed++;
      c_iter->second->LastUsed = m_lastUsed;
      value = c_iter->second->Value;
      return true;
    } else {
      return false;
    }
  }

  bool SetEvictable(const T1& key, bool evictable) {
    //Todo: See if we should acquire the writer lock
    boost::shared_lock<boost::shared_mutex> lock(m_mutex);

    auto c_iter = m_map.find(key);

    if (c_iter != m_map.end()) {
      c_iter->second->Evictable = evictable;
      return true;
    } else {
      return false;
    }
  }

  void PerformEviction() {
    std::list<std::pair<int64_t, T1>> keysToEvict;

    {
      boost::shared_lock<boost::shared_mutex> lock(m_mutex);

      auto itemsToEvictCount = m_map.size() - m_maxCacheSize;

      if (itemsToEvictCount > 0) {
        for (auto iter = m_map.begin(); iter != m_map.end(); iter++) {
          //Add the key in a sorted manner
          auto insertionIter = keysToEvict.begin();

          bool itemInserted = false;
          for (auto keysToEvictIter = keysToEvict.begin();
              keysToEvictIter != keysToEvict.end(); keysToEvictIter++) {
            if (iter->second->Evictable
                && iter->second->LastUsed < keysToEvictIter->first) {
              keysToEvict.insert(
                  keysToEvictIter,
                  std::pair<int64_t, T1>(iter->second->LastUsed, iter->first));
              itemInserted = true;
              break;
            }

            insertionIter = keysToEvictIter;
          }

          if (!itemInserted && iter->second->Evictable
              && keysToEvict.size() < itemsToEvictCount) {
            keysToEvict.push_back(
                std::pair<int64_t, T1>(iter->second->LastUsed, iter->first));
          }

          if (keysToEvict.size() > itemsToEvictCount) {
            keysToEvict.pop_back();
          }
        }
      }
    }

    if (keysToEvict.size() > 0) {
      boost::unique_lock<boost::shared_mutex> lock(m_mutex);

      for (auto& item : keysToEvict) {
        m_map.erase(item.second);
      }
    }
  }

 private:
  std::unordered_map<T1, std::unique_ptr<LRUCacheEntry<T2>>>m_map;
  boost::shared_mutex m_mutex;
  std::atomic<int64_t> m_lastUsed;
  size_t m_maxCacheSize;
};
}
  // jonoondb_api
