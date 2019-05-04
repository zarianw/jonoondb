#pragma once

#include <atomic>
#include <boost/thread/lock_types.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <list>
#include <memory>
#include <unordered_map>

namespace jonoondb_api {
template <class T1, class T2>
class ConcurrentLRUCache {
 private:
  template <class U1>
  class LRUCacheEntry {
   public:
    std::shared_ptr<U1> Value;
    int64_t LastUsed;
    bool Evictable;

    LRUCacheEntry(const std::shared_ptr<U1>& value, int64_t lastUsed,
                  bool evictable)
        : Value(value), LastUsed(lastUsed), Evictable(evictable) {}
  };

  template <typename V1, typename V2>
  struct EvictionEntry {
   public:
    EvictionEntry(int64_t lastUsed, V1 key, std::shared_ptr<V2>& val)
        : LastUsed(lastUsed), Key(std::move(key)), Value(val) {}
    int64_t LastUsed;
    V1 Key;
    std::shared_ptr<V2> Value;
  };

 public:
  ConcurrentLRUCache(size_t maxCacheSize) : m_maxCacheSize(maxCacheSize) {}

  void Add(const T1& key, const std::shared_ptr<T2>& value, bool evictable) {
    boost::unique_lock<boost::shared_mutex> lock(m_mutex);

    m_lastUsed++;

    m_map[key] = std::unique_ptr<LRUCacheEntry<T2>>(
        new LRUCacheEntry<T2>(value, m_lastUsed, evictable));
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
    // Todo: See if we should acquire the writer lock
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
    std::list<EvictionEntry<T1, T2>> keysToEvict;

    {
      boost::shared_lock<boost::shared_mutex> lock(m_mutex);

      if (m_map.size() > m_maxCacheSize) {
        auto itemsToEvictCount = m_map.size() - m_maxCacheSize;
        for (auto iter = m_map.begin(); iter != m_map.end(); iter++) {
          // Add the key in a sorted manner. We are doing insertion sort.
          auto insertionIter = keysToEvict.begin();

          bool itemInserted = false;
          for (auto keysToEvictIter = keysToEvict.begin();
               keysToEvictIter != keysToEvict.end(); keysToEvictIter++) {
            if (iter->second->Evictable &&
                iter->second->LastUsed < keysToEvictIter->LastUsed) {
              keysToEvict.insert(
                  keysToEvictIter,
                  EvictionEntry<T1, T2>(iter->second->LastUsed, iter->first,
                                        iter->second->Value));

              itemInserted = true;
              break;
            }

            insertionIter = keysToEvictIter;
          }

          // Add in the end if required
          if (!itemInserted && iter->second->Evictable &&
              keysToEvict.size() < itemsToEvictCount) {
            keysToEvict.push_back(EvictionEntry<T1, T2>(
                iter->second->LastUsed, iter->first, iter->second->Value));
          }

          // Drop item from end if required. This helps in maintaining
          // top LRU entries
          if (keysToEvict.size() > itemsToEvictCount) {
            keysToEvict.pop_back();
          }
        }
      }
    }

    if (keysToEvict.size() > 0) {
      boost::unique_lock<boost::shared_mutex> lock(m_mutex);

      for (auto& item : keysToEvict) {
        // keyToEvict has a shared_ptr to value, the dtor for value
        // will be called on the destruction of keysToEvict.
        // This is useful because we can destruct them outside of the
        // scope of the unique lock.
        m_map.erase(item.Key);
      }
    }
  }

 private:
  std::unordered_map<T1, std::unique_ptr<LRUCacheEntry<T2>>> m_map;
  boost::shared_mutex m_mutex;
  std::atomic<int64_t> m_lastUsed;
  size_t m_maxCacheSize;
};
}  // namespace jonoondb_api
// jonoondb_api
