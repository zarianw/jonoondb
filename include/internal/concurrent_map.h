#pragma once

#include <map>
#include <memory>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/lock_types.hpp>

namespace jonoondb_api {
template<class T1, class T2> class ConcurrentMap {
public:
  void Add(const T1& key, const std::shared_ptr<T2>& value) {
    boost::unique_lock<boost::shared_mutex> lock(m_mutex);

    m_map[key] = value;
  }

  bool Find(const T1& key, std::shared_ptr<T2>& value) {
    boost::shared_lock<boost::shared_mutex> lock(m_mutex);

    auto c_iter = m_map.find(key);
    if (c_iter != m_map.end()) {
      value = c_iter->second;
      return true;
    } else {
      return false;
    }
  }

private:
  std::map<T1, std::shared_ptr<T2>> m_map;
  boost::shared_mutex m_mutex;
};
}// namespace jonoondb_api
