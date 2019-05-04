#pragma once

#include <chrono>

namespace jonoondb_utils {
class Stopwatch {
 private:
  typedef std::chrono::high_resolution_clock hires_clock;
  hires_clock::time_point m_startCounter;
  hires_clock::time_point m_stopCounter;
  std::chrono::nanoseconds m_prevDuration;
  bool m_isRunning;

 public:
  Stopwatch(bool startNow = false)
      : m_isRunning(false), m_prevDuration(std::chrono::nanoseconds::zero()) {
    if (startNow) {
      m_startCounter = hires_clock::now();
      m_isRunning = true;
    }
  }

  void Start() {
    if (!m_isRunning) {
      m_prevDuration += m_stopCounter - m_startCounter;
      m_startCounter = hires_clock::now();
      m_isRunning = true;
    }
  }

  void Stop() {
    if (m_isRunning) {
      m_stopCounter = hires_clock::now();
      m_isRunning = false;
    }
  }

  void Reset() {
    Stop();
    m_startCounter = hires_clock::time_point::min();
    m_stopCounter = hires_clock::time_point::min();
    m_prevDuration = std::chrono::nanoseconds::zero();
  }

  void Restart() {
    Reset();
    Start();
  }

  int64_t ElapsedSeconds() {
    std::chrono::nanoseconds dur;
    if (m_isRunning) {
      dur = hires_clock::now() - m_startCounter + m_prevDuration;
    } else {
      dur = (m_stopCounter - m_startCounter) + m_prevDuration;
    }
    return std::chrono::duration_cast<std::chrono::seconds>(dur).count();
  }

  int64_t ElapsedMilliSeconds() {
    std::chrono::nanoseconds dur;
    if (m_isRunning) {
      dur = hires_clock::now() - m_startCounter + m_prevDuration;
    } else {
      dur = (m_stopCounter - m_startCounter) + m_prevDuration;
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(dur).count();
  }

  int64_t ElapsedMircoSeconds() {
    std::chrono::nanoseconds dur;
    if (m_isRunning) {
      dur = hires_clock::now() - m_startCounter + m_prevDuration;
    } else {
      dur = (m_stopCounter - m_startCounter) + m_prevDuration;
    }
    return std::chrono::duration_cast<std::chrono::microseconds>(dur).count();
  }
};
}  // namespace jonoondb_utils