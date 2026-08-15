#ifndef CHRONOS_COMMON_TIME_SOURCE_HPP_
#define CHRONOS_COMMON_TIME_SOURCE_HPP_

#include <chrono>

namespace chronos::common {

using WallTimePoint = std::chrono::system_clock::time_point;
using MonotonicTimePoint = std::chrono::steady_clock::time_point;

// Injectable source for civil timestamps and elapsed-time/deadline accounting. Implementations
// must be safe for every thread that can call them. Wall time may move in either direction and must
// never be used for elapsed-time or durability ordering; monotonic time has no civil-time meaning.
class TimeSource {
public:
  TimeSource() = default;
  virtual ~TimeSource() = default;

  TimeSource(const TimeSource&) = delete;
  TimeSource& operator=(const TimeSource&) = delete;
  TimeSource(TimeSource&&) = delete;
  TimeSource& operator=(TimeSource&&) = delete;

  [[nodiscard]] virtual WallTimePoint wall_now() const noexcept = 0;
  [[nodiscard]] virtual MonotonicTimePoint monotonic_now() const noexcept = 0;
};

class SystemTimeSource final : public TimeSource {
public:
  [[nodiscard]] WallTimePoint wall_now() const noexcept override;
  [[nodiscard]] MonotonicTimePoint monotonic_now() const noexcept override;
};

// Returns a thread-safe stateless source with process lifetime.
[[nodiscard]] const SystemTimeSource& system_time_source() noexcept;

} // namespace chronos::common

#endif // CHRONOS_COMMON_TIME_SOURCE_HPP_
