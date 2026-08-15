#include "chronos/common/time_source.hpp"

namespace chronos::common {

WallTimePoint SystemTimeSource::wall_now() const noexcept {
  return std::chrono::system_clock::now();
}

MonotonicTimePoint SystemTimeSource::monotonic_now() const noexcept {
  return std::chrono::steady_clock::now();
}

const SystemTimeSource& system_time_source() noexcept {
  static const SystemTimeSource source;
  return source;
}

} // namespace chronos::common
