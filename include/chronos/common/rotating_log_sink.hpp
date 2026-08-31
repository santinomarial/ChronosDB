#ifndef CHRONOS_COMMON_ROTATING_LOG_SINK_HPP_
#define CHRONOS_COMMON_ROTATING_LOG_SINK_HPP_

#include "chronos/common/log.hpp"
#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace chronos::common {

inline constexpr std::size_t kMaximumRetainedLogFiles = 64U;

struct RotatingJsonLogSinkConfig {
  std::string path;
  std::uint64_t maximum_file_bytes{std::uint64_t{64U} * 1024U * 1024U};
  std::size_t retained_file_count{5U};
};

// Owns one append-only active JSON-lines file and an exclusive advisory lock for its configured
// path. Rotation renames PATH.(N-1) to PATH.N and PATH to PATH.1, where PATH.1 is newest. Calls to
// write are serialized by the sink; after an I/O or rotation failure the sink fails terminally.
class RotatingJsonLogSink final {
public:
  [[nodiscard]] static Result<std::unique_ptr<RotatingJsonLogSink>>
  open(RotatingJsonLogSinkConfig config);

  ~RotatingJsonLogSink();

  RotatingJsonLogSink(const RotatingJsonLogSink&) = delete;
  RotatingJsonLogSink& operator=(const RotatingJsonLogSink&) = delete;
  RotatingJsonLogSink(RotatingJsonLogSink&&) = delete;
  RotatingJsonLogSink& operator=(RotatingJsonLogSink&&) = delete;

  // Encodes and flushes one complete JSON line. Success means stdio accepted and flushed the line;
  // it is not a filesystem durability guarantee.
  [[nodiscard]] Status write(const LogRecord& record);

private:
  class Impl;
  explicit RotatingJsonLogSink(std::unique_ptr<Impl> implementation) noexcept;

  std::unique_ptr<Impl> implementation_;
};

} // namespace chronos::common

#endif // CHRONOS_COMMON_ROTATING_LOG_SINK_HPP_
