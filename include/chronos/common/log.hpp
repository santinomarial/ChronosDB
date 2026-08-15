#ifndef CHRONOS_COMMON_LOG_HPP_
#define CHRONOS_COMMON_LOG_HPP_

#include "chronos/common/result.hpp"
#include "chronos/common/status.hpp"

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <span>
#include <string>
#include <string_view>

namespace chronos::common {

enum class LogSeverity : std::uint8_t {
  kDebug = 0,
  kInfo,
  kWarning,
  kError,
  kCritical,
};

[[nodiscard]] std::string_view log_severity_name(LogSeverity severity) noexcept;

struct LogField {
  std::string_view name;
  std::string_view value;
};

inline constexpr std::size_t kMaximumLogFields = 32U;
inline constexpr std::size_t kMaximumLogComponentBytes = 64U;
inline constexpr std::size_t kMaximumLogEventBytes = 128U;
inline constexpr std::size_t kMaximumLogMessageBytes = std::size_t{16U} * 1024U;
inline constexpr std::size_t kMaximumLogFieldNameBytes = 64U;
inline constexpr std::size_t kMaximumLogFieldValueBytes = std::size_t{4U} * 1024U;
inline constexpr std::size_t kMaximumLogRecordTextBytes = std::size_t{64U} * 1024U;

struct LogRecord {
  std::chrono::sys_time<std::chrono::milliseconds> timestamp{
      std::chrono::floor<std::chrono::milliseconds>(std::chrono::system_clock::now())};
  LogSeverity severity{LogSeverity::kInfo};
  std::string_view component;
  std::string_view event;
  std::string_view message;
  std::span<const LogField> fields;
};

// Returns one newline-free JSON object. Invalid UTF-8 bytes are represented as U+FFFD so the
// result always remains valid UTF-8 JSON. Field names use an ASCII identifier syntax, must be
// unique, and cannot shadow the five built-in keys. Record text and field counts are bounded by the
// constants above.
[[nodiscard]] Result<std::string> encode_json_log(const LogRecord& record);

// Encodes, writes, and flushes exactly one JSON line while serializing concurrent calls in this
// process. The FILE remains caller-owned and must outlive the call.
[[nodiscard]] Status write_json_log(std::FILE* output, const LogRecord& record);

} // namespace chronos::common

#endif // CHRONOS_COMMON_LOG_HPP_
